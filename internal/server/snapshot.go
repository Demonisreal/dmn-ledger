package server

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"path/filepath"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)

	errSnapshotStream = errors.New("server: broken snapshot stream")
)

type spool struct {
	peer *peer
	path string
	file disk.File
	size int64
}

type link struct {
	conn    net.Conn
	cluster uint64
	size    uint64
	err     error
}

func (l *link) write(frame []byte) error {
	_ = l.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, l.err = l.conn.Write(frame)
	return l.err
}

func (l *link) stream(r io.Reader) error {
	chunk := make([]byte, wire.ChunkSize)
	frame := make([]byte, 0, wire.HeaderSize+wire.ChunkSize)
	var crc uint32
	for {
		n, err := io.ReadFull(r, chunk)
		if n > 0 {
			crc = crc32.Update(crc, castagnoli, chunk[:n])
			l.size += uint64(n) //nolint:gosec // G115: n is at most ChunkSize
			if err := l.write(wire.AppendChunk(frame[:0], l.cluster, chunk[:n])); err != nil {
				return err
			}
		}
		switch {
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			return l.write(wire.AppendSnapshotEnd(frame[:0], l.cluster, l.size, crc))
		case err != nil:
			return err
		}
	}
}

func (s *Server) sendSnapshot(ctx context.Context, p *peer, m raft.Message, head []byte) {
	conn, done, err := s.dial(ctx, p, wire.KindSnapshot)
	if err != nil {
		return
	}
	defer done()
	s.logf("sending snapshot %d/%d to node %d", m.Index, m.LogTerm, p.id)
	l := &link{conn: conn, cluster: s.cfg.Cluster}
	if l.write(head) == nil {
		err = wal.ReadSnapshot(s.cfg.FS, filepath.Join(s.cfg.Dir, snapDir), wal.SnapshotMeta{Index: m.Index, Term: m.LogTerm}, l.stream)
	}
	switch {
	case l.err != nil:
		s.logf("snapshot %d/%d to node %d broke off: %v", m.Index, m.LogTerm, p.id, l.err)
	case errors.Is(err, wal.ErrNoSnapshot):
		s.logf("snapshot %d/%d for node %d is gone", m.Index, m.LogTerm, p.id)
	case err != nil:
		s.logf("snapshot %d/%d for node %d cannot be read: %v", m.Index, m.LogTerm, p.id, err)
		s.stop(err)
	default:
		s.logf("sent snapshot %d/%d to node %d, %d bytes", m.Index, m.LogTerm, p.id, l.size)
	}
}

func (s *Server) receiveSnapshot(ctx context.Context, conn net.Conn, p *peer, h wire.Hello) {
	err := s.welcome(conn, h)
	var m raft.Message
	var sp *spool
	if err == nil {
		m, sp, err = s.spool(conn, p)
	}
	switch {
	case err != nil && m.Type == raft.MsgSnap:
		s.logf("dropped snapshot %d/%d from node %d: %v", m.Index, m.LogTerm, p.id, err)
	case err != nil:
		s.refused("snapshot", conn, err)
	default:
		s.logf("received snapshot %d/%d from node %d, %d bytes", m.Index, m.LogTerm, p.id, sp.size)
	}
	if err != nil {
		p.receiving.Store(false)
		return
	}
	if !s.post(ctx, event{kind: gotSnapshot, msg: m, spool: sp}) {
		s.discard(sp)
	}
}

func (s *Server) spool(conn net.Conn, p *peer) (raft.Message, *spool, error) {
	_ = conn.SetReadDeadline(time.Now().Add(bodyTimeout))
	frame, buf, err := wire.ReadFrame(conn, nil, s.cfg.Cluster, wire.ChunkSize)
	if err != nil {
		return raft.Message{}, nil, err
	}
	m, err := frame.Message()
	switch {
	case err != nil:
		return raft.Message{}, nil, err
	case m.Type != raft.MsgSnap || m.From != p.id || m.To != s.cfg.ID:
		return raft.Message{}, nil, errSnapshotStream
	}
	s.logf("receiving snapshot %d/%d from node %d", m.Index, m.LogTerm, p.id)
	sp := &spool{peer: p, path: filepath.Join(s.cfg.Dir, recvDir, fmt.Sprintf("%d.tmp", p.id))}
	if sp.file, err = s.cfg.FS.Create(sp.path); err != nil {
		return m, nil, err
	}
	if err := s.fill(conn, sp, buf); err != nil {
		_ = sp.file.Close()
		_ = s.cfg.FS.Remove(sp.path)
		return m, nil, err
	}
	return m, sp, nil
}

func (s *Server) fill(conn net.Conn, sp *spool, buf []byte) error {
	var crc uint32
	for {
		_ = conn.SetReadDeadline(time.Now().Add(bodyTimeout))
		var frame wire.Frame
		var err error
		frame, buf, err = wire.ReadFrame(conn, buf, s.cfg.Cluster, wire.ChunkSize)
		if err != nil {
			return err
		}
		if frame.Type == wire.TypeSnapshotEnd {
			size, sum, err := frame.SnapshotEnd()
			if err != nil || size != uint64(sp.size) || sum != crc { //nolint:gosec // G115: the size is never negative
				return errSnapshotStream
			}
			return nil
		}
		chunk, err := frame.Chunk()
		if err != nil {
			return err
		}
		if sp.size+int64(len(chunk)) > s.spoolMax {
			return errSnapshotStream
		}
		if _, err := sp.file.WriteAt(chunk, sp.size); err != nil {
			return err
		}
		crc = crc32.Update(crc, castagnoli, chunk)
		sp.size += int64(len(chunk))
	}
}

func (s *Server) discard(sp *spool) {
	_ = sp.file.Close()
	_ = s.cfg.FS.Remove(sp.path)
	sp.peer.receiving.Store(false)
}
