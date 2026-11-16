package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	logQueue   = 64
	refusalGap = time.Second
)

type note struct {
	reason string
	line   string
}

func (s *Server) logf(format string, args ...any) {
	s.note(note{line: fmt.Sprintf(format, args...)})
}

func (s *Server) refused(reason string, conn net.Conn, err error) {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return
	}
	s.note(note{reason: reason, line: fmt.Sprintf("refused %s from %s: %v", reason, conn.RemoteAddr(), err)})
}

func (s *Server) note(n note) {
	if s.cfg.Log == nil {
		return
	}
	select {
	case s.notes <- n:
	default:
	}
}

func (s *Server) writeLog(quiet <-chan struct{}) {
	last := make(map[string]time.Time)
	show := func(n note) {
		if n.reason != "" {
			if at, ok := last[n.reason]; ok && time.Since(at) < refusalGap {
				return
			}
			last[n.reason] = time.Now()
		}
		s.cfg.Log.Print(n.line)
	}
	for {
		select {
		case n := <-s.notes:
			show(n)
		case <-quiet:
			for len(s.notes) > 0 {
				show(<-s.notes)
			}
			return
		}
	}
}

func (s *Server) watch() {
	if s.cfg.Log == nil {
		return
	}
	st := s.node.Status()
	if st.Role != s.role {
		s.role = st.Role
		s.logf("%s in term %d, leader %d", st.Role, st.Term, st.Lead)
	}
	if st.Repair != s.repair {
		s.repair = st.Repair
		if st.Repair {
			s.logf("repair flag set in term %d", st.Term)
		} else {
			s.logf("repair flag cleared in term %d", st.Term)
		}
	}
}
