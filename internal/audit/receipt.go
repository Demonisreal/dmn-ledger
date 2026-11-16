package audit

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	version   = 1
	headSize  = 2 + 8 + HashSize
	stampSize = 8 + 32 + 64
)

var ErrBadReceipt = errors.New("audit: bad receipt")

var (
	errVersion = fmt.Errorf("%w: unknown version", ErrBadReceipt)
	errKind    = fmt.Errorf("%w: unknown kind", ErrBadReceipt)
	errLength  = fmt.Errorf("%w: wrong length", ErrBadReceipt)
	errBatch   = fmt.Errorf("%w: batch receipt with a leaf index or a path", ErrBadReceipt)
	errSeq     = fmt.Errorf("%w: links count from 1", ErrBadReceipt)
	errStart   = fmt.Errorf("%w: head 0 with a hash", ErrBadReceipt)
	errPath    = fmt.Errorf("%w: path does not lead to the root of the link", ErrBadReceipt)
)

type Receipt struct {
	Kind   byte
	Record []byte
	Index  uint32
	Path   []Hash
	Seq    uint64
	Prev   Hash
	Link   Link
}

func recordSize(kind byte) (int, bool) {
	switch kind {
	case KindBatch:
		return 0, true
	case KindAccount:
		return accountSize, true
	case KindTransfer, KindExpiry:
		return transferSize, true
	}
	return 0, false
}

func AppendReceipt(dst []byte, r Receipt) []byte {
	dst = append(dst, version, r.Kind)
	dst = append(dst, r.Record...)
	dst = binary.BigEndian.AppendUint32(dst, r.Index)
	dst = append(dst, byte(len(r.Path))) //nolint:gosec // G115: a path has at most MaxHops hashes
	for _, p := range r.Path {
		dst = append(dst, p[:]...)
	}
	dst = binary.BigEndian.AppendUint64(dst, r.Seq)
	dst = append(dst, r.Prev[:]...)
	return AppendLink(dst, r.Link)
}

func AppendHead(dst []byte, h Head) []byte {
	dst = append(dst, version, KindHead)
	dst = binary.BigEndian.AppendUint64(dst, h.Seq)
	return append(dst, h.Hash[:]...)
}

func Decode(proof []byte) (Receipt, Head, []byte, error) {
	switch {
	case len(proof) < 2:
		return Receipt{}, Head{}, nil, errLength
	case proof[0] != version:
		return Receipt{}, Head{}, nil, errVersion
	case proof[1] == KindHead:
		return decodeHead(proof)
	}
	kind := proof[1]
	size, ok := recordSize(kind)
	if !ok {
		return Receipt{}, Head{}, nil, errKind
	}
	fixed := 2 + size + 4 + 1
	if len(proof) < fixed {
		return Receipt{}, Head{}, nil, errLength
	}
	hops := int(proof[fixed-1])
	end := fixed + hops*HashSize + 8 + HashSize + LinkSize
	if hops > MaxHops || len(proof) < end {
		return Receipt{}, Head{}, nil, errLength
	}
	stamp, err := trailer(proof[end:])
	if err != nil {
		return Receipt{}, Head{}, nil, err
	}
	r := Receipt{Kind: kind, Index: binary.BigEndian.Uint32(proof[2+size:])}
	if kind == KindBatch && (r.Index != 0 || hops != 0) {
		return Receipt{}, Head{}, nil, errBatch
	}
	chain := proof[end-LinkSize-HashSize-8 : end]
	if r.Seq = binary.BigEndian.Uint64(chain); r.Seq == 0 {
		return Receipt{}, Head{}, nil, errSeq
	}
	r.Prev = Hash(chain[8:])
	r.Link = DecodeLink(chain[8+HashSize:])
	if kind != KindBatch {
		r.Record = proof[2 : 2+size : 2+size]
		r.Path = make([]Hash, hops)
		for i := range r.Path {
			r.Path[i] = Hash(proof[fixed+i*HashSize:])
		}
		root, ok := RootFromPath(Leaf(kind, r.Record), r.Index, r.Link.Leaves, r.Path)
		if !ok || root != r.Link.Root {
			return Receipt{}, Head{}, nil, errPath
		}
	}
	return r, Head{Seq: r.Seq - 1, Hash: r.Prev}.Next(r.Link), stamp, nil
}

func decodeHead(proof []byte) (Receipt, Head, []byte, error) {
	if len(proof) < headSize {
		return Receipt{}, Head{}, nil, errLength
	}
	stamp, err := trailer(proof[headSize:])
	if err != nil {
		return Receipt{}, Head{}, nil, err
	}
	h := Head{Seq: binary.BigEndian.Uint64(proof[2:]), Hash: Hash(proof[10:])}
	if h.Seq == 0 && h.Hash != (Hash{}) {
		return Receipt{}, Head{}, nil, errStart
	}
	return Receipt{Kind: KindHead, Seq: h.Seq}, h, stamp, nil
}

func trailer(rest []byte) ([]byte, error) {
	switch len(rest) {
	case 0:
		return nil, nil
	case stampSize:
		return rest, nil
	}
	return nil, errLength
}
