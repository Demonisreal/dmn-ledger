package audit_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
)

func sampleLink(n uint64) audit.Link {
	l := audit.Link{
		Time:   0x0102030405060708 + n,
		Op:     2,
		Events: 0x11121314,
		Leaves: 0x21222324,
	}
	for i := range audit.HashSize {
		l.Body[i] = 0x40 + byte(i) + byte(n)
		l.Reply[i] = 0x80 + byte(i) + byte(n)
		l.Root[i] = 0xc0 + byte(i) + byte(n)
	}
	return l
}

func TestEveryFieldOfTheLinkCounts(t *testing.T) {
	l := sampleLink(0)
	enc := audit.AppendLink([]byte{0xff}, l)[1:]
	if len(enc) != audit.LinkSize {
		t.Fatalf("link is %d bytes, want %d", len(enc), audit.LinkSize)
	}
	if got := audit.DecodeLink(enc); got != l {
		t.Fatalf("decoded %+v, want %+v", got, l)
	}

	prev := audit.Head{Seq: 41, Hash: sampleLink(1).Root}
	head := prev.Next(l)
	if head.Seq != 42 {
		t.Fatalf("next head is number %d, want 42", head.Seq)
	}
	for bit := range 8 * audit.LinkSize {
		flipped := slices.Clone(enc)
		flipped[bit/8] ^= 1 << (bit % 8)
		changed := audit.DecodeLink(flipped)
		if again := audit.AppendLink(nil, changed); !bytes.Equal(again, flipped) {
			t.Fatalf("bit %d: link does not encode back", bit)
		}
		if prev.Next(changed) == head {
			t.Errorf("bit %d of the link does not change the head", bit)
		}
	}
	for bit := range 8 * audit.HashSize {
		other := prev
		other.Hash[bit/8] ^= 1 << (bit % 8)
		if other.Next(l).Hash == head.Hash {
			t.Errorf("bit %d of the previous head does not change the head", bit)
		}
	}
	for _, seq := range []uint64{0, 40, 42, 41 + 1<<32, 41 ^ 1<<63} {
		if (audit.Head{Seq: seq, Hash: prev.Hash}).Next(l).Hash == head.Hash {
			t.Errorf("number %d gives the same head as 41", seq)
		}
	}
}

func TestConnect(t *testing.T) {
	start := audit.Head{}
	links := make([]audit.Link, 6)
	heads := []audit.Head{start}
	for i := range links {
		links[i] = sampleLink(uint64(i))
		heads = append(heads, heads[i].Next(links[i]))
	}
	last := heads[len(links)]

	if got := audit.Connect(start, links); got != last {
		t.Fatalf("Connect gives %d %x, want %d %x", got.Seq, got.Hash, last.Seq, last.Hash)
	}
	if got := audit.Connect(heads[2], links[2:]); got != last {
		t.Errorf("from head 2: %d %x", got.Seq, got.Hash)
	}
	if got := audit.Connect(last, nil); got != last {
		t.Errorf("without links: %d %x", got.Seq, got.Hash)
	}
	for i := range links {
		if got := audit.Connect(start, slices.Delete(slices.Clone(links), i, i+1)); got.Hash == last.Hash {
			t.Errorf("link %d missing, still connected", i+1)
		}
		changed := slices.Clone(links)
		changed[i].Events++
		if got := audit.Connect(start, changed); got.Hash == last.Hash {
			t.Errorf("link %d changed, still connected", i+1)
		}
		if i+1 < len(links) {
			swapped := slices.Clone(links)
			swapped[i], swapped[i+1] = swapped[i+1], swapped[i]
			if got := audit.Connect(start, swapped); got.Hash == last.Hash {
				t.Errorf("links %d and %d swapped, still connected", i+1, i+2)
			}
		}
	}
}
