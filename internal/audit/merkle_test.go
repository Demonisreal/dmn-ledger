package audit_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
)

func mth(leaves []audit.Hash) audit.Hash {
	switch len(leaves) {
	case 0:
		return sha256.Sum256(nil)
	case 1:
		return leaves[0]
	}
	k := 1
	for k*2 < len(leaves) {
		k *= 2
	}
	l, r := mth(leaves[:k]), mth(leaves[k:])
	return sha256.Sum256(slices.Concat([]byte{1}, l[:], r[:]))
}

func treeOf(leaves []audit.Hash) *audit.Tree {
	t := new(audit.Tree)
	for _, l := range leaves {
		t.Add(l)
	}
	return t
}

func numbered(n int) []audit.Hash {
	leaves := make([]audit.Hash, n)
	for i := range leaves {
		leaves[i] = audit.Leaf(audit.KindTransfer, binary.BigEndian.AppendUint32(nil, uint32(i)))
	}
	return leaves
}

func TestRFC6962Roots(t *testing.T) {
	inputs := []string{"", "00", "10", "2021", "3031", "40414243", "5051525354555657", "606162636465666768696a6b6c6d6e6f"}
	roots := []string{
		"6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
		"fac54203e7cc696cf0dfcb42c92a1d9dbaf70ad9e621f4bd8d98662f00e3c125",
		"aeb6bcfe274b70a14fb067a5e5578264db0fa9b51af5e0ba159158f329e06e77",
		"d37ee418976dd95753c1c73862b9398fa2a2cf9b4ff0fdfe8b30cd95209614b7",
		"4e3bbb1f7b478dcfe71fb631631519a3bca12c9aefca1612bfce4c13a86264d4",
		"76e67dadbcdf1e10e1b74ddc608abd2f98dfb16fbce75277b5232a127f2087ef",
		"ddb89be403809e325750d3d263cd78929c2942b7942a34b77e122c9594a74c8c",
		"5dc9da79a70659a9ad559cb701ded9a2ab9d823aad2f4960cfe370eff4604328",
	}
	var tree audit.Tree
	if got := tree.Root(); hex.EncodeToString(got[:]) != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("empty tree has root %x", got)
	}
	for i, in := range inputs {
		d, err := hex.DecodeString(in)
		if err != nil {
			t.Fatal(err)
		}
		tree.Add(sha256.Sum256(append([]byte{0}, d...)))
		if got := tree.Root(); hex.EncodeToString(got[:]) != roots[i] {
			t.Errorf("%d leaves: root %x, want %s", i+1, got, roots[i])
		}
	}
	if tree.Len() != uint32(len(inputs)) {
		t.Errorf("Len %d, want %d", tree.Len(), len(inputs))
	}
}

func TestPathsAllSizes(t *testing.T) {
	for n := range 301 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			leaves := numbered(n)
			root := mth(leaves)
			if got := treeOf(leaves).Root(); got != root {
				t.Fatalf("tree root %x, recursion %x", got, root)
			}
			for i := range leaves {
				path := audit.Path(leaves, i)
				if got, ok := audit.RootFromPath(leaves[i], uint32(i), uint32(n), path); !ok || got != root {
					t.Fatalf("path of leaf %d gives %x %v", i, got, ok)
				}
				for j := range n + 2 {
					if got, ok := audit.RootFromPath(leaves[i], uint32(j), uint32(n), path); j != i && ok && got == root {
						t.Fatalf("path of leaf %d also proves index %d", i, j)
					}
				}
				if _, ok := audit.RootFromPath(leaves[i], uint32(i), uint32(n), append(slices.Clip(path), root)); ok {
					t.Fatalf("path of leaf %d accepted with one hash more", i)
				}
				if len(path) > 0 {
					if _, ok := audit.RootFromPath(leaves[i], uint32(i), uint32(n), path[:len(path)-1]); ok {
						t.Fatalf("path of leaf %d accepted with one hash less", i)
					}
				}
			}
		})
	}
}

func TestTreeDoesNotAllocate(t *testing.T) {
	var tree audit.Tree
	record := make([]byte, 100)
	allocs := testing.AllocsPerRun(100, func() {
		tree.Reset()
		for i := range 37 {
			record[0] = byte(i)
			tree.Add(audit.Leaf(audit.KindTransfer, record))
		}
		_ = audit.Head{}.Next(audit.Link{Leaves: tree.Len(), Root: tree.Root()})
	})
	if allocs != 0 {
		t.Errorf("%v allocs per batch", allocs)
	}
}

func TestNodeIsNeverALeaf(t *testing.T) {
	records := make([][]byte, 4)
	for i := range records {
		records[i] = bytes.Repeat([]byte{byte(i + 1)}, 64)
	}

	plain := func(parts ...[]byte) audit.Hash { return sha256.Sum256(slices.Concat(parts...)) }
	var bare [4]audit.Hash
	for i, rec := range records {
		bare[i] = plain(rec)
	}
	left, right := plain(bare[0][:], bare[1][:]), plain(bare[2][:], bare[3][:])
	forged := slices.Concat(bare[0][:], bare[1][:])
	if inner := plain(forged); plain(inner[:], right[:]) != plain(left[:], right[:]) {
		t.Fatal("without prefixes the inner node does not pass as a leaf, the test shows nothing")
	}

	leaves := make([]audit.Hash, 4)
	for i, rec := range records {
		leaves[i] = audit.Leaf(audit.KindTransfer, rec)
	}
	root := treeOf(leaves).Root()
	forged = slices.Concat(leaves[0][:], leaves[1][:])
	up := audit.Path(leaves, 0)[1:]
	for kind := range byte(4) {
		for size := range uint32(5) {
			for index := range size {
				if got, ok := audit.RootFromPath(audit.Leaf(kind, forged), index, size, up); ok && got == root {
					t.Errorf("inner node passes as leaf %d of %d with kind %d", index, size, kind)
				}
			}
		}
	}
}

func FuzzPath(f *testing.F) {
	f.Add(uint16(1), uint16(0), []byte{}, uint8(0), uint8(0))
	f.Add(uint16(7), uint16(6), []byte{1, 2}, uint8(1), uint8(9))
	f.Add(uint16(300), uint16(255), []byte("salt"), uint8(200), uint8(77))
	f.Fuzz(func(t *testing.T, size, index uint16, salt []byte, change, bit uint8) {
		n := int(size%1024) + 1
		i := int(index) % n
		leaves := make([]audit.Hash, n)
		for k := range leaves {
			leaves[k] = audit.Leaf(audit.KindAccount, binary.BigEndian.AppendUint32(slices.Clip(salt), uint32(k)))
		}
		root := treeOf(leaves).Root()
		path := audit.Path(leaves, i)
		if got, ok := audit.RootFromPath(leaves[i], uint32(i), uint32(n), path); !ok || got != root {
			t.Fatalf("%d leaves: path of leaf %d gives %x %v, root %x", n, i, got, ok, root)
		}

		altered := slices.Clone(path)
		at := int(change / 4)
		switch {
		case change%4 == 0 && len(altered) > 0:
			altered[at%len(altered)][bit%32] ^= 1 << (bit % 8)
		case change%4 == 1:
			altered = slices.Insert(altered, at%(len(altered)+1), leaves[int(bit)%n])
		case change%4 == 2 && len(altered) > 0:
			altered = slices.Delete(altered, at%len(altered), at%len(altered)+1)
		default:
			j := (i + 1 + int(bit)) % (n + 1)
			if j == i {
				return
			}
			if got, ok := audit.RootFromPath(leaves[i], uint32(j), uint32(n), path); ok && got == root {
				t.Fatalf("%d leaves: path of leaf %d also proves index %d", n, i, j)
			}
			return
		}
		if got, ok := audit.RootFromPath(leaves[i], uint32(i), uint32(n), altered); ok && got == root {
			t.Fatalf("%d leaves: altered path of leaf %d still leads to the root", n, i)
		}
	})
}
