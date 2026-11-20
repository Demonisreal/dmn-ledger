package server

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

var testKey = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x5e}, ed25519.SeedSize))

func keyed(t *testing.T) (*pipeNet, Config) {
	t.Helper()
	pn, cfg := single(t, nil)
	cfg.Key = testKey
	return pn, cfg
}

func asked(kind byte, id uint64) []byte {
	return u128.From64(id).AppendBytes([]byte{kind})
}

func trusted() []ed25519.PublicKey {
	return []ed25519.PublicKey{testKey.Public().(ed25519.PublicKey)}
}

func booked(c *testClient, transfers int) {
	c.t.Helper()
	c.leading()
	c.register()
	c.ok(client.CreateAccounts, accounts(1, 2))
	for i := range transfers {
		c.ok(client.CreateTransfers, transfer(100+uint64(i), 1, 2, 1))
	}
}

func TestStampIsOverTheHeadOfTheReceipt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := keyed(t)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		booked(c, 6)
		st := c.status()
		if st.Key != [32]byte(trusted()[0]) || st.Head.Seq != 7 {
			t.Fatalf("status shows key %x and head %d, want %x and 7", st.Key, st.Head.Seq, trusted()[0])
		}

		r := c.ok(client.Prove, asked(audit.KindTransfer, 100))
		receipt, h, key, err := notary.Verify(r.Body, testCluster, trusted())
		if err != nil {
			t.Fatalf("receipt of transfer 100: %v", err)
		}
		_, under, rest, err := audit.Decode(r.Body[:len(r.Body)-notary.StampSize])
		switch {
		case err != nil || rest != nil:
			t.Fatalf("the receipt without its stamp: %v, %d bytes left", err, len(rest))
		case h != under || h.Seq != 2 || h == st.Head:
			t.Fatalf("stamp over head %d %x, the receipt gives %d %x, the ledger is at %d", h.Seq, h.Hash[:4], under.Seq, under.Hash[:4], st.Head.Seq)
		case !key.Equal(trusted()[0]) || state.DecodeTransfer(receipt.Record).ID != u128.From64(100):
			t.Fatalf("signed by %x for record %x", key, receipt.Record)
		}

		r = c.ok(client.Head, nil)
		if receipt, h, _, err := notary.Verify(r.Body, testCluster, trusted()); err != nil || receipt.Kind != audit.KindHead || h != st.Head {
			t.Fatalf("head: kind %d, %d %x, %v; want the head of the status, %d %x", receipt.Kind, h.Seq, h.Hash[:4], err, st.Head.Seq, st.Head.Hash[:4])
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWithoutAKeyNothingIsStamped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		booked(c, 2)
		st := c.status()
		if st.Key != ([32]byte{}) || st.Head.Seq != 3 {
			t.Fatalf("status without a key shows key %x and head %d", st.Key, st.Head.Seq)
		}
		for _, ask := range []struct {
			op   client.Op
			body []byte
		}{
			{client.Prove, asked(audit.KindTransfer, 101)},
			{client.Prove, asked(audit.KindAccount, 2)},
			{client.Head, nil},
		} {
			r := c.ok(ask.op, ask.body)
			if _, h, rest, err := audit.Decode(r.Body); err != nil || rest != nil || h.Seq == 0 {
				t.Errorf("%s %x: head %d, %d bytes behind it, %v", ask.op, ask.body, h.Seq, len(rest), err)
			}
			if _, _, _, err := notary.Verify(r.Body, testCluster, trusted()); !errors.Is(err, notary.ErrUnsigned) {
				t.Errorf("%s %x: %v, want ErrUnsigned", ask.op, ask.body, err)
			}
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEmptyReceiptsAndOtherRepliesAreNotStamped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := keyed(t)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		booked(c, 1)
		for _, body := range [][]byte{
			asked(audit.KindTransfer, 999),
			asked(audit.KindAccount, 999),
			asked(audit.KindExpiry, 100),
			append([]byte{audit.KindBatch}, make([]byte, 8)...),
		} {
			if r := c.ok(client.Prove, body); len(r.Body) != 0 {
				t.Errorf("prove %x found nothing and answered %d bytes", body, len(r.Body))
			}
		}
		if r := c.ok(client.LookupAccounts, client.AppendIDs(nil, []u128.Uint128{u128.From64(1), u128.From64(2)})); len(r.Body) != 2*state.AccountSize {
			t.Errorf("lookup of two accounts: %d bytes", len(r.Body))
		}
		if r := c.ok(client.LookupTransfers, u128.From64(100).AppendBytes(nil)); len(r.Body) != state.TransferSize {
			t.Errorf("lookup of one transfer: %d bytes", len(r.Body))
		}
		links := c.ok(client.Links, make([]byte, 16))
		if len(links.Body) != 0 {
			t.Errorf("links 0 to 0: %d bytes", len(links.Body))
		}
		links = c.ok(client.Links, append(make([]byte, 8), 0, 0, 0, 0, 0, 0, 0, 9))
		if len(links.Body) != 2*audit.LinkSize {
			t.Errorf("links 0 to 9 with a head at 2: %d bytes, want %d", len(links.Body), 2*audit.LinkSize)
		}
		if r := c.call(client.CreateTransfers, transfer(100, 1, 2, 1)); r.Status != client.OK || len(r.Body) != client.FailureSize {
			t.Errorf("a transfer that exists: %s with %d bytes", r.Status, len(r.Body))
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStampedRepliesWaitForTheirRound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		two := pn.fake(t, 2, 1, following)
		three := pn.fake(t, 3, 1, following)
		cfg := pn.config(1, 3, dataDir(t, 1, 3))
		cfg.Key = testKey
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		booked(c, 1)
		two.set(listening)
		three.set(listening)
		head, proof := pn.connect(t, clientAddr(1), 2), pn.connect(t, clientAddr(1), 3)
		head.send(client.Head, nil)
		proof.send(client.Prove, asked(audit.KindTransfer, 100))
		for _, rc := range []*testClient{head, proof} {
			_ = rc.conn.SetReadDeadline(time.Now().Add(2 * heartbeatTicks * testTick))
			if frame, _, err := wire.ReadFrame(rc.conn, nil, testCluster, wire.MaxClientBody); err == nil {
				r, _ := frame.Reply()
				t.Fatalf("client %v got %s %s with %d bytes while no peer confirmed the round", rc.id, r.Op, r.Status, len(r.Body))
			}
		}
		two.set(following)
		three.set(following)
		for _, rc := range []*testClient{head, proof} {
			r := rc.recv()
			if _, _, _, err := notary.Verify(r.Body, testCluster, trusted()); r.Status != client.OK || err != nil {
				t.Fatalf("client %v: %s %s after the peers came back: %v", rc.id, r.Op, r.Status, err)
			}
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}
