package state_test

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func TestGoldenLayout(t *testing.T) {
	t.Parallel()
	u := func(hi, lo uint64) u128.Uint128 { return u128.Uint128{Hi: hi, Lo: lo} }
	a := state.Account{
		ID:             u(0x0102030405060708, 0x090a0b0c0d0e0f10),
		DebitsPending:  u(0xa0, 0xa1),
		DebitsPosted:   u(0xb0, 0xb1),
		CreditsPending: u(0xc0, 0xc1),
		CreditsPosted:  u(0xd0, 0xd1),
		Ledger:         0x11121314,
		Code:           0x1516,
		Flags:          0x0102,
		Timestamp:      0x1718191a1b1c1d1e,
	}
	wantAccount := strings.Join([]string{
		"0102030405060708090a0b0c0d0e0f10",
		"00000000000000a000000000000000a1",
		"00000000000000b000000000000000b1",
		"00000000000000c000000000000000c1",
		"00000000000000d000000000000000d1",
		"11121314", "1516", "0102", "1718191a1b1c1d1e",
	}, "")
	if got := hex.EncodeToString(state.AppendAccount(nil, a)); got != wantAccount {
		t.Errorf("account\n got %s\nwant %s", got, wantAccount)
	}

	tr := state.Transfer{
		ID:              u(0x0102030405060708, 0x090a0b0c0d0e0f10),
		DebitAccountID:  u(0xa0, 0xa1),
		CreditAccountID: u(0xb0, 0xb1),
		Amount:          u(0xc0, 0xc1),
		PendingID:       u(0xd0, 0xd1),
		Timeout:         0x21222324,
		Ledger:          0x11121314,
		Code:            0x1516,
		Flags:           0x0102,
		Timestamp:       0x1718191a1b1c1d1e,
	}
	wantTransfer := strings.Join([]string{
		"0102030405060708090a0b0c0d0e0f10",
		"00000000000000a000000000000000a1",
		"00000000000000b000000000000000b1",
		"00000000000000c000000000000000c1",
		"00000000000000d000000000000000d1",
		"21222324", "11121314", "1516", "0102", "1718191a1b1c1d1e",
	}, "")
	if got := hex.EncodeToString(state.AppendTransfer(nil, tr)); got != wantTransfer {
		t.Errorf("transfer\n got %s\nwant %s", got, wantTransfer)
	}
	if len(wantAccount)/2 != state.AccountSize || len(wantTransfer)/2 != state.TransferSize {
		t.Errorf("record sizes %d and %d", state.AccountSize, state.TransferSize)
	}
	if state.DecodeAccount(state.AppendAccount(nil, a)) != a || state.DecodeTransfer(state.AppendTransfer(nil, tr)) != tr {
		t.Error("decoding does not invert encoding")
	}
}

func TestReasonNames(t *testing.T) {
	t.Parallel()
	seen := map[string]state.Reason{}
	for r := state.OK; r <= state.LastReason; r++ {
		name := r.String()
		if name == "" || strings.HasPrefix(name, "reason(") {
			t.Errorf("reason %d has no name", r)
		}
		if prev, ok := seen[name]; ok {
			t.Errorf("reasons %d and %d share the name %s", prev, r, name)
		}
		seen[name] = r
	}
	if got, want := (state.LastReason + 1).String(), fmt.Sprintf("reason(%d)", state.LastReason+1); got != want {
		t.Errorf("unknown reason prints as %q", got)
	}
}
