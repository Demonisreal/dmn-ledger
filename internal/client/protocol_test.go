package client

import (
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func TestBodiesRoundTrip(t *testing.T) {
	ids := []u128.Uint128{{Hi: 1, Lo: 2}, {Lo: 9}}
	b := AppendIDs(nil, ids)
	if len(b) != 2*IDSize || !slices.Equal(DecodeIDs(b), ids) {
		t.Fatalf("ids %x decode to %v", b, DecodeIDs(b))
	}
	fails := []state.Failure{{Index: 0, Reason: state.Exists}, {Index: 7, Reason: state.ExistsWithDifferentFields}}
	b = AppendFailures(nil, fails)
	if len(b) != 2*FailureSize || !slices.Equal(DecodeFailures(b), fails) {
		t.Fatalf("failures %x decode to %v", b, DecodeFailures(b))
	}
	if len(DecodeFailures(nil)) != 0 || len(DecodeIDs(nil)) != 0 {
		t.Fatal("empty bodies decode to something")
	}
}
