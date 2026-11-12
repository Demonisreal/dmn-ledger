//go:build simbug

package raft

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

var bug = flag.String("bug", "", "run the tests with this planted bug, B6 to B10 or B15 to B17")

func TestMain(m *testing.M) {
	flag.Parse()
	if err := simbug.Set(*bug); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(m.Run())
}

func TestRuleTestsCatchTheCoreBugs(t *testing.T) {
	if *bug != "" {
		t.Skip("runs the other tests with each bug, not under one")
	}
	caught := []struct {
		bug   string
		tests []string
	}{
		{"B6", []string{"TestCommitOnlyOwnTerm", "TestFigure8", "TestRandomSchedules"}},
		{"B7", []string{"TestVoteGrant", "TestPreVoteGrant", "TestRandomSchedules"}},
		{"B8", []string{"TestFollowerAppend", "TestFigure7", "TestFigure8", "TestStaleAckAfterReelection", "TestRandomSchedules"}},
		{"B9", []string{"TestFollowerAppend", "TestRandomSchedules"}},
		{"B10", []string{"TestReadIndex", "TestReadConfirmedByQuorum", "TestDeposedLeaderGetsNoRead", "TestDeposedLeaderAfterPartitionGetsNoRead", "TestReadSurvivesStepDown", "TestRandomSchedules"}},
		{"B15", []string{"TestRepairingNodeDoesNotVote", "TestRepairedNodeVotesAgain", "TestRepairingNodeDoesNotHelpElect", "TestTwoRepairingNodes", "TestRandomSchedules"}},
		{"B16", []string{"TestRepairProbe", "TestRandomSchedules"}},
		{"B17", []string{"TestRepairEndsOneReadyLater", "TestRandomSchedules"}},
	}
	for _, c := range caught {
		t.Run(c.bug, func(t *testing.T) {
			t.Parallel()
			args := []string{"-bug", c.bug, "-test.run", "^(" + strings.Join(c.tests, "|") + ")$"}
			if d, ok := t.Deadline(); ok {
				args = append(args, "-test.timeout", time.Until(d).String())
			}
			out, err := exec.CommandContext(t.Context(), os.Args[0], args...).CombinedOutput() //nolint:gosec // G204: reruns this test binary
			if err == nil {
				t.Fatalf("all of %v pass with %s", c.tests, c.bug)
			}
			for _, test := range c.tests {
				if !strings.Contains(string(out), "--- FAIL: "+test+" ") {
					t.Errorf("%s passes with %s", test, c.bug)
				}
			}
		})
	}
}
