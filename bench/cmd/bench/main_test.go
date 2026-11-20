package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestUsageGoesToStderr(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{nil, 2},
		{[]string{"-h"}, 0},
		{[]string{"help"}, 0},
		{[]string{"report"}, 2},
		{[]string{"plot"}, 2},
	} {
		var stdout, stderr bytes.Buffer
		code := dispatch(tc.args, &stdout, &stderr)
		if code != tc.code || stdout.Len() > 0 || !strings.HasPrefix(stderr.String(), "usage:") {
			t.Errorf("bench %v: exit %d, %d bytes on stdout, stderr starts %.20q, want exit %d and the usage on stderr only", tc.args, code, stdout.Len(), stderr.String(), tc.code)
		}
	}
}
