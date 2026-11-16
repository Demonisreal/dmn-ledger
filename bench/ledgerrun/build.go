package ledgerrun

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func Build(ctx context.Context, root, out string, tags ...string) (string, error) {
	args := []string{"build", "-o", out + string(filepath.Separator)}
	if len(tags) > 0 {
		args = append(args, "-tags", strings.Join(tags, ","))
	}
	cmd := exec.CommandContext(ctx, "go", append(args, "./cmd/ledgerd")...) //nolint:gosec // G204: go build of ledgerd, only the output directory and the tags vary
	cmd.Dir = root
	if msg, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("ledgerrun: build ledgerd: %w\n%s", err, msg)
	}
	return filepath.Join(out, "ledgerd"), nil
}
