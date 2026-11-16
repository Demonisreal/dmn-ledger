package load

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestOnlyThePublicSDK(t *testing.T) {
	const module = "github.com/Demonisreal/dmn-ledger/"
	files := 0
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		files++
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			name, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			rest, ours := strings.CutPrefix(name, module)
			if ours && rest != "ledger" && !strings.HasPrefix(rest, "bench/") {
				t.Errorf("%s imports %s; bench may use only the ledger package of the main module", path, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 10 {
		t.Fatalf("only %d Go files under bench", files)
	}
}
