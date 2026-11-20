package report

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntroCountsTheTriesOfPostgres(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "pg", "retry.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	tries := ""
	ast.Inspect(f, func(n ast.Node) bool {
		if spec, ok := n.(*ast.ValueSpec); ok && spec.Names[0].Name == "maxTries" {
			if lit, ok := spec.Values[0].(*ast.BasicLit); ok {
				tries = lit.Value
			}
		}
		return tries == ""
	})
	if want := "gave up after " + tries + " tries;"; tries == "" || !strings.Contains(intro, want) {
		t.Errorf("maxTries is %q in pg/retry.go, the intro of RESULTS.md lacks %q", tries, want)
	}
}
