package oracle

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func parseDir(t *testing.T, fset *token.FileSet, dir string) []*ast.File {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

func TestSharesOnlyTypesWithState(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	funcs := map[string]bool{}
	for _, f := range parseDir(t, fset, "../state") {
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
				funcs[fn.Name.Name] = true
			}
		}
	}
	if !funcs["New"] {
		t.Fatal("no functions found in package state")
	}
	for _, f := range parseDir(t, fset, ".") {
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "state" && funcs[sel.Sel.Name] {
				t.Errorf("%v: oracle uses state.%s", fset.Position(sel.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
}

func TestCloneIsIndependent(t *testing.T) {
	t.Parallel()
	id := u128.From64
	accts := state.AppendAccount(state.AppendAccount(nil,
		state.Account{ID: id(1), Ledger: 1, Code: 1}),
		state.Account{ID: id(2), Ledger: 1, Code: 1})
	trs := state.AppendTransfer(state.AppendTransfer(nil,
		state.Transfer{ID: id(10), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(5), Ledger: 1, Code: 1}),
		state.Transfer{ID: id(11), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(3), Ledger: 1, Code: 1,
			Flags: state.Pending, Timeout: 1})
	later := []struct {
		ts   uint64
		body []byte
	}{
		{2e9, state.AppendTransfer(nil, state.Transfer{ID: id(12), DebitAccountID: id(2), CreditAccountID: id(1), Amount: id(1),
			Ledger: 1, Code: 1})},
		{5e9, nil},
	}

	l := New()
	if fails, err := l.Apply(state.CreateAccounts, 1e9, accts); err != nil || len(fails) > 0 {
		t.Fatal(fails, err)
	}
	if fails, err := l.Apply(state.CreateTransfers, 1e9, trs); err != nil || len(fails) > 0 {
		t.Fatal(fails, err)
	}
	before := l.Digest()
	c := l.Clone()
	if c.Digest() != before {
		t.Fatal("clone has another digest")
	}
	for _, b := range later {
		if fails, err := c.Apply(state.CreateTransfers, b.ts, b.body); err != nil || len(fails) > 0 {
			t.Fatal(fails, err)
		}
	}
	if l.Digest() != before {
		t.Fatal("applying to the clone changed the original")
	}
	for _, b := range later {
		if _, err := l.Apply(state.CreateTransfers, b.ts, b.body); err != nil {
			t.Fatal(err)
		}
	}
	if l.Digest() != c.Digest() {
		t.Fatal("original and clone diverged on the same batches")
	}
}
