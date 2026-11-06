package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// modernc sqlite registers vtab modules into an unsynchronised map that every
// new connection iterates, so registering on a request path races with
// concurrent queries. Registration belongs in a once-guarded register* func.
func TestSqliteModuleRegistrationOnlyInRegisterFuncs(t *testing.T) {
	t.Parallel()

	var offenders []string

	err := filepath.WalkDir("services", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		f, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		require.NoError(t, perr)

		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || strings.HasPrefix(fn.Name.Name, "register") {
				continue
			}

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}

				if sel, isSel := call.Fun.(*ast.SelectorExpr); isSel && sel.Sel.Name == "RegisterModule" {
					offenders = append(offenders, path+": "+fn.Name.Name)
				}

				return true
			})
		}

		return nil
	})
	require.NoError(t, err)

	assert.Empty(t, offenders, "sqlite RegisterModule outside a register* func")
}
