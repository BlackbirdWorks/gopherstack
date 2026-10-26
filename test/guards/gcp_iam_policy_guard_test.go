package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const gcpIAMPolicyImport = "github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"

// gcpIAMViolations returns the package directories under root whose SetIamPolicy method handles an
// etag but whose files never import pkgs/gcpiampolicy.
func gcpIAMViolations(t *testing.T, dirs []string) []string {
	t.Helper()

	var bad []string

	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)

		handlesEtag, imports := false, false
		fset := token.NewFileSet()

		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}

			file, parseErr := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
			require.NoError(t, parseErr, f)

			for _, imp := range file.Imports {
				imports = imports || strings.Trim(imp.Path.Value, `"`) == gcpIAMPolicyImport
			}

			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if ok && fn.Recv != nil && fn.Body != nil && isSetIAMPolicy(fn.Name.Name) && mentionsEtag(fn.Body) {
					handlesEtag = true
				}
			}
		}

		if handlesEtag && !imports {
			bad = append(bad, dir)
		}
	}

	return bad
}

func isSetIAMPolicy(name string) bool { return name == "SetIamPolicy" || name == "SetIAMPolicy" }

func mentionsEtag(body *ast.BlockStmt) bool {
	found := false

	ast.Inspect(body, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && (id.Name == "Etag" || id.Name == "GetEtag") {
			found = true
		}

		return !found
	})

	return found
}

func TestGCPServicesUseSharedIAMPolicy(t *testing.T) {
	t.Parallel()

	dirs, err := filepath.Glob("services/gcp*")
	require.NoError(t, err)
	require.NotEmpty(t, dirs)

	assert.Empty(t, gcpIAMViolations(t, dirs),
		"a SetIamPolicy that checks etags must use pkgs/gcpiampolicy for etag, version and condition semantics")
}

func TestGCPIAMGuardDetects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dir  string
		want bool
	}{
		{"test/guards/testdata/gcpiampolicy/bad", true},
		{"test/guards/testdata/gcpiampolicy/good", false},
	}

	for _, tt := range tests {
		t.Run(tt.dir, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, len(gcpIAMViolations(t, []string{tt.dir})) == 1)
		})
	}
}
