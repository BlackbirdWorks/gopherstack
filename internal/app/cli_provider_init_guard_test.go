package app

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var siblingAccessor = regexp.MustCompile(`^Get[A-Za-z0-9]+Handler$`)

// siblingCaptureExclusions are packages allowed to resolve sibling handlers
// during Init.
func siblingCaptureExclusions() map[string]string {
	return map[string]string{
		"cloudformation": "initialised explicitly by cli.go after every handler is stored",
	}
}

// siblingCaptures reports Get*Handler() calls evaluated eagerly: inside Init or
// a package-level helper / Provider method, outside any closure. Methods on
// other receivers (lazy lookups) and function literals run later and are allowed.
func siblingCaptures(fset *token.FileSet, file *ast.File) []string {
	var out []string

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !eagerFunc(fn) {
			continue
		}

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				return false
			case *ast.CallExpr:
				sel, isSel := x.Fun.(*ast.SelectorExpr)
				if isSel && len(x.Args) == 0 && siblingAccessor.MatchString(sel.Sel.Name) {
					out = append(out, fmt.Sprintf("%s: %s() called eagerly in %s",
						fset.Position(x.Pos()), sel.Sel.Name, fn.Name.Name))
				}
			}

			return true
		})
	}

	return out
}

func eagerFunc(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return true
	}

	t := fn.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}

	id, ok := t.(*ast.Ident)

	return ok && id.Name == "Provider"
}

func capturesInSource(t *testing.T, src string) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "x.go", src, 0)
	require.NoError(t, err)

	return siblingCaptures(fset, file)
}

func TestSiblingCaptureDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{"sagemakerruntime pre-fix", `package p
func wireEndpointLookup(ctx *service.AppContext, backend *InMemoryBackend) {
	hp, ok := ctx.Config.(sagemakerHandlerProvider)
	if !ok { return }
	sh, ok := hp.GetSageMakerHandler().(*sagemaker.Handler)
	backend.SetEndpointLookup(sh.Backend)
}`, 1},
		{"cloudwatchlogs pre-fix", `package p
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	hp := ctx.Config.(s3HandlerProvider)
	sh := hp.GetS3Handler().(*s3.S3Handler)
	backend.SetExportSink(newS3ExportSink(sh.Backend))
	return nil, nil
}`, 1},
		{"lazy method", `package p
func (l lazyLookup) backend() any { return l.provider.GetSageMakerHandler() }`, 0},
		{"closure", `package p
func (p *Provider) Init(ctx *service.AppContext) {
	b.SetLookup(func() any { return hp.GetS3Handler() })
}`, 0},
		{"accessor stored", `package p
func (p *Provider) Init(ctx *service.AppContext) {
	b.SetProvider(hp)
}`, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Len(t, capturesInSource(t, tt.src), tt.want)
		})
	}
}

func TestProviderInitDoesNotCaptureSiblingHandlers(t *testing.T) {
	t.Parallel()

	dirs, err := filepath.Glob("services/*/provider.go")
	require.NoError(t, err)
	require.Greater(t, len(dirs), 100)

	sort.Strings(dirs)

	for _, providerFile := range dirs {
		dir := filepath.Dir(providerFile)
		name := filepath.Base(dir)

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if reason, skip := siblingCaptureExclusions()[name]; skip {
				t.Skip(reason)
			}

			files, globErr := filepath.Glob(filepath.Join(dir, "*.go"))
			require.NoError(t, globErr)

			fset := token.NewFileSet()

			for _, f := range files {
				if strings.HasSuffix(f, "_test.go") {
					continue
				}

				src, readErr := os.ReadFile(f)
				require.NoError(t, readErr)

				file, parseErr := parser.ParseFile(fset, f, src, 0)
				require.NoError(t, parseErr)

				for _, hit := range siblingCaptures(fset, file) {
					assert.Fail(t, "sibling handler resolved during Init; resolve lazily per call", hit)
				}
			}
		})
	}
}
