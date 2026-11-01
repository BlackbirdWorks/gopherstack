package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pickedPortHelpers returns the names of funcs in src that return an int port read from a ":0"
// listener whose Close() runs before the caller binds it (a TOCTOU under parallel tests).
func pickedPortHelpers(t *testing.T, src string) []string {
	t.Helper()

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "x_test.go", src, 0)
	require.NoError(t, err)

	var names []string

	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Type.Results == nil || len(fn.Type.Results.List) != 1 {
			continue
		}

		if id, isIdent := fn.Type.Results.List[0].Type.(*ast.Ident); !isIdent || id.Name != "int" {
			continue
		}

		body := src[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset]
		if strings.Contains(body, `:0"`) && strings.Contains(body, ".Port") && strings.Contains(body, "Close()") {
			names = append(names, fn.Name.Name)
		}
	}

	return names
}

func TestPickedPortHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{
			name: "close_and_return",
			src: `package x
func freePort(t *testing.T) int {
	ln, _ := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}`,
			want: 1,
		},
		{
			name: "renamed",
			src: `package x
func pickPort() int {
	ln, _ := net.Listen("tcp", ":0")
	ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}`,
			want: 1,
		},
		{
			name: "no_return_int",
			src: `package x
func serve(t *testing.T) {
	ln, _ := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	defer ln.Close()
	_ = ln.Addr().(*net.TCPAddr).Port
}`,
			want: 0,
		},
		{name: "unrelated", src: "package x\nfunc n() int { return 1 }", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Len(t, pickedPortHelpers(t, tt.src), tt.want)
		})
	}
}

// pickedPortAllowlist holds helpers that predate the guard in services not yet converted; each entry must
// still exist, so converting a service forces its removal here.
func pickedPortAllowlist() map[string]string {
	const debt = "not yet converted to Port 0 / portalloc.AcquireEphemeral"

	return map[string]string{
		"services/azurearm/handler_test.go: freeEphemeralPort":            debt,
		"services/azurearm/handler_test.go: reserveEphemeralPort":         debt,
		"services/azureblob/coverage_test.go: freeEphemeralPort":          debt,
		"services/azureblob/coverage_test.go: reserveEphemeralPort":       debt,
		"services/azurequeue/coverage_test.go: freeEphemeralPort":         debt,
		"services/azurequeue/coverage_test.go: reserveEphemeralPort":      debt,
		"services/azuretable/coverage_test.go: freeEphemeralPort":         debt,
		"services/azuretable/coverage_test.go: reserveEphemeralPort":      debt,
		"services/cosmosdb/coverage_test.go: reserveEphemeralPort":        debt,
		"services/ec2/docker_front_internal_test.go: echoServer":          "keeps its listener open",
		"services/ec2/docker_front_internal_test.go: freeTCPPort":         debt,
		"services/iot/broker_test.go: freeTCPPort":                        debt,
		"services/lambda/function_url_host_test.go: freeLoopbackPort":     debt,
		"services/lambda/kafka_integration_internal_test.go: freeTCPPort": debt,
		"services/neptune/gremlin_ws_proxy_test.go: wsFreePort":           debt,
	}
}

// TestNoPickedPortHelpersInServiceTests forbids helpers that probe a free port, close the listener
// and hand the number back: another parallel listener can take it before the service binds it.
// Start the service on Port 0 and read the bound port back, or use portalloc.AcquireEphemeral.
func TestNoPickedPortHelpersInServiceTests(t *testing.T) {
	t.Parallel()

	allow := pickedPortAllowlist()
	seen := map[string]bool{}

	var offenders []string

	err := filepath.WalkDir("services", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		for _, name := range pickedPortHelpers(t, string(src)) {
			key := filepath.ToSlash(path) + ": " + name
			if _, ok := allow[key]; ok {
				seen[key] = true

				continue
			}

			offenders = append(offenders, key)
		}

		return nil
	})
	require.NoError(t, err)

	for key := range allow {
		if !seen[key] {
			offenders = append(offenders, key+": stale allowlist entry")
		}
	}

	assert.Empty(t, offenders, "start on Port 0 and read the bound port, or use portalloc.AcquireEphemeral")
}
