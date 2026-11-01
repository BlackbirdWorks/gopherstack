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

const readinessHelper = "waitForServerReady"

func readinessPaths() []string {
	return []string{"/_gopherstack/health", "/_localstack/health", "/_localstack/init/ready"}
}

func mentionsReadinessPath(n ast.Node) bool {
	found := false

	ast.Inspect(n, func(x ast.Node) bool {
		if lit, ok := x.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			for _, p := range readinessPaths() {
				if strings.Contains(lit.Value, p) {
					found = true
				}
			}
		}

		return !found
	})

	return found
}

func isFixedDeadlineExpr(x ast.Node) bool {
	sel, ok := x.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkg, isIdent := sel.X.(*ast.Ident)
	if !isIdent {
		return false
	}

	switch pkg.Name + "." + sel.Sel.Name {
	case "time.After", "time.Since", "time.Until", "context.WithTimeout":
		return true
	}

	return false
}

func isNowAdd(x ast.Node) bool {
	call, ok := x.(*ast.CallExpr)
	if !ok {
		return false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Add" {
		return false
	}

	inner, ok := sel.X.(*ast.CallExpr)
	if !ok {
		return false
	}

	s2, ok := inner.Fun.(*ast.SelectorExpr)

	return ok && s2.Sel.Name == "Now"
}

func hasFixedDeadline(fn ast.Node) bool {
	found := false

	ast.Inspect(fn, func(x ast.Node) bool {
		if isFixedDeadlineExpr(x) || isNowAdd(x) {
			found = true
		}

		return !found
	})

	return found
}

// fixedBudgetReadinessWaits returns "func: reason" for readiness polls bounded by a fixed wall-clock
// budget: an Eventually* whose condition requests a health/ready path, or a hand-rolled loop polling
// one under time.After/Now().Add/WithTimeout. Conditions that hide the request in a helper are not seen.
func fixedBudgetReadinessWaits(t *testing.T, src string) []string {
	t.Helper()

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "x_test.go", src, 0)
	require.NoError(t, err)

	var out []string

	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name == readinessHelper {
			continue
		}

		eventually, loop := false, false

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				sel, isSel := x.Fun.(*ast.SelectorExpr)
				if !isSel || !strings.HasPrefix(sel.Sel.Name, "Eventually") {
					return true
				}

				for _, a := range x.Args {
					if lit, isLit := a.(*ast.FuncLit); isLit && mentionsReadinessPath(lit) {
						eventually = true
					}
				}
			case *ast.ForStmt:
				if mentionsReadinessPath(x) {
					loop = true
				}
			}

			return true
		})

		if eventually {
			out = append(out, fn.Name.Name+": Eventually polls a readiness path")
		}

		if loop && hasFixedDeadline(fn.Body) {
			out = append(out, fn.Name.Name+": hand-rolled readiness loop with a fixed deadline")
		}
	}

	return out
}

func TestFixedBudgetReadinessWaits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{
			name: "eventually_health",
			src: `package x
func TestA(t *testing.T) {
	require.Eventually(t, func() bool {
		resp, err := c.Get("http://localhost/_gopherstack/health")
		return err == nil && resp.StatusCode == 200
	}, 3*time.Second, 50*time.Millisecond, "not ready")
}`,
			want: 1,
		},
		{
			name: "eventually_with_t_sprintf_init_ready",
			src: `package x
func TestA(t *testing.T) {
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		_, _ = c.Get(fmt.Sprintf("http://h:%d/_localstack/init/ready", 1))
	}, time.Second, time.Millisecond)
}`,
			want: 1,
		},
		{
			name: "eventually_other_condition",
			src: `package x
func TestA(t *testing.T) {
	require.Eventually(t, func() bool { _, err := os.ReadFile("m"); return err == nil }, time.Second, time.Millisecond)
}`,
			want: 0,
		},
		{
			name: "helper_use",
			src: `package x
func TestA(t *testing.T) {
	waitForServerReady(t, 1)
}`,
			want: 0,
		},
		{
			name: "helper_itself",
			src: `package x
func waitForServerReady(t *testing.T, port int) {
	for {
		c.Get("http://h/_gopherstack/health")
		select { case <-t.Context().Done(): return; case <-time.After(time.Second): }
	}
}`,
			want: 0,
		},
		{
			name: "hand_rolled_fixed_deadline",
			src: `package x
func TestA(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.Get("http://h/_gopherstack/health")
	}
}`,
			want: 1,
		},
		{
			name: "single_request_no_loop",
			src: `package x
func TestA(t *testing.T) {
	c.Get("http://h/_gopherstack/health")
	<-time.After(time.Second)
}`,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Len(t, fixedBudgetReadinessWaits(t, tt.src), tt.want)
		})
	}
}

// TestNoFixedBudgetServerReadinessWaits forbids polling health/ready endpoints under a fixed
// wall-clock budget: full startup under -race outgrows any constant as services are added.
// Use internal/app waitForServerReady, which is bounded by the test context instead.
func TestNoFixedBudgetServerReadinessWaits(t *testing.T) {
	t.Parallel()

	var offenders []string

	for _, root := range []string{"internal", "services", "pkgs", "test", "cmd", "modules"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
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

			for _, w := range fixedBudgetReadinessWaits(t, string(src)) {
				offenders = append(offenders, filepath.ToSlash(path)+": "+w)
			}

			return nil
		})
		if os.IsNotExist(err) {
			continue
		}

		require.NoError(t, err)
	}

	assert.Empty(t, offenders, "use waitForServerReady (internal/app/cli_test.go), bounded by the test context")
}
