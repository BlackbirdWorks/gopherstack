package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	stackSetWaitPattern = regexp.MustCompile(
		`WaitForStackSetOperations|DescribeStackSetOperation|(?i:wait\w*stackset|stackset\w*wait)`,
	)
	// Harnesses that block on WaitForStackSetOperations after every request.
	stackSetAutoWaitHarness = regexp.MustCompile(
		`\b(postForm|postFormValues|postFormBody|newTestHandlerAndClient\w*|newTestClientForBackend)\b`,
	)
)

// allowedStackSetAsyncReads maps "file:Func" to the reason an unwaited read is safe.
func allowedStackSetAsyncReads() map[string]string {
	return map[string]string{
		"services/cloudformation/stack_instances_account_filter_test.go:" +
			"TestStackInstances_AccountFilterType_UpdateUnion": "client from newTestClientForBackend, which waits per request",
	}
}

func stackSetMutatingOps() []string {
	return []string{
		"CreateStackInstances", "UpdateStackInstances", "DeleteStackInstances",
		"ImportStacksToStackSet", "UpdateStackSet", "DeleteStackSet",
	}
}

func stackSetReadOps() []string {
	return []string{"ListStackInstances", "DescribeStackInstance", "ListStackSetOperationResults"}
}

// unwaitedStackSetReads returns functions where a mutating op is followed by a read with no wait between.
func unwaitedStackSetReads(fset *token.FileSet, file *ast.File, src string) []string {
	var out []string

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		body := src[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset]
		if stackSetAutoWaitHarness.MatchString(body) {
			continue
		}

		var lastMut token.Pos

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			var name string

			switch v := n.(type) {
			case *ast.SelectorExpr:
				name = v.Sel.Name
			case *ast.Ident:
				name = v.Name
			default:
				return true
			}

			switch {
			case stackSetWaitPattern.MatchString(name):
				lastMut = token.NoPos
			case slices.Contains(stackSetMutatingOps(), name):
				lastMut = n.Pos()
			case lastMut != token.NoPos && slices.Contains(stackSetReadOps(), name):
				out = append(out, fn.Name.Name+" ("+fset.Position(n.Pos()).String()+")")
				lastMut = token.NoPos
			}

			return true
		})
	}

	return out
}

func TestStackSetAsyncReadsWaitForOperation(t *testing.T) {
	t.Parallel()

	allowed := allowedStackSetAsyncReads()

	var files []string

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}

		if d.IsDir() {
			base := d.Name()
			if base == ".git" || base == "node_modules" || base == "cosmosdb" || strings.HasPrefix(base, "azure") {
				return filepath.SkipDir
			}

			return nil
		}

		if strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err)

	var offenders []string

	for _, path := range files {
		srcBytes, rerr := os.ReadFile(path)
		require.NoError(t, rerr)

		src := string(srcBytes)
		if !strings.Contains(src, "StackSet") && !strings.Contains(src, "StackInstance") {
			continue
		}

		fset := token.NewFileSet()
		file, perr := parser.ParseFile(fset, path, srcBytes, parser.SkipObjectResolution)
		require.NoError(t, perr)

		for _, hit := range unwaitedStackSetReads(fset, file, src) {
			key := filepath.ToSlash(path) + ":" + strings.SplitN(hit, " ", 2)[0]
			if _, ok := allowed[key]; !ok {
				offenders = append(offenders, key+" "+hit)
			}
		}
	}

	require.Empty(t, offenders,
		"stack-set mutation followed by an instance/result read without waiting on the operation; "+
			"poll DescribeStackSetOperation (or WaitForStackSetOperations in-package) first")
}

func TestStackSetAsyncGuardDetects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "unwaited_read", want: 1, body: `c.CreateStackInstances(); c.ListStackInstances()`},
		{
			name: "describe_op_wait", want: 0,
			body: `c.ImportStacksToStackSet(); c.DescribeStackSetOperation(); c.ListStackInstances()`,
		},
		{name: "helper_wait", want: 0, body: `c.DeleteStackInstances(); waitStackSetOp(); c.DescribeStackInstance()`},
		{
			name: "backend_wait", want: 0,
			body: `b.UpdateStackSet(); b.WaitForStackSetOperations(); c.ListStackSetOperationResults()`,
		},
		{name: "read_only", want: 0, body: `c.ListStackInstances()`},
		{name: "auto_wait_harness", want: 0, body: `postForm(); c.UpdateStackInstances(); c.ListStackInstances()`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := "package p\nfunc F() {\n" + tt.body + "\n}\n"
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "x_test.go", src, parser.SkipObjectResolution)
			require.NoError(t, err)

			assert.Len(t, unwaitedStackSetReads(fset, file, src), tt.want)
		})
	}
}
