package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultTransportTestUse = regexp.MustCompile(
	`\bhttp\.(?:DefaultClient|DefaultTransport|Get\(|Post\(|Head\(|PostForm\()`)

var clientMention = regexp.MustCompile(`http\.Client\b`)

func defaultClientTestAllowlist() map[string]string {
	return map[string]string{
		"pkgs/httpclient/httpclient_test.go":                   "proves clients are isolated from http.DefaultTransport",
		"services/cognitoidp/localstack_alias_test.go":         "TEMPORARY: fixed on the branch that merges first",
		"services/cognitoidp/oauth_helpers_test.go":            "TEMPORARY: fixed on the branch that merges first",
		"services/cognitoidp/saml_federation_test.go":          "TEMPORARY: fixed on the branch that merges first",
		"services/sns/leak_main_test.go":                       "TestMain restricts dialing; httpclient clones inherit it",
		"test/guards/http_transport_guard_test.go":             "fixture strings",
		"test/guards/no_default_client_in_tests_guard_test.go": "fixture strings",
	}
}

func stripLineComments(src string) string {
	lines := strings.Split(src, "\n")
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "//") {
			lines[i] = ""
		}
	}

	return strings.Join(lines, "\n")
}

// bareClientLiterals reports http.Client literals without a Transport field and new(http.Client).
func bareClientLiterals(src string) []string {
	if !clientMention.MatchString(src) {
		return nil
	}

	f, err := parser.ParseFile(token.NewFileSet(), "", src, 0)
	if err != nil {
		return nil
	}

	isClient := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Client" {
			return false
		}

		id, isID := sel.X.(*ast.Ident)

		return isID && id.Name == "http"
	}

	var found []string

	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CompositeLit:
			if !isClient(x.Type) {
				break
			}

			hasTransport := false

			for _, el := range x.Elts {
				if kv, ok := el.(*ast.KeyValueExpr); ok {
					if k, isKey := kv.Key.(*ast.Ident); isKey && k.Name == "Transport" {
						hasTransport = true
					}
				}
			}

			if !hasTransport {
				found = append(found, "http.Client literal without Transport")
			}
		case *ast.CallExpr:
			if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "new" && len(x.Args) == 1 && isClient(x.Args[0]) {
				found = append(found, "new(http.Client)")
			}
		}

		return true
	})

	return found
}

func defaultClientTestUses(src string) []string {
	return append(defaultTransportTestUse.FindAllString(stripLineComments(src), -1), bareClientLiterals(src)...)
}

func TestDefaultClientTestUses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{name: "default_client", src: "resp, err := http.DefaultClient.Do(req)", want: 1},
		{name: "default_transport", src: "tr := http.DefaultTransport", want: 1},
		{name: "get", src: "resp, err := http.Get(u)", want: 1},
		{name: "post_form", src: "http.PostForm(u, v)", want: 1},
		{name: "bare_literal", src: "package p\nvar c = &http.Client{Timeout: 1}", want: 1},
		{name: "empty_literal", src: "package p\nvar c = http.Client{}", want: 1},
		{name: "new_client", src: "package p\nvar c = new(http.Client)", want: 1},
		{name: "literal_with_transport", src: "package p\nvar c = &http.Client{Transport: tr}", want: 0},
		{name: "comment_only", src: "// http.Get(u) is banned", want: 0},
		{name: "own_client", src: "resp, err := testclient.New(t).Get(u)", want: 0},
		{name: "status_const", src: "http.StatusOK, http.MethodGet", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Len(t, defaultClientTestUses(tt.src), tt.want)
		})
	}
}

// TestNoDefaultClientInTests keeps tests off http.DefaultTransport: httptest.Server.Close
// closes its idle connections and breaks in-flight requests of parallel sibling tests.
// Use srv.Client(), testclient.New(t) (or testclient.New(t).Transport inside a custom http.Client)
// or httpclient.New. Also bans http.Client literals without a Transport.
func TestNoDefaultClientInTests(t *testing.T) {
	t.Parallel()

	allow := defaultClientTestAllowlist()
	used := map[string]bool{}

	var offenders []string

	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if n := d.Name(); n == "node_modules" || (n != "." && strings.HasPrefix(n, ".")) {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		uses := defaultClientTestUses(string(src))
		if len(uses) == 0 {
			return nil
		}

		slashed := filepath.ToSlash(path)
		if _, ok := allow[slashed]; ok {
			used[slashed] = true

			return nil
		}

		offenders = append(offenders, slashed+": "+strings.Join(uses, ", "))

		return nil
	})
	require.NoError(t, err)

	for path := range allow {
		if !used[path] {
			offenders = append(offenders, path+": stale allowlist entry")
		}
	}

	assert.Empty(t, offenders,
		"use testclient.New(t), or &http.Client{Transport: testclient.New(t).Transport, ...}; "+
			"srv.Client() for httptest servers")
}
