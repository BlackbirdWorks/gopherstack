package guards_test

import (
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

func defaultClientTestAllowlist() map[string]string {
	return map[string]string{
		"pkgs/httpclient/httpclient_test.go":                   "proves clients are isolated from http.DefaultTransport",
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

func defaultClientTestUses(src string) []string {
	return defaultTransportTestUse.FindAllString(stripLineComments(src), -1)
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
// Use srv.Client(), pkgs/testclient or pkgs/httpclient.
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

	assert.Empty(t, offenders, "use srv.Client(), testclient.New(t) or httpclient.New")
}
