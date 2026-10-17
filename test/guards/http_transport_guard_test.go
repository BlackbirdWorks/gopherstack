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

var (
	sharedTransportCall = regexp.MustCompile(`\bhttp\.(?:DefaultClient|Get\(|Post\(|Head\(|PostForm\()`)
	clientLiteral       = regexp.MustCompile(`http\.Client\{`)
)

// outboundHTTPAllowlist lists files exempt from the shared-transport ban, with the reason.
func outboundHTTPAllowlist() map[string]string {
	return map[string]string{}
}

// sharedTransportUses reports uses of http.DefaultClient, http.Get-style helpers and
// http.Client literals without a Transport field.
func sharedTransportUses(src string) []string {
	var found []string

	found = append(found, sharedTransportCall.FindAllString(src, -1)...)

	for _, loc := range clientLiteral.FindAllStringIndex(src, -1) {
		depth, end := 1, loc[1]
		for ; end < len(src) && depth > 0; end++ {
			switch src[end] {
			case '{':
				depth++
			case '}':
				depth--
			}
		}

		if !strings.Contains(src[loc[1]:end], "Transport:") {
			found = append(found, "http.Client literal without Transport")
		}
	}

	return found
}

func TestSharedTransportUses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{name: "bare_client", src: `c := &http.Client{Timeout: time.Second}`, want: 1},
		{name: "empty_client", src: `c := &http.Client{}`, want: 1},
		{name: "own_transport", src: `c := &http.Client{Timeout: 1, Transport: tr}`, want: 0},
		{name: "nested_func_no_transport", src: `&http.Client{CheckRedirect: func() error { return nil }}`, want: 1},
		{name: "default_client", src: `http.DefaultClient.Do(req)`, want: 1},
		{name: "http_get", src: `http.Get(u)`, want: 1},
		{name: "request_builder", src: `http.NewRequest(http.MethodGet, u, nil)`, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Len(t, sharedTransportUses(tt.src), tt.want)
		})
	}
}

// TestNoOutboundHTTPOnDefaultTransport keeps services from sharing http.DefaultTransport,
// whose CloseIdleConnections (called by httptest.Server.Close) breaks unrelated in-flight requests.
func TestNoOutboundHTTPOnDefaultTransport(t *testing.T) {
	t.Parallel()

	allow := outboundHTTPAllowlist()
	used := map[string]bool{}

	var offenders []string

	for _, root := range []string{"services", "pkgs"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if skipGuardDir(d.Name()) {
					return filepath.SkipDir
				}

				return nil
			}

			if !isGuardedSource(path) {
				return nil
			}

			src, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}

			slashed := filepath.ToSlash(path)

			if uses := sharedTransportUses(string(src)); len(uses) > 0 {
				if _, ok := allow[slashed]; ok {
					used[slashed] = true

					return nil
				}

				offenders = append(offenders, slashed+": "+strings.Join(uses, ", "))
			}

			return nil
		})
		require.NoError(t, err)
	}

	for path := range allow {
		if !used[path] {
			offenders = append(offenders, path+": stale outboundHTTPAllowlist() entry")
		}
	}

	assert.Empty(t, offenders, "build clients with pkgs/httpclient (own Transport)")
}
