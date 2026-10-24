package app

import (
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/routegates"
)

//nolint:paralleltest // testing.AllocsPerRun panics under t.Parallel
func TestMatchersAheadOfS3DoNotAllocate(t *testing.T) {
	reg, _ := routingFixture(t)
	entries := reg.GetAll()
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Priority > entries[j].Priority })

	targetGates, pathGates := routegates.TargetGates(), routegates.PathGates()
	passes := func(gate []string, s string) bool {
		return len(gate) == 0 || slices.ContainsFunc(gate, func(p string) bool { return strings.HasPrefix(s, p) })
	}

	const auth = "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=x"

	tests := []struct{ name, method, uri, auth string }{
		{"get_object", "GET", "/e2e-bucket/key-1", auth},
		{"put_object", "PUT", "/e2e-bucket/key-1", auth},
		{"list_objects", "GET", "/e2e-bucket?list-type=2&prefix=a", auth},
		{"list_objects_unsigned", "GET", "/e2e-bucket?list-type=2&prefix=a", ""},
		{"head_bucket", "HEAD", "/e2e-bucket", auth},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, "http://localhost:4566"+tt.uri, nil)
		if tt.auth != "" {
			req.Header.Set("Authorization", tt.auth)
		}

		c := echo.NewContext(req, httptest.NewRecorder())

		for _, e := range entries {
			name := e.Registerable.Name()
			if name == "S3" {
				break
			}

			if !passes(targetGates[name], "") || !passes(pathGates[name], req.URL.Path) {
				continue
			}

			require.False(t, e.Matcher(c), "%s claimed %s %s", name, tt.method, tt.uri)
			allocs := testing.AllocsPerRun(20, func() { e.Matcher(c) })
			assert.Zero(t, allocs, "%s matcher allocates on %s", name, tt.name)
		}
	}
}
