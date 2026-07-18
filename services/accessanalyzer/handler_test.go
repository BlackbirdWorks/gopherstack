package accessanalyzer_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// newTestHandler constructs a Handler backed by a fresh InMemoryBackend, for
// tests across this package that don't need direct access to the backend.
func newTestHandler(t *testing.T) *accessanalyzer.Handler {
	t.Helper()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")

	return accessanalyzer.NewHandler(b)
}

// doRequest fires a request at the handler and returns the recorder.
func doRequest(t *testing.T, h *accessanalyzer.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))

	if len(bodyBytes) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// mustAnalyzer creates an analyzer and returns its ARN.
func mustAnalyzer(t *testing.T, b *accessanalyzer.InMemoryBackend, name string) string {
	t.Helper()

	a, err := b.CreateAnalyzer(name, accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	return a.Arn
}

// mustFinding creates a finding for an analyzer and returns its ID.
func mustFinding(t *testing.T, b *accessanalyzer.InMemoryBackend, analyzerName string) string {
	t.Helper()

	f, err := b.AddFinding(analyzerName, "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil)
	require.NoError(t, err)

	return f.ID
}

func TestAccessAnalyzerHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		// Access Analyzer-owned paths.
		{name: "analyzer_collection", path: "/analyzer", wantMatch: true},
		{name: "analyzer_resource", path: "/analyzer/my-analyzer", wantMatch: true},
		{
			name:      "tags_access_analyzer_arn",
			path:      "/tags/arn:aws:access-analyzer:us-east-1:000000000000:analyzer/my-analyzer",
			wantMatch: true,
		},
		{name: "resource_scan", path: "/resource/scan", wantMatch: true},
		{name: "analyzedResource", path: "/analyzedResource", wantMatch: true},
		// GetFinding/ListFindings/UpdateFindings live at the top-level
		// /finding and /finding/{id} -- NOT nested under /analyzer/{name}/...
		// (see handleGetFinding's doc comment). These must be routed on a
		// segment boundary so they do not also swallow /findingv2.
		{name: "finding_collection", path: "/finding", wantMatch: true},
		{name: "finding_resource", path: "/finding/abc-123", wantMatch: true},
		{name: "finding_v2_collection", path: "/findingv2", wantMatch: true},
		{name: "finding_v2_resource", path: "/findingv2/abc-123", wantMatch: true},

		// Non-access-analyzer /tags/ paths must NOT be claimed.
		{
			name:      "tags_fis_arn",
			path:      "/tags/arn:aws:fis:us-east-1:000000000000:experiment-template/EXTabcdef",
			wantMatch: false,
		},
		{
			name:      "tags_fis_experiment",
			path:      "/tags/arn:aws:fis:us-east-1:000000000000:experiment/EXPabcdef",
			wantMatch: false,
		},
		{
			name:      "tags_guardduty_arn",
			path:      "/tags/arn:aws:guardduty:us-east-1:000000000000:detector/abc123",
			wantMatch: false,
		},
		{
			name:      "tags_kinesis_arn",
			path:      "/tags/arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			wantMatch: false,
		},

		// Unrelated paths.
		{name: "root", path: "/", wantMatch: false},
		{name: "experimentTemplates", path: "/experimentTemplates", wantMatch: false},
		{name: "tables", path: "/tables", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}
