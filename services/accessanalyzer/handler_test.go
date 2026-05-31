package accessanalyzer_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

func newTestHandler(t *testing.T) *accessanalyzer.Handler {
	t.Helper()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")

	return accessanalyzer.NewHandler(b)
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
