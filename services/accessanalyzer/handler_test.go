package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestGetFinding verifies GET /finding/{id}?analyzerArn=... -- the real wire
// route (top-level /finding, analyzer carried as an ARN query param), NOT
// /analyzer/{name}/finding/{id}. Exercised through h.Handler() (not the
// backend directly) so both the RouteMatcher and the path parser are proven
// to accept the path a real SDK client sends.
func TestGetFinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		badArn     bool
		missingID  bool
	}{
		{name: "existing_finding", wantStatus: http.StatusOK},
		{name: "wrong_analyzer_arn", wantStatus: http.StatusNotFound, badArn: true},
		{name: "missing_finding_id", wantStatus: http.StatusNotFound, missingID: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)

			analyzerArn := mustAnalyzer(t, b, "get-finding-analyzer")
			findingID := mustFinding(t, b, "get-finding-analyzer")

			if tt.badArn {
				analyzerArn = "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/missing"
			}

			if tt.missingID {
				findingID = "no-such-id"
			}

			rec := doRequest(t, h, http.MethodGet, "/finding/"+findingID+"?analyzerArn="+analyzerArn, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			finding, ok := resp["finding"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, findingID, finding["id"])
			// Real GetFinding wire shape serializes the resource under
			// "resource", not "resourceArn" (that key is only correct for
			// AnalyzedResource); resourceOwnerAccount and analyzedAt are
			// required fields.
			assert.Equal(t, "arn:aws:s3:::bucket", finding["resource"])
			assert.Equal(t, "000000000000", finding["resourceOwnerAccount"])
			assert.NotEmpty(t, finding["analyzedAt"])

			_, hasWrongKey := finding["resourceArn"]
			assert.False(t, hasWrongKey, `wire shape must use "resource", not "resourceArn"`)
		})
	}
}

// TestListFindings verifies POST /finding -- the real wire route (top-level
// /finding, analyzer carried as an ARN in the JSON body), NOT
// /analyzer/{name}/findings.
func TestListFindings(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)

	analyzerArn := mustAnalyzer(t, b, "list-findings-analyzer")
	mustFinding(t, b, "list-findings-analyzer")
	mustFinding(t, b, "list-findings-analyzer")

	t.Run("lists_findings_for_analyzer", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPost, "/finding", map[string]any{"analyzerArn": analyzerArn})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		findings, ok := resp["findings"].([]any)
		require.True(t, ok)
		require.Len(t, findings, 2)

		first, ok := findings[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "arn:aws:s3:::bucket", first["resource"])
		assert.Equal(t, "000000000000", first["resourceOwnerAccount"])
	})

	t.Run("missing_analyzer_arn_is_validation_error", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPost, "/finding", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestUpdateFindings verifies PUT /finding -- the real wire route (top-level
// /finding, analyzer carried as an ARN in the JSON body), NOT
// /analyzer/{name}/findings.
func TestUpdateFindings(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)

	analyzerArn := mustAnalyzer(t, b, "update-findings-analyzer")
	findingID := mustFinding(t, b, "update-findings-analyzer")

	t.Run("archives_via_put_finding", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPut, "/finding", map[string]any{
			"analyzerArn": analyzerArn,
			"ids":         []string{findingID},
			"status":      "ARCHIVED",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		got, err := b.GetFinding("update-findings-analyzer", findingID)
		require.NoError(t, err)
		assert.Equal(t, accessanalyzer.FindingStatusArchived, got.Status)
	})

	t.Run("missing_analyzer_arn_is_validation_error", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPut, "/finding", map[string]any{
			"ids":    []string{findingID},
			"status": "ARCHIVED",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
