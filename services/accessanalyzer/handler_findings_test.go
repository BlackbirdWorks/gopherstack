package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

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

			// "condition" is a required Finding member -- it must always be
			// present (as {}, since mustFinding creates a finding with no
			// condition), never omitted.
			condition, hasCondition := finding["condition"]
			assert.True(t, hasCondition, `"condition" is required and must be present even when empty`)
			assert.Equal(t, map[string]any{}, condition)
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

// TestFindingV2Lifecycle verifies GetFindingV2 and ListFindingsV2.
func TestFindingV2Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
		name string
	}{
		{
			name: "get_finding_v2",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "fv2-get-analyzer")
				findingID := mustFinding(t, b, "fv2-get-analyzer")

				rec := doRequest(t, h, http.MethodGet, "/findingv2/"+findingID+"?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, findingID, resp["id"])
				// findingType and the ExternalAccessDetails union member of
				// findingDetails are required GetFindingV2Output members.
				assert.Equal(t, "ExternalAccess", resp["findingType"])
				details, ok := resp["findingDetails"].([]any)
				require.True(t, ok)
				require.Len(t, details, 1)
				member, ok := details[0].(map[string]any)
				require.True(t, ok)
				external, ok := member["externalAccessDetails"].(map[string]any)
				require.True(t, ok)
				_, hasCondition := external["condition"]
				assert.True(t, hasCondition, `"condition" is required on ExternalAccessDetails`)
			},
		},
		{
			name: "list_findings_v2",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "fv2-list-analyzer")
				mustFinding(t, b, "fv2-list-analyzer")
				mustFinding(t, b, "fv2-list-analyzer")

				rec := doRequest(t, h, http.MethodPost, "/findingv2", map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				findings := resp["findings"].([]any)
				assert.Len(t, findings, 2)
				first, ok := findings[0].(map[string]any)
				require.True(t, ok)
				// findingType is a required FindingSummaryV2 member.
				assert.Equal(t, "ExternalAccess", first["findingType"])
			},
		},
		{
			name: "get_missing_finding_v2",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "fv2-miss-analyzer")

				rec := doRequest(t, h, http.MethodGet, "/findingv2/no-such-id?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestGetFindingsStatistics verifies POST /analyzer/findings/statistics.
func TestGetFindingsStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn    func(b *accessanalyzer.InMemoryBackend) string
		name       string
		wantStatus int
		wantActive int
	}{
		{
			name: "returns_counts",
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				arn := mustAnalyzer(t, b, "stats-analyzer")
				mustFinding(t, b, "stats-analyzer")
				mustFinding(t, b, "stats-analyzer")

				return arn
			},
			wantStatus: http.StatusOK,
			wantActive: 2,
		},
		{
			name: "missing_analyzer",
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/missing"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			arn := tt.setupFn(b)

			rec := doRequest(t, h, http.MethodPost, "/analyzer/findings/statistics", map[string]any{
				"analyzerArn": arn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantActive > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				stats := resp["findingsStatistics"].([]any)
				require.Len(t, stats, 1)

				extStats := stats[0].(map[string]any)["externalAccessFindingsStatistics"].(map[string]any)
				assert.InDelta(t, float64(tt.wantActive), extStats["totalActiveFindings"], 0.0001)
			}
		})
	}
}

// TestFindingRecommendationLifecycle verifies Generate/Get finding recommendation.
func TestFindingRecommendationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
		name string
	}{
		{
			name: "generate_and_get",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "rec-analyzer")
				findingID := mustFinding(t, b, "rec-analyzer")

				rec := doRequest(t, h, http.MethodPost, "/recommendation/"+findingID, map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doRequest(t, h, http.MethodGet, "/recommendation/"+findingID+"?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["recommendationType"])
			},
		},
		{
			name: "get_missing_recommendation",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodGet, "/recommendation/no-such-id?analyzerArn=arn", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}
