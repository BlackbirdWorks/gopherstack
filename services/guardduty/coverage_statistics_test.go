package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

func TestCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "list_and_statistics",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				// ListCoverage
				rec := doRequest(t, h, http.MethodPost, "/detector/"+id+"/coverage", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				assert.NotNil(t, listResp["resources"])

				// GetCoverageStatistics requires statisticsType (gopherstack-h910: this
				// used to be silently ignored, so any request -- even one missing the
				// required field -- succeeded and always computed both count maps).
				rec = doRequest(t, h, http.MethodPost, "/detector/"+id+"/coverage/statistics", nil)
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				rec = doRequest(t, h, http.MethodPost, "/detector/"+id+"/coverage/statistics", map[string]any{
					"statisticsType": []string{"COUNT_BY_RESOURCE_TYPE"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var byTypeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &byTypeResp))
				stats, ok := byTypeResp["coverageStatistics"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, stats, "countByResourceType")
				assert.NotContains(t, stats, "countByCoverageStatus")

				rec = doRequest(t, h, http.MethodPost, "/detector/"+id+"/coverage/statistics", map[string]any{
					"statisticsType": []string{"COUNT_BY_COVERAGE_STATUS"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var byStatusResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &byStatusResp))
				stats, ok = byStatusResp["coverageStatistics"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, stats, "countByCoverageStatus")
				assert.NotContains(t, stats, "countByResourceType")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.fn(t, h)
		})
	}
}
