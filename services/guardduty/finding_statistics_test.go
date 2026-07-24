package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFindingsStatistics_GroupBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupBy   string
		wantKey   string
		checkKeys []string
	}{
		{
			name:      "account",
			groupBy:   "ACCOUNT",
			wantKey:   "groupedByAccount",
			checkKeys: []string{"accountId", "lastGeneratedAt", "totalFindings"},
		},
		{
			name:      "date",
			groupBy:   "DATE",
			wantKey:   "groupedByDate",
			checkKeys: []string{"date", "lastGeneratedAt", "totalFindings"},
		},
		{
			name:      "finding_type",
			groupBy:   "FINDING_TYPE",
			wantKey:   "groupedByFindingType",
			checkKeys: []string{"findingType", "lastGeneratedAt", "totalFindings"},
		},
		{
			name:      "resource",
			groupBy:   "RESOURCE",
			wantKey:   "groupedByResource",
			checkKeys: []string{"accountId", "resourceId", "resourceType", "lastGeneratedAt", "totalFindings"},
		},
		{
			name:      "severity",
			groupBy:   "SEVERITY",
			wantKey:   "groupedBySeverity",
			checkKeys: []string{"severity", "lastGeneratedAt", "totalFindings"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			detID := createTestDetector(t, h)

			doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/create", map[string]any{
				"findingTypes": []string{"Backdoor:EC2/DenialOfService.Tcp", "Recon:IAMUser/TorIPCaller"},
			})

			rec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/statistics", map[string]any{
				"groupBy": tt.groupBy,
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			fs, ok := resp["findingStatistics"].(map[string]any)
			require.True(t, ok)

			// Only the requested groupedByX field is populated -- matches
			// FindingStatistics' doc: "please use GroupBy instead" of the
			// deprecated countBySeverity.
			_, hasCountBySeverity := fs["countBySeverity"]
			assert.False(t, hasCountBySeverity, "countBySeverity must not appear when groupBy is set")

			groups, ok := fs[tt.wantKey].([]any)
			require.True(t, ok, "%s must be present", tt.wantKey)
			require.NotEmpty(t, groups)

			first, ok := groups[0].(map[string]any)
			require.True(t, ok)

			for _, key := range tt.checkKeys {
				assert.Containsf(t, first, key, "%s entry must include %s", tt.wantKey, key)
			}
		})
	}
}

// TestGetFindingsStatistics_GroupBy_Unset_ReturnsCountBySeverity guards the
// deprecated-but-still-supported default behavior: no GroupBy in the
// request must still return countBySeverity, not an empty document.
func TestGetFindingsStatistics_GroupBy_Unset_ReturnsCountBySeverity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/create", map[string]any{
		"findingTypes": []string{"Backdoor:EC2/DenialOfService.Tcp"},
	})

	rec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/statistics", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fs := resp["findingStatistics"].(map[string]any)

	assert.Contains(t, fs, "countBySeverity")
	assert.NotContains(t, fs, "groupedByAccount")
}

// TestGetFindingsStatistics_FindingCriteria locks that FindingCriteria
// filters the findings statistics are computed over, not just ListFindings.
func TestGetFindingsStatistics_FindingCriteria(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/create", map[string]any{
		"findingTypes": []string{"Backdoor:EC2/DenialOfService.Tcp", "Recon:IAMUser/TorIPCaller"},
	})

	rec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/statistics", map[string]any{
		"groupBy": "FINDING_TYPE",
		"findingCriteria": map[string]any{
			"criterion": map[string]any{
				"type": map[string]any{"equals": []string{"Recon:IAMUser/TorIPCaller"}},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fs := resp["findingStatistics"].(map[string]any)

	groups, ok := fs["groupedByFindingType"].([]any)
	require.True(t, ok)
	require.Len(t, groups, 1, "FindingCriteria must filter which findings are aggregated")

	first := groups[0].(map[string]any)
	assert.Equal(t, "Recon:IAMUser/TorIPCaller", first["findingType"])
}
