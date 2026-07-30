package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestListCoverageEmpty(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/coverage/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	resources, ok := resp["coveredResources"].([]any)
	require.True(t, ok)
	assert.Empty(t, resources)
}

// TestListCoverageSeeded exercises the real SeedCoverage -> ListCoverage
// path (real CoveredResource wire shape), replacing the prior
// hardwired-empty ListCoverage stub.
func TestListCoverageSeeded(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	_, err := h.Backend.SeedCoverage(inspector2.CoverageEntry{
		ResourceID:   "i-0123456789",
		ResourceType: "AWS_EC2_INSTANCE",
		ScanType:     "EC2",
		ScanStatus:   &inspector2.CoverageScanStatus{StatusCode: "ACTIVE"},
	})
	require.NoError(t, err)

	_, err = h.Backend.SeedCoverage(inspector2.CoverageEntry{
		ResourceID:   "repo-1",
		ResourceType: "AWS_ECR_REPOSITORY",
		ScanType:     "ECR",
	})
	require.NoError(t, err)

	rec := auditDo(t, h, http.MethodPost, "/coverage/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	resources, ok := resp["coveredResources"].([]any)
	require.True(t, ok)
	require.Len(t, resources, 2)

	first, ok := resources[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "123456789012", first["accountId"])
	assert.NotContains(t, first, "vulnerabilityId")
	assert.NotEmpty(t, first["lastScannedAt"])

	// Filter down to just the EC2 scan type.
	rec = auditDo(t, h, http.MethodPost, "/coverage/list", map[string]any{
		"filterCriteria": map[string]any{
			"scanType": []any{map[string]any{"comparison": "EQUALS", "value": "EC2"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	resources, ok = resp["coveredResources"].([]any)
	require.True(t, ok)
	require.Len(t, resources, 1)

	entry, ok := resources[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "i-0123456789", entry["resourceId"])
}

func TestListCoverageStatistics(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	_, err := h.Backend.SeedCoverage(inspector2.CoverageEntry{
		ResourceID: "i-1", ResourceType: "AWS_EC2_INSTANCE", ScanType: "EC2",
	})
	require.NoError(t, err)

	_, err = h.Backend.SeedCoverage(inspector2.CoverageEntry{
		ResourceID: "i-2", ResourceType: "AWS_EC2_INSTANCE", ScanType: "EC2",
	})
	require.NoError(t, err)

	_, err = h.Backend.SeedCoverage(inspector2.CoverageEntry{
		ResourceID: "repo-1", ResourceType: "AWS_ECR_REPOSITORY", ScanType: "ECR",
	})
	require.NoError(t, err)

	// Ungrouped: only the overall total is populated.
	rec := auditDo(t, h, http.MethodPost, "/coverage/statistics/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InDelta(t, float64(3), resp["totalCounts"], 0)
	groups, ok := resp["countsByGroup"].([]any)
	require.True(t, ok)
	assert.Empty(t, groups)

	// Grouped by RESOURCE_TYPE: two buckets.
	rec = auditDo(t, h, http.MethodPost, "/coverage/statistics/list", map[string]any{
		"groupBy": "RESOURCE_TYPE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InDelta(t, float64(3), resp["totalCounts"], 0)

	groups, ok = resp["countsByGroup"].([]any)
	require.True(t, ok)
	require.Len(t, groups, 2)

	counts := make(map[string]float64, len(groups))

	for _, raw := range groups {
		g, groupOk := raw.(map[string]any)
		require.True(t, groupOk)
		key, keyOk := g["groupKey"].(string)
		require.True(t, keyOk)
		count, countOk := g["count"].(float64)
		require.True(t, countOk)
		counts[key] = count
	}

	assert.InDelta(t, float64(2), counts["AWS_EC2_INSTANCE"], 0)
	assert.InDelta(t, float64(1), counts["AWS_ECR_REPOSITORY"], 0)
}

// TestListCoverage_FilterCriteriaFacets exercises the CoverageFilterCriteria
// facets that ListCoverage genuinely evaluates against real stored
// CoverageEntry data -- scanStatusCode, scanStatusReason, scanMode, and the
// lastScannedAt date range -- confirming they now actually narrow results.
// A prior revision accepted these in the request shape (per
// CoverageFilterCriteria's real wire fields) but silently never applied them,
// so any client-supplied filter on these facets was a no-op that returned
// every seeded entry regardless.
func TestListCoverage_FilterCriteriaFacets(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	seed := func(t *testing.T, h *inspector2.Handler) {
		t.Helper()

		_, err := h.Backend.SeedCoverage(inspector2.CoverageEntry{
			ResourceID: "r-active", ResourceType: "AWS_EC2_INSTANCE", ScanType: "EC2",
			ScanStatus: &inspector2.CoverageScanStatus{StatusCode: "ACTIVE"},
			ScanMode:   "EC2_AGENTLESS", LastScannedAt: older,
		})
		require.NoError(t, err)

		_, err = h.Backend.SeedCoverage(inspector2.CoverageEntry{
			ResourceID: "r-inactive", ResourceType: "AWS_EC2_INSTANCE", ScanType: "EC2",
			ScanStatus: &inspector2.CoverageScanStatus{StatusCode: "INACTIVE", Reason: "UNSUPPORTED_OS"},
			ScanMode:   "EC2_SSM_AGENT_BASED", LastScannedAt: newer,
		})
		require.NoError(t, err)
	}

	tests := []struct {
		filterCriteria map[string]any
		name           string
		wantResourceID string
	}{
		{
			name: "scan_status_code_narrows",
			filterCriteria: map[string]any{
				"scanStatusCode": []any{map[string]any{"comparison": "EQUALS", "value": "ACTIVE"}},
			},
			wantResourceID: "r-active",
		},
		{
			name: "scan_status_reason_narrows",
			filterCriteria: map[string]any{
				"scanStatusReason": []any{map[string]any{"comparison": "EQUALS", "value": "UNSUPPORTED_OS"}},
			},
			wantResourceID: "r-inactive",
		},
		{
			name: "scan_mode_narrows",
			filterCriteria: map[string]any{
				"scanMode": []any{map[string]any{"comparison": "EQUALS", "value": "EC2_SSM_AGENT_BASED"}},
			},
			wantResourceID: "r-inactive",
		},
		{
			name: "last_scanned_at_start_inclusive_narrows_to_newer",
			filterCriteria: map[string]any{
				"lastScannedAt": []any{
					map[string]any{"startInclusive": float64(older.Add(24 * time.Hour).Unix())},
				},
			},
			wantResourceID: "r-inactive",
		},
		{
			name: "last_scanned_at_end_inclusive_narrows_to_older",
			filterCriteria: map[string]any{
				"lastScannedAt": []any{
					map[string]any{"endInclusive": float64(newer.Add(-24 * time.Hour).Unix())},
				},
			},
			wantResourceID: "r-active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			seed(t, h)

			rec := auditDo(t, h, http.MethodPost, "/coverage/list", map[string]any{
				"filterCriteria": tt.filterCriteria,
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			resources, ok := resp["coveredResources"].([]any)
			require.True(t, ok)
			require.Len(t, resources, 1)

			entry, ok := resources[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantResourceID, entry["resourceId"])
		})
	}
}

func TestSeedCoverageValidation(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.SeedCoverage(inspector2.CoverageEntry{})
	require.Error(t, err)
}
