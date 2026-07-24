package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

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

func TestSeedCoverageValidation(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.SeedCoverage(inspector2.CoverageEntry{})
	require.Error(t, err)
}
