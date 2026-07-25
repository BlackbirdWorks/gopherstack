package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestUsageTotals verifies ListUsageTotals reports at least an
// (empty-usage) entry for the caller's own account with no state seeded.
func TestUsageTotals(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/usage/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	totals, ok := resp["totals"].([]any)
	require.True(t, ok)
	require.Len(t, totals, 1)

	total, ok := totals[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "123456789012", total["accountId"])
	usage, ok := total["usage"].([]any)
	require.True(t, ok)
	assert.Empty(t, usage, "no resource type enabled and no coverage seeded")
}

// TestUsageTotalsReflectsEnabledCoverage verifies ListUsageTotals derives
// real usage entries from enabled resource types and seeded coverage,
// replacing the prior hardwired-always-empty-usage stub.
func TestUsageTotalsReflectsEnabledCoverage(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := auditDo(t, h, http.MethodPost, "/enable", map[string]any{
		"resourceTypes": []string{"EC2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	_, err := h.Backend.SeedCoverage(inspector2.CoverageEntry{
		ResourceID: "i-0123456789", ResourceType: "AWS_EC2_INSTANCE", ScanType: "EC2",
	})
	require.NoError(t, err)

	rec = auditDo(t, h, http.MethodPost, "/usage/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	totals, ok := resp["totals"].([]any)
	require.True(t, ok)
	require.Len(t, totals, 1)

	total, ok := totals[0].(map[string]any)
	require.True(t, ok)
	usage, ok := total["usage"].([]any)
	require.True(t, ok)
	require.Len(t, usage, 1)

	entry, ok := usage[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "EC2_INSTANCE_HOURS", entry["type"])
	assert.Equal(t, "USD", entry["currency"])
	assert.InDelta(t, float64(1), entry["total"], 0)
}

func TestBatchGetFreeTrialInfo(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/freetrialinfo/batchget", map[string]any{
		"accountIds": []string{"123456789012"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	accounts, _ := resp["accounts"].([]any)
	assert.Len(t, accounts, 1)
}
