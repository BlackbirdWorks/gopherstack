package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetUsageStatistics_WireShape locks the GetUsageStatistics fix: the
// previous response used an ad hoc field set (bare "sumByAccount": []
// arrays with no Total object, a nonexistent "topResources" placeholder
// that didn't match sumByFeature/topAccountsByFeature at all) instead of
// the real UsageStatistics shape.
func TestGetUsageStatistics_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/usage/statistics", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	us, ok := resp["usageStatistics"].(map[string]any)
	require.True(t, ok, "response must be wrapped under usageStatistics")

	for _, key := range []string{
		"sumByAccount", "sumByDataSource", "sumByFeature",
		"sumByResource", "topAccountsByFeature", "topResources",
	} {
		assert.Containsf(t, us, key, "UsageStatistics must include %s", key)
	}

	accounts, ok := us["sumByAccount"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, accounts)

	entry, ok := accounts[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, entry, "accountId")

	total, ok := entry["total"].(map[string]any)
	require.True(t, ok, "each sumByAccount entry must carry a Total{amount,unit} object, not a bare number")
	assert.Contains(t, total, "amount")
	assert.Contains(t, total, "unit")
}

// TestGetUsageStatistics_UsageStatisticType locks that requesting a specific
// usageStatisticType nulls out every other UsageStatistics field, per real
// GetUsageStatisticsOutput's doc ("If a UsageStatisticType was provided, the
// objects representing other types will be null.").
func TestGetUsageStatistics_UsageStatisticType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/usage/statistics", map[string]any{
		"usageStatisticType": "SUM_BY_ACCOUNT",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	us := resp["usageStatistics"].(map[string]any)

	assert.Contains(t, us, "sumByAccount")
	assert.NotContains(t, us, "sumByFeature")
	assert.NotContains(t, us, "topAccountsByFeature")
	assert.Len(t, us, 1, "only the requested field should be populated")
}

// TestGetUsageStatistics_SumByFeature_ReflectsEnabledFeatures locks that
// sumByFeature/topAccountsByFeature are derived from the detector's actually
// enabled features, not a hardcoded placeholder.
func TestGetUsageStatistics_SumByFeature_ReflectsEnabledFeatures(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
		"features": []map[string]any{
			{"name": "S3_DATA_EVENTS", "status": "ENABLED"},
			{"name": "EKS_AUDIT_LOGS", "status": "DISABLED"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	detID := createResp["detectorId"].(string)

	rec = doRequest(t, h, http.MethodPost, "/detector/"+detID+"/usage/statistics", map[string]any{
		"usageStatisticType": "SUM_BY_FEATURES",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	us := resp["usageStatistics"].(map[string]any)

	features, ok := us["sumByFeature"].([]any)
	require.True(t, ok)
	require.Len(t, features, 1, "only the ENABLED feature should be reported")

	entry := features[0].(map[string]any)
	assert.Equal(t, "S3_DATA_EVENTS", entry["feature"])
}

// TestGetRemainingFreeTrialDays_HonorsAccountIDs locks two real bugs: (1)
// GetRemainingFreeTrialDaysInput.AccountIds (a required member) was
// completely ignored -- every call returned a single hardcoded entry for the
// detector's own account regardless of which member accountIds were
// requested; (2) the response carried an invented top-level
// "freeTrialDaysRemaining" field that does not exist on the real
// AccountFreeTrialInfo shape at all -- real per-feature remainders live
// under features[].freeTrialDaysRemaining (types.AccountFreeTrialInfo).
func TestGetRemainingFreeTrialDays_HonorsAccountIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	rec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/member", map[string]any{
		"accountDetails": []map[string]any{
			{"accountId": "666666666666", "email": "f@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doRequest(t, h, http.MethodPost, "/detector/"+detID+"/freeTrial/daysRemaining", map[string]any{
		"accountIds": []string{"666666666666", "999999999999"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	accounts, ok := resp["accounts"].([]any)
	require.True(t, ok)
	require.Len(t, accounts, 1, "only the requested, real member account should be reported")

	entry, ok := accounts[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "666666666666", entry["accountId"])
	assert.NotContains(t, entry, "freeTrialDaysRemaining",
		"AccountFreeTrialInfo has no top-level freeTrialDaysRemaining member on the real API")

	features, ok := entry["features"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, features)

	feature, ok := features[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, feature, "name")
	assert.Contains(t, feature, "freeTrialDaysRemaining")

	unprocessed, ok := resp["unprocessedAccounts"].([]any)
	require.True(t, ok)
	require.Len(t, unprocessed, 1, "the non-member account id must be reported unprocessed, not silently dropped")

	unprocessedEntry, ok := unprocessed[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "999999999999", unprocessedEntry["accountId"])
}
