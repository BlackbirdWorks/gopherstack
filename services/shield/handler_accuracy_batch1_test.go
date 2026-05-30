package shield_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// --- Batch-1 accuracy gap: DescribeAttackStatistics response is flat (no "AttackStatistics" wrapper) ---

// TestBatch1_DescribeAttackStatisticsFlatResponse verifies the response has DataItems and TimeRange
// at the top level — not wrapped in "AttackStatistics".
// AWS SDK deserializes DataItems and TimeRange directly from the response body.
func TestBatch1_DescribeAttackStatisticsFlatResponse(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasWrapper := resp["AttackStatistics"]
	assert.False(t, hasWrapper, "response must NOT be wrapped in 'AttackStatistics'")

	_, hasDataItems := resp["DataItems"]
	assert.True(t, hasDataItems, "DataItems must be at the top level")

	_, hasTimeRange := resp["TimeRange"]
	assert.True(t, hasTimeRange, "TimeRange must be at the top level")
}

// TestBatch1_DescribeAttackStatisticsTimeRangeIsFloat verifies TimeRange values are float seconds.
func TestBatch1_DescribeAttackStatisticsTimeRangeIsFloat(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tr := resp["TimeRange"].(map[string]any)

	fromVal, ok := tr["FromInclusive"].(float64)
	assert.True(t, ok, "TimeRange.FromInclusive must be float64")
	assert.Greater(t, fromVal, float64(1e9), "FromInclusive must be a real epoch second")

	toVal, ok := tr["ToExclusive"].(float64)
	assert.True(t, ok, "TimeRange.ToExclusive must be float64")
	assert.Greater(t, toVal, fromVal, "ToExclusive must be after FromInclusive")
}

// --- Batch-1 accuracy gap: ListAttacks StartTime/EndTime use TimeRange objects ---

// TestBatch1_ListAttacksStartTimeRangeObject verifies StartTime is parsed as TimeRange.
// AWS SDK sends: {"StartTime": {"FromInclusive": 1234567890.0}}, not {"StartTime": 1234567890}.
func TestBatch1_ListAttacksStartTimeRangeObject(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	// Add an attack at a known recent time (always within range).
	b.AddAttackInternal("atk-recent", eipARN("1"))

	h := shield.NewHandler(b)

	// Request with a TimeRange that covers all time.
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{
		"StartTime": map[string]any{
			"FromInclusive": float64(0),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	assert.Len(t, summaries, 1, "StartTime TimeRange object must be accepted")
}

// TestBatch1_ListAttacksEndTimeRangeObject verifies EndTime is parsed as TimeRange.
func TestBatch1_ListAttacksEndTimeRangeObject(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-old", eipARN("1"))

	h := shield.NewHandler(b)

	// EndTime with a ToExclusive far in the future — all attacks should be included.
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{
		"EndTime": map[string]any{
			"ToExclusive": float64(9_999_999_999),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	assert.Len(t, summaries, 1, "EndTime TimeRange object must be accepted")
}

// TestBatch1_ListAttacksEndTimeRangeFiltersOut verifies attacks after ToExclusive are excluded.
func TestBatch1_ListAttacksEndTimeRangeFiltersOut(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-recent", eipARN("1"))

	h := shield.NewHandler(b)

	// EndTime with ToExclusive in the distant past — attack should be excluded.
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{
		"EndTime": map[string]any{
			"ToExclusive": float64(1_000_000),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	assert.Empty(t, summaries, "attacks after ToExclusive must be excluded")
}
