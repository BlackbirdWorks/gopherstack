package shield_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestBatch1_DescribeAttackStatisticsFlatResponse verifies the response has DataItems and TimeRange
// at the top level — not wrapped in "AttackStatistics".
// AWS SDK deserializes DataItems and TimeRange directly from the response body.
func TestHandler_DescribeAttackStatisticsFlatResponse(t *testing.T) {
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
func TestHandler_DescribeAttackStatisticsTimeRangeIsFloat(t *testing.T) {
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
func TestHandler_ListAttacksStartTimeRangeObject(t *testing.T) {
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
func TestHandler_ListAttacksEndTimeRangeObject(t *testing.T) {
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
func TestHandler_ListAttacksEndTimeRangeFiltersOut(t *testing.T) {
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

// TestParity_OpaquePageToken_ListAttacks verifies attack pagination tokens are opaque.
func TestHandler_ListAttacksOpaquePageToken(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	resourceARN := "arn:aws:ec2:us-east-1:123456789012:eip/eipalloc-00000001"
	b.AddProtectionInternal("prot-eip", resourceARN)

	for range 5 {
		_, err := b.SimulateAttack(resourceARN, nil)
		require.NoError(t, err)
	}

	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string `json:"NextToken"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.NextToken)

	offset := opaqueOffset(t, out.NextToken)
	assert.Equal(t, 3, offset)
}

// TestHandler_DescribeAttack tests DescribeAttack.
func TestHandler_DescribeAttack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.InMemoryBackend) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
		wantFields bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) string {
				a := b.AddAttackInternal("attack-001", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")

				return a.AttackID
			},
			body: func(id string) map[string]any {
				return map[string]any{"AttackId": id}
			},
			wantStatus: http.StatusOK,
			wantFields: true,
		},
		{
			name: "not found",
			setup: func(_ *shield.InMemoryBackend) string {
				return "nonexistent-attack"
			},
			body: func(id string) map[string]any {
				return map[string]any{"AttackId": id}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing attack id",
			setup: func(_ *shield.InMemoryBackend) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			h := shield.NewHandler(b)
			id := tt.setup(b)
			rec := doShieldRequest(t, h, "DescribeAttack", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFields {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				attack, ok := result["Attack"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, id, attack["AttackId"])
				assert.NotEmpty(t, attack["ResourceArn"])
			}
		})
	}
}

// TestHandler_DescribeAttackStatistics tests DescribeAttackStatistics.
func TestHandler_DescribeAttackStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.InMemoryBackend)
		name       string
		wantCount  int64
		wantStatus int
	}{
		{
			name:       "no attacks",
			setup:      func(_ *shield.InMemoryBackend) {},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "two attacks",
			setup: func(b *shield.InMemoryBackend) {
				b.AddAttackInternal("atk-1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")
				b.AddAttackInternal("atk-2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2")
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			h := shield.NewHandler(b)
			tt.setup(b)

			rec := doShieldRequest(t, h, "DescribeAttackStatistics", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			items, ok := result["DataItems"].([]any)
			require.True(t, ok)
			require.Len(t, items, 1)

			item, ok := items[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantCount, int64(item["AttackCount"].(float64)))
		})
	}
}

// TestAudit_Gap5_AttackRichFields verifies DescribeAttack returns rich fields.
func TestHandler_AttackRichFields(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	a := b.AddAttackInternal("atk-001", eipARN("1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeAttack", map[string]any{
		"AttackId": a.AttackID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	atk := resp["Attack"].(map[string]any)
	assert.NotNil(t, atk["AttackVectors"])
	assert.NotNil(t, atk["AttackCounters"])
	assert.NotNil(t, atk["Mitigations"])
}

// --- Gap 6: ListAttacks supports multiple ResourceArns ---

// TestAudit_Gap6_ListAttacksHTTPMultipleARNs verifies HTTP multiple ARN filtering.
func TestHandler_ListAttacksMultipleARNs(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-1", eipARN("1"))
	b.AddAttackInternal("atk-2", eipARN("2"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{
		"ResourceArns": []string{eipARN("1"), eipARN("2")},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	assert.Len(t, summaries, 2)
}

// --- Gap 7: Pagination for ListAttacks, ListProtections, ListProtectionGroups ---

// TestAudit_Gap7_ListAttacksPagination verifies pagination for attacks.
func TestHandler_ListAttacksPagination(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")

	for i := range 5 {
		b.AddAttackInternal("atk-"+string(rune('a'+i)), eipARN("1"))
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	assert.Len(t, summaries, 2)
	assert.NotEmpty(t, resp["NextToken"])
}

// --- Gap 8: ListProtections InclusionFilters ---

// --- Gap 12: DescribeAttackStatistics per-bucket with AttackVolume ---

// TestAudit_Gap12_DescribeAttackStatisticsWithVolume verifies AttackVolume in statistics.
func TestHandler_DescribeAttackStatisticsWithVolume(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-1", eipARN("1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotNil(t, resp["TimeRange"])
	assert.NotNil(t, resp["DataItems"])

	items := resp["DataItems"].([]any)
	assert.NotEmpty(t, items)
}

// TestAudit_Gap12_DescribeAttackStatisticsEmptyWhenNoAttacks verifies empty stats.
func TestHandler_DescribeAttackStatisticsEmptyWhenNoAttacks(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items := resp["DataItems"].([]any)
	assert.Len(t, items, 1)
}

// --- Gap 13: Timestamps emitted as float seconds (not int) ---

// TestAudit_Gap13_AttackTimestampIsFloat verifies attack timestamps are floats.
func TestHandler_AttackTimestampIsFloat(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	a := b.AddAttackInternal("atk-1", eipARN("1"))
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeAttack", map[string]any{"AttackId": a.AttackID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	atk := raw["Attack"].(map[string]any)
	st, ok := atk["StartTime"].(float64)
	assert.True(t, ok, "StartTime should be float64")
	assert.Greater(t, st, float64(1e9))
}

// --- Gap 14: CreateProtection ResourceType validation ---

// TestAudit_Gap25_SimulateAttackHTTPEndpoint verifies __SimulateAttack HTTP endpoint.
func TestHandler_SimulateAttackHTTPEndpoint(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "__SimulateAttack", map[string]any{
		"ResourceArn":       eipARN("1"),
		"AttackVectorTypes": []string{"SYN_FLOOD"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["AttackId"])
	assert.Equal(t, 1, shield.AttackCount(b))
}

// TestAudit_Gap25_SimulateAttackNoProtectionFails verifies protection required.
func TestHandler_SimulateAttackNoProtectionFails(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "__SimulateAttack", map[string]any{
		"ResourceArn": eipARN("notprotected"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Combined scenario: attack simulation visible in statistics and list ---

// TestAudit_AttackSimulationEndToEnd verifies simulated attacks flow through all endpoints.
func TestHandler_AttackSimulationEndToEnd(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("prot", eipARN("42"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)

	// Simulate an attack.
	simRec := doShieldRequest(t, h, "__SimulateAttack", map[string]any{
		"ResourceArn": eipARN("42"),
	})
	require.Equal(t, http.StatusOK, simRec.Code)

	var simResp map[string]any
	require.NoError(t, json.Unmarshal(simRec.Body.Bytes(), &simResp))
	attackID := simResp["AttackId"].(string)

	// ListAttacks must show it.
	listRec := doShieldRequest(t, h, "ListAttacks", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries := listResp["AttackSummaries"].([]any)
	require.Len(t, summaries, 1)

	// DescribeAttack must show it with rich fields.
	descRec := doShieldRequest(t, h, "DescribeAttack", map[string]any{"AttackId": attackID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	atk := descResp["Attack"].(map[string]any)
	assert.NotEmpty(t, atk["AttackVectors"])
	assert.NotEmpty(t, atk["AttackCounters"])
	assert.NotEmpty(t, atk["Mitigations"])

	// Statistics must reflect the attack.
	statsRec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, statsRec.Code)

	var statsResp map[string]any
	require.NoError(t, json.Unmarshal(statsRec.Body.Bytes(), &statsResp))
	items := statsResp["DataItems"].([]any)
	require.NotEmpty(t, items)

	item0 := items[0].(map[string]any)
	count, _ := item0["AttackCount"].(float64)
	assert.InDelta(t, 1.0, count, 0)
}

// --- ListProtections includes ALAR in response ---

// TestAudit_ListAttacksIncludesVectors verifies AttackVectors in ListAttacks response.
func TestHandler_ListAttacksIncludesVectors(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-1", eipARN("1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	require.Len(t, summaries, 1)

	atk := summaries[0].(map[string]any)
	vectors, ok := atk["AttackVectors"]
	assert.True(t, ok, "AttackSummaries items should include AttackVectors")
	assert.NotEmpty(t, vectors)
}

// TestRefinement1_HTTPListAttacks tests via HTTP.
func TestHandler_ListAttacksBasic(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddAttackInternal("atk-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{})
	require.Equal(t, 200, rec.Code)
}
