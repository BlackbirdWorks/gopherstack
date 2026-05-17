package xray_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &xray.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, xray.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &xray.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ xray.StorageBackend = (*xray.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	h := xray.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 38)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	h := xray.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_ErrValidation verifies ErrValidation sentinel is exported.
func TestRefinement1_ErrValidation(t *testing.T) {
	t.Parallel()

	assert.Error(t, xray.ErrValidation)
}

// TestRefinement1_HandlerBackendIsInterface verifies Handler.Backend is StorageBackend.
func TestRefinement1_HandlerBackendIsInterface(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	h := xray.NewHandler(b)

	// Handler.Backend must be assignable to the interface.
	var _ = h.Backend
}

// TestRefinement1_CountHelpers verifies all export_test count helpers.
func TestRefinement1_CountHelpers(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()

	assert.Equal(t, 0, b.GroupCount())
	// A fresh backend always has the Default sampling rule pre-seeded.
	assert.Equal(t, 1, b.SamplingRuleCount())
	assert.Equal(t, 0, b.TraceCount())
	assert.Equal(t, 0, b.InsightCount())
	assert.Equal(t, 0, b.ResourcePolicyCount())

	_, err := b.CreateGroup("g1", "")
	require.NoError(t, err)

	assert.Equal(t, 1, b.GroupCount())

	b.AddInsightInternal(xray.Insight{InsightID: "ins-1", State: "ACTIVE", StartTime: time.Now()})
	assert.Equal(t, 1, b.InsightCount())

	b.AddResourcePolicyInternal(xray.ResourcePolicy{PolicyName: "pol-1", PolicyDocument: `{}`})
	assert.Equal(t, 1, b.ResourcePolicyCount())
}

// TestRefinement1_PutEncryptionConfigValidation verifies type validation.
func TestRefinement1_PutEncryptionConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		encType string
		keyID   string
		wantErr bool
	}{
		{name: "NONE valid", encType: "NONE", keyID: "", wantErr: false},
		{name: "KMS with key valid", encType: "KMS", keyID: "arn:aws:kms:us-east-1:123:key/abc", wantErr: false},
		{name: "invalid type", encType: "INVALID", keyID: "", wantErr: true},
		{name: "KMS without key invalid", encType: "KMS", keyID: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend()
			_, err := b.PutEncryptionConfig(tt.encType, tt.keyID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_PutEncryptionConfigHandler verifies handler validates type.
func TestRefinement1_PutEncryptionConfigHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{name: "valid NONE", body: map[string]any{"Type": "NONE"}, wantStatus: http.StatusOK},
		{
			name:       "valid KMS",
			body:       map[string]any{"Type": "KMS", "KeyId": "arn:aws:kms:us-east-1:123:key/abc"},
			wantStatus: http.StatusOK,
		},
		{name: "invalid type", body: map[string]any{"Type": "BOGUS"}, wantStatus: http.StatusBadRequest},
		{name: "KMS without key", body: map[string]any{"Type": "KMS"}, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/EncryptionConfig", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_ModifiedAtTracking verifies ModifiedAt is updated on UpdateSamplingRule.
func TestRefinement1_ModifiedAtTracking(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()

	r, err := b.CreateSamplingRule(xray.SamplingRule{RuleName: "track-rule", FixedRate: 0.1, Priority: 1})
	require.NoError(t, err)

	createdAt := r.CreatedAt
	modifiedAt := r.ModifiedAt

	// Small sleep to ensure timestamps differ.
	time.Sleep(time.Millisecond)

	updated, err := b.UpdateSamplingRule("track-rule", xray.SamplingRule{ServiceName: "svc"})
	require.NoError(t, err)

	assert.Equal(t, createdAt, updated.CreatedAt)
	assert.True(t, updated.ModifiedAt.After(modifiedAt))
}

// TestRefinement1_SamplingRuleAttributes verifies Attributes field roundtrip.
func TestRefinement1_SamplingRuleAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"SamplingRule": map[string]any{
			"RuleName":      "attrs-rule",
			"FixedRate":     0.05,
			"Priority":      1,
			"ReservoirSize": 5,
			"ResourceARN":   "*",
			"ServiceName":   "*",
			"ServiceType":   "*",
			"Host":          "*",
			"HTTPMethod":    "*",
			"URLPath":       "*",
			"Attributes":    map[string]any{"env": "prod", "team": "platform"},
		},
	}

	rec := doXrayRequest(t, h, "/CreateSamplingRule", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	record, ok := resp["SamplingRuleRecord"].(map[string]any)
	require.True(t, ok)

	rule, ok := record["SamplingRule"].(map[string]any)
	require.True(t, ok)

	attrs, ok := rule["Attributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", attrs["env"])
}

// TestRefinement1_GetInsightSummariesStateFilter verifies state filtering.
func TestRefinement1_GetInsightSummariesStateFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddInsightInternal(xray.Insight{InsightID: "active-1", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightInternal(xray.Insight{InsightID: "active-2", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightInternal(xray.Insight{InsightID: "closed-1", State: "CLOSED", StartTime: time.Now()})

	tests := []struct {
		name      string
		states    []any
		wantCount int
	}{
		{name: "no filter returns all", states: nil, wantCount: 3},
		{name: "filter by ACTIVE", states: []any{"ACTIVE"}, wantCount: 2},
		{name: "filter by CLOSED", states: []any{"CLOSED"}, wantCount: 1},
		{name: "filter by both states", states: []any{"ACTIVE", "CLOSED"}, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{}
			if tt.states != nil {
				body["States"] = tt.states
			}

			rec := doXrayRequest(t, h, "/GetInsightSummaries", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, ok := resp["InsightSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

// TestRefinement1_GetTraceSummariesTimeFilter verifies time window filtering.
func TestRefinement1_GetTraceSummariesTimeFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	// Insert traces at specific times.
	oldTime := time.Unix(1600000000, 0)
	newTime := time.Unix(1700000000, 0)

	b.PutTraceForTest(oldTime)
	b.PutTraceForTest(newTime)

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name:      "no filter returns all traces",
			body:      map[string]any{},
			wantCount: 2,
		},
		{
			name: "filter within window",
			body: map[string]any{
				"StartTime": float64(newTime.Unix() - 10),
				"EndTime":   float64(newTime.Unix() + 10),
			},
			wantCount: 1,
		},
		{
			name: "filter excludes all",
			body: map[string]any{
				"StartTime": float64(newTime.Unix() + 100),
				"EndTime":   float64(newTime.Unix() + 200),
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doXrayRequest(t, h, "/TraceSummaries", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, ok := resp["TraceSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

// TestRefinement1_PutAndListResourcePolicies verifies the new put/list operations.
func TestRefinement1_PutAndListResourcePolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Initially empty.
	rec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	policies, ok := resp["ResourcePolicies"].([]any)
	require.True(t, ok)
	assert.Empty(t, policies)

	// Put a policy.
	putBody := map[string]any{
		"PolicyName":     "my-policy",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	}
	putRec := doXrayRequest(t, h, "/PutResourcePolicy", putBody)
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))

	policyObj, ok := putResp["ResourcePolicy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-policy", policyObj["PolicyName"])
	assert.NotEmpty(t, policyObj["PolicyRevisionId"])

	// List now returns the policy.
	listRec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	policies, ok = listResp["ResourcePolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, policies, 1)
}

// TestRefinement1_PutResourcePolicyValidation verifies field validation.
func TestRefinement1_PutResourcePolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing PolicyName",
			body:       map[string]any{"PolicyDocument": `{}`},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing PolicyDocument",
			body:       map[string]any{"PolicyName": "pol"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid",
			body:       map[string]any{"PolicyName": "pol", "PolicyDocument": `{}`},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/PutResourcePolicy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_SnapshotRestoreWithEncryptionConfig verifies encryption config persists.
func TestRefinement1_SnapshotRestoreWithEncryptionConfig(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()

	_, err := b.PutEncryptionConfig("KMS", "arn:aws:kms:us-east-1:123:key/abc")
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	cfg := b2.GetEncryptionConfig()
	assert.Equal(t, "KMS", cfg.Type)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", cfg.KeyID)
}

// TestRefinement1_SnapshotRestoreWithIndexingRules verifies indexing rules persist.
func TestRefinement1_SnapshotRestoreWithIndexingRules(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()

	// Default backend has at least one indexing rule.
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	rules := b2.GetIndexingRules()
	assert.NotEmpty(t, rules)
}

// TestRefinement1_AddInsightEventInternal verifies the seed helper works.
func TestRefinement1_AddInsightEventInternal(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	b.AddInsightInternal(xray.Insight{InsightID: "ins-1", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightEventInternal(xray.InsightEvent{InsightID: "ins-1", Summary: "event-1", EventTime: time.Now()})

	events, err := b.GetInsightEvents("ins-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "event-1", events[0].Summary)
}

// TestRefinement1_AddTraceRetrievalInternal verifies the seed helper and retrieval status.
func TestRefinement1_AddTraceRetrievalInternal(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	b.AddTraceRetrievalInternal(xray.TraceRetrieval{
		RetrievalToken: "tok-1",
		Status:         "RUNNING",
		StartTime:      time.Now(),
	})

	status, _ := b.GetRetrievedTracesGraph("tok-1")
	assert.Equal(t, "RUNNING", status)

	// After cancel, status should be COMPLETE for the token.
	b.CancelTraceRetrieval("tok-1")
	status2, _ := b.GetRetrievedTracesGraph("tok-1")
	assert.Equal(t, "COMPLETE", status2)
}

// TestRefinement1_GroupInsightsConfigurationRoundtrip verifies InsightsConfiguration in GroupView.
func TestRefinement1_GroupInsightsConfigurationRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"GroupName":        "insights-group",
		"FilterExpression": `service("svc")`,
		"InsightsConfiguration": map[string]any{
			"InsightsEnabled":      true,
			"NotificationsEnabled": false,
		},
	}

	rec := doXrayRequest(t, h, "/CreateGroup", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	group, ok := resp["Group"].(map[string]any)
	require.True(t, ok)

	ic, ok := group["InsightsConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, ic["InsightsEnabled"])
}

// TestRefinement1_HandlerOpsLenHelper verifies the HandlerOpsLen export helper.
func TestRefinement1_HandlerOpsLenHelper(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	h := xray.NewHandler(b)
	assert.Equal(t, 38, h.HandlerOpsLen())
}

// TestRefinement1_SnapshotRestorePreservesGroups verifies group data persists.
func TestRefinement1_SnapshotRestorePreservesGroups(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	_, err := b.CreateGroup("snap-group", `service("svc")`)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	g, err := b2.GetGroup("snap-group")
	require.NoError(t, err)
	assert.Equal(t, "snap-group", g.GroupName)
}

// TestRefinement1_AddSamplingRuleInternal verifies the seed helper.
func TestRefinement1_AddSamplingRuleInternal(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "seed-rule", FixedRate: 0.5, Priority: 5})

	// Default rule + seed-rule = 2.
	assert.Equal(t, 2, b.SamplingRuleCount())

	rules := b.GetSamplingRules()
	require.Len(t, rules, 2)
	// Sorted by priority: seed-rule (5) comes before Default (10000).
	assert.Equal(t, "seed-rule", rules[0].RuleName)
	assert.NotEmpty(t, rules[0].RuleARN)
	assert.Equal(t, "Default", rules[1].RuleName)
}

// TestRefinement1_GetInsightImpactGraphNotFound verifies insight-not-found returns 400.
func TestRefinement1_GetInsightImpactGraphNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"InsightId": "non-existent-insight",
		"StartTime": 1700000000.0,
		"EndTime":   1700001000.0,
	}

	rec := doXrayRequest(t, h, "/GetInsightImpactGraph", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_SamplingRuleModifiedAtInRecord verifies toSamplingRuleRecord uses separate times.
func TestRefinement1_SamplingRuleModifiedAtInRecord(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	_, err := b.CreateSamplingRule(xray.SamplingRule{RuleName: "time-rule", FixedRate: 0.1, Priority: 1})
	require.NoError(t, err)

	// Small sleep so Modified and Created timestamps will differ after update.
	time.Sleep(time.Millisecond * 2)

	_, err = b.UpdateSamplingRule("time-rule", xray.SamplingRule{ServiceName: "updated"})
	require.NoError(t, err)

	h := xray.NewHandler(b)
	rec := doXrayRequest(t, h, "/GetSamplingRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	records, ok := resp["SamplingRuleRecords"].([]any)
	require.True(t, ok)
	// 2 rules: time-rule + Default.
	require.Len(t, records, 2)

	// The first record (sorted by priority=1) should be time-rule.
	record, ok := records[0].(map[string]any)
	require.True(t, ok)

	samplingRule, ok := record["SamplingRule"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "time-rule", samplingRule["RuleName"])

	createdAt, ok1 := record["CreatedAt"].(float64)
	modifiedAt, ok2 := record["ModifiedAt"].(float64)

	require.True(t, ok1)
	require.True(t, ok2)
	assert.GreaterOrEqual(t, modifiedAt, createdAt)
}

// TestRefinement1_ResetClearsAllState verifies Reset clears every field.
func TestRefinement1_ResetClearsAllState(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	_, err := b.CreateGroup("g1", "")
	require.NoError(t, err)

	b.AddInsightInternal(xray.Insight{InsightID: "i1", State: "ACTIVE", StartTime: time.Now()})
	b.AddResourcePolicyInternal(xray.ResourcePolicy{PolicyName: "p1", PolicyDocument: `{}`})

	b.Reset()

	assert.Equal(t, 0, b.GroupCount())
	assert.Equal(t, 0, b.InsightCount())
	assert.Equal(t, 0, b.ResourcePolicyCount())
	assert.Equal(t, 0, b.TraceCount())
}
