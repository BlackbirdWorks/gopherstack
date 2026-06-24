package xray_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestGroupARNUsesBackendRegion verifies that group ARNs contain the region
// and account ID passed to NewInMemoryBackend, not config.DefaultRegion.
func TestGroupARNUsesBackendRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		groupName string
	}{
		{
			name:      "custom region and account",
			accountID: "123456789012",
			region:    "eu-west-1",
			groupName: "my-group",
		},
		{
			name:      "us-west-2 region",
			accountID: "999999999999",
			region:    "us-west-2",
			groupName: "another-group",
		},
		{
			name:      "default test values",
			accountID: "000000000000",
			region:    "us-east-1",
			groupName: "default",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend(tc.accountID, tc.region)
			g, err := b.CreateGroup(tc.groupName, "")
			require.NoError(t, err)

			assert.Contains(t, g.GroupARN, tc.region, "ARN should contain the configured region")
			assert.Contains(t, g.GroupARN, tc.accountID, "ARN should contain the configured account ID")
			assert.True(t, strings.HasPrefix(g.GroupARN, "arn:aws:xray:"), "ARN should start with arn:aws:xray:")
		})
	}
}

// TestSamplingRuleARNUsesBackendRegion verifies that sampling rule ARNs contain the
// region and account ID passed to NewInMemoryBackend.
func TestSamplingRuleARNUsesBackendRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		ruleName  string
	}{
		{
			name:      "custom region",
			accountID: "123456789012",
			region:    "ap-southeast-1",
			ruleName:  "my-rule",
		},
		{
			name:      "another region",
			accountID: "000000000000",
			region:    "ca-central-1",
			ruleName:  "test-rule",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend(tc.accountID, tc.region)
			rule := xray.SamplingRule{
				RuleName:      tc.ruleName,
				ResourceARN:   "*",
				ServiceName:   "*",
				ServiceType:   "*",
				Host:          "*",
				HTTPMethod:    "*",
				URLPath:       "*",
				FixedRate:     0.05,
				Priority:      100,
				ReservoirSize: 1,
			}
			created, err := b.CreateSamplingRule(rule)
			require.NoError(t, err)

			assert.Contains(t, created.RuleARN, tc.region, "rule ARN should contain the configured region")
			assert.Contains(t, created.RuleARN, tc.accountID, "rule ARN should contain the configured account ID")
			assert.True(
				t,
				strings.HasPrefix(created.RuleARN, "arn:aws:xray:"),
				"rule ARN should start with arn:aws:xray:",
			)
		})
	}
}

// TestGetGroupByARN_UsesIndex verifies that GetGroupByARN works correctly
// when groups are created with custom regions and uses the O(1) ARN index.
func TestGetGroupByARN_UsesIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		groupName string
		wantFound bool
	}{
		{
			name:      "found by ARN",
			accountID: "123456789012",
			region:    "eu-central-1",
			groupName: "test-group",
			wantFound: true,
		},
		{
			name:      "not found returns error",
			accountID: "123456789012",
			region:    "eu-central-1",
			groupName: "", // will try to look up a nonexistent ARN
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend(tc.accountID, tc.region)

			if tc.groupName != "" {
				g, err := b.CreateGroup(tc.groupName, "service(\"MyService\")")
				require.NoError(t, err)

				// Look up by the ARN that was returned from Create.
				found, err := b.GetGroupByARN(g.GroupARN)
				require.NoError(t, err)
				assert.Equal(t, g.GroupARN, found.GroupARN)
				assert.Equal(t, tc.groupName, found.GroupName)
			} else {
				_, err := b.GetGroupByARN("arn:aws:xray:eu-central-1:123456789012:group/default/nonexistent")
				assert.Error(t, err)
			}
		})
	}
}

// buildFaultSegmentJSON returns a raw segment JSON string for a named service.
func buildFaultSegmentJSON(t *testing.T, traceID, name string, fault bool) string {
	t.Helper()

	seg := map[string]any{
		"trace_id":   traceID,
		"id":         "seg-" + name,
		"name":       name,
		"start_time": 1.0,
		"end_time":   1.1,
		"fault":      fault,
	}

	data, err := json.Marshal(seg)
	require.NoError(t, err)

	return string(data)
}

// TestInsightDetection_FaultRateTriggers verifies that ingesting enough fault
// segments for a service creates an ACTIVE insight automatically.
func TestInsightDetection_FaultRateTriggers(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	const traceID = "1-fault-trace-0000000000000001"
	const svcName = "fault-service"

	// Send 20 segments: 10 fault, 10 ok — 50% fault rate, above 5% threshold.
	segs := make([]string, 0, 20)
	for range 10 {
		segs = append(segs, buildFaultSegmentJSON(t, traceID, svcName, true))
	}
	for range 10 {
		segs = append(segs, buildFaultSegmentJSON(t, traceID, svcName, false))
	}

	unprocessed := b.PutTraceSegments(segs)
	assert.Empty(t, unprocessed, "all segments should be processed")

	// An insight should have been created.
	assert.Positive(t, b.InsightCount(), "expected at least one insight to be auto-detected")

	summaries, err := b.GetInsightSummaries([]string{"ACTIVE"})
	require.NoError(t, err)
	assert.NotEmpty(t, summaries, "expected active insight summaries")
	assert.Equal(t, "ACTIVE", summaries[0].State)
}

// TestInsightDetection_NoInsightBelowThreshold verifies that no insight is
// created when the fault rate is below the threshold.
func TestInsightDetection_NoInsightBelowThreshold(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	const traceID = "1-ok-trace-00000000000000001"
	const svcName = "healthy-service"

	// 20 ok segments — 0% fault rate.
	segs := make([]string, 0, 20)
	for range 20 {
		segs = append(segs, buildFaultSegmentJSON(t, traceID, svcName, false))
	}

	unprocessed := b.PutTraceSegments(segs)
	assert.Empty(t, unprocessed)

	// No insights should be created.
	assert.Equal(t, 0, b.InsightCount(), "expected no insights for healthy service")
}

// TestRetrievalCleanup_JanitorSweepsOldTokens verifies that the janitor removes
// retrieval tokens that have exceeded the trace TTL.
func TestRetrievalCleanup_JanitorSweepsOldTokens(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	// Add a trace and start a retrieval.
	traceID := b.PutTraceForTest(time.Now().Add(-2 * time.Hour))
	token := b.StartTraceRetrieval([]string{traceID})

	// Back-date the retrieval token creation time so it appears old.
	b.SetRetrievalTimeForTest(token, time.Now().Add(-2*time.Hour))

	// Verify retrieval exists before sweep.
	status, traces := b.ListRetrievedTraces(token)
	assert.Equal(t, "COMPLETE", status)
	assert.NotNil(t, traces)

	// Run janitor with a 30-minute TTL — token is 2h old so it gets swept.
	j := xray.NewJanitor(b, time.Minute, 30*time.Minute)
	j.SweepOnce(context.Background())

	// After sweep, token should be gone (returns COMPLETE with nil).
	status2, traces2 := b.ListRetrievedTraces(token)
	assert.Equal(t, "COMPLETE", status2)
	assert.Nil(t, traces2, "retrieval state should have been cleaned up by janitor")
}

// TestListRetrievedTraces_IncludesSegments verifies that ListRetrievedTraces
// returns trace views with non-empty Segments when segment data exists.
func TestListRetrievedTraces_IncludesSegments(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	h := xray.NewHandler(b)

	const traceID = "1-seg-trace-000000000000001"

	// Put a segment into the backend.
	seg := map[string]any{
		"trace_id":   traceID,
		"id":         "seg-abc123",
		"name":       "my-service",
		"start_time": 1700000000.0,
		"end_time":   1700000001.5,
	}
	segJSON, err := json.Marshal(seg)
	require.NoError(t, err)

	unprocessed := b.PutTraceSegments([]string{string(segJSON)})
	assert.Empty(t, unprocessed)

	// Start retrieval and list.
	startResp := doXrayRequest(t, h, "/StartTraceRetrieval", map[string]any{"TraceIds": []string{traceID}})
	require.Equal(t, 200, startResp.Code)

	var startResult map[string]any
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startResult))

	token, ok := startResult["RetrievalToken"].(string)
	require.True(t, ok, "expected RetrievalToken in response")

	listResp := doXrayRequest(t, h, "/ListRetrievedTraces", map[string]any{"RetrievalToken": token})
	require.Equal(t, 200, listResp.Code)

	var listResult map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listResult))

	traces, ok := listResult["Traces"].([]any)
	require.True(t, ok, "expected Traces array in response")
	require.NotEmpty(t, traces, "expected at least one trace")

	firstTrace, ok := traces[0].(map[string]any)
	require.True(t, ok)

	segments, ok := firstTrace["Segments"].([]any)
	require.True(t, ok, "expected Segments field")
	assert.NotEmpty(t, segments, "expected non-empty Segments in trace view")

	// Duration should be computed from segment timing.
	duration, ok := firstTrace["Duration"].(float64)
	assert.True(t, ok)
	assert.Greater(t, duration, 0.0, "expected non-zero Duration")
}
