package waf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_GetSampledRequestsReturnsTimeWindow verifies that the
// GetSampledRequests response echoes back the TimeWindow from the request.
// Real AWS always includes TimeWindow in the response; the SDK's
// GetSampledRequestsOutput has it as a required field — callers that access
// output.TimeWindow.StartTime get a nil-pointer panic without it.
//
// TimeWindow.StartTime/EndTime are unixTimestamp shapes on the wire: the
// awsjson1.1 protocol (aws-sdk-go-v2/service/waf's
// serializeDocumentTimeWindow) always sends/expects a JSON number of seconds
// since the epoch, never an ISO8601 string.
func TestParity_GetSampledRequestsReturnsTimeWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startTime float64
		endTime   float64
	}{
		{
			name:      "epoch_window",
			startTime: 1_704_067_200, // 2024-01-01T00:00:00Z
			endTime:   1_704_070_800, // 2024-01-01T01:00:00Z
		},
		{
			name:      "different_window",
			startTime: 1_748_779_200, // 2025-06-01T12:00:00Z
			endTime:   1_748_782_800, // 2025-06-01T13:00:00Z
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			aclID := wafCreateWebACL(t, h, "sampled-acl-"+tt.name)
			ruleID := wafCreateRule(t, h, "sampled-rule-"+tt.name)

			rec := wafDo(t, h, "GetSampledRequests", map[string]any{
				"WebAclId": aclID,
				"RuleId":   ruleID,
				"MaxItems": 100,
				"TimeWindow": map[string]any{
					"StartTime": tt.startTime,
					"EndTime":   tt.endTime,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			tw, ok := resp["TimeWindow"].(map[string]any)
			require.True(t, ok, "TimeWindow must be present in response")
			assert.InDelta(t, tt.startTime, tw["StartTime"], 0,
				"TimeWindow.StartTime must match request")
			assert.InDelta(t, tt.endTime, tw["EndTime"], 0,
				"TimeWindow.EndTime must match request")
		})
	}
}
