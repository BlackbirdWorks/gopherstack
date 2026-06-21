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
func TestParity_GetSampledRequestsReturnsTimeWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startTime string
		endTime   string
		name      string
	}{
		{
			name:      "iso8601_window",
			startTime: "2024-01-01T00:00:00Z",
			endTime:   "2024-01-01T01:00:00Z",
		},
		{
			name:      "different_window",
			startTime: "2025-06-01T12:00:00Z",
			endTime:   "2025-06-01T13:00:00Z",
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
			assert.Equal(t, tt.startTime, tw["StartTime"],
				"TimeWindow.StartTime must match request")
			assert.Equal(t, tt.endTime, tw["EndTime"],
				"TimeWindow.EndTime must match request")
		})
	}
}
