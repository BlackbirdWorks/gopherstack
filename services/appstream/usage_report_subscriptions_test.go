package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppStream_UsageReports covers UsageReportSubscription lifecycle.
func TestAppStream_UsageReports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateUsageReportSubscription returns subscription",
			action:   "CreateUsageReportSubscription",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.Equal(t, "DAILY", resp["Schedule"])
				assert.NotEmpty(t, resp["S3BucketName"])
			},
		},
		{
			name:   "DescribeUsageReportSubscriptions returns subscription",
			action: "DescribeUsageReportSubscriptions",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateUsageReportSubscription", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				subs := resp["UsageReportSubscriptions"].([]any)
				assert.Len(t, subs, 1)
			},
		},
		{
			name:   "DeleteUsageReportSubscription removes it",
			action: "DeleteUsageReportSubscription",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateUsageReportSubscription", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteUsageReportSubscription when none returns error",
			action:   "DeleteUsageReportSubscription",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_UsageReportSubscriptionRoundtrip verifies usage report
// subscription lifecycle. CreateUsageReportSubscriptionInput takes no
// parameters on real AWS (aws-sdk-go-v2 appstream@v1.64.5
// api_op_CreateUsageReportSubscription.go) -- the schedule and S3 bucket are
// both derived server-side, not supplied by the client.
func TestAppStream_UsageReportSubscriptionRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateUsageReportSubscription", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeUsageReportSubscriptions", map[string]any{})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	subs := resp["UsageReportSubscriptions"].([]any)
	require.Len(t, subs, 1)
	sub := subs[0].(map[string]any)
	assert.Equal(t, "DAILY", sub["Schedule"])
	assert.NotEmpty(t, sub["S3BucketName"])
}
