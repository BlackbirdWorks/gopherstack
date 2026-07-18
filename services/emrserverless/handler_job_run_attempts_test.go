package emrserverless_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// --- ListJobRunAttempts ---

func TestHandler_ListJobRunAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) (appID, jobRunID string)
		name       string
		query      string
		wantStatus int
		wantCount  int
		wantAppID  bool
	}{
		{
			name:       "success_returns_single_attempt",
			wantStatus: http.StatusOK,
			wantCount:  1,
			wantAppID:  true,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "attempts-app")
				jobRunID := startJobRun(t, h, appID)

				return appID, jobRunID
			},
		},
		{
			name:       "app_not_found",
			wantStatus: http.StatusNotFound,
			wantCount:  0,
			setup: func(_ *emrserverless.Handler) (string, string) {
				return "nonexistent-app", "nonexistent-run"
			},
		},
		{
			name:       "job_run_not_found",
			wantStatus: http.StatusNotFound,
			wantCount:  0,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "attempts-app-2")

				return appID, "nonexistent-run"
			},
		},
		{
			name:       "pagination_max_results",
			query:      "?maxResults=1",
			wantStatus: http.StatusOK,
			wantCount:  1,
			wantAppID:  true,
			setup: func(h *emrserverless.Handler) (string, string) {
				appID := createApp(t, h, "attempts-app-3")
				jobRunID := startJobRun(t, h, appID)

				return appID, jobRunID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			appID, jobRunID := tt.setup(h)

			rec := doRequest(t, h, http.MethodGet,
				"/applications/"+appID+"/jobruns/"+jobRunID+"/attempts"+tt.query, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				attempts, ok := out["jobRunAttempts"].([]any)
				require.True(t, ok, "jobRunAttempts should be an array")
				assert.Len(t, attempts, tt.wantCount)

				if tt.wantAppID && len(attempts) > 0 {
					attempt := attempts[0].(map[string]any)
					assert.Equal(t, appID, attempt["applicationId"])
					assert.Equal(t, jobRunID, attempt["id"])
					assert.NotEmpty(t, attempt["state"])
				}
			}
		})
	}
}

// TestHandler_ListJobRunAttempts_WithNextToken verifies the nextToken path
// when maxResults is 0 (returns all).
func TestHandler_ListJobRunAttempts_WithNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "attempts-nexttoken-app")
	jobRunID := startJobRun(t, h, appID)

	rec := doRequest(t, h, http.MethodGet,
		"/applications/"+appID+"/jobruns/"+jobRunID+"/attempts?nextToken=0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	attempts, ok := out["jobRunAttempts"].([]any)
	require.True(t, ok)
	assert.Len(t, attempts, 1)
}
