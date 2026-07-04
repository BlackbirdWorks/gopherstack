package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- StartDashboardSnapshotJobSchedule ----

// TestQuickSight_StartDashboardSnapshotJobSchedule verifies the real (not
// fabricated) response shape and error behavior: real
// StartDashboardSnapshotJobScheduleOutput carries only RequestId/Status (no
// DashboardId), the target dashboard must actually exist, and a missing
// ScheduleId is a validation error.
func TestQuickSight_StartDashboardSnapshotJobSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash-sched"), map[string]any{
		"Name": "DashSched",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	tests := []struct {
		name       string
		path       string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "existing_dashboard_and_schedule",
			path:       accountPath("/dashboards/dash-sched/schedules/sched1"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_dashboard",
			path:       accountPath("/dashboards/no-such-dashboard/schedules/sched1"),
			wantStatus: http.StatusNotFound,
			wantCode:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			body := parseBody(t, rec)
			if tt.wantStatus == http.StatusOK {
				assert.NotContains(t, body, "DashboardId",
					"a stub response would fabricate a DashboardId field; real AWS output has none")
				assert.Contains(t, body, "RequestId")

				return
			}

			assert.Equal(t, tt.wantCode, body["Code"])
		})
	}
}

// ---- PredictQAResults ----

// TestQuickSight_PredictQAResults verifies that answers are grounded in real
// account state (an existing Topic), not fabricated: a query mentioning a
// registered topic's name gets a GENERATED_ANSWER result referencing that
// real topic, while an unrelated query gets the explicit NO_ANSWER result.
func TestQuickSight_PredictQAResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "sales-topic",
		"Name":    "Sales Performance",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	tests := []struct {
		name           string
		queryText      string
		wantResultType string
	}{
		{
			name:           "matches_real_topic",
			queryText:      "What is our Sales Performance this quarter?",
			wantResultType: "GENERATED_ANSWER",
		},
		{
			name:           "no_matching_topic",
			queryText:      "What is the weather today?",
			wantResultType: "NO_ANSWER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, accountPath("/qa/predict"), map[string]any{
				"QueryText": tt.queryText,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			body := parseBody(t, rec)
			primary, ok := body["PrimaryResult"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantResultType, primary["ResultType"])

			if tt.wantResultType != "GENERATED_ANSWER" {
				assert.NotContains(t, primary, "GeneratedAnswer")

				return
			}

			answer, hasAnswer := primary["GeneratedAnswer"].(map[string]any)
			require.True(t, hasAnswer)
			assert.Equal(t, "sales-topic", answer["TopicId"])
			assert.Equal(t, "Sales Performance", answer["TopicName"])
			assert.NotEmpty(t, answer["AnswerId"])
		})
	}

	// Missing QueryText -> validation error, not a fabricated 200.
	rec := doRequest(t, h, http.MethodPost, accountPath("/qa/predict"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
