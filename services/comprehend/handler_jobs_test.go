package comprehend_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Async job lifecycle field shapes ---

func TestAsyncJobFieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefix      string
		objectField string
	}{
		{prefix: "SentimentDetectionJob", objectField: "SentimentDetectionJobProperties"},
		{prefix: "EntitiesDetectionJob", objectField: "EntitiesDetectionJobProperties"},
		{prefix: "TopicsDetectionJob", objectField: "TopicsDetectionJobProperties"},
	}

	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			startResp := request(t, h, "Start"+tt.prefix, map[string]any{"JobName": "audit-job", "LanguageCode": "en"})
			assert.NotEmpty(t, startResp["JobId"], "start response must have JobId")
			assert.NotEmpty(t, startResp["JobArn"], "start response must have JobArn")
			assert.Equal(t, "SUBMITTED", startResp["JobStatus"], "initial status must be SUBMITTED")

			jobID := startResp["JobId"].(string)
			descResp := request(t, h, "Describe"+tt.prefix, map[string]any{"JobId": jobID})
			props, ok := descResp[tt.objectField].(map[string]any)
			require.True(t, ok, "describe response must have %s key", tt.objectField)
			assert.NotEmpty(t, props["JobId"], "job properties must have JobId")
			assert.NotEmpty(t, props["JobArn"], "job properties must have JobArn")
			assert.NotEmpty(t, props["JobName"], "job properties must have JobName")
			assert.NotEmpty(t, props["JobStatus"], "job properties must have JobStatus")
			assert.NotEmpty(t, props["SubmitTime"], "job properties must have SubmitTime")
		})
	}
}

func TestAsyncJobStopLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()
	startResp := request(t, h, "StartSentimentDetectionJob", map[string]any{"JobName": "stop-me", "LanguageCode": "en"})
	jobID := startResp["JobId"].(string)

	stopResp := request(t, h, "StopSentimentDetectionJob", map[string]any{"JobId": jobID})
	assert.Equal(t, jobID, stopResp["JobId"])
	assert.Equal(t, "STOP_REQUESTED", stopResp["JobStatus"])

	descResp := request(t, h, "DescribeSentimentDetectionJob", map[string]any{"JobId": jobID})
	props := descResp["SentimentDetectionJobProperties"].(map[string]any)
	assert.Equal(t, "STOPPED", props["JobStatus"])
	assert.NotEmpty(t, props["EndTime"], "stopped job must have EndTime")
}

// --- StopJob terminal-state guard ---

func TestStopJobTerminalStateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobOp      string // Start/Stop operation prefix
		advanceTo  string // target job status before the bad Stop call
		stopAction string
	}{
		{
			name:       "stop_completed_sentiment_job",
			jobOp:      "SentimentDetection",
			advanceTo:  "COMPLETED",
			stopAction: "StopSentimentDetectionJob",
		},
		{
			name:       "stop_completed_entities_job",
			jobOp:      "EntitiesDetection",
			advanceTo:  "COMPLETED",
			stopAction: "StopEntitiesDetectionJob",
		},
		{
			name:       "stop_already_stopped_job",
			jobOp:      "KeyPhrasesDetection",
			advanceTo:  "STOPPED",
			stopAction: "StopKeyPhrasesDetectionJob",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			started := request(t, h, "Start"+tc.jobOp+"Job", map[string]any{"JobName": "terminal-guard"})
			jobID := started["JobId"].(string)

			describeAction := "Describe" + tc.jobOp + "Job"
			if tc.advanceTo == "STOPPED" {
				// Advance to IN_PROGRESS first.
				request(t, h, describeAction, map[string]any{"JobId": jobID})
				// Stop while IN_PROGRESS.
				stopResp := request(t, h, "Stop"+tc.jobOp+"Job", map[string]any{"JobId": jobID})
				assert.Equal(t, "STOP_REQUESTED", stopResp["JobStatus"])
				// Advance STOP_REQUESTED → STOPPED.
				request(t, h, describeAction, map[string]any{"JobId": jobID})
			} else {
				// Advance SUBMITTED → IN_PROGRESS → COMPLETED.
				request(t, h, describeAction, map[string]any{"JobId": jobID})
				request(t, h, describeAction, map[string]any{"JobId": jobID})
			}

			// Verify desired status reached.
			checkResp := request(t, h, describeAction, map[string]any{"JobId": jobID})
			props := checkResp[tc.jobOp+"JobProperties"].(map[string]any)
			assert.Equal(t, tc.advanceTo, props["JobStatus"])

			// Stop on terminal state must fail with InvalidRequestException.
			rec := rawRequest(t, h, tc.stopAction, `{"JobId":"`+jobID+`"}`)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			resp := decodeBody(t, rec)
			assert.Equal(t, "InvalidRequestException", resp["__type"])
		})
	}
}
