package comprehend_test

// Audit batch-2: AWS-accuracy coverage for Comprehend ops layer (go-8nom).
//
// Covers:
//   - StopJob terminal-state guard: COMPLETED/FAILED/STOPPED jobs reject Stop
//     with InvalidRequestException (400).
//   - Classifier/recognizer training lifecycle: the emulator skips async
//     training so classifiers/recognizers start immediately at TRAINED,
//     allowing the Terraform provider waiter to exit without a long delay.
//   - Delete training-resource: classifiers/recognizers start TRAINED so they
//     can be deleted immediately (no SUBMITTED/IN_PROGRESS state guard needed).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// --- helpers ---

func b2Handler(t *testing.T) *comprehend.Handler {
	t.Helper()

	return comprehend.NewHandler(comprehend.NewInMemoryBackend("000000000000", "us-east-1"))
}

func b2Do(t *testing.T, h *comprehend.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "Comprehend_20171127."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(ctx))

	return rec
}

func b2Unmarshal(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// --- StopJob terminal-state guard ---

func TestBatch2_StopJob_TerminalStateGuard(t *testing.T) {
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

			h := b2Handler(t)

			// Start a job.
			startRec := b2Do(t, h, "Start"+tc.jobOp+"Job",
				`{"JobName":"terminal-guard","LanguageCode":"en"}`)
			require.Equal(t, http.StatusOK, startRec.Code)
			jobID := b2Unmarshal(t, startRec)["JobId"].(string)

			// Advance job to the desired terminal state.
			describeAction := "Describe" + tc.jobOp + "Job"
			if tc.advanceTo == "STOPPED" {
				// Advance to IN_PROGRESS first.
				b2Do(t, h, describeAction, `{"JobId":"`+jobID+`"}`)
				// Stop while IN_PROGRESS.
				stopRec := b2Do(t, h, "Stop"+tc.jobOp+"Job", `{"JobId":"`+jobID+`"}`)
				require.Equal(t, http.StatusOK, stopRec.Code)
				// Advance STOP_REQUESTED → STOPPED.
				b2Do(t, h, describeAction, `{"JobId":"`+jobID+`"}`)
			} else {
				// Advance SUBMITTED → IN_PROGRESS → COMPLETED.
				b2Do(t, h, describeAction, `{"JobId":"`+jobID+`"}`)
				b2Do(t, h, describeAction, `{"JobId":"`+jobID+`"}`)
			}

			// Verify desired status reached.
			checkRec := b2Do(t, h, describeAction, `{"JobId":"`+jobID+`"}`)
			props := b2Unmarshal(t, checkRec)[tc.jobOp+"JobProperties"].(map[string]any)
			assert.Equal(t, tc.advanceTo, props["JobStatus"])

			// Stop on terminal state must fail with InvalidRequestException.
			rec := b2Do(t, h, tc.stopAction, `{"JobId":"`+jobID+`"}`)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			resp := b2Unmarshal(t, rec)
			assert.Equal(t, "InvalidRequestException", resp["__type"])
		})
	}
}

// --- Classifier/recognizer training lifecycle ---

func TestBatch2_TrainingResourceLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createAction string
		descAction   string
		arnField     string
		propsField   string
		createBody   string
	}{
		{
			name:         "document_classifier",
			createAction: "CreateDocumentClassifier",
			descAction:   "DescribeDocumentClassifier",
			arnField:     "DocumentClassifierArn",
			propsField:   "DocumentClassifierProperties",
			createBody:   `{"DocumentClassifierName":"lifecycle-clf","LanguageCode":"en"}`,
		},
		{
			name:         "entity_recognizer",
			createAction: "CreateEntityRecognizer",
			descAction:   "DescribeEntityRecognizer",
			arnField:     "EntityRecognizerArn",
			propsField:   "EntityRecognizerProperties",
			createBody:   `{"RecognizerName":"lifecycle-rec","LanguageCode":"en"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)

			// Create → immediately TRAINED (emulator skips async training).
			createRec := b2Do(t, h, tc.createAction, tc.createBody)
			require.Equal(t, http.StatusOK, createRec.Code)
			arn := b2Unmarshal(t, createRec)[tc.arnField].(string)
			require.NotEmpty(t, arn)

			// First Describe → still TRAINED (no lifecycle advancement needed).
			desc1 := b2Do(t, h, tc.descAction, `{"`+tc.arnField+`":"`+arn+`"}`)
			require.Equal(t, http.StatusOK, desc1.Code)
			props1 := b2Unmarshal(t, desc1)[tc.propsField].(map[string]any)
			assert.Equal(t, "TRAINED", props1["Status"])

			// Second Describe → still TRAINED.
			desc2 := b2Do(t, h, tc.descAction, `{"`+tc.arnField+`":"`+arn+`"}`)
			require.Equal(t, http.StatusOK, desc2.Code)
			props2 := b2Unmarshal(t, desc2)[tc.propsField].(map[string]any)
			assert.Equal(t, "TRAINED", props2["Status"])
		})
	}
}

// --- Delete training-resource: immediate deletion allowed ---

func TestBatch2_DeleteTrainingResource_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createAction string
		deleteAction string
		arnField     string
		createBody   string
	}{
		{
			name:         "delete_classifier_immediately",
			createAction: "CreateDocumentClassifier",
			deleteAction: "DeleteDocumentClassifier",
			arnField:     "DocumentClassifierArn",
			createBody:   `{"DocumentClassifierName":"del-guard-clf","LanguageCode":"en"}`,
		},
		{
			name:         "delete_recognizer_immediately",
			createAction: "CreateEntityRecognizer",
			deleteAction: "DeleteEntityRecognizer",
			arnField:     "EntityRecognizerArn",
			createBody:   `{"RecognizerName":"del-guard-rec","LanguageCode":"en"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)

			// Create resource — immediately TRAINED, so delete succeeds right away.
			createRec := b2Do(t, h, tc.createAction, tc.createBody)
			require.Equal(t, http.StatusOK, createRec.Code)
			arn := b2Unmarshal(t, createRec)[tc.arnField].(string)

			okRec := b2Do(t, h, tc.deleteAction, `{"`+tc.arnField+`":"`+arn+`"}`)
			assert.Equal(t, http.StatusOK, okRec.Code)
		})
	}
}
