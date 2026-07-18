package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccuracy_ModelInvocationJob_StopTransitionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stop Submitted job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			job, err := b.CreateModelInvocationJob("stop-invoc-job", nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost,
				"/model-invocation-job/"+url.PathEscape(job.JobArn)+"/stop", nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			recGet := doRequest(t, h, http.MethodGet,
				"/model-invocation-job/"+url.PathEscape(job.JobArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, "Stopped", out["status"])
		})
	}
}

func TestAccuracy_ModelInvocationJob_ListViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobNames []string
		wantLen  int
	}{
		{name: "empty", jobNames: nil, wantLen: 0},
		{name: "two jobs", jobNames: []string{"invoc-a", "invoc-b"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.jobNames {
				_, err := b.CreateModelInvocationJob(name, nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/model-invocation-jobs", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["invocationJobSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)
		})
	}
}

func TestAccuracy_ModelInvocationJob_JobNamePreserved(t *testing.T) {
	t.Parallel()

	tests := []string{"batch-run-1", "eval-batch-2026", "my-bulk-inference"}

	for _, jobName := range tests {
		t.Run(jobName, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model-invocation-job",
				map[string]any{"jobName": jobName})
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jobArn := out["jobArn"].(string)

			getRec := doRequest(t, h, http.MethodGet, "/model-invocation-job/"+jobArn, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
			assert.Equal(t, jobName, getOut["jobName"])
		})
	}
}

func TestAccuracy_ModelInvocationJob_StatusProgression(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-job",
		map[string]any{"jobName": "status-test-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/model-invocation-job/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Contains(t, []any{"Submitted", "InProgress", "Completed", "Failed", "Stopped"},
		getOut["status"], "status should be a valid model invocation job status")
}

func TestAccuracy_ModelInvocationJob_StopChangesStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-job",
		map[string]any{"jobName": "stoppable-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	stopRec := doRequest(t, h, http.MethodPost, "/model-invocation-job/"+jobArn+"/stop", nil)
	require.Equal(t, http.StatusNoContent, stopRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/model-invocation-job/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "Stopped", getOut["status"])
}

func TestHandler_ModelInvocationJob_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-job", map[string]any{"jobName": "inv-job-1"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)
	assert.NotEmpty(t, jobARN)

	// Get
	rec2 := doRequest(t, h, http.MethodGet, "/model-invocation-job/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, jobARN, out["jobArn"])
	assert.Equal(t, "inv-job-1", out["jobName"])

	// List
	rec3 := doRequest(t, h, http.MethodGet, "/model-invocation-jobs", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listOut map[string]any
	mustUnmarshal(t, rec3, &listOut)
	assert.Len(t, listOut["invocationJobSummaries"], 1)

	// Stop
	rec4 := doRequest(t, h, http.MethodPost, "/model-invocation-job/"+url.PathEscape(jobARN)+"/stop", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Verify stopped
	rec5 := doRequest(t, h, http.MethodGet, "/model-invocation-job/"+url.PathEscape(jobARN), nil)
	var stopped map[string]any
	mustUnmarshal(t, rec5, &stopped)
	assert.Equal(t, "Stopped", stopped[keyStatus])
}

func TestHandler_ModelInvocationJob_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-job", map[string]any{"jobName": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ModelInvocationJob_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/model-invocation-job", map[string]any{"jobName": "dup-job"})
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-job", map[string]any{"jobName": "dup-job"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestBatch2Ops_StopModelInvocationJob_AlreadyStopped_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mij-double-stop"},
		{name: "mij-stopped-stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model-invocation-job",
				map[string]any{"jobName": tc.name})
			require.Equal(t, http.StatusCreated, rec.Code)

			var created map[string]any
			mustUnmarshal(t, rec, &created)
			jobARN := created["jobArn"].(string)

			// First stop must succeed (InProgress → Stopped).
			rec2 := doRequest(t, h, http.MethodPost, "/model-invocation-job/"+url.PathEscape(jobARN)+"/stop", nil)
			assert.Equal(t, http.StatusNoContent, rec2.Code, "first stop should return 204")

			// Second stop must fail (Stopped is not stoppable).
			rec3 := doRequest(t, h, http.MethodPost, "/model-invocation-job/"+url.PathEscape(jobARN)+"/stop", nil)
			assert.Equal(t, http.StatusBadRequest, rec3.Code,
				"stop of already-stopped invocation job should return 400 ValidationException")

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &errBody))
			assert.Equal(t, "ValidationException", errBody["__type"])
		})
	}
}

func TestBatch2Ops_StopModelInvocationJob_InProgress_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-job",
		map[string]any{"jobName": "mij-stop-ok"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/model-invocation-job/"+url.PathEscape(jobARN)+"/stop", nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify status is Stopped.
	rec3 := doRequest(t, h, http.MethodGet, "/model-invocation-job/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var out map[string]any
	mustUnmarshal(t, rec3, &out)
	assert.Equal(t, "Stopped", out["status"])
}
