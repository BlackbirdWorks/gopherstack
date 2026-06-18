package bedrock_test

// handler_ops_batch2_audit_test.go — batch-2 ops AWS-accuracy audit for services/bedrock (go-gm4e).
// Covers three gaps discovered in the ops layer:
//
//  1. StopEvaluationJob missing state guard — AWS rejects stop requests for
//     jobs not in InProgress state (Stopped, Completed, Failed) with ValidationException.
//
//  2. StopModelInvocationJob missing state guard — same constraint: AWS only
//     allows stopping InProgress invocation jobs.
//
//  3. BatchDeleteEvaluationJob accepts InProgress jobs — AWS requires jobs to
//     be in a terminal state (Stopped, Completed, Failed) before deletion;
//     InProgress jobs must be reported in the errors array, not deleted.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── 1. StopEvaluationJob state guard ─────────────────────────────────────────

func TestBatch2Ops_StopEvaluationJob_AlreadyStopped_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "second_stop_fails"},
		{name: "rapid_double_stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": tc.name})
			require.Equal(t, http.StatusCreated, rec.Code)

			var created map[string]any
			mustUnmarshal(t, rec, &created)
			jobARN := created["jobArn"].(string)

			// First stop: must succeed.
			rec2 := doRequest(t, h, http.MethodDelete, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
			assert.Equal(t, http.StatusOK, rec2.Code, "first stop should succeed")

			// Second stop: must fail — job is now Stopped.
			rec3 := doRequest(t, h, http.MethodDelete, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
			assert.Equal(t, http.StatusBadRequest, rec3.Code,
				"stop of already-stopped job should return 400 ValidationException")

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &errBody))
			assert.Equal(t, "ValidationException", errBody["__type"],
				"error type must be ValidationException")
		})
	}
}

func TestBatch2Ops_StopEvaluationJob_InProgress_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "stop-inprogress"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	// Job starts InProgress — stop must succeed.
	rec2 := doRequest(t, h, http.MethodDelete, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Verify status is now Stopped.
	rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var out map[string]any
	mustUnmarshal(t, rec3, &out)
	assert.Equal(t, "Stopped", out["status"])
}

// ── 2. StopModelInvocationJob state guard ────────────────────────────────────

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
			rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs",
				map[string]any{"jobName": tc.name})
			require.Equal(t, http.StatusCreated, rec.Code)

			var created map[string]any
			mustUnmarshal(t, rec, &created)
			jobARN := created["jobArn"].(string)

			// First stop must succeed (InProgress → Stopped).
			rec2 := doRequest(t, h, http.MethodDelete, "/model-invocation-jobs/"+url.PathEscape(jobARN), nil)
			assert.Equal(t, http.StatusNoContent, rec2.Code, "first stop should return 204")

			// Second stop must fail (Stopped is not stoppable).
			rec3 := doRequest(t, h, http.MethodDelete, "/model-invocation-jobs/"+url.PathEscape(jobARN), nil)
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
	rec := doRequest(t, h, http.MethodPost, "/model-invocation-jobs",
		map[string]any{"jobName": "mij-stop-ok"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	rec2 := doRequest(t, h, http.MethodDelete, "/model-invocation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify status is Stopped.
	rec3 := doRequest(t, h, http.MethodGet, "/model-invocation-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var out map[string]any
	mustUnmarshal(t, rec3, &out)
	assert.Equal(t, "Stopped", out["status"])
}

// ── 3. BatchDeleteEvaluationJob rejects InProgress jobs ──────────────────────

func TestBatch2Ops_BatchDeleteEvaluationJob_InProgress_ReportedAsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "single-inprogress-job"},
		{name: "inprogress-not-deleted"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
				map[string]any{"jobName": tc.name})
			require.Equal(t, http.StatusCreated, rec.Code)

			var created map[string]any
			mustUnmarshal(t, rec, &created)
			jobARN := created["jobArn"].(string)

			// BatchDelete an InProgress job — must appear in errors, not in deleted list.
			rec2 := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete",
				map[string]any{"jobIdentifiers": []string{jobARN}})
			require.Equal(t, http.StatusOK, rec2.Code)

			var out map[string]any
			mustUnmarshal(t, rec2, &out)

			errors, _ := out["errors"].([]any)
			deleted, _ := out["evaluationJobs"].([]any)

			assert.Len(t, errors, 1,
				"InProgress job must appear in errors array")
			assert.Empty(t, deleted,
				"InProgress job must NOT appear in deleted list")

			errEntry, _ := errors[0].(map[string]any)
			assert.Equal(t, "ValidationException", errEntry["code"])
			assert.Equal(t, jobARN, errEntry["jobIdentifier"])

			// Verify the job still exists (was not deleted).
			rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
			assert.Equal(t, http.StatusOK, rec3.Code, "InProgress job must still exist after rejected batch-delete")
		})
	}
}

func TestBatch2Ops_BatchDeleteEvaluationJob_StoppedJob_Deleted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create then stop (→ terminal state) before batch-delete.
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
		map[string]any{"jobName": "batch-del-stopped"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	// Stop it first.
	doRequest(t, h, http.MethodDelete, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)

	// BatchDelete the stopped job — must succeed.
	rec2 := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete",
		map[string]any{"jobIdentifiers": []string{jobARN}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)

	errors, _ := out["errors"].([]any)
	deleted, _ := out["evaluationJobs"].([]any)

	assert.Empty(t, errors, "stopped job should have no errors")
	assert.Len(t, deleted, 1, "stopped job should appear in deleted list")

	// Verify it's gone.
	rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestBatch2Ops_BatchDeleteEvaluationJob_MixedStates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two jobs: one will be stopped (terminal), one stays InProgress.
	recA := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
		map[string]any{"jobName": "batch-mix-stopped"})
	require.Equal(t, http.StatusCreated, recA.Code)

	recB := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
		map[string]any{"jobName": "batch-mix-inprogress"})
	require.Equal(t, http.StatusCreated, recB.Code)

	var outA, outB map[string]any
	mustUnmarshal(t, recA, &outA)
	mustUnmarshal(t, recB, &outB)

	arnA := outA["jobArn"].(string)
	arnB := outB["jobArn"].(string)

	// Stop job A.
	doRequest(t, h, http.MethodDelete, "/evaluation-jobs/"+url.PathEscape(arnA), nil)

	// BatchDelete both: A (stopped) should succeed, B (InProgress) should error.
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete",
		map[string]any{"jobIdentifiers": []string{arnA, arnB}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	errors, _ := out["errors"].([]any)
	deleted, _ := out["evaluationJobs"].([]any)

	assert.Len(t, deleted, 1, "one stopped job should be deleted")
	assert.Len(t, errors, 1, "one InProgress job should appear in errors")

	// Confirm arnB is the error entry.
	errEntry, _ := errors[0].(map[string]any)
	assert.Equal(t, arnB, errEntry["jobIdentifier"])
	assert.Equal(t, "ValidationException", errEntry["code"])
}
