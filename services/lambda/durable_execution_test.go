package lambda_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch1_DurableExecution_CheckpointCreatesExecution(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Before checkpoint: GET returns 404
	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Checkpoint creates the execution
	rec = callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{"marker":"step1"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// After checkpoint: GET returns 200
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), durableExecARN)
	assert.Contains(t, rec.Body.String(), "RUNNING")
}

func TestBatch1_DurableExecution_GetHistory(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint twice to build history
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/history"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Events")
	assert.Contains(t, rec.Body.String(), "Checkpoint")
}

func TestBatch1_DurableExecution_GetHistoryEmpty(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// ARN that was never touched
	noneARN := "arn:aws:lambda:us-east-1:000000000000:durable:none"
	path := "/2025-12-01/durable-executions/" + url.PathEscape(noneARN) + "/history"
	rec := callInMemoryHandler(t, h, http.MethodGet, path, "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Events")
}

func TestBatch1_DurableExecution_GetState(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint creates the execution
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/state"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), durableExecARN)
	assert.Contains(t, rec.Body.String(), "Status")
}

func TestBatch1_DurableExecution_GetStateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	nopeARN := "arn:aws:lambda:us-east-1:000000000000:durable:nope"
	path := "/2025-12-01/durable-executions/" + url.PathEscape(nopeARN) + "/state"
	rec := callInMemoryHandler(t, h, http.MethodGet, path, "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_DurableExecution_Stop(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint to create execution
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	// Stop
	rec := callInMemoryHandler(t, h, http.MethodDelete, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "STOPPED")

	// Get reflects stopped status
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "STOPPED")
}

func TestBatch1_DurableExecution_StopNonExistent(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	path := "/2025-12-01/durable-executions/" + url.PathEscape("arn:aws:lambda:us-east-1:000000000000:durable:none")
	rec := callInMemoryHandler(t, h, http.MethodDelete, path, "{}")
	// Stop on non-existent returns 200 (idempotent)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "STOPPED")
}

func TestBatch1_DurableExecution_CallbackSuccess(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint to create execution
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	// Send success callback
	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/callback/success"), `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Execution now shows SUCCEEDED
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Contains(t, rec.Body.String(), "SUCCEEDED")
}

func TestBatch1_DurableExecution_CallbackFailure(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/callback/failure"), `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Contains(t, rec.Body.String(), "FAILED")
}

func TestBatch1_DurableExecution_CallbackHeartbeat(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/callback/heartbeat"), `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Still running after heartbeat
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Contains(t, rec.Body.String(), "RUNNING")
}

func TestBatch1_DurableExecution_ListByFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Checkpoint two different executions
	e1 := "arn:aws:lambda:us-east-1:000000000000:durable:exec-1"
	e2 := "arn:aws:lambda:us-east-1:000000000000:durable:exec-2"
	p1 := "/2025-12-01/durable-executions/" + url.PathEscape(e1) + "/checkpoint"
	p2 := "/2025-12-01/durable-executions/" + url.PathEscape(e2) + "/checkpoint"
	callInMemoryHandler(t, h, http.MethodPost, p1, `{}`)
	callInMemoryHandler(t, h, http.MethodPost, p2, `{}`)

	// List all (no function filter)
	rec := callInMemoryHandler(t, h, http.MethodGet, "/2025-12-01/durable-executions/", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DurableExecutions")
}

// --- CheckpointDurableExecution tests ---

func TestNewOps_CheckpointDurableExecution(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	tests := []struct {
		name       string
		path       string
		method     string
		wantStatus int
	}{
		{
			name:       "checkpoint_success",
			path:       "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:durable:abc/checkpoint",
			method:     http.MethodPost,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_not_found",
			path:       "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:durable:never-created",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := callInMemoryHandler(t, h, tt.method, tt.path, `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
