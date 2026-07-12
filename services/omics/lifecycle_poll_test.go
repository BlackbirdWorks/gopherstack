package omics_test

// Real AWS HealthOmics clients poll Get* operations via generated waiters
// (WorkflowActiveWaiter, AnnotationStoreCreatedWaiter, VariantStoreCreatedWaiter,
// WorkflowVersionActiveWaiter, RunRunningWaiter/RunCompletedWaiter,
// TaskRunningWaiter/TaskCompletedWaiter) until the resource's Status field
// leaves its transient CREATING/PENDING/RUNNING state. Previously these
// resources never transitioned past their initial transient status, so a real
// SDK client's waiter would poll forever until it hit its own timeout. These
// tests verify the emulator advances status across repeated polls the same
// way real AWS eventually does.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GetWorkflow_AdvancesCreatingToActiveOnPoll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "my-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	id := created["id"].(string)
	require.Equal(t, "CREATING", created["status"])

	getRec := doRequest(t, h, http.MethodGet, "/workflow/"+id, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
	assert.Equal(t, "ACTIVE", got["status"], "waiter clients poll GetWorkflow until Status==ACTIVE")
}

func Test_GetAnnotationStore_AdvancesCreatingToActiveOnPoll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
		"name":        "my-store",
		"storeFormat": "VCF",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	require.Equal(t, "CREATING", created["status"])

	getRec := doRequest(t, h, http.MethodGet, "/annotationStore/my-store", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
	assert.Equal(t, "ACTIVE", got["status"])
}

func Test_GetVariantStore_AdvancesCreatingToActiveOnPoll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{
		"name": "my-vstore",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	require.Equal(t, "CREATING", created["status"])

	getRec := doRequest(t, h, http.MethodGet, "/variantStore/my-vstore", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
	assert.Equal(t, "ACTIVE", got["status"])
}

func Test_GetWorkflowVersion_AdvancesCreatingToActiveOnPoll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wfRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "my-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, wfRec.Code)

	var wf map[string]any
	require.NoError(t, json.Unmarshal(wfRec.Body.Bytes(), &wf))
	workflowID := wf["id"].(string)

	verRec := doRequest(t, h, http.MethodPost, "/workflow/"+workflowID+"/version", map[string]any{
		"versionName": "v1",
	})
	require.Equal(t, http.StatusCreated, verRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &created))
	require.Equal(t, "CREATING", created["status"])

	getRec := doRequest(t, h, http.MethodGet, "/workflow/"+workflowID+"/version/v1", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
	assert.Equal(t, "ACTIVE", got["status"])
}

// Test_GetRun_AdvancesPendingToRunningToCompletedAcrossPolls verifies the
// PENDING -> RUNNING -> COMPLETED progression a RunRunningWaiter (succeeds at
// RUNNING) and RunCompletedWaiter (succeeds at COMPLETED) both depend on.
func Test_GetRun_AdvancesPendingToRunningToCompletedAcrossPolls(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-run",
	})
	require.Equal(t, http.StatusCreated, startRec.Code)

	var started map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))
	id := started["id"].(string)
	require.Equal(t, "PENDING", started["status"])

	poll1 := doRequest(t, h, http.MethodGet, "/run/"+id, nil)
	require.Equal(t, http.StatusOK, poll1.Code)

	var p1 map[string]any
	require.NoError(t, json.Unmarshal(poll1.Body.Bytes(), &p1))
	assert.Equal(t, "RUNNING", p1["status"], "RunRunningWaiter polls until Status==RUNNING")
	assert.NotEmpty(t, p1["startTime"])

	poll2 := doRequest(t, h, http.MethodGet, "/run/"+id, nil)
	require.Equal(t, http.StatusOK, poll2.Code)

	var p2 map[string]any
	require.NoError(t, json.Unmarshal(poll2.Body.Bytes(), &p2))
	assert.Equal(t, "COMPLETED", p2["status"], "RunCompletedWaiter polls until Status==COMPLETED")
	assert.NotEmpty(t, p2["stopTime"])
}

// Test_GetRunTask_AdvancesPendingToRunningToCompletedAcrossPolls mirrors the
// Run progression for the task nested inside it (TaskRunningWaiter /
// TaskCompletedWaiter).
func Test_GetRunTask_AdvancesPendingToRunningToCompletedAcrossPolls(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-run",
	})
	require.Equal(t, http.StatusCreated, startRec.Code)

	var started map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))
	runID := started["id"].(string)

	tasksRec := doRequest(t, h, http.MethodGet, "/run/"+runID+"/task", nil)
	require.Equal(t, http.StatusOK, tasksRec.Code)

	var tasksResp map[string]any
	require.NoError(t, json.Unmarshal(tasksRec.Body.Bytes(), &tasksResp))
	tasks := tasksResp["tasks"].([]any)
	require.Len(t, tasks, 1)
	taskID := tasks[0].(map[string]any)["taskId"].(string)

	poll1 := doRequest(t, h, http.MethodGet, "/run/"+runID+"/task/"+taskID, nil)
	require.Equal(t, http.StatusOK, poll1.Code)

	var p1 map[string]any
	require.NoError(t, json.Unmarshal(poll1.Body.Bytes(), &p1))
	assert.Equal(t, "RUNNING", p1["status"])

	poll2 := doRequest(t, h, http.MethodGet, "/run/"+runID+"/task/"+taskID, nil)
	require.Equal(t, http.StatusOK, poll2.Code)

	var p2 map[string]any
	require.NoError(t, json.Unmarshal(poll2.Body.Bytes(), &p2))
	assert.Equal(t, "COMPLETED", p2["status"])
}

// Test_CancelRun_TerminalStateIsNotAdvancedByFurtherPolls verifies that once a
// run leaves the PENDING/RUNNING progression via CancelRun, further GetRun
// polls leave the terminal CANCELLED status untouched.
func Test_CancelRun_TerminalStateIsNotAdvancedByFurtherPolls(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	startRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-run",
	})
	require.Equal(t, http.StatusCreated, startRec.Code)

	var started map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))
	id := started["id"].(string)

	cancelRec := doRequest(t, h, http.MethodPost, "/run/"+id+"/cancel", nil)
	require.Equal(t, http.StatusOK, cancelRec.Code)

	for range 3 {
		getRec := doRequest(t, h, http.MethodGet, "/run/"+id, nil)
		require.Equal(t, http.StatusOK, getRec.Code)

		var got map[string]any
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
		assert.Equal(t, "CANCELLED", got["status"])
	}
}
