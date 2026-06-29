package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_RunStartsAsPending verifies that StartRun returns status PENDING,
// not COMPLETED. AWS puts runs in PENDING state until a workflow engine picks them up.
func TestParity_RunStartsAsPending(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-run",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "PENDING", resp["status"])
}

// TestParity_StartRunReturnsPartialResponse verifies that StartRun only returns
// {arn, id, status, tags} — not the full Run object. Real AWS returns a minimal response.
func TestParity_StartRunReturnsPartialResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-run",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["arn"])
	assert.NotEmpty(t, resp["id"])
	assert.Equal(t, "PENDING", resp["status"])
	assert.Nil(t, resp["workflowId"], "full run object must not be returned")
	assert.Nil(t, resp["name"], "full run object must not be returned")
}

// TestParity_WorkflowStartsAsCreating verifies that CreateWorkflow returns status CREATING,
// not ACTIVE. AWS sets workflows to CREATING while it processes the definition.
func TestParity_WorkflowStartsAsCreating(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "my-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "CREATING", resp["status"])
}

// TestParity_CreateWorkflowReturnsPartialResponse verifies CreateWorkflow returns only
// {arn, id, status, tags} per AWS behavior.
func TestParity_CreateWorkflowReturnsPartialResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "my-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["arn"])
	assert.NotEmpty(t, resp["id"])
	assert.Nil(t, resp["name"], "full workflow object must not be returned")
	assert.Nil(t, resp["description"], "full workflow object must not be returned")
}

// TestParity_WorkflowHasTypeField verifies that GetWorkflow returns a type field.
// AWS always sets type to PRIVATE for user-created workflows.
func TestParity_WorkflowHasTypeField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "typed-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	wfID := createResp["id"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/workflow/"+wfID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "PRIVATE", getResp["type"])
}

// TestParity_UpdateWorkflowReturnsWorkflow verifies that UpdateWorkflow returns the
// updated Workflow object, not an empty body. Real AWS returns the full workflow.
func TestParity_UpdateWorkflowReturnsWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "original-name",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	wfID := createResp["id"].(string)

	updateRec := doRequest(t, h, http.MethodPost, "/workflow/"+wfID, map[string]any{
		"name":        "updated-name",
		"description": "new description",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated-name", updateResp["name"])
	assert.Equal(t, "new description", updateResp["description"])
}

// TestParity_SequenceStoreHasStatusAndUpdateTime verifies that a sequence store
// exposes status and updateTime fields. AWS always includes these.
func TestParity_SequenceStoreHasStatusAndUpdateTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{
		"name": "my-store",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACTIVE", resp["status"])
	assert.NotEmpty(t, resp["updateTime"])
}

// TestParity_CreateWorkflowAcceptsDefinitionUri verifies that CreateWorkflow accepts
// the definitionUri field (S3 URI) in addition to definitionZip.
func TestParity_CreateWorkflowAcceptsDefinitionUri(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":          "uri-workflow",
		"engine":        "WDL",
		"definitionUri": "s3://my-bucket/workflow.zip",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

// TestParity_CreateAnnotationStoreStoresReference verifies that CreateAnnotationStore
// accepts and returns a reference field. Real AWS requires this for VCF/TSV stores.
func TestParity_CreateAnnotationStoreStoresReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
		"name":        "ann-store-ref",
		"storeFormat": "VCF",
		"reference": map[string]any{
			"referenceArn": "arn:aws:omics:us-east-1:000000000000:referencestore/abc/reference/xyz",
		},
		"sseConfig":    map[string]any{"type": "KMS"},
		"storeOptions": map[string]any{"tsvStoreOptions": map[string]any{}},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["reference"])
	assert.Equal(t, "CREATING", resp["status"])
}

// TestParity_CreateVariantStoreStoresReference verifies that CreateVariantStore
// accepts and returns a reference field. Real AWS requires a reference genome.
func TestParity_CreateVariantStoreStoresReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{
		"name": "var-store-ref",
		"reference": map[string]any{
			"referenceArn": "arn:aws:omics:us-east-1:000000000000:referencestore/abc/reference/xyz",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["reference"])
	assert.Equal(t, "CREATING", resp["status"])
}

// TestParity_DeleteReferenceStoreReturnsID verifies that DeleteReferenceStore returns
// {id: "..."} in the response body. Real AWS returns the deleted resource's ID.
func TestParity_DeleteReferenceStoreReturnsID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{
		"name": "to-delete",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	storeID := createResp["id"].(string)

	delRec := doRequest(t, h, http.MethodDelete, "/referencestore/"+storeID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	var delResp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delResp))
	assert.Equal(t, storeID, delResp["id"])
}

// TestParity_DeleteSequenceStoreReturnsID verifies that DeleteSequenceStore returns
// {id: "..."} in the response body.
func TestParity_DeleteSequenceStoreReturnsID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{
		"name": "to-delete",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	storeID := createResp["id"].(string)

	delRec := doRequest(t, h, http.MethodDelete, "/sequencestore/"+storeID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	var delResp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delResp))
	assert.Equal(t, storeID, delResp["id"])
}

// TestParity_UpdateRunCachePersistsDescription verifies that UpdateRunCache saves
// the description field. Previously it was silently discarded.
func TestParity_UpdateRunCachePersistsDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/runCache", map[string]any{
		"name":            "my-cache",
		"cacheS3Location": "s3://my-bucket/cache",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cacheID := createResp["id"].(string)

	updateRec := doRequest(t, h, http.MethodPost, "/runCache/"+cacheID, map[string]any{
		"description": "updated description",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/runCache/"+cacheID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "updated description", getResp["description"])
}

// TestParity_ListRunsInBatchReturnsAssociatedRuns verifies that ListRunsInBatch
// returns runs that were started with that batch's ID. Previously always returned empty.
func TestParity_ListRunsInBatchReturnsAssociatedRuns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a run batch.
	batchRec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-batch",
	})
	require.Equal(t, http.StatusCreated, batchRec.Code)

	var batchResp map[string]any
	require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batchResp))
	batchID := batchResp["id"].(string)

	// Start a run associated with this batch.
	runRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "batch-run",
		"runBatchId": batchID,
	})
	require.Equal(t, http.StatusCreated, runRec.Code)

	// List runs in the batch — should include our run.
	listRec := doRequest(t, h, http.MethodPost, "/runBatch/"+batchID+"/run", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	runs, ok := listResp["runs"].([]any)
	require.True(t, ok)
	assert.Len(t, runs, 1)
}

// TestParity_ListRunsInBatchEmptyForOtherBatch verifies that ListRunsInBatch does not
// leak runs from other batches.
func TestParity_ListRunsInBatchEmptyForOtherBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two batches.
	batch1Rec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
		"workflowId": "wf1",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "batch-1",
	})
	require.Equal(t, http.StatusCreated, batch1Rec.Code)

	batch2Rec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
		"workflowId": "wf2",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "batch-2",
	})
	require.Equal(t, http.StatusCreated, batch2Rec.Code)

	var b1 map[string]any
	require.NoError(t, json.Unmarshal(batch1Rec.Body.Bytes(), &b1))
	batchID1 := b1["id"].(string)

	var b2 map[string]any
	require.NoError(t, json.Unmarshal(batch2Rec.Body.Bytes(), &b2))
	batchID2 := b2["id"].(string)

	// Associate a run with batch 1.
	doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf1",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "run-for-batch1",
		"runBatchId": batchID1,
	})

	// List runs in batch 2 — must be empty.
	listRec := doRequest(t, h, http.MethodPost, "/runBatch/"+batchID2+"/run", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	runs, ok := listResp["runs"].([]any)
	require.True(t, ok)
	assert.Empty(t, runs)
}

// TestParity_CancelRunGuardsTerminalState verifies that CancelRun rejects a request
// to cancel an already-cancelled run. AWS returns a ValidationException in this case.
func TestParity_CancelRunGuardsTerminalState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name string
	}{
		{name: "cancel already-cancelled run returns 400"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			startRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
				"workflowId": "wf123",
				"roleArn":    "arn:aws:iam::000000000000:role/role",
				"name":       "run-to-cancel",
			})
			require.Equal(t, http.StatusCreated, startRec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
			runID := startResp["id"].(string)

			// Cancel once.
			cancel1Rec := doRequest(t, h, http.MethodPost, "/run/"+runID+"/cancel", nil)
			require.Equal(t, http.StatusOK, cancel1Rec.Code)

			// Cancel again — should fail.
			cancel2Rec := doRequest(t, h, http.MethodPost, "/run/"+runID+"/cancel", nil)
			assert.Equal(t, http.StatusBadRequest, cancel2Rec.Code)
		})
	}
}
