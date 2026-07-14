package omics_test

// Cross-checked against aws-sdk-go-v2/service/omics@v1.45.0's generated
// serializers.go, real AWS wire shapes for the RunBatch family are:
//
//	GET    /runBatch/{batchId}/run  -> ListRunsInBatch
//	DELETE /runBatch/{batchId}      -> DeleteBatch      (deletes the batch resource)
//	POST   /runBatch/delete         -> DeleteRunBatch   (deletes the runs inside a batch,
//	                                                      body: {"batchId": "<single id>"})
//
// Previously ListRunsInBatch was wired to POST instead of GET (unreachable by
// a real SDK client, which always sends GET), and DeleteBatch/DeleteRunBatch
// were swapped: DELETE /runBatch/{id} called "DeleteRunBatch" behavior and
// POST /runBatch/delete expected a {"batchIds": [...]} array and bulk-deleted
// batch *records* — the reverse of what those two real operations do.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_ListRunsInBatch_ReachableOnlyViaGET proves the real AWS wire shape
// (GET /runBatch/{batchId}/run) is routed to ListRunsInBatch, and that POST to
// the same path — which real SDK clients never send for this op — is not.
func Test_ListRunsInBatch_ReachableOnlyViaGET(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	batchRec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-batch",
	})
	require.Equal(t, http.StatusCreated, batchRec.Code)

	var batch map[string]any
	require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batch))
	batchID := batch["id"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/runBatch/"+batchID+"/run", nil)
	assert.Equal(t, http.StatusOK, getRec.Code, "real ListRunsInBatch is GET")

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.NotNil(t, getResp["runs"], "GET must be classified as ListRunsInBatch")

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/runBatch/"+batchID+"/run", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNotImplemented, rec.Code,
		"POST to this path is not a real HealthOmics operation and must not be misclassified as ListRunsInBatch")
}

// Test_DeleteBatch_DeletesBatchResourceViaURIID proves DELETE
// /runBatch/{batchId} implements real DeleteBatch semantics: it removes the
// batch resource itself (subsequent GetBatch 404s).
func Test_DeleteBatch_DeletesBatchResourceViaURIID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	batchRec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-batch",
	})
	require.Equal(t, http.StatusCreated, batchRec.Code)

	var batch map[string]any
	require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batch))
	batchID := batch["id"].(string)

	delRec := doRequest(t, h, http.MethodDelete, "/runBatch/"+batchID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/runBatch/"+batchID, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code, "the batch resource must be gone")
}

// Test_DeleteRunBatch_DeletesOnlyRunsLeavesBatchIntact proves POST
// /runBatch/delete implements real DeleteRunBatch semantics: it takes a
// single batchId in the JSON body and deletes the runs that belong to that
// batch, while leaving the batch resource itself (and its own GetBatch
// visibility) intact.
func Test_DeleteRunBatch_DeletesOnlyRunsLeavesBatchIntact(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	batchRec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "my-batch",
	})
	require.Equal(t, http.StatusCreated, batchRec.Code)

	var batch map[string]any
	require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batch))
	batchID := batch["id"].(string)

	runRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
		"workflowId": "wf123",
		"roleArn":    "arn:aws:iam::000000000000:role/role",
		"name":       "batch-run",
		"runBatchId": batchID,
	})
	require.Equal(t, http.StatusCreated, runRec.Code)

	var run map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &run))
	runID := run["id"].(string)

	delRec := doRequest(t, h, http.MethodPost, "/runBatch/delete", map[string]any{
		"batchId": batchID,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	var delResp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delResp))
	assert.Empty(t, delResp, "real DeleteRunBatchOutput is empty — no errors array")

	getBatchRec := doRequest(t, h, http.MethodGet, "/runBatch/"+batchID, nil)
	assert.Equal(t, http.StatusOK, getBatchRec.Code, "the batch resource itself must survive DeleteRunBatch")

	getRunRec := doRequest(t, h, http.MethodGet, "/run/"+runID, nil)
	assert.Equal(t, http.StatusNotFound, getRunRec.Code, "the run belonging to the batch must be deleted")
}
