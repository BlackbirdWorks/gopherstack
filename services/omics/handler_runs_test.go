package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOmics_RunGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateRunGroup returns 201 with arn",
			method:   http.MethodPost,
			path:     "/runGroup",
			body:     map[string]any{"name": "rg1", "maxCpus": 4, "maxRuns": 2},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "ListRunGroups returns empty list",
			method:   http.MethodGet,
			path:     "/runGroup",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["runGroups"])
			},
		},
		{
			name:     "GetRunGroup unknown returns 404",
			method:   http.MethodGet,
			path:     "/runGroup/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteRunGroup unknown returns 404",
			method:   http.MethodDelete,
			path:     "/runGroup/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "StartRun returns 201",
			method: http.MethodPost,
			path:   "/run",
			body: map[string]any{
				"workflowId": "wf123",
				"roleArn":    "arn:aws:iam::000000000000:role/omics-role",
				"name":       "my-run",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "ListRuns returns empty",
			method:   http.MethodGet,
			path:     "/run",
			wantCode: http.StatusOK,
		},
		{
			name:     "GetRun unknown returns 404",
			method:   http.MethodGet,
			path:     "/run/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteRun unknown returns 404",
			method:   http.MethodDelete,
			path:     "/run/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestRunStartsAsPending verifies that StartRun returns status PENDING,
// not COMPLETED. AWS puts runs in PENDING state until a workflow engine picks them up.
func TestRunStartsAsPending(t *testing.T) {
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

// TestStartRunReturnsPartialResponse verifies that StartRun only returns
// {arn, id, status, tags} — not the full Run object. Real AWS returns a minimal response.
func TestStartRunReturnsPartialResponse(t *testing.T) {
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

// TestUpdateRunCachePersistsDescription verifies that UpdateRunCache saves
// the description field. Previously it was silently discarded.
func TestUpdateRunCachePersistsDescription(t *testing.T) {
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

// TestListRunsInBatchReturnsAssociatedRuns verifies that ListRunsInBatch
// returns runs that were started with that batch's ID. Previously always returned empty.
func TestListRunsInBatchReturnsAssociatedRuns(t *testing.T) {
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

	// List runs in the batch — should include our run. Real AWS ListRunsInBatch
	// is a GET request.
	listRec := doRequest(t, h, http.MethodGet, "/runBatch/"+batchID+"/run", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	runs, ok := listResp["runs"].([]any)
	require.True(t, ok)
	assert.Len(t, runs, 1)
}

// TestListRunsInBatchEmptyForOtherBatch verifies that ListRunsInBatch does not
// leak runs from other batches.
func TestListRunsInBatchEmptyForOtherBatch(t *testing.T) {
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

	// List runs in batch 2 — must be empty. Real AWS ListRunsInBatch is a GET
	// request.
	listRec := doRequest(t, h, http.MethodGet, "/runBatch/"+batchID2+"/run", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	runs, ok := listResp["runs"].([]any)
	require.True(t, ok)
	assert.Empty(t, runs)
}

// TestCancelRunGuardsTerminalState verifies that CancelRun rejects a request
// to cancel an already-cancelled run. AWS returns a ValidationException in this case.
func TestCancelRunGuardsTerminalState(t *testing.T) {
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
