package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// ─── ARN Format ───────────────────────────────────────────────────────────────

func TestDescribeMapRun_NotFound(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	// Non-existent MapRun ARN → 404 (real backend, not stub).
	rec := sfnPost(
		ctx,
		t,
		h,
		e,
		"DescribeMapRun",
		`{"mapRunArn":"arn:aws:states:us-east-1:123:mapRun:sm:exec:uuid"}`,
	)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListMapRuns_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(
		ctx,
		t,
		h,
		e,
		"ListMapRuns",
		`{"executionArn":"arn:aws:states:us-east-1:123:execution:sm:exec"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	mapRuns, _ := out["mapRuns"].([]any)
	assert.Empty(t, mapRuns)
}

// ─── RedriveExecution ─────────────────────────────────────────────────────────

// mapIterStateDef is a minimal ASL with a Map state that iterates over input items.
const mapIterStateDef = `{
	"StartAt":"M",
	"States":{
		"M":{
			"Type":"Map",
			"End":true,
			"ItemsPath":"$",
			"MaxConcurrency":1,
			"Iterator":{
				"StartAt":"P",
				"States":{
					"P":{"Type":"Pass","End":true}
				}
			}
		}
	}
}`

// TestParity_MapRunStorage verifies that Map state executions are tracked in
// the MapRun store and accessible via DescribeMapRun/ListMapRuns/UpdateMapRun.
func TestMapRunStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "StartSyncExecution with Map state populates MapRun store",
			fn: func(t *testing.T) {
				t.Helper()

				bk := stepfunctions.NewInMemoryBackend()
				h, e := newSFNHandlerWithBackend(bk)
				ctx := context.Background()

				createBody, _ := json.Marshal(map[string]any{
					"name":       "map-sm-1",
					"definition": mapIterStateDef,
					"roleArn":    "arn:aws:iam::123456789012:role/test",
					"type":       "EXPRESS",
				})
				rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(createBody))
				require.Equal(t, http.StatusOK, rec.Code)

				var smResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &smResp))
				smARN := smResp["stateMachineArn"].(string)

				syncBody, _ := json.Marshal(map[string]any{
					"stateMachineArn": smARN,
					"name":            "sync-exec-1",
					"input":           `[1,2,3]`,
				})
				syncRec := sfnPost(ctx, t, h, e, "StartSyncExecution", string(syncBody))
				require.Equal(t, http.StatusOK, syncRec.Code)

				var syncResp map[string]any
				require.NoError(t, json.Unmarshal(syncRec.Body.Bytes(), &syncResp))
				execARN := syncResp["executionArn"].(string)

				assert.GreaterOrEqual(t, bk.MapRunCountForTest(execARN), 1,
					"expected at least one MapRun for execution %s", execARN)
			},
		},
		{
			name: "DescribeMapRun returns correct data",
			fn: func(t *testing.T) {
				t.Helper()

				bk := stepfunctions.NewInMemoryBackend()
				h, e := newSFNHandlerWithBackend(bk)
				ctx := context.Background()

				createBody, _ := json.Marshal(map[string]any{
					"name":       "map-sm-2",
					"definition": mapIterStateDef,
					"roleArn":    "arn:aws:iam::123456789012:role/test",
					"type":       "EXPRESS",
				})
				rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(createBody))
				require.Equal(t, http.StatusOK, rec.Code)

				var smResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &smResp))
				smARN := smResp["stateMachineArn"].(string)

				syncBody, _ := json.Marshal(map[string]any{
					"stateMachineArn": smARN,
					"name":            "sync-exec-2",
					"input":           `[1,2]`,
				})
				syncRec := sfnPost(ctx, t, h, e, "StartSyncExecution", string(syncBody))
				require.Equal(t, http.StatusOK, syncRec.Code)

				var syncResp map[string]any
				require.NoError(t, json.Unmarshal(syncRec.Body.Bytes(), &syncResp))
				execARN := syncResp["executionArn"].(string)

				require.GreaterOrEqual(t, bk.MapRunCountForTest(execARN), 1)

				// ListMapRuns via handler.
				listBody, _ := json.Marshal(map[string]any{"executionArn": execARN})
				listRec := sfnPost(ctx, t, h, e, "ListMapRuns", string(listBody))
				require.Equal(t, http.StatusOK, listRec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				mapRuns, _ := listResp["mapRuns"].([]any)
				require.NotEmpty(t, mapRuns, "expected non-empty mapRuns list")

				firstRun := mapRuns[0].(map[string]any)
				mapRunARN, _ := firstRun["mapRunArn"].(string)
				require.NotEmpty(t, mapRunARN)

				descBody, _ := json.Marshal(map[string]any{"mapRunArn": mapRunARN})
				descRec := sfnPost(ctx, t, h, e, "DescribeMapRun", string(descBody))
				require.Equal(t, http.StatusOK, descRec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
				assert.Equal(t, mapRunARN, descResp["mapRunArn"])
				assert.Equal(t, execARN, descResp["executionArn"])
				assert.Equal(t, "SUCCEEDED", descResp["status"])
			},
		},
		{
			name: "UpdateMapRun updates MaxConcurrency",
			fn: func(t *testing.T) {
				t.Helper()

				bk := stepfunctions.NewInMemoryBackend()
				h, e := newSFNHandlerWithBackend(bk)
				ctx := context.Background()

				createBody, _ := json.Marshal(map[string]any{
					"name":       "map-sm-3",
					"definition": mapIterStateDef,
					"roleArn":    "arn:aws:iam::123456789012:role/test",
					"type":       "EXPRESS",
				})
				rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(createBody))
				require.Equal(t, http.StatusOK, rec.Code)

				var smResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &smResp))
				smARN := smResp["stateMachineArn"].(string)

				syncBody, _ := json.Marshal(map[string]any{
					"stateMachineArn": smARN,
					"name":            "sync-exec-3b",
					"input":           `[1]`,
				})
				syncRec := sfnPost(ctx, t, h, e, "StartSyncExecution", string(syncBody))
				require.Equal(t, http.StatusOK, syncRec.Code)

				var syncResp map[string]any
				require.NoError(t, json.Unmarshal(syncRec.Body.Bytes(), &syncResp))
				execARN := syncResp["executionArn"].(string)

				require.GreaterOrEqual(t, bk.MapRunCountForTest(execARN), 1)

				listBody, _ := json.Marshal(map[string]any{"executionArn": execARN})
				listRec := sfnPost(ctx, t, h, e, "ListMapRuns", string(listBody))
				require.Equal(t, http.StatusOK, listRec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				mapRuns, _ := listResp["mapRuns"].([]any)
				require.NotEmpty(t, mapRuns)

				firstRun := mapRuns[0].(map[string]any)
				mapRunARN := firstRun["mapRunArn"].(string)

				updateBody, _ := json.Marshal(map[string]any{
					"mapRunArn":      mapRunARN,
					"maxConcurrency": 10,
				})
				updateRec := sfnPost(ctx, t, h, e, "UpdateMapRun", string(updateBody))
				require.Equal(t, http.StatusOK, updateRec.Code)

				var updateResp map[string]any
				require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
				maxConc, _ := updateResp["maxConcurrency"].(float64)
				assert.InEpsilon(t, float64(10), maxConc, 1e-9)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}
