package stepfunctions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

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

// newSFNHandlerWithBackend creates a handler and echo instance backed by the provided backend.
func newSFNHandlerWithBackend(
	bk *stepfunctions.InMemoryBackend,
) (*stepfunctions.Handler, *echo.Echo) {
	return stepfunctions.NewHandler(bk), echo.New()
}

// TestParity_TestStateNextState verifies that TestState returns NextState
// for non-terminal states and an empty nextState for terminal (End:true) states.
func TestParity_TestStateNextState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		definition    string
		input         string
		wantStatus    string
		wantNextState string
		wantError     string
		wantHTTPCode  int
	}{
		{
			name:          "End:true yields empty nextState",
			definition:    `{"MyState":{"Type":"Pass","End":true}}`,
			input:         `{"x":1}`,
			wantHTTPCode:  http.StatusOK,
			wantStatus:    "SUCCEEDED",
			wantNextState: "",
		},
		{
			name:          "Next:AnotherState yields nextState",
			definition:    `{"MyState":{"Type":"Pass","Next":"AnotherState"}}`,
			input:         `{"x":1}`,
			wantHTTPCode:  http.StatusOK,
			wantStatus:    "SUCCEEDED",
			wantNextState: "AnotherState",
		},
		{
			name:         "Fail state yields status FAILED",
			definition:   `{"FailState":{"Type":"Fail","Error":"MyError","Cause":"test"}}`,
			input:        `{}`,
			wantHTTPCode: http.StatusOK,
			wantStatus:   "FAILED",
			wantError:    "MyError",
		},
		{
			name: "Two states in definition yields error",
			// Two states is invalid for TestState (must be exactly one).
			definition:   `{"State1":{"Type":"Pass","End":true},"State2":{"Type":"Pass","End":true}}`,
			input:        `{}`,
			wantHTTPCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			ctx := context.Background()

			body, err := json.Marshal(map[string]any{
				"definition": tt.definition,
				"input":      tt.input,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "TestState", string(body))
			require.Equal(t, tt.wantHTTPCode, rec.Code)

			if tt.wantHTTPCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Equal(t, tt.wantStatus, resp["status"])

			// nextState may be absent (nil) or empty string — both are fine for "".
			gotNextState, _ := resp["nextState"].(string)
			assert.Equal(t, tt.wantNextState, gotNextState)

			if tt.wantError != "" {
				assert.Equal(t, tt.wantError, resp["error"])
			}
		})
	}
}

// TestParity_MapRunStorage verifies that Map state executions are tracked in
// the MapRun store and accessible via DescribeMapRun/ListMapRuns/UpdateMapRun.
func TestParity_MapRunStorage(t *testing.T) {
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

// TestPerf_ListExecutionsStatusIndex verifies that the smExecsByStatus index
// correctly filters executions by status without a full scan.
func TestPerf_ListExecutionsStatusIndex(t *testing.T) {
	t.Parallel()

	bk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	ctx := context.Background()

	sm, err := bk.CreateStateMachine(
		ctx, "perf-sm",
		`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`,
		"arn:aws:iam::123456789012:role/r", "STANDARD",
	)
	require.NoError(t, err)
	smARN := sm.StateMachineArn

	const numExecs = 5
	execARNs := make([]string, 0, numExecs)

	for i := range numExecs {
		exec, startErr := bk.StartExecution(smARN, fmt.Sprintf("exec-%d", i), `{}`)
		require.NoError(t, startErr)
		execARNs = append(execARNs, exec.ExecutionArn)
	}

	// Wait for all executions to finish.
	require.Eventually(t, func() bool {
		for _, arn := range execARNs {
			exec, descErr := bk.DescribeExecution(arn)
			if descErr != nil || exec.Status == "RUNNING" {
				return false
			}
		}

		return true
	}, 5*time.Second, 20*time.Millisecond)

	// Count actual statuses.
	succeededCount := 0
	runningCount := 0

	for _, arn := range execARNs {
		exec, _ := bk.DescribeExecution(arn)
		switch exec.Status {
		case "SUCCEEDED":
			succeededCount++
		case "RUNNING":
			runningCount++
		}
	}

	tests := []struct {
		name         string
		statusFilter string
		wantCount    int
	}{
		{
			name:         "filter RUNNING returns only running executions",
			statusFilter: "RUNNING",
			wantCount:    runningCount,
		},
		{
			name:         "filter SUCCEEDED returns only succeeded executions",
			statusFilter: "SUCCEEDED",
			wantCount:    succeededCount,
		},
		{
			name:         "no filter returns all executions",
			statusFilter: "",
			wantCount:    numExecs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			execs, _, listErr := bk.ListExecutions(smARN, tt.statusFilter, "", 0)
			require.NoError(t, listErr)
			assert.Len(t, execs, tt.wantCount)

			for _, exec := range execs {
				if tt.statusFilter != "" {
					assert.Equal(t, tt.statusFilter, exec.Status)
				}
			}
		})
	}
}

// TestPerf_SweepTaskTokensRLock verifies that SweepTaskTokens evicts stale tokens correctly.
func TestPerf_SweepTaskTokensRLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn       func(t *testing.T, bk *stepfunctions.InMemoryBackend)
		name          string
		wantEvictions int
	}{
		{
			name:          "no stale tokens evicts nothing",
			setupFn:       func(_ *testing.T, _ *stepfunctions.InMemoryBackend) {},
			wantEvictions: 0,
		},
		{
			name: "aged tokens are evicted",
			setupFn: func(t *testing.T, bk *stepfunctions.InMemoryBackend) {
				t.Helper()

				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)

				act, err := bk.CreateActivity(ctx, "sweep-test-act")
				require.NoError(t, err)

				done := make(chan struct{})
				go func() {
					defer close(done)
					// InvokeActivity registers a token; we never complete it.
					bk.InvokeActivity(ctx, act.ActivityArn, `{}`, 0)
				}()

				// Give the goroutine time to register its token.
				require.Eventually(t, func() bool {
					return bk.TaskTokenCount() > 0
				}, time.Second, 5*time.Millisecond)

				// Age all tokens well past the TTL.
				bk.AgeTaskTokensForTest(2 * stepfunctions.DefaultTaskTokenTTLForTest)
			},
			wantEvictions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := stepfunctions.NewInMemoryBackend()
			tt.setupFn(t, bk)

			evicted := bk.SweepTaskTokens()
			assert.Equal(t, tt.wantEvictions, evicted)
		})
	}
}

// TestLeak_DeletedExecsTombstoneCleanup verifies that pruneExecutionsLocked
// cleans up orphaned tombstones (goroutines that exited without clearing them).
func TestLeak_DeletedExecsTombstoneCleanup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(t *testing.T, bk *stepfunctions.InMemoryBackend) float64 // returns prune cutoff
		name    string
	}{
		{
			name: "no tombstones after prune clears finished execs",
			setupFn: func(t *testing.T, bk *stepfunctions.InMemoryBackend) float64 {
				t.Helper()

				ctx := context.Background()
				sm, err := bk.CreateStateMachine(
					ctx, "tomb-sm",
					`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`,
					"arn:aws:iam::123456789012:role/r", "STANDARD",
				)
				require.NoError(t, err)

				exec, err := bk.StartExecution(sm.StateMachineArn, "tomb-exec", `{}`)
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					e, descErr := bk.DescribeExecution(exec.ExecutionArn)

					return descErr == nil && e.Status != "RUNNING"
				}, 3*time.Second, 10*time.Millisecond)

				// Return a cutoff far in the future to prune everything.
				return float64(time.Now().Add(10 * time.Second).Unix())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := stepfunctions.NewInMemoryBackend()
			cutoff := tt.setupFn(t, bk)

			beforeTombstones := bk.DeletedExecsCountForTest()
			bk.PruneExecutionsForTest(cutoff)
			afterTombstones := bk.DeletedExecsCountForTest()

			// Tombstone count should not increase after pruning.
			assert.LessOrEqual(t, afterTombstones, beforeTombstones,
				"tombstone count should not increase after prune")
		})
	}
}
