package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestDurableExecution_CheckpointCreatesExecution(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Before checkpoint: GET returns 404
	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Checkpoint creates the execution
	rec = callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// After checkpoint: GET returns 200
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), durableExecARN)
	assert.Contains(t, rec.Body.String(), "RUNNING")
}

func TestDurableExecution_GetHistory(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// First checkpoint creates the execution (ExecutionStarted) and starts a STEP.
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"),
		`{"Updates":[{"Id":"step-1","Type":"STEP","Action":"START"}]}`)
	// Second checkpoint completes it.
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"),
		`{"Updates":[{"Id":"step-1","Type":"STEP","Action":"SUCCEED"}]}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/history"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetDurableExecutionHistoryOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Events, 3)
	assert.Equal(t, "ExecutionStarted", out.Events[0].EventType)
	assert.Equal(t, "StepStarted", out.Events[1].EventType)
	assert.Equal(t, "StepSucceeded", out.Events[2].EventType)
}

func TestDurableExecution_GetHistoryEmpty(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// An ARN that was never touched by a checkpoint does not exist — 404,
	// matching GetDurableExecution/GetDurableExecutionState's behavior for a
	// required-but-unknown DurableExecutionArn.
	noneARN := "arn:aws:lambda:us-east-1:000000000000:durable:none"
	path := "/2025-12-01/durable-executions/" + url.PathEscape(noneARN) + "/history"
	rec := callInMemoryHandler(t, h, http.MethodGet, path, "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDurableExecution_GetState(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/state"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetDurableExecutionStateOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Operations, 1)
	assert.Equal(t, "execution", out.Operations[0].ID)
	assert.Equal(t, lambda.DurableOperationTypeExecution, out.Operations[0].Type)
	assert.Equal(t, lambda.DurableOperationStatusStarted, out.Operations[0].Status)
}

func TestDurableExecution_GetStateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	nopeARN := "arn:aws:lambda:us-east-1:000000000000:durable:nope"
	path := "/2025-12-01/durable-executions/" + url.PathEscape(nopeARN) + "/state"
	rec := callInMemoryHandler(t, h, http.MethodGet, path, "{}")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDurableExecution_Stop(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)

	// Real wire: POST .../stop (not DELETE on the bare execution path).
	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/stop"), `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var stopOut lambda.StopDurableExecutionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopOut))
	assert.Positive(t, stopOut.StopTimestamp)

	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "STOPPED")
}

func TestDurableExecution_StopNonExistent(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	path := "/2025-12-01/durable-executions/" +
		url.PathEscape("arn:aws:lambda:us-east-1:000000000000:durable:none") + "/stop"
	rec := callInMemoryHandler(t, h, http.MethodPost, path, "{}")
	// DurableExecutionArn is required and must refer to a real execution — 404,
	// matching Get/GetState (this previously, incorrectly, returned 200).
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDurableExecution_CallbackSuccess(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// SendDurableExecutionCallbackSuccess addresses a CALLBACK operation by
	// CallbackId (a separate resource/path from DurableExecutionArn) — it
	// must first exist via a checkpoint Update.
	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"),
		`{"Updates":[{"Id":"cb-success","Type":"CALLBACK","Action":"START"}]}`)

	rec := callInMemoryHandler(
		t, h, http.MethodPost, durableExecCallbackURL("cb-success", "/succeed"), "result-payload",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/state"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"Id":"cb-success"`)
	assert.Contains(t, rec.Body.String(), "SUCCEEDED")
}

func TestDurableExecution_CallbackFailure(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"),
		`{"Updates":[{"Id":"cb-failure","Type":"CALLBACK","Action":"START"}]}`)

	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecCallbackURL("cb-failure", "/fail"),
		`{"Error":{"ErrorMessage":"boom"}}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/state"), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"Id":"cb-failure"`)
	assert.Contains(t, rec.Body.String(), "FAILED")
}

func TestDurableExecution_CallbackHeartbeat(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"),
		`{"Updates":[{"Id":"cb-heartbeat","Type":"CALLBACK","Action":"START"}]}`)

	rec := callInMemoryHandler(t, h, http.MethodPost, durableExecCallbackURL("cb-heartbeat", "/heartbeat"), "{}")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Heartbeat has no OperationStatus transition or history event in the real
	// API — the execution is still RUNNING and the callback op still STARTED.
	rec = callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
	assert.Contains(t, rec.Body.String(), "RUNNING")
}

func TestDurableExecution_ListByFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	e1 := "arn:aws:lambda:us-east-1:000000000000:durable:exec-1"
	e2 := "arn:aws:lambda:us-east-1:000000000000:durable:exec-2"
	p1 := "/2025-12-01/durable-executions/" + url.PathEscape(e1) + "/checkpoint"
	p2 := "/2025-12-01/durable-executions/" + url.PathEscape(e2) + "/checkpoint"
	callInMemoryHandler(t, h, http.MethodPost, p1, `{}`)
	callInMemoryHandler(t, h, http.MethodPost, p2, `{}`)

	// Real wire path: /2025-12-01/functions/{FunctionName}/durable-executions
	// (previously gopherstack served this — wrongly — at
	// /2025-12-01/durable-executions?FunctionArn=...).
	rec := callInMemoryHandler(t, h, http.MethodGet, durableExecListURL("some-func", ""), "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DurableExecutions")

	var out lambda.ListDurableExecutionsByFunctionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	// Neither exec-1 nor exec-2 is associated with "some-func": checkpoint-only
	// auto-creation has no function-association entry point (see
	// durableExecutionStore's doc comment) — the correctly wired, correctly
	// filtered endpoint legitimately returns none.
	assert.Empty(t, out.DurableExecutions)
}

// --- CheckpointDurableExecution tests ---

func TestCheckpointDurableExecution(t *testing.T) {
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

// --- Wire-shape field verification (the closed PARITY.md gap) ---

func TestDurableExecution_WireShapeFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *lambda.Handler)
		name  string
	}{
		{
			name: "arn_and_name_are_separate_fields",
			check: func(t *testing.T, h *lambda.Handler) {
				t.Helper()

				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
				rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")

				var out lambda.GetDurableExecutionOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, durableExecARN, out.DurableExecutionArn)
				assert.Equal(t, "test-exec-1", out.DurableExecutionName)
				assert.NotEqual(t, out.DurableExecutionArn, out.DurableExecutionName)
			},
		},
		{
			name: "timestamps_are_unix_epoch_numbers_not_iso_strings",
			check: func(t *testing.T, h *lambda.Handler) {
				t.Helper()

				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
				rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")

				body := lambdaParseBody(t, rec)
				st, ok := body["StartTimestamp"].(float64)
				require.True(t, ok, "StartTimestamp must decode as a JSON number, got %T", body["StartTimestamp"])
				assert.Greater(t, st, float64(1_700_000_000))
				_, hasStartTime := body["StartTime"]
				assert.False(t, hasStartTime, "StartTime (the old ISO-string field) must not be present")
			},
		},
		{
			name: "include_execution_data_false_omits_payload_fields",
			check: func(t *testing.T, h *lambda.Handler) {
				t.Helper()

				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
				rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("")+"?IncludeExecutionData=false", "{}")

				var out lambda.GetDurableExecutionOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.ExecutionDataIncluded)
				assert.False(t, *out.ExecutionDataIncluded)
				assert.Nil(t, out.InputPayload)
				assert.Nil(t, out.Result)
				assert.Nil(t, out.Error)
			},
		},
		{
			name: "include_execution_data_defaults_true",
			check: func(t *testing.T, h *lambda.Handler) {
				t.Helper()

				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
				rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")

				var out lambda.GetDurableExecutionOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.ExecutionDataIncluded)
				assert.True(t, *out.ExecutionDataIncluded)
			},
		},
		{
			name: "durable_config_omitted_when_unset",
			check: func(t *testing.T, h *lambda.Handler) {
				t.Helper()

				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
				rec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")

				body := lambdaParseBody(t, rec)
				_, hasDurableConfig := body["DurableConfig"]
				assert.False(t, hasDurableConfig)
			},
		},
		{
			name: "timed_out_status_value_exists_and_round_trips",
			check: func(t *testing.T, _ *lambda.Handler) {
				t.Helper()

				b, err := json.Marshal(lambda.DurableExecutionStatusTimedOut)
				require.NoError(t, err)
				assert.JSONEq(t, `"TIMED_OUT"`, string(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			tt.check(t, h)
		})
	}
}

// TestDurableExecution_CheckpointUpdatesOperations verifies that
// CheckpointDurableExecution's Updates (STEP/WAIT/CALLBACK/CONTEXT/
// CHAINED_INVOKE start+succeed) surface as real EventType history entries
// and as tracked Operations in GetDurableExecutionState — previously
// checkpoint discarded its request body entirely and GetDurableExecutionState
// always returned an empty/fabricated shape.
func TestDurableExecution_CheckpointUpdatesOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		opType       string
		startEvent   string
		succeedEvent string
	}{
		{name: "step", opType: "STEP", startEvent: "StepStarted", succeedEvent: "StepSucceeded"},
		{name: "wait", opType: "WAIT", startEvent: "WaitStarted", succeedEvent: "WaitSucceeded"},
		{name: "callback", opType: "CALLBACK", startEvent: "CallbackStarted", succeedEvent: "CallbackSucceeded"},
		{name: "context", opType: "CONTEXT", startEvent: "ContextStarted", succeedEvent: "ContextSucceeded"},
		{
			name: "chained_invoke", opType: "CHAINED_INVOKE",
			startEvent: "ChainedInvokeStarted", succeedEvent: "ChainedInvokeSucceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			opID := "op-" + tt.name

			startBody := fmt.Sprintf(`{"Updates":[{"Id":%q,"Type":%q,"Action":"START"}]}`, opID, tt.opType)
			rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), startBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var checkpointOut lambda.CheckpointDurableExecutionOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &checkpointOut))
			require.NotNil(t, checkpointOut.NewExecutionState)
			require.Len(t, checkpointOut.NewExecutionState.Operations, 1)
			assert.Equal(t, opID, checkpointOut.NewExecutionState.Operations[0].ID)
			assert.Equal(t, lambda.DurableOperationStatusStarted, checkpointOut.NewExecutionState.Operations[0].Status)

			succeedBody := fmt.Sprintf(`{"Updates":[{"Id":%q,"Type":%q,"Action":"SUCCEED"}]}`, opID, tt.opType)
			callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), succeedBody)

			histRec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/history"), "{}")
			var histOut lambda.GetDurableExecutionHistoryOutput
			require.NoError(t, json.Unmarshal(histRec.Body.Bytes(), &histOut))

			eventTypes := make([]string, 0, len(histOut.Events))
			for _, ev := range histOut.Events {
				eventTypes = append(eventTypes, ev.EventType)
			}
			assert.Contains(t, eventTypes, tt.startEvent)
			assert.Contains(t, eventTypes, tt.succeedEvent)

			stateRec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL("/state"), "{}")
			var stateOut lambda.GetDurableExecutionStateOutput
			require.NoError(t, json.Unmarshal(stateRec.Body.Bytes(), &stateOut))
			require.Len(t, stateOut.Operations, 2) // implicit root EXECUTION + this one

			var found bool
			for _, op := range stateOut.Operations {
				if op.ID != opID {
					continue
				}

				found = true
				assert.Equal(t, lambda.DurableOperationStatusSucceeded, op.Status)
				assert.NotNil(t, op.EndTimestamp)
			}
			assert.True(t, found, "operation %q not found in GetDurableExecutionState Operations", opID)
		})
	}
}

func TestDurableExecution_CallbacksTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		suffix     string
		body       string
		wantStatus int
		setup      bool
	}{
		{name: "succeed_known_callback", setup: true, suffix: "/succeed", body: "result", wantStatus: http.StatusOK},
		{name: "fail_known_callback", setup: true, suffix: "/fail", body: `{}`, wantStatus: http.StatusOK},
		{name: "heartbeat_known_callback", setup: true, suffix: "/heartbeat", body: `{}`, wantStatus: http.StatusOK},
		{
			name: "succeed_unknown_callback", setup: false, suffix: "/succeed", body: "result",
			wantStatus: http.StatusNotFound,
		},
		{name: "fail_unknown_callback", setup: false, suffix: "/fail", body: `{}`, wantStatus: http.StatusNotFound},
		{
			name: "heartbeat_unknown_callback", setup: false, suffix: "/heartbeat", body: `{}`,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			callbackID := "cb-" + tt.name

			if tt.setup {
				startBody := fmt.Sprintf(`{"Updates":[{"Id":%q,"Type":"CALLBACK","Action":"START"}]}`, callbackID)
				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), startBody)
			}

			rec := callInMemoryHandler(t, h, http.MethodPost, durableExecCallbackURL(callbackID, tt.suffix), tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDurableExecution_ListByFunctionQueryParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "no_filters", query: ""},
		{name: "name_filter", query: "DurableExecutionName=my-exec"},
		{name: "status_filter", query: "Statuses=RUNNING&Statuses=STOPPED"},
		{name: "reverse_order", query: "ReverseOrder=true"},
		{name: "time_range", query: "StartedAfter=2020-01-01T00%3A00%3A00Z&StartedBefore=2030-01-01T00%3A00%3A00Z"},
		{name: "pagination", query: "MaxItems=5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			rec := callInMemoryHandler(t, h, http.MethodGet, durableExecListURL("some-func", tt.query), "{}")
			require.Equal(t, http.StatusOK, rec.Code)

			var out lambda.ListDurableExecutionsByFunctionOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		})
	}
}

func TestDurableExecution_StopVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *lambda.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "with_custom_error",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
			},
			body:       `{"Error":{"ErrorMessage":"cancelled by operator"}}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "idempotent_second_stop",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/checkpoint"), `{}`)
				callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/stop"), `{}`)
			},
			body:       `{}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			tt.setup(t, h)

			rec := callInMemoryHandler(t, h, http.MethodPost, durableExecURL("/stop"), tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			getRec := callInMemoryHandler(t, h, http.MethodGet, durableExecURL(""), "{}")
			assert.Contains(t, getRec.Body.String(), "STOPPED")

			if tt.name == "with_custom_error" {
				var out lambda.GetDurableExecutionOutput
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
				require.NotNil(t, out.Error)
				require.NotNil(t, out.Error.ErrorMessage)
				assert.Equal(t, "cancelled by operator", *out.Error.ErrorMessage)
			}
		})
	}
}
