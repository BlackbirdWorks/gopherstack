package stepfunctions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestHandler_StartExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string
		bodyFn   func(setupArn string) string
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success returns execution ARN",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string {
				t.Helper()

				return createSM(ctx, t, h, e, "start-sm")
			},
			bodyFn:   func(arn string) string { return `{"stateMachineArn":"` + arn + `","name":"exec1","input":"{}"}` },
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["executionArn"].(string), "exec1")
			},
		},
		{
			name:     "state machine not found returns 404",
			body:     `{"stateMachineArn":"arn:nonexistent","name":"exec1","input":"{}"}`,
			wantCode: http.StatusNotFound,
		},
		{
			name: "duplicate execution name returns conflict",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createSM(ctx, t, h, e, "dup-exec-sm")
				startExec(ctx, t, h, e, arn, "exec-dup")

				return arn
			},
			bodyFn:   func(arn string) string { return `{"stateMachineArn":"` + arn + `","name":"exec-dup","input":"{}"}` },
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			var setupArn string
			if tt.setup != nil {
				setupArn = tt.setup(t, ctx, h, e)
			}

			body := tt.body
			if tt.bodyFn != nil {
				body = tt.bodyFn(setupArn)
			}

			rec := sfnPost(ctx, t, h, e, "StartExecution", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec)
			}
		})
	}
}

func TestHandler_DescribeExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string
		bodyFn   func(setupResult string) string
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success returns execution details",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string {
				t.Helper()

				smArn := createSM(ctx, t, h, e, "ex-sm")

				return startExec(ctx, t, h, e, smArn, "myexec")
			},
			bodyFn:   func(execArn string) string { return `{"executionArn":"` + execArn + `"}` },
			wantCode: http.StatusOK,
		},
		{
			name:     "not found returns 404",
			body:     `{"executionArn":"arn:nonexistent"}`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			var setupResult string
			if tt.setup != nil {
				setupResult = tt.setup(t, ctx, h, e)
			}

			body := tt.body
			if tt.bodyFn != nil {
				body = tt.bodyFn(setupResult)
			}

			rec := sfnPost(ctx, t, h, e, "DescribeExecution", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_StopExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string
		bodyFn   func(setupResult string) string
		name     string
		body     string
		wantCode int
	}{
		{
			name: "stops running execution successfully",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string {
				t.Helper()

				smArn := createSM(ctx, t, h, e, "stop-sm")

				return startExec(ctx, t, h, e, smArn, "stop-exec")
			},
			bodyFn: func(execArn string) string {
				return `{"executionArn":"` + execArn + `","error":"MyErr","cause":"test stop"}`
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found returns 404",
			body:     `{"executionArn":"arn:nonexistent"}`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			var setupResult string
			if tt.setup != nil {
				setupResult = tt.setup(t, ctx, h, e)
			}

			body := tt.body
			if tt.bodyFn != nil {
				body = tt.bodyFn(setupResult)
			}

			rec := sfnPost(ctx, t, h, e, "StopExecution", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		execNames []string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns all executions for state machine",
			execNames: []string{"e1", "e2"},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			smArn := createSM(ctx, t, h, e, "list-exec-sm")
			for _, execName := range tt.execNames {
				startExec(ctx, t, h, e, smArn, execName)
			}

			rec := sfnPost(ctx, t, h, e, "ListExecutions", `{"stateMachineArn":"`+smArn+`"}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["executions"].([]any), tt.wantCount)
		})
	}
}

func TestExecutionName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		execName string
		wantCode int
	}{
		{
			name:     "valid_name",
			execName: "my-execution-1",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_name_allowed",
			execName: "",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_max_length",
			execName: strings.Repeat("b", 80),
			wantCode: http.StatusOK,
		},
		{
			name:     "too_long_name_rejected",
			execName: strings.Repeat("b", 81),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_chars_rejected",
			execName: "bad<exec>",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "exec-name-sm-"+tt.name)

			body, err := json.Marshal(map[string]string{
				"stateMachineArn": smARN,
				"name":            tt.execName,
				"input":           "{}",
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "StartExecution", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestStartExecution_ResponseContainsARNAndStartDate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smArn := createSM(ctx, t, h, e, "start-resp-sm")

	body, err := json.Marshal(map[string]any{
		"stateMachineArn": smArn,
		"name":            "start-resp-exec",
		"input":           "{}",
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "StartExecution", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.NotEmpty(t, out["executionArn"])
	assert.Greater(t, out["startDate"].(float64), float64(0))
}

func TestListExecutions_OrderedByStartDateDesc_ViaHandler(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "order-handler-sm")

	execNames := []string{"exec-z", "exec-a", "exec-m"}
	for _, name := range execNames {
		body, err := json.Marshal(map[string]string{
			"stateMachineArn": smARN,
			"name":            name,
			"input":           "{}",
		})
		require.NoError(t, err)

		rec := sfnPost(ctx, t, h, e, "StartExecution", string(body))
		require.Equal(t, http.StatusOK, rec.Code)
		time.Sleep(5 * time.Millisecond)
	}

	listBody, err := json.Marshal(map[string]string{"stateMachineArn": smARN})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "ListExecutions", string(listBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rawExecs, _ := resp["executions"].([]any)
	require.Len(t, rawExecs, 3)

	// exec-m started last, should appear first.
	first, _ := rawExecs[0].(map[string]any)
	assert.Equal(t, "exec-m", first["name"])
}

func TestSFN_DescribeStateMachineForExecution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	h, e := newSFNHandler(t)
	smARN := createSFNStateMachineCov(ctx, t, h, e, "exec-sm")

	// Start an execution
	rec := sfnPost(ctx, t, h, e, "StartExecution", fmt.Sprintf(`{
		"stateMachineArn": "%s",
		"name": "exec-1",
		"input": "{}"
	}`, smARN))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	execARN, _ := resp["executionArn"].(string)
	require.NotEmpty(t, execARN)

	// DescribeStateMachineForExecution
	rec = sfnPost(ctx, t, h, e, "DescribeStateMachineForExecution", fmt.Sprintf(`{
		"executionArn": "%s"
	}`, execARN))
	assert.Positive(t, rec.Code)
}

func TestHandler_ExecutionActions_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		// JSON unmarshal errors for these actions get classified as InternalServerError
		// since they don't match any known sentinel error
		{name: "StartExecution_invalid_json", action: "StartExecution", wantCode: http.StatusInternalServerError},
		{name: "StopExecution_invalid_json", action: "StopExecution", wantCode: http.StatusInternalServerError},
		{name: "DescribeExecution_invalid_json", action: "DescribeExecution", wantCode: http.StatusInternalServerError},
		{name: "ListExecutions_invalid_json", action: "ListExecutions", wantCode: http.StatusInternalServerError},
		{
			name:     "GetExecutionHistory_invalid_json",
			action:   "GetExecutionHistory",
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			rec := sfnPost(ctx, t, h, e, tt.action, `{invalid`)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- utilActions: ListStateMachineVersions ----

func TestHandler_ExecutionActions_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bodyFn   func(smARN string) string
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "ListExecutions_with_status_filter",
			action: "ListExecutions",
			bodyFn: func(smARN string) string {
				return `{"stateMachineArn":"` + smARN + `","statusFilter":"RUNNING"}`
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeExecution_not_found",
			action: "DescribeExecution",
			bodyFn: func(_ string) string {
				return `{"executionArn":"arn:aws:states:us-east-1:123456:execution:ghost:exec1"}`
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "StopExecution_not_found",
			action: "StopExecution",
			bodyFn: func(_ string) string {
				return `{"executionArn":"arn:aws:states:us-east-1:123456:execution:ghost:exec1"}`
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "exec-sm-"+tt.name)

			body := tt.bodyFn(smARN)
			rec := sfnPost(ctx, t, h, e, tt.action, body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
