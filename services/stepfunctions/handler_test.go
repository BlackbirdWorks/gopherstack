package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// validPassDef is a minimal valid ASL definition used across handler tests.
const validPassDef = `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`

func newSFNHandler(t *testing.T) (*stepfunctions.Handler, *echo.Echo) {
	t.Helper()

	bk := stepfunctions.NewInMemoryBackend()

	return stepfunctions.NewHandler(bk), echo.New()
}

func sfnPost(
	ctx context.Context,
	t *testing.T,
	h *stepfunctions.Handler,
	e *echo.Echo,
	action, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	}

	req.Header.Set("X-Amz-Target", "AmazonStates."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// makeSMBody builds a well-formed CreateStateMachine JSON request body.
// If smType is empty, the "type" field is omitted.
func makeSMBody(name, def, smType string) string {
	input := map[string]any{
		"name":       name,
		"definition": def,
		"roleArn":    "arn:role",
	}
	if smType != "" {
		input["type"] = smType
	}

	b, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func createSM(ctx context.Context, t *testing.T, h *stepfunctions.Handler, e *echo.Echo, name string) string {
	t.Helper()

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine",
		makeSMBody(name, validPassDef, ""))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["stateMachineArn"].(string)
}

func startExec(
	ctx context.Context,
	t *testing.T,
	h *stepfunctions.Handler,
	e *echo.Echo,
	smArn, execName string,
) string {
	t.Helper()

	rec := sfnPost(ctx, t, h, e, "StartExecution",
		`{"stateMachineArn":"`+smArn+`","name":"`+execName+`","input":"{}"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["executionArn"].(string)
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns service name", want: "StepFunctions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_Routing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		name     string
		method   string
		path     string
		target   string
		body     string
		wantCode int
	}{
		{
			name:     "GET / returns supported operations",
			method:   http.MethodGet,
			path:     "/",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var ops []string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
				assert.Contains(t, ops, "CreateStateMachine")
			},
		},
		{
			name:     "GET with path returns method not allowed",
			method:   http.MethodGet,
			path:     "/path",
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:     "POST without target returns bad request",
			method:   http.MethodPost,
			path:     "/",
			body:     "{}",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "POST with invalid target returns bad request",
			method:   http.MethodPost,
			path:     "/",
			body:     "{}",
			target:   "InvalidTarget",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "POST with unknown operation returns bad request",
			method:   http.MethodPost,
			path:     "/",
			body:     "{}",
			target:   "AmazonStates.UnknownOp",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "POST with invalid JSON returns internal server error",
			method:   http.MethodPost,
			path:     "/",
			body:     "not-json",
			target:   "AmazonStates.CreateStateMachine",
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequestWithContext(ctx, tt.method, tt.path, strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequestWithContext(ctx, tt.method, tt.path, nil)
			}

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec)
			}
		})
	}
}

func TestHandler_Shutdown_CancelsRunningExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "shutdown cancels execution goroutines",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			// Verify the handler satisfies the Shutdowner interface.
			var _ service.Shutdowner = h

			// Create a state machine with a Wait state so the execution stays RUNNING.
			waitStateDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
			smRec := sfnPost(ctx, t, h, e, "CreateStateMachine", makeSMBody("test-sm", waitStateDef, ""))
			require.Equal(t, http.StatusOK, smRec.Code)

			var smResp struct {
				StateMachineArn string `json:"stateMachineArn"`
			}
			require.NoError(t, json.Unmarshal(smRec.Body.Bytes(), &smResp))

			// Start an execution.
			execBody, err := json.Marshal(map[string]string{
				"stateMachineArn": smResp.StateMachineArn,
				"name":            "exec-1",
				"input":           "{}",
			})
			require.NoError(t, err)

			execRec := sfnPost(ctx, t, h, e, "StartExecution", string(execBody))
			require.Equal(t, http.StatusOK, execRec.Code)

			var execResp struct {
				ExecutionArn string `json:"executionArn"`
			}
			require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

			// Shutdown should cancel the long-running Wait execution and return promptly.
			h.Shutdown(t.Context())

			// After Shutdown, the cancelled execution goroutine should eventually
			// transition the execution out of RUNNING (to FAILED due to cancellation).
			require.Eventuallyf(t, func() bool {
				descBody, marshalErr := json.Marshal(map[string]string{"executionArn": execResp.ExecutionArn})
				if marshalErr != nil {
					return false
				}

				descRec := sfnPost(ctx, t, h, e, "DescribeExecution", string(descBody))
				if descRec.Code != http.StatusOK {
					return false
				}

				var descResp struct {
					Status string `json:"status"`
				}

				return json.Unmarshal(descRec.Body.Bytes(), &descResp) == nil && descResp.Status != "RUNNING"
			}, 5*time.Second, 50*time.Millisecond,
				"[%s] execution should leave RUNNING state after Shutdown", tt.name)
		})
	}
}

func TestClassifyError_InvalidExecutionInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "input_within_limit_succeeds",
			input:    `{}`,
			wantCode: http.StatusOK,
			wantErr:  false,
		},
		{
			name:     "input_over_256kib_returns_400",
			input:    `{"data":"` + strings.Repeat("x", 256*1024+1) + `"}`,
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "input-limit-sm-"+tt.name)

			body, err := json.Marshal(map[string]string{
				"stateMachineArn": smARN,
				"input":           tt.input,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "StartExecution", string(body))

			if tt.wantErr {
				assert.Equal(t, tt.wantCode, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "InvalidExecutionInput", resp["__type"])
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestClassifyError_InvalidArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleArn  string
		wantCode int
	}{
		{
			name:     "valid_role_arn",
			roleArn:  "arn:aws:iam::123456789012:role/sfn-role",
			wantCode: http.StatusOK,
		},
		{
			name:     "non_arn_prefix_returns_400",
			roleArn:  "not-an-arn",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "arn_with_whitespace_returns_400",
			roleArn:  "arn:aws:iam::123456 789012:role/sfn-role",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			body, err := json.Marshal(map[string]string{
				"name":       "arn-check-sm-" + tt.name,
				"definition": sfnPassDefinition,
				"roleArn":    tt.roleArn,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "InvalidArn", resp["__type"])
			}
		})
	}
}

// ─── Name Validation ─────────────────────────────────────────────────────────

func TestHTTP_StateMachineDoesNotExist_Returns404(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "DescribeStateMachine",
		`{"stateMachineArn":"arn:aws:states:us-east-1:123:stateMachine:ghost"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHTTP_ExecutionDoesNotExist_Returns404(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "DescribeExecution",
		`{"executionArn":"arn:aws:states:us-east-1:123:execution:sm:ghost"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHTTP_StateMachineAlreadyExists_Returns409(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	createSM(ctx, t, h, e, "dupe-http-sm")

	// Different definition triggers StateMachineAlreadyExists (409).
	altDef := `{"StartAt":"T","States":{"T":{"Type":"Succeed"}}}`
	rec := sfnPost(ctx, t, h, e, "CreateStateMachine",
		makeSMBody("dupe-http-sm", altDef, ""))
	assert.Equal(t, http.StatusConflict, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "StateMachineAlreadyExists", out["__type"])
}

func TestHTTP_InvalidDefinition_Returns400(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine",
		`{"name":"inv-def-sm","definition":"{\"bad\":true}","roleArn":"arn:role"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "InvalidDefinition", out["__type"])
}

func TestHTTP_ActivityDoesNotExist_Returns404(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "DescribeActivity",
		`{"activityArn":"arn:aws:states:us-east-1:123:activity:ghost"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── CreateStateMachine HTTP response fields ──────────────────────────────────

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantPrio int
	}{
		{
			name:     "returns_100",
			wantPrio: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			assert.Equal(t, tt.wantPrio, h.MatchPriority())
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "sfn_target_matches",
			target:    "AmazonStates.CreateStateMachine",
			wantMatch: true,
		},
		{
			name:      "sqs_target_no_match",
			target:    "AmazonSQS.CreateQueue",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			matcher := h.RouteMatcher()
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			assert.Equal(t, tt.wantMatch, matcher(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "known_operation",
			target: "AmazonStates.ListStateMachines",
			wantOp: "ListStateMachines",
		},
		{
			name:   "no_target_header",
			target: "",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			assert.Equal(t, tt.wantOp, h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "name_field",
			body:         `{"name":"my-sm"}`,
			wantResource: "my-sm",
		},
		{
			name:         "state_machine_arn",
			body:         `{"stateMachineArn":"arn:aws:states:us-east-1:123:stateMachine:test"}`,
			wantResource: "arn:aws:states:us-east-1:123:stateMachine:test",
		},
		{
			name:         "execution_arn",
			body:         `{"executionArn":"arn:aws:states:us-east-1:123:execution:test:exec1"}`,
			wantResource: "arn:aws:states:us-east-1:123:execution:test:exec1",
		},
		{
			name:         "empty_body",
			body:         `{}`,
			wantResource: "",
		},
		{
			name:         "bad_json",
			body:         `not-json`,
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", stringReader(tt.body))
			assert.Equal(t, tt.wantResource, h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func stringReader(s string) *strings.Reader {
	return strings.NewReader(s)
}

func newSFBackend() *stepfunctions.InMemoryBackend {
	return stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

// newSFNHandlerWithBackend creates a handler and echo instance backed by the provided backend.
func newSFNHandlerWithBackend(
	bk *stepfunctions.InMemoryBackend,
) (*stepfunctions.Handler, *echo.Echo) {
	return stepfunctions.NewHandler(bk), echo.New()
}

func TestHandler_ExtractResource_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		target   string
		body     string
		wantName string
	}{
		{
			name:     "name_field_extracted",
			target:   "AmazonStates.CreateStateMachine",
			body:     `{"name":"my-sm"}`,
			wantName: "my-sm",
		},
		{
			name:     "stateMachineArn_extracted",
			target:   "AmazonStates.DescribeStateMachine",
			body:     `{"stateMachineArn":"arn:aws:states:us-east-1:123456:stateMachine:my-sm"}`,
			wantName: "arn:aws:states:us-east-1:123456:stateMachine:my-sm",
		},
		{
			name:     "executionArn_extracted",
			target:   "AmazonStates.DescribeExecution",
			body:     `{"executionArn":"arn:aws:states:us-east-1:123456:execution:my-sm:exec1"}`,
			wantName: "arn:aws:states:us-east-1:123456:execution:my-sm:exec1",
		},
		{
			name:     "empty_json_returns_empty",
			target:   "AmazonStates.ListStateMachines",
			body:     `{}`,
			wantName: "",
		},
		{
			name:     "invalid_json_returns_empty",
			target:   "AmazonStates.CreateStateMachine",
			body:     `not-json`,
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			ctx := t.Context()

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/", strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
			}
			req.Header.Set("X-Amz-Target", tt.target)

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

// ---- stateMachineActions: invalid JSON for each operation ----

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), h.HandlerOpsLen())
}

// TestRefinement1_HandlerOpsLenHelper verifies the HandlerOpsLen export helper.
func TestHandlerOpsLenHelper(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(b)
	assert.Equal(t, 38, h.HandlerOpsLen())
}

// TestRefinement1_HandlersErrorResponse verifies that unknown operations return 400.
func TestHandlersErrorResponse(t *testing.T) {
	t.Parallel()

	h, e := newSFNHandler(t)
	rec := sfnPost(t.Context(), t, h, e, "NonExistentOperation", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
