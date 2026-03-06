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

func TestHandler_CreateStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo)
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "success returns ARN containing name",
			body:     makeSMBody("test-sm", validPassDef, "STANDARD"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["stateMachineArn"].(string), "test-sm")
			},
		},
		{
			name: "duplicate name returns conflict",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) {
				t.Helper()

				sfnPost(ctx, t, h, e, "CreateStateMachine",
					makeSMBody("dup", validPassDef, ""))
			},
			body:     makeSMBody("dup", validPassDef, ""),
			wantCode: http.StatusConflict,
		},
		{
			name:     "invalid definition returns bad request",
			body:     makeSMBody("invalid-sm", "{}", "STANDARD"),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			if tt.setup != nil {
				tt.setup(t, ctx, h, e)
			}

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec)
			}
		})
	}
}

func TestHandler_DeleteStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string
		bodyFn   func(setupArn string) string
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success deletes existing state machine",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string {
				t.Helper()

				return createSM(ctx, t, h, e, "del-sm")
			},
			bodyFn:   func(arn string) string { return `{"stateMachineArn":"` + arn + `"}` },
			wantCode: http.StatusOK,
		},
		{
			name:     "not found returns 404",
			body:     `{"stateMachineArn":"arn:aws:states:us-east-1:123:stateMachine:nonexistent"}`,
			wantCode: http.StatusNotFound,
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

			rec := sfnPost(ctx, t, h, e, "DeleteStateMachine", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListStateMachines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		smNames   []string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns all created state machines",
			smNames:   []string{"sm-1", "sm-2"},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			for _, smName := range tt.smNames {
				createSM(ctx, t, h, e, smName)
			}

			rec := sfnPost(ctx, t, h, e, "ListStateMachines", `{}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["stateMachines"].([]any), tt.wantCount)
		})
	}
}

func TestHandler_DescribeStateMachine(t *testing.T) {
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
			name: "success returns state machine details",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string {
				t.Helper()

				rec := sfnPost(ctx, t, h, e, "CreateStateMachine",
					makeSMBody("desc-sm", validPassDef, "EXPRESS"))
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["stateMachineArn"].(string)
			},
			bodyFn:   func(arn string) string { return `{"stateMachineArn":"` + arn + `"}` },
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var sm map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sm))
				assert.Equal(t, "EXPRESS", sm["type"])
			},
		},
		{
			name:     "not found returns 404",
			body:     `{"stateMachineArn":"arn:nonexistent"}`,
			wantCode: http.StatusNotFound,
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

			rec := sfnPost(ctx, t, h, e, "DescribeStateMachine", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec)
			}
		})
	}
}

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

func TestHandler_GetExecutionHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string
		bodyFn     func(setupResult string) string
		name       string
		body       string
		wantCode   int
		wantEvents int
	}{
		{
			name: "returns history events for execution",
			setup: func(t *testing.T, ctx context.Context, h *stepfunctions.Handler, e *echo.Echo) string {
				t.Helper()

				smArn := createSM(ctx, t, h, e, "hist-sm")
				execArn := startExec(ctx, t, h, e, smArn, "hist-exec")

				// Wait for the async execution to complete before checking history.
				require.Eventually(t, func() bool {
					rec := sfnPost(ctx, t, h, e, "DescribeExecution",
						`{"executionArn":"`+execArn+`"}`)
					if rec.Code != http.StatusOK {
						return false
					}
					var resp map[string]any
					if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
						return false
					}

					return resp["status"] != "RUNNING"
				}, 5*time.Second, 50*time.Millisecond)

				return execArn
			},
			bodyFn:     func(execArn string) string { return `{"executionArn":"` + execArn + `"}` },
			wantCode:   http.StatusOK,
			wantEvents: 4,
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

			rec := sfnPost(ctx, t, h, e, "GetExecutionHistory", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantEvents > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp["events"].([]any), tt.wantEvents)
			}
		})
	}
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     string
		wantCode int
	}{
		{
			name:     "tags state machine successfully",
			tags:     `{"env":"prod","team":"infra"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			arn := createSM(ctx, t, h, e, "tag-sm")
			rec := sfnPost(ctx, t, h, e, "TagResource",
				`{"resourceArn":"`+arn+`","tags":`+tt.tags+`}`)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "returns tags for tagged resource",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			arn := createSM(ctx, t, h, e, "list-tag-sm")
			sfnPost(ctx, t, h, e, "TagResource", `{"resourceArn":"`+arn+`","tags":{"env":"prod"}}`)

			rec := sfnPost(ctx, t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tags := resp["tags"].([]any)
			assert.NotEmpty(t, tags)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tagKeys      string
		wantTagKey   string
		wantTagValue string
		wantCode     int
		wantTagCount int
	}{
		{
			name:         "removes specified tag and leaves remaining tags",
			tagKeys:      `["team"]`,
			wantCode:     http.StatusOK,
			wantTagCount: 1,
			wantTagKey:   "env",
			wantTagValue: "prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			arn := createSM(ctx, t, h, e, "untag-sm")
			sfnPost(ctx, t, h, e, "TagResource",
				`{"resourceArn":"`+arn+`","tags":{"env":"prod","team":"infra"}}`)

			rec := sfnPost(ctx, t, h, e, "UntagResource",
				`{"resourceArn":"`+arn+`","tagKeys":`+tt.tagKeys+`}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			listRec := sfnPost(ctx, t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			tags := resp["tags"].([]any)
			assert.Len(t, tags, tt.wantTagCount)

			tag := tags[0].(map[string]any)
			assert.Equal(t, tt.wantTagKey, tag["key"])
			assert.Equal(t, tt.wantTagValue, tag["value"])
		})
	}
}

func TestHandler_ValidateStateMachineDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		wantResult string
		wantCode   int
		wantDiags  bool
	}{
		{
			name: "valid definition returns OK result with no diagnostics",
			definition: `{
"StartAt": "S",
"States": {"S": {"Type": "Pass", "End": true}}
}`,
			wantCode:   http.StatusOK,
			wantResult: "OK",
			wantDiags:  false,
		},
		{
			name:       "invalid definition returns FAIL result with diagnostics",
			definition: `{"StartAt": "Missing", "States": {"S": {"Type": "Pass", "End": true}}}`,
			wantCode:   http.StatusOK,
			wantResult: "FAIL",
			wantDiags:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			reqBody, err := json.Marshal(map[string]string{"definition": tt.definition})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "ValidateStateMachineDefinition", string(reqBody))
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantResult, resp["result"])

			diag, ok := resp["diagnostics"].([]any)
			require.True(t, ok)

			if tt.wantDiags {
				assert.NotEmpty(t, diag)
			} else {
				assert.Empty(t, diag)
			}
		})
	}
}
