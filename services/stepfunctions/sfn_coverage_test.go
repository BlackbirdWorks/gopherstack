package stepfunctions_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const sfnPassDefinition = `{
"StartAt": "Step1",
"States": {
  "Step1": {"Type": "Pass", "End": true}
}}`

const sfnFailDefinition = `{
"StartAt": "Step1",
"States": {
  "Step1": {"Type": "Fail", "Error": "MyErr", "Cause": "test reason"}
}}`

// ---- getTags: resource with no tags vs. with tags ----

func TestHandler_GetTags_EmptyAndNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupTags   bool
		wantTagsLen int
	}{
		{
			name:        "no_tags_returns_empty_map",
			setupTags:   false,
			wantTagsLen: 0,
		},
		{
			name:        "with_tags_returns_them",
			setupTags:   true,
			wantTagsLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "tags-sm-"+tt.name)

			if tt.setupTags {
				rec := sfnPost(ctx, t, h, e, "TagResource",
					`{"resourceArn":"`+smARN+`","tags":{"mykey":"myval"}}`)
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			rec := sfnPost(ctx, t, h, e, "ListTagsForResource",
				`{"resourceArn":"`+smARN+`"}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			tags := resp["tags"].([]any)
			assert.Len(t, tags, tt.wantTagsLen)
		})
	}
}

// ---- ExtractResource edge cases ----

func TestHandler_ExtractResource_Coverage(t *testing.T) {
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

func TestHandler_StateMachineActions_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		// JSON unmarshal errors fall through to InternalServerError
		{
			name:     "CreateStateMachine_invalid_json",
			action:   "CreateStateMachine",
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "DeleteStateMachine_invalid_json",
			action:   "DeleteStateMachine",
			wantCode: http.StatusInternalServerError,
		},
		{name: "ListStateMachines_invalid_json", action: "ListStateMachines", wantCode: http.StatusInternalServerError},
		{
			name:     "DescribeStateMachine_invalid_json",
			action:   "DescribeStateMachine",
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "ListTagsForResource_invalid_json",
			action:   "ListTagsForResource",
			wantCode: http.StatusInternalServerError,
		},
		{name: "TagResource_invalid_json", action: "TagResource", wantCode: http.StatusInternalServerError},
		{name: "UntagResource_invalid_json", action: "UntagResource", wantCode: http.StatusInternalServerError},
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

// ---- executionActions: invalid JSON for each operation ----

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

func TestHandler_ListStateMachineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "success_empty_list",
			body:     `{"stateMachineArn":"arn:aws:states:us-east-1:123456:stateMachine:my-sm"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			rec := sfnPost(ctx, t, h, e, "ListStateMachineVersions", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "stateMachineVersions")
		})
	}
}

// ---- UpdateStateMachine ----

func TestHandler_UpdateStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "always_succeeds",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "update-sm")

			rec := sfnPost(ctx, t, h, e, "UpdateStateMachine",
				`{"stateMachineArn":"`+smARN+`","definition":"{}"}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "updateDate")
		})
	}
}

// ---- ListExecutions with status filter ----

func TestBackend_ListExecutions_WithStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusFilter string
	}{
		{
			name:         "filter_running_returns_no_error",
			statusFilter: "RUNNING",
		},
		{
			name:         "filter_succeeded_returns_no_error",
			statusFilter: "SUCCEEDED",
		},
		{
			name:         "no_filter_returns_all",
			statusFilter: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine(
				"filter-sm",
				`{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			_, err = b.StartExecution(sm.StateMachineArn, "exec1", `{}`)
			require.NoError(t, err)

			// Verify ListExecutions returns no error for each status filter
			execs, _, err := b.ListExecutions(sm.StateMachineArn, tt.statusFilter, "", 0)
			require.NoError(t, err)

			if tt.statusFilter == "" {
				assert.GreaterOrEqual(t, len(execs), 1, "unfiltered list should contain at least the started execution")
			}
		})
	}
}

// ---- parseNextToken via ListExecutions with pagination ----

func TestBackend_ListExecutions_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nextToken  string
		maxResults int
		wantLen    int
	}{
		{
			name:       "page_1_max_1",
			nextToken:  "",
			maxResults: 1,
			wantLen:    1,
		},
		{
			name:       "page_2_via_token",
			nextToken:  "1", // start from index 1
			maxResults: 10,
			wantLen:    1,
		},
		{
			name:       "invalid_token_treated_as_zero",
			nextToken:  "not-a-number",
			maxResults: 10,
			wantLen:    2,
		},
		{
			name:       "negative_token_treated_as_zero",
			nextToken:  "-5",
			maxResults: 10,
			wantLen:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine(
				"page-sm",
				`{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			// Create two executions so we have something to paginate
			_, _ = b.StartExecution(sm.StateMachineArn, "exec-a", `{}`)
			_, _ = b.StartExecution(sm.StateMachineArn, "exec-b", `{}`)

			// Wait for executions to complete to avoid race condition
			require.Eventually(t, func() bool {
				execs, _, listErr := b.ListExecutions(sm.StateMachineArn, "", "", 0)
				if listErr != nil {
					return false
				}

				doneCount := 0
				for _, e := range execs {
					if e.Status != "RUNNING" {
						doneCount++
					}
				}

				return doneCount == 2
			}, 5*time.Second, 50*time.Millisecond)

			execs, _, err := b.ListExecutions(sm.StateMachineArn, "", tt.nextToken, tt.maxResults)
			require.NoError(t, err)
			assert.Len(t, execs, tt.wantLen)
		})
	}
}

// ---- runParsedExecution: exec result with error in result object ----

func TestBackend_RunParsedExecution_FailState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		wantStatus string
	}{
		{
			name:       "fail_state_execution",
			definition: sfnFailDefinition,
			wantStatus: "FAILED",
		},
		{
			name:       "pass_state_execution",
			definition: sfnPassDefinition,
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine("run-sm-"+tt.name, tt.definition, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "run-exec", `{}`)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				desc, descErr := b.DescribeExecution(exec.ExecutionArn)

				return descErr == nil && desc.Status == tt.wantStatus
			}, 5*time.Second, 50*time.Millisecond)

			desc, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.Status)
		})
	}
}

// ---- Persistence: Handler Snapshot/Restore delegation ----

func TestSFNHandler_SnapshotRestore_Delegation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *stepfunctions.InMemoryBackend)
		check func(t *testing.T, b *stepfunctions.InMemoryBackend)
		name  string
	}{
		{
			name: "with_state_machine",
			setup: func(b *stepfunctions.InMemoryBackend) {
				_, _ = b.CreateStateMachine("snap-sm", sfnPassDefinition, "arn:role", "STANDARD")
			},
			check: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
				t.Helper()

				sms, _, err := b.ListStateMachines("", 0)
				require.NoError(t, err)
				assert.Len(t, sms, 1)
				assert.Equal(t, "snap-sm", sms[0].Name)
			},
		},
		{
			name:  "empty_backend",
			setup: func(_ *stepfunctions.InMemoryBackend) {},
			check: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
				t.Helper()

				sms, _, err := b.ListStateMachines("", 0)
				require.NoError(t, err)
				assert.Empty(t, sms)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			origBackend := stepfunctions.NewInMemoryBackend()
			tt.setup(origBackend)

			h, _ := newSFNHandler(t)
			// Create a handler wrapping origBackend
			origH := stepfunctions.NewHandler(origBackend)

			snap := origH.Snapshot()
			require.NotNil(t, snap)

			freshBackend := stepfunctions.NewInMemoryBackend()
			freshH := stepfunctions.NewHandler(freshBackend)
			require.NoError(t, freshH.Restore(snap))

			tt.check(t, freshBackend)

			_ = h
		})
	}
}

// ---- GetExecutionHistory: reverse order ----

func TestBackend_GetExecutionHistory_ReverseOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantFirst    string
		reverseOrder bool
	}{
		{
			name:         "normal_order",
			reverseOrder: false,
			wantFirst:    "ExecutionStarted",
		},
		{
			name:         "reverse_order_last_event_first",
			reverseOrder: true,
			wantFirst:    "ExecutionSucceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine("hist-sm", sfnPassDefinition, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "hist-exec", `{}`)
			require.NoError(t, err)

			// Wait for execution to complete
			require.Eventually(t, func() bool {
				desc, descErr := b.DescribeExecution(exec.ExecutionArn)

				return descErr == nil && desc.Status == "SUCCEEDED"
			}, 5*time.Second, 50*time.Millisecond)

			events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, tt.reverseOrder)
			require.NoError(t, err)
			require.NotEmpty(t, events)
			assert.Equal(t, tt.wantFirst, events[0].Type)
		})
	}
}

// ---- Handler: executionActions via HTTP ----

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
