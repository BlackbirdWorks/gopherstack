package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// TestHandler_ActivityOperations exercises all activity handler paths via HTTP.
func TestHandler_ActivityOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bodyFn   func(actARN string) string
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateActivity_ok",
			action: "CreateActivity",
			bodyFn: func(_ string) string {
				return `{"name":"test-activity"}`
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "CreateActivity_duplicate",
			action: "CreateActivity",
			bodyFn: func(_ string) string {
				return `{"name":"dup-act"}`
			},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DescribeActivity_ok",
			action: "DescribeActivity",
			bodyFn: func(actARN string) string {
				return `{"activityArn":"` + actARN + `"}`
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeActivity_not_found",
			action: "DescribeActivity",
			bodyFn: func(_ string) string {
				return `{"activityArn":"arn:aws:states:us-east-1:123456789012:activity:nosuch"}`
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListActivities_ok",
			action: "ListActivities",
			bodyFn: func(_ string) string {
				return `{}`
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteActivity_ok",
			action: "DeleteActivity",
			bodyFn: func(actARN string) string {
				return `{"activityArn":"` + actARN + `"}`
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteActivity_not_found",
			action: "DeleteActivity",
			bodyFn: func(_ string) string {
				return `{"activityArn":"arn:aws:states:us-east-1:123456789012:activity:nosuch"}`
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			// Create a reusable activity for tests that need an ARN.
			var actARN string

			switch tt.action {
			case "DescribeActivity", "DeleteActivity":
				if tt.bodyFn(actARN) != `{"activityArn":"arn:aws:states:us-east-1:123456789012:activity:nosuch"}` {
					rec := sfnPost(ctx, t, h, e, "CreateActivity", `{"name":"setup-act-`+tt.name+`"}`)
					require.Equal(t, http.StatusOK, rec.Code)
					var resp map[string]any
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
					actARN = resp["activityArn"].(string)
				}
			case "CreateActivity":
				if tt.name == "CreateActivity_duplicate" {
					// Pre-create to force duplicate error.
					rec := sfnPost(ctx, t, h, e, "CreateActivity", `{"name":"dup-act"}`)
					require.Equal(t, http.StatusOK, rec.Code)
				}
			}

			body := tt.bodyFn(actARN)
			rec := sfnPost(ctx, t, h, e, tt.action, body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_CreateAndListActivities verifies create + list activity workflow.
func TestHandler_CreateAndListActivities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		actNames   []string
		wantMinLen int
		wantMaxLen int
	}{
		{
			name:       "no_activities",
			actNames:   nil,
			wantMinLen: 0,
			wantMaxLen: 0,
		},
		{
			name:       "two_activities",
			actNames:   []string{"act-alpha", "act-beta"},
			wantMinLen: 2,
			wantMaxLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			for _, name := range tt.actNames {
				rec := sfnPost(ctx, t, h, e, "CreateActivity", `{"name":"`+name+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := sfnPost(ctx, t, h, e, "ListActivities", `{}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			activities, ok := resp["activities"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(activities), tt.wantMinLen)
			assert.LessOrEqual(t, len(activities), tt.wantMaxLen)
		})
	}
}

// TestHandler_SendTaskOperations exercises SendTaskSuccess, Failure, and Heartbeat.
func TestHandler_SendTaskOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildBody func(token string) string
		name      string
		action    string
		wantCode  int
	}{
		{
			name:   "SendTaskSuccess_invalid_token",
			action: "SendTaskSuccess",
			buildBody: func(_ string) string {
				return `{"taskToken":"invalid","output":"{}"}`
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "SendTaskFailure_invalid_token",
			action: "SendTaskFailure",
			buildBody: func(_ string) string {
				return `{"taskToken":"invalid","error":"MyErr","cause":"reason"}`
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "SendTaskHeartbeat_invalid_token",
			action: "SendTaskHeartbeat",
			buildBody: func(_ string) string {
				return `{"taskToken":"invalid"}`
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			rec := sfnPost(ctx, t, h, e, tt.action, tt.buildBody(""))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_SendTaskSuccess_WithRealToken verifies the full activity task cycle
// via the HTTP handler.
func TestHandler_SendTaskSuccess_WithRealToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		output string
	}{
		{
			name:   "task_success",
			output: `{"result":"done"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			// Create an activity.
			rec := sfnPost(ctx, t, h, e, "CreateActivity", `{"name":"send-act-`+tt.name+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var actResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &actResp))
			actARN := actResp["activityArn"].(string)

			// Enqueue a task by calling InvokeActivity from the backend.
			bk, ok := h.Backend.(*stepfunctions.InMemoryBackend)
			require.True(t, ok)

			taskCh := make(chan string, 1)
			go func() {
				out, err := bk.InvokeActivity(t.Context(), actARN, `{"in":1}`, 0)
				if err == nil {
					taskCh <- out
				} else {
					taskCh <- ""
				}
			}()

			// Poll for the task via the handler.
			var taskToken string

			require.Eventually(t, func() bool {
				pollCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()

				// Use the backend directly since GetActivityTask is context-aware.
				task, pollErr := bk.GetActivityTask(pollCtx, actARN, "worker")
				if pollErr != nil || task == nil || task.TaskToken == "" {
					return false
				}

				taskToken = task.TaskToken

				return true
			}, 5*time.Second, 50*time.Millisecond)

			require.NotEmpty(t, taskToken)

			// Send success via HTTP handler.
			body, _ := json.Marshal(map[string]string{
				"taskToken": taskToken,
				"output":    tt.output,
			})
			rec = sfnPost(ctx, t, h, e, "SendTaskSuccess", string(body))
			assert.Equal(t, http.StatusOK, rec.Code)

			select {
			case out := <-taskCh:
				assert.Equal(t, tt.output, out)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for InvokeActivity to complete")
			}
		})
	}
}

// TestHandler_GetActivityTask_HTTP verifies GetActivityTask via the HTTP handler.
func TestHandler_GetActivityTask_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  time.Duration
		wantCode int
	}{
		{
			name:     "no_task_available_returns_empty",
			timeout:  100 * time.Millisecond,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			// Create an activity.
			rec := sfnPost(ctx, t, h, e, "CreateActivity", `{"name":"poll-act-`+tt.name+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var actResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &actResp))
			actARN := actResp["activityArn"].(string)

			// GetActivityTask with a short timeout — should return empty.
			pollCtx, cancel := context.WithTimeout(ctx, tt.timeout)
			defer cancel()

			body, _ := json.Marshal(map[string]string{"activityArn": actARN, "workerName": "w1"})
			rec = sfnPost(pollCtx, t, h, e, "GetActivityTask", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			// With no task available, taskToken should be absent or empty.
			token, _ := resp["taskToken"].(string)
			assert.Empty(t, token)
		})
	}
}

// TestHandler_GetActivityTask_NotFound verifies 404 for unknown activity ARN.
func TestHandler_GetActivityTask_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "unknown_activity_arn",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			rec := sfnPost(ctx, t, h, e, "GetActivityTask",
				`{"activityArn":"arn:aws:states:us-east-1:123456789012:activity:nosuch","workerName":"w1"}`)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_StartSyncExecution_HTTP exercises the StartSyncExecution handler path.
func TestHandler_StartSyncExecution_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		smType     string
		definition string
		wantStatus string
		wantCode   int
	}{
		{
			name:       "express_success",
			smType:     "EXPRESS",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`,
			wantCode:   http.StatusOK,
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "standard_sm_rejected",
			smType:     "STANDARD",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			// Create state machine.
			rec := sfnPost(ctx, t, h, e, "CreateStateMachine",
				makeSMBody("sync-sm-"+tt.name, tt.definition, tt.smType))
			require.Equal(t, http.StatusOK, rec.Code)

			var smResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &smResp))
			smARN := smResp["stateMachineArn"].(string)

			body, _ := json.Marshal(map[string]string{
				"stateMachineArn": smARN,
				"name":            "sync-exec-" + tt.name,
				"input":           `{}`,
			})
			rec = sfnPost(ctx, t, h, e, "StartSyncExecution", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantStatus != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantStatus, resp["status"])
			}
		})
	}
}

// TestHandler_Reset clears all in-memory state including tags.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "reset_clears_state_machines_and_tags",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			// Create a state machine and tag it.
			smARN := createSM(ctx, t, h, e, "reset-sm-"+tt.name)
			rec := sfnPost(ctx, t, h, e, "TagResource",
				`{"resourceArn":"`+smARN+`","tags":{"env":"test"}}`)
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify the SM exists.
			rec = sfnPost(ctx, t, h, e, "DescribeStateMachine",
				`{"stateMachineArn":"`+smARN+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			// Reset the handler.
			h.Reset()

			// State machine should no longer exist.
			rec = sfnPost(ctx, t, h, e, "DescribeStateMachine",
				`{"stateMachineArn":"`+smARN+`"}`)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// Tags should be cleared.
			rec = sfnPost(ctx, t, h, e, "ListTagsForResource",
				`{"resourceArn":"`+smARN+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Empty(t, resp["tags"])
		})
	}
}

// TestBackend_Reset verifies that InMemoryBackend.Reset clears all state.
func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "reset_clears_all_maps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			// Create some resources.
			sm, err := b.CreateStateMachine(context.Background(), "reset-sm-"+tt.name, exprPassDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			_, err = b.StartExecution(sm.StateMachineArn, "exec-1", `{}`)
			require.NoError(t, err)

			_, err = b.CreateActivity(context.Background(), "reset-act-" + tt.name)
			require.NoError(t, err)

			// Reset.
			b.Reset()

			// State machines should be gone.
			sms, _, err := b.ListStateMachines(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Empty(t, sms)

			// Activities should be gone.
			acts, _, err := b.ListActivities(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Empty(t, acts)
		})
	}
}

// TestBackend_ListActivities_Pagination verifies that ListActivities paginates correctly.
func TestBackend_ListActivities_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		count      int
		maxResults int
		wantLen    int
		wantNext   bool
	}{
		{
			name:       "no_activities",
			count:      0,
			maxResults: 10,
			wantLen:    0,
			wantNext:   false,
		},
		{
			name:       "all_fit",
			count:      3,
			maxResults: 10,
			wantLen:    3,
			wantNext:   false,
		},
		{
			name:       "paginated",
			count:      5,
			maxResults: 2,
			wantLen:    2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			for i := range tt.count {
				_, err := b.CreateActivity(context.Background(), "pag-act-" + tt.name + "-" + strconv.Itoa(i))
				require.NoError(t, err)
			}

			acts, next, err := b.ListActivities(context.Background(), "", tt.maxResults)
			require.NoError(t, err)
			assert.Len(t, acts, tt.wantLen)

			if tt.wantNext {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
		})
	}
}

// TestHandler_Persistence_WithActivitiesAndTags verifies that activities and tags
// survive a Snapshot/Restore cycle.
func TestHandler_Persistence_WithActivitiesAndTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		actName  string
		tagKey   string
		tagValue string
	}{
		{
			name:     "activity_and_tag_survive_restore",
			actName:  "persist-act",
			tagKey:   "env",
			tagValue: "staging",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			origBk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			origH := stepfunctions.NewHandler(origBk)

			_, err := origBk.CreateActivity(context.Background(), tt.actName)
			require.NoError(t, err)

			sm, err := origBk.CreateStateMachine(context.Background(), "persist-sm", exprPassDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			// Tag the SM via the helper.
			origH.SetTagsForTest(sm.StateMachineArn, map[string]string{tt.tagKey: tt.tagValue})

			// Snapshot and restore.
			snap := origH.Snapshot()
			require.NotNil(t, snap)

			freshBk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			freshH := stepfunctions.NewHandler(freshBk)
			require.NoError(t, freshH.Restore(snap))

			// Activity should be restored.
			acts, _, err := freshBk.ListActivities(context.Background(), "", 0)
			require.NoError(t, err)
			require.Len(t, acts, 1)
			assert.Equal(t, tt.actName, acts[0].Name)

			// Tags should be restored.
			tagMap := freshH.GetTagsForTest(sm.StateMachineArn)
			assert.Equal(t, tt.tagValue, tagMap[tt.tagKey])
		})
	}
}

// TestHandler_Restore_LegacyFormat verifies that a backend-only snapshot
// (without the wrapper) is accepted.
func TestHandler_Restore_LegacyFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "legacy_backend_only_snapshot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			origBk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			_, err := origBk.CreateStateMachine(context.Background(), "legacy-sm", exprPassDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			// Take a backend-only snapshot (bypasses handler wrapper).
			legacySnap := origBk.Snapshot()
			require.NotNil(t, legacySnap)

			freshBk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			freshH := stepfunctions.NewHandler(freshBk)

			// Restore with raw backend snapshot — no handlerSnapshot wrapper.
			require.NoError(t, freshH.Restore(legacySnap))

			sms, _, err := freshBk.ListStateMachines(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Len(t, sms, 1)
			assert.Equal(t, "legacy-sm", sms[0].Name)
		})
	}
}
