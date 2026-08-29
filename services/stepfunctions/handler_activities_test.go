package stepfunctions_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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
			// AWS: DeleteActivity's own error switch models only InvalidArn --
			// no ActivityDoesNotExist -- so it is idempotent on a missing
			// activity.
			name:   "DeleteActivity_not_found_is_idempotent",
			action: "DeleteActivity",
			bodyFn: func(_ string) string {
				return `{"activityArn":"arn:aws:states:us-east-1:123456789012:activity:nosuch"}`
			},
			wantCode: http.StatusOK,
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
			sm, err := b.CreateStateMachine(
				context.Background(),
				"reset-sm-"+tt.name,
				exprPassDef,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			_, err = b.StartExecution(sm.StateMachineArn, "exec-1", `{}`)
			require.NoError(t, err)

			_, err = b.CreateActivity(context.Background(), "reset-act-"+tt.name)
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
				_, err := b.CreateActivity(context.Background(), "pag-act-"+tt.name+"-"+strconv.Itoa(i))
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

			sm, err := origBk.CreateStateMachine(
				context.Background(),
				"persist-sm",
				exprPassDef,
				"arn:role",
				"STANDARD",
			)
			require.NoError(t, err)

			// Tag the SM via the helper.
			origH.SetTagsForTest(sm.StateMachineArn, map[string]string{tt.tagKey: tt.tagValue})

			// Snapshot and restore.
			snap := origH.Snapshot(t.Context())
			require.NotNil(t, snap)

			freshBk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			freshH := stepfunctions.NewHandler(freshBk)
			require.NoError(t, freshH.Restore(t.Context(), snap))

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
			legacySnap := origBk.Snapshot(t.Context())
			require.NotNil(t, legacySnap)

			freshBk := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			freshH := stepfunctions.NewHandler(freshBk)

			// Restore with raw backend snapshot — no handlerSnapshot wrapper.
			require.NoError(t, freshH.Restore(t.Context(), legacySnap))

			sms, _, err := freshBk.ListStateMachines(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Len(t, sms, 1)
			assert.Equal(t, "legacy-sm", sms[0].Name)
		})
	}
}

func TestActivityName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		actName  string
		wantCode int
	}{
		{
			name:     "valid_name",
			actName:  "my-activity",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_name_rejected",
			actName:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "too_long_name_rejected",
			actName:  strings.Repeat("c", 81),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_chars_rejected",
			actName:  "bad|name",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			body, err := json.Marshal(map[string]string{"name": tt.actName})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateActivity", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ─── Tags in CreateStateMachine ──────────────────────────────────────────────

func TestBackend_ValidateName_Activity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		actName string
	}{
		{
			name:    "valid_name",
			actName: "my-activity",
			wantErr: nil,
		},
		{
			name:    "empty_name_invalid",
			actName: "",
			wantErr: stepfunctions.ErrInvalidName,
		},
		{
			name:    "name_too_long_invalid",
			actName: strings.Repeat("a", 81),
			wantErr: stepfunctions.ErrInvalidName,
		},
		{
			name:    "name_with_invalid_chars",
			actName: "bad#activity",
			wantErr: stepfunctions.ErrInvalidName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			_, err := b.CreateActivity(context.Background(), tt.actName)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ─── SetStateMachineConfigurations with Encryption ───────────────────────────

func TestARN_Activity(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	act, err := b.CreateActivity(context.Background(), "my-activity")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:states:us-east-1:123456789012:activity:my-activity", act.ActivityArn)
}

func TestActivityName_TooLong(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	longName := strings.Repeat("x", 81)
	_, err := b.CreateActivity(context.Background(), longName)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidName)
}

func TestActivity_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	act, err := b.CreateActivity(context.Background(), "my-act")
	require.NoError(t, err)
	assert.Equal(t, "my-act", act.Name)
	assert.NotEmpty(t, act.ActivityArn)
	assert.Greater(t, act.CreationDate, float64(0))

	got, err := b.DescribeActivity(act.ActivityArn)
	require.NoError(t, err)
	assert.Equal(t, act.ActivityArn, got.ActivityArn)
}

func TestActivity_DuplicateReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateActivity(context.Background(), "dup-act")
	require.NoError(t, err)

	_, err = b.CreateActivity(context.Background(), "dup-act")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityAlreadyExists)
}

func TestActivity_Delete(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	act, err := b.CreateActivity(context.Background(), "del-act")
	require.NoError(t, err)

	require.NoError(t, b.DeleteActivity(act.ActivityArn))

	_, err = b.DescribeActivity(act.ActivityArn)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityDoesNotExist)
}

// AWS: DeleteActivity's own error switch models only InvalidArn -- no
// ActivityDoesNotExist -- so it is idempotent on a missing activity.
func TestActivity_DeleteNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.DeleteActivity("arn:aws:states:us-east-1:123:activity:ghost")
	require.NoError(t, err)
}

func TestActivity_ListAndPaginate(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	for i := range 5 {
		_, err := b.CreateActivity(context.Background(), fmt.Sprintf("list-act-%d", i))
		require.NoError(t, err)
	}

	all, _, err := b.ListActivities(context.Background(), "", 100)
	require.NoError(t, err)
	assert.Len(t, all, 5)

	page1, next, err := b.ListActivities(context.Background(), "", 2)
	require.NoError(t, err)
	require.NotEmpty(t, next)
	assert.Len(t, page1, 2)
}

func TestActivity_SendTaskSuccess(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	defer b.Destroy()

	act, err := b.CreateActivity(context.Background(), "send-act")
	require.NoError(t, err)

	actDef := fmt.Sprintf(`{"StartAt":"A","States":{"A":{"Type":"Task","Resource":%q,"End":true}}}`,
		act.ActivityArn)
	sm, err := b.CreateStateMachine(
		context.Background(),
		"act-sm",
		actDef,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "act-exec", `{"in":1}`)
	require.NoError(t, err)

	// Poll for the task.
	var task *stepfunctions.ActivityTask

	require.Eventually(t, func() bool {
		ctx2, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		t2, e := b.GetActivityTask(ctx2, act.ActivityArn, "worker1")

		if e == nil && t2 != nil {
			task = t2

			return true
		}

		return false
	}, 5*time.Second, 50*time.Millisecond)

	require.NotNil(t, task)
	require.NoError(t, b.SendTaskSuccess(task.TaskToken, `{"out":2}`))

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)
}

func TestActivity_SendTaskFailure(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	defer b.Destroy()

	act, err := b.CreateActivity(context.Background(), "fail-act")
	require.NoError(t, err)

	actDef := fmt.Sprintf(`{"StartAt":"A","States":{"A":{"Type":"Task","Resource":%q,"End":true}}}`,
		act.ActivityArn)
	sm, err := b.CreateStateMachine(
		context.Background(),
		"act-fail-sm",
		actDef,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "act-fail-exec", "{}")
	require.NoError(t, err)

	var task *stepfunctions.ActivityTask

	require.Eventually(t, func() bool {
		ctx2, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		t2, e := b.GetActivityTask(ctx2, act.ActivityArn, "worker1")

		if e == nil && t2 != nil {
			task = t2

			return true
		}

		return false
	}, 5*time.Second, 50*time.Millisecond)

	require.NotNil(t, task)
	require.NoError(t, b.SendTaskFailure(task.TaskToken, "MyErr", "failed on purpose"))

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "FAILED"
	}, 5*time.Second, 20*time.Millisecond)
}

func TestActivity_SendTaskSuccessUnknownToken(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.SendTaskSuccess("unknown-token", `{}`)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrTaskTokenNotFound)
}

func TestActivity_SendTaskHeartbeat(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	defer b.Destroy()

	act, err := b.CreateActivity(context.Background(), "hb-act")
	require.NoError(t, err)

	actDef := fmt.Sprintf(
		`{"StartAt":"A","States":{"A":{"Type":"Task","Resource":%q,"HeartbeatSeconds":60,"End":true}}}`,
		act.ActivityArn,
	)
	sm, err := b.CreateStateMachine(context.Background(), "hb-sm", actDef, validRoleARN, "STANDARD")
	require.NoError(t, err)

	_, err = b.StartExecution(sm.StateMachineArn, "hb-exec", "{}")
	require.NoError(t, err)

	var task *stepfunctions.ActivityTask

	require.Eventually(t, func() bool {
		ctx2, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		t2, e := b.GetActivityTask(ctx2, act.ActivityArn, "hb-worker")

		if e == nil && t2 != nil {
			task = t2

			return true
		}

		return false
	}, 5*time.Second, 50*time.Millisecond)

	require.NotNil(t, task)
	require.NoError(t, b.SendTaskHeartbeat(task.TaskToken))
}

// ─── Versions ─────────────────────────────────────────────────────────────────

// TestActivity_CreateDescribeDelete tests the activity lifecycle.
func TestActivity_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		actName   string
		duplicate bool
	}{
		{
			name:    "create_and_describe",
			actName: "my-activity",
		},
		{
			name:      "duplicate_create",
			actName:   "dup-activity",
			duplicate: true,
			wantErr:   stepfunctions.ErrActivityAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()

			a, err := b.CreateActivity(context.Background(), tt.actName)
			require.NoError(t, err)
			assert.Equal(t, tt.actName, a.Name)
			assert.Contains(t, a.ActivityArn, ":activity:"+tt.actName)
			assert.Greater(t, a.CreationDate, float64(0))

			if tt.duplicate {
				_, err = b.CreateActivity(context.Background(), tt.actName)
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			described, err := b.DescribeActivity(a.ActivityArn)
			require.NoError(t, err)
			assert.Equal(t, tt.actName, described.Name)

			err = b.DeleteActivity(a.ActivityArn)
			require.NoError(t, err)

			_, err = b.DescribeActivity(a.ActivityArn)
			require.ErrorIs(t, err, stepfunctions.ErrActivityDoesNotExist)
		})
	}
}

// TestActivity_GetActivityTaskAndSendSuccess tests the activity task polling and success flow.
func TestActivity_GetActivityTaskAndSendSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		output     string
		wantOutput string
	}{
		{
			name:       "success_with_output",
			input:      `{"key":"val"}`,
			output:     `{"result":"ok"}`,
			wantOutput: `{"result":"ok"}`,
		},
		{
			name:       "empty_output",
			input:      `{}`,
			output:     `{}`,
			wantOutput: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "task-act-"+tt.name)
			require.NoError(t, err)

			resultCh := make(chan string, 1)
			errCh := make(chan error, 1)

			go func() {
				out, invokeErr := b.InvokeActivity(t.Context(), a.ActivityArn, tt.input, 0)
				if invokeErr != nil {
					errCh <- invokeErr

					return
				}
				resultCh <- out
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			task, err := b.GetActivityTask(ctx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken, "expected a task to be available")
			assert.Equal(t, tt.input, task.Input)

			err = b.SendTaskSuccess(task.TaskToken, tt.output)
			require.NoError(t, err)

			select {
			case out := <-resultCh:
				assert.Equal(t, tt.wantOutput, out)
			case invokeErr := <-errCh:
				require.NoError(t, invokeErr)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for InvokeActivity result")
			}
		})
	}
}

// TestActivity_GetActivityTaskAndSendFailure tests the activity task failure flow.
func TestActivity_GetActivityTaskAndSendFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		errCode     string
		cause       string
		wantErrCode string
	}{
		{
			name:        "task_failed",
			errCode:     "ActivityFailed",
			cause:       "worker error",
			wantErrCode: "ActivityFailed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "fail-act-"+tt.name)
			require.NoError(t, err)

			invokeErrCh := make(chan error, 1)

			go func() {
				_, invokeErr := b.InvokeActivity(t.Context(), a.ActivityArn, `{}`, 0)
				invokeErrCh <- invokeErr
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			task, err := b.GetActivityTask(ctx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken, "expected a task to be available")

			err = b.SendTaskFailure(task.TaskToken, tt.errCode, tt.cause)
			require.NoError(t, err)

			select {
			case invokeErr := <-invokeErrCh:
				require.Error(t, invokeErr)
				assert.Contains(t, invokeErr.Error(), tt.wantErrCode)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for InvokeActivity failure")
			}
		})
	}
}

// TestActivity_GetActivityTask_Timeout verifies that GetActivityTask returns an empty task
// when no task is available within the poll timeout.
func TestActivity_GetActivityTask_Timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{
			name:    "returns_empty_on_timeout",
			timeout: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "timeout-act-"+tt.name)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), tt.timeout)
			defer cancel()

			task, err := b.GetActivityTask(ctx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			// AWS returns empty response (no task token) when poll times out.
			assert.Empty(t, task.TaskToken)
		})
	}
}

func TestActivity_InvokeCancellationRemovesTaskToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sendResult func(*stepfunctions.InMemoryBackend, string) error
		name       string
	}{
		{
			name: "cancelled_invoke_rejects_send_task_success",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskSuccess(taskToken, `{"status":"late"}`)
			},
		},
		{
			name: "cancelled_invoke_rejects_send_task_failure",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskFailure(taskToken, "ActivityFailed", "late failure")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "cancel-act-"+tt.name)
			require.NoError(t, err)

			invokeCtx, cancelInvoke := context.WithCancel(t.Context())
			defer cancelInvoke()

			invokeErrCh := make(chan error, 1)
			go func() {
				_, invokeErr := b.InvokeActivity(invokeCtx, a.ActivityArn, `{}`, 0)
				invokeErrCh <- invokeErr
			}()

			pollCtx, cancelPoll := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelPoll()

			task, err := b.GetActivityTask(pollCtx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken)

			cancelInvoke()

			require.Eventually(t, func() bool {
				select {
				case invokeErr := <-invokeErrCh:
					return errors.Is(invokeErr, context.Canceled)
				default:
					return false
				}
			}, 2*time.Second, 25*time.Millisecond)

			err = tt.sendResult(b, task.TaskToken)
			require.ErrorIs(t, err, stepfunctions.ErrTaskTokenNotFound)
		})
	}
}

func TestActivity_DeleteActivityRemovesOutstandingTaskTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sendResult func(*stepfunctions.InMemoryBackend, string) error
		name       string
	}{
		{
			name: "delete_activity_rejects_send_task_failure",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskFailure(taskToken, "ActivityFailed", "worker failed")
			},
		},
		{
			name: "delete_activity_rejects_send_task_success",
			sendResult: func(
				b *stepfunctions.InMemoryBackend,
				taskToken string,
			) error {
				return b.SendTaskSuccess(taskToken, `{"ok":true}`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSFBackend()
			a, err := b.CreateActivity(context.Background(), "delete-act-"+tt.name)
			require.NoError(t, err)

			invokeCtx, cancelInvoke := context.WithCancel(t.Context())
			defer cancelInvoke()

			invokeErrCh := make(chan error, 1)
			go func() {
				_, invokeErr := b.InvokeActivity(invokeCtx, a.ActivityArn, `{}`, 0)
				invokeErrCh <- invokeErr
			}()

			pollCtx, cancelPoll := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelPoll()

			task, err := b.GetActivityTask(pollCtx, a.ActivityArn, "worker-1")
			require.NoError(t, err)
			require.NotNil(t, task)
			require.NotEmpty(t, task.TaskToken)

			err = b.DeleteActivity(a.ActivityArn)
			require.NoError(t, err)

			err = tt.sendResult(b, task.TaskToken)
			require.ErrorIs(t, err, stepfunctions.ErrTaskTokenNotFound)

			// DeleteActivity signals resultCh for in-flight tasks, so InvokeActivity
			// must unblock and return an error without requiring context cancellation.
			require.Eventually(t, func() bool {
				select {
				case invokeErr := <-invokeErrCh:
					return invokeErr != nil
				default:
					return false
				}
			}, 2*time.Second, 25*time.Millisecond)
			cancelInvoke()
		})
	}
}

// TestPerf_SweepTaskTokensRLock verifies that SweepTaskTokens evicts stale tokens correctly.
func TestSweepTaskTokensRLock(t *testing.T) {
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

// TestRefinement1_ActivityAlreadyExists verifies creating duplicate activity returns error.
func TestActivityAlreadyExists(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateActivity(context.Background(), "my-act")
	require.NoError(t, err)

	_, err = b.CreateActivity(context.Background(), "my-act")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityAlreadyExists)
}

// TestDeleteActivityNotFound verifies deleting a nonexistent activity is
// idempotent. AWS: DeleteActivity's own error switch models only InvalidArn
// -- no ActivityDoesNotExist.
func TestDeleteActivityNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.DeleteActivity("arn:aws:states:us-east-1:123:activity:nonexistent")
	require.NoError(t, err)
}

// TestCreateActivity_EncryptionConfiguration verifies CreateActivity's
// encryptionConfiguration input field (present on the real AWS
// CreateActivityInput but previously entirely unparsed by this emulator) is
// applied and echoed back by DescribeActivity.
func TestCreateActivity_EncryptionConfiguration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body, err := json.Marshal(map[string]any{
		"name": "kms-activity",
		"encryptionConfiguration": map[string]any{
			"type":                         "CUSTOMER_MANAGED_KMS_KEY",
			"kmsKeyId":                     "arn:aws:kms:us-east-1:123456789012:key/test-key",
			"kmsDataKeyReusePeriodSeconds": 300,
		},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "CreateActivity", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	actARN, _ := createResp["activityArn"].(string)
	require.NotEmpty(t, actARN)

	descBody, _ := json.Marshal(map[string]string{"activityArn": actARN})
	descRec := sfnPost(ctx, t, h, e, "DescribeActivity", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	encCfg, ok := descResp["encryptionConfiguration"].(map[string]any)
	require.True(t, ok, "expected encryptionConfiguration echoed back on DescribeActivity")
	assert.Equal(t, "CUSTOMER_MANAGED_KMS_KEY", encCfg["type"])
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/test-key", encCfg["kmsKeyId"])
}

// TestCreateActivity_InlineTags verifies CreateActivity's tags input field
// applies tags visible to ListTagsForResource.
func TestCreateActivity_InlineTags(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body, err := json.Marshal(map[string]any{
		"name": "tagged-activity",
		"tags": []map[string]string{
			{"key": "env", "value": "prod"},
		},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "CreateActivity", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	actARN, _ := createResp["activityArn"].(string)
	require.NotEmpty(t, actARN)

	tagsBody, _ := json.Marshal(map[string]string{"resourceArn": actARN})
	tagsRec := sfnPost(ctx, t, h, e, "ListTagsForResource", string(tagsBody))
	require.Equal(t, http.StatusOK, tagsRec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))
	tagList, _ := tagsResp["tags"].([]any)
	require.Len(t, tagList, 1)

	tagEntry, _ := tagList[0].(map[string]any)
	assert.Equal(t, "env", tagEntry["key"])
	assert.Equal(t, "prod", tagEntry["value"])
}

// TestDeleteActivity_ClearsTags verifies DeleteActivity cleans up the
// handler's tags map (mirroring DeleteStateMachine's cleanup), so repeated
// create/tag/delete cycles don't leak tombstone entries.
func TestDeleteActivity_ClearsTags(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	createBody, _ := json.Marshal(map[string]any{"name": "del-tagged-activity"})
	rec := sfnPost(ctx, t, h, e, "CreateActivity", string(createBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	actARN, _ := createResp["activityArn"].(string)
	require.NotEmpty(t, actARN)

	tagBody, _ := json.Marshal(map[string]any{
		"resourceArn": actARN,
		"tags":        map[string]string{"k": "v"},
	})
	tagRec := sfnPost(ctx, t, h, e, "TagResource", string(tagBody))
	require.Equal(t, http.StatusOK, tagRec.Code)

	delBody, _ := json.Marshal(map[string]string{"activityArn": actARN})
	delRec := sfnPost(ctx, t, h, e, "DeleteActivity", string(delBody))
	require.Equal(t, http.StatusOK, delRec.Code)

	tagsBody, _ := json.Marshal(map[string]string{"resourceArn": actARN})
	tagsRec := sfnPost(ctx, t, h, e, "ListTagsForResource", string(tagsBody))
	require.Equal(t, http.StatusOK, tagsRec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))
	tagList, _ := tagsResp["tags"].([]any)
	assert.Empty(t, tagList, "tags must be cleared after DeleteActivity")
}
