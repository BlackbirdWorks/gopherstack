package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestAutomationExecution_Lifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Start
	rec := doRequest(t, h, "StartAutomationExecution", `{"DocumentName":"AWS-RunShellScript"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AutomationExecutionId")

	// Describe
	rec = doRequest(t, h, "DescribeAutomationExecutions", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AWS-RunShellScript")

	// Stop with an empty body must reject: AutomationExecutionId is required
	// on the real op (api_op_StopAutomationExecution.go).
	rec = doRequest(t, h, "StopAutomationExecution", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
func TestChangeRequest(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// gopherstack-4ggy: Runbooks is a required StartChangeRequestExecutionInput
	// member that the pre-fix request never read at all.
	rec := doRequest(t, h, "StartChangeRequestExecution", `{"DocumentName":"AWS-ChangeRequest"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doRequest(t, h, "StartChangeRequestExecution", `{
		"DocumentName":"AWS-ChangeRequest",
		"Runbooks":[{"DocumentName":"AWS-RunShellScript","MaxConcurrency":"1"}]
	}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "AutomationExecutionId")

	var startResp struct {
		AutomationExecutionID string `json:"AutomationExecutionId"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&startResp))

	// Runbooks must round-trip on GetAutomationExecution.
	rec = doRequest(t, h, "GetAutomationExecution",
		`{"AutomationExecutionId":"`+startResp.AutomationExecutionID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		AutomationExecution struct {
			Runbooks []struct {
				DocumentName   string `json:"DocumentName"`
				MaxConcurrency string `json:"MaxConcurrency"`
			} `json:"Runbooks"`
		} `json:"AutomationExecution"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&getResp))
	require.Len(t, getResp.AutomationExecution.Runbooks, 1)
	assert.Equal(t, "AWS-RunShellScript", getResp.AutomationExecution.Runbooks[0].DocumentName)
	assert.Equal(t, "1", getResp.AutomationExecution.Runbooks[0].MaxConcurrency)

	// A runbook with a missing DocumentName is rejected too.
	rec = doRequest(t, h, "StartChangeRequestExecution",
		`{"DocumentName":"AWS-ChangeRequest","Runbooks":[{}]}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
func TestExecutionPreview(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Start preview
	rec := doRequest(t, h, "StartExecutionPreview", `{"DocumentName":"AWS-Test"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "ExecutionPreviewId")

	// GetExecutionPreview with an empty body must reject: ExecutionPreviewId
	// is required on the real op (api_op_GetExecutionPreview.go).
	rec = doRequest(t, h, "GetExecutionPreview", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetCalendarState_RequiresCalendarNames locks in that CalendarNames is
// required on the real op (api_op_GetCalendarState.go marks it "This member
// is required."); an empty body previously defaulted to State:"OPEN" instead
// of rejecting with ValidationException.
func TestGetCalendarState_RequiresCalendarNames(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "GetCalendarState", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}
func TestGetAutomationExecution_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		execID string
	}{
		{
			name:   "backend_empty_id_returns_error",
			execID: "",
		},
		{
			name:   "backend_unknown_id_returns_error",
			execID: "auto-does-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.GetAutomationExecution(context.TODO(), &ssm.GetAutomationExecutionInput{
				AutomationExecutionID: tt.execID,
			})
			require.ErrorIs(t, err, ssm.ErrAutomationExecutionNotFound,
				"non-existent execution must return ErrAutomationExecutionNotFound")
		})
	}
}
func TestGetAutomationExecution_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		docID string
	}{
		{
			name:  "started_execution_is_returned",
			docID: "AWS-RunPatchBaseline",
		},
		{
			name:  "started_execution_with_version",
			docID: "AWS-ApplyPatchBaseline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			out, err := b.StartAutomationExecution(context.TODO(), &ssm.StartAutomationExecutionInput{
				DocumentName: tt.docID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, out.AutomationExecutionID)

			got, err := b.GetAutomationExecution(context.TODO(), &ssm.GetAutomationExecutionInput{
				AutomationExecutionID: out.AutomationExecutionID,
			})
			require.NoError(t, err)
			require.NotNil(t, got.AutomationExecution)
			assert.Equal(t, out.AutomationExecutionID, got.AutomationExecution.AutomationExecutionID)
			assert.Equal(t, tt.docID, got.AutomationExecution.DocumentName)
		})
	}
}
func TestGetAutomationExecution_Handler_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "empty_body_returns_error",
			body: `{}`,
			want: http.StatusBadRequest,
		},
		{
			name: "unknown_id_returns_error",
			body: `{"AutomationExecutionId":"auto-ghost"}`,
			want: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "GetAutomationExecution", tt.body)
			assert.Equal(t, tt.want, rec.Code)
			assert.Contains(t, rec.Body.String(), "AutomationExecutionNotFoundException")
		})
	}
}
func TestStartAutomationExecution_CompletesWithSteps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		docContent string
		wantSteps  []string
	}{
		{
			name:       "synthetic_step_for_unknown_doc",
			docContent: "",
			wantSteps:  []string{"AWS-Doc"},
		},
		{
			name: "steps_extracted_from_document",
			docContent: `{"schemaVersion":"0.3","mainSteps":[` +
				`{"name":"stepA","action":"aws:runInstances"},` +
				`{"name":"stepB","action":"aws:sleep"}]}`,
			wantSteps: []string{"stepA", "stepB"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.TODO()

			docName := "AWS-Doc"
			if tt.docContent != "" {
				_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{
					Name:         docName,
					Content:      tt.docContent,
					DocumentType: "Automation",
				})
				require.NoError(t, err)
			}

			start, err := b.StartAutomationExecution(ctx, &ssm.StartAutomationExecutionInput{
				DocumentName: docName,
			})
			require.NoError(t, err)

			got, err := b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
				AutomationExecutionID: start.AutomationExecutionID,
			})
			require.NoError(t, err)

			exec := got.AutomationExecution
			require.NotNil(t, exec)
			assert.Equal(t, "Success", exec.Status, "automation must reach a terminal Success status")
			require.NotNil(t, exec.EndTime, "terminal execution must have an EndTime")
			require.Len(t, exec.Steps, len(tt.wantSteps))

			for i, want := range tt.wantSteps {
				assert.Equal(t, want, exec.Steps[i].StepName)
				assert.Equal(t, "Success", exec.Steps[i].StepStatus,
					"each step must be advanced to Success")
			}

			// DescribeAutomationStepExecutions returns the populated steps too.
			steps, err := b.DescribeAutomationStepExecutions(
				ctx,
				&ssm.DescribeAutomationStepExecutionsInput{
					AutomationExecutionID: start.AutomationExecutionID,
				},
			)
			require.NoError(t, err)
			assert.Len(t, steps.StepExecutions, len(tt.wantSteps))
		})
	}
}
func TestStartAutomationExecution_InProgressObservable(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend().WithAutomationExecDelay(time.Hour)
	ctx := context.TODO()

	start, err := b.StartAutomationExecution(ctx, &ssm.StartAutomationExecutionInput{
		DocumentName: "AWS-DoSomething",
	})
	require.NoError(t, err)

	got, err := b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
		AutomationExecutionID: start.AutomationExecutionID,
	})
	require.NoError(t, err)
	assert.Equal(t, "InProgress", got.AutomationExecution.Status,
		"with an exec delay the automation must be observable as InProgress")

	b.ForceCompleteAutomations()

	got, err = b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
		AutomationExecutionID: start.AutomationExecutionID,
	})
	require.NoError(t, err)
	assert.Equal(t, "Success", got.AutomationExecution.Status)
}
func TestFull_AutomationExecution_StartDescribeStop(t *testing.T) {
	t.Parallel()
	h := newHandler()

	code, out := postJSON(t, h, "StartAutomationExecution", map[string]any{
		"DocumentName": "AWS-RestartEC2Instance",
		"Parameters":   map[string]any{"InstanceId": []string{"i-auto"}},
	})
	assert.Equal(t, http.StatusOK, code)
	execID := out["AutomationExecutionId"].(string)
	assert.NotEmpty(t, execID)

	code, out = postJSON(t, h, "GetAutomationExecution", map[string]any{
		"AutomationExecutionId": execID,
	})
	assert.Equal(t, http.StatusOK, code)
	exec := out["AutomationExecution"].(map[string]any)
	assert.Equal(t, execID, exec["AutomationExecutionId"])

	code, out = postJSON(t, h, "DescribeAutomationExecutions", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	executions := out["AutomationExecutionMetadataList"].([]any)
	assert.NotEmpty(t, executions)

	code, _ = postJSON(t, h, "StopAutomationExecution", map[string]any{
		"AutomationExecutionId": execID,
		"Type":                  "Cancel",
	})
	assert.Equal(t, http.StatusOK, code)
}
func TestAutomationExecution_ParametersRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parameters map[string][]string
		mode       string
	}{
		{
			name:       "with_parameters",
			parameters: map[string][]string{"InstanceId": {"i-12345678"}, "Action": {"Install"}},
			mode:       "Auto",
		},
		{
			name:       "interactive_mode",
			parameters: map[string][]string{"DocumentName": {"AWS-UpdateSSMAgent"}},
			mode:       "Interactive",
		},
		{
			name:       "empty_parameters",
			parameters: nil,
			mode:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body, _ := json.Marshal(map[string]any{
				"DocumentName": "AWS-RunShellScript",
				"Parameters":   tt.parameters,
				"Mode":         tt.mode,
			})
			rec := doRequest(t, h, "StartAutomationExecution", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			execID := startResp["AutomationExecutionId"].(string)
			assert.NotEmpty(t, execID)

			// GetAutomationExecution should return parameters.
			body, _ = json.Marshal(map[string]any{"AutomationExecutionId": execID})
			rec = doRequest(t, h, "GetAutomationExecution", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			if len(tt.parameters) > 0 {
				for key := range tt.parameters {
					assert.Contains(t, rec.Body.String(), key)
				}
			}
		})
	}
}
func TestAutomationApprovals_ApproveSignal(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Start an automation execution.
	rec := doRequest(t, h, "StartAutomationExecution", `{"DocumentName":"AWS-ChangeRequest"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execID := startResp["AutomationExecutionId"].(string)

	// Send Approve signal.
	body, _ := json.Marshal(map[string]any{
		"AutomationExecutionId": execID,
		"SignalType":            "Approve",
	})
	rec = doRequest(t, h, "SendAutomationSignal", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// GetAutomationExecution should show Approved status.
	body, _ = json.Marshal(map[string]any{"AutomationExecutionId": execID})
	rec = doRequest(t, h, "GetAutomationExecution", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Approved")
}
func TestAutomationApprovals_RejectSignal(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "StartAutomationExecution", `{"DocumentName":"AWS-ChangeRequest"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execID := startResp["AutomationExecutionId"].(string)

	body, _ := json.Marshal(map[string]any{
		"AutomationExecutionId": execID,
		"SignalType":            "Reject",
	})
	rec = doRequest(t, h, "SendAutomationSignal", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ = json.Marshal(map[string]any{"AutomationExecutionId": execID})
	rec = doRequest(t, h, "GetAutomationExecution", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Rejected")
}
func TestAutomationExecution_WarningMessageAbsentFromWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body func(execID string) string
		name string
		op   string
	}{
		{
			name: "get_automation_execution",
			op:   "GetAutomationExecution",
			body: func(execID string) string {
				return `{"AutomationExecutionId":"` + execID + `"}`
			},
		},
		{
			name: "describe_automation_executions",
			op:   "DescribeAutomationExecutions",
			body: func(string) string { return `{}` },
		},
		{
			name: "describe_automation_step_executions",
			op:   "DescribeAutomationStepExecutions",
			body: func(execID string) string {
				return `{"AutomationExecutionId":"` + execID + `"}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			start := doRequest(t, h, "StartAutomationExecution", `{"DocumentName":"AWS-RunShellScript"}`)
			require.Equal(t, http.StatusOK, start.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(start.Body.Bytes(), &startResp))
			execID, _ := startResp["AutomationExecutionId"].(string)
			require.NotEmpty(t, execID)

			rec := doRequest(t, h, tt.op, tt.body(execID))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.NotContains(t, rec.Body.String(), "WarningMessage",
				"WarningMessage must be genuinely absent from the wire, not merely empty")
		})
	}
}
func TestGetCalendarState_MissingDocumentReturnsError(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	body, _ := json.Marshal(map[string]any{
		"CalendarNames": []string{"DoesNotExist"},
	})
	rec := doRequest(t, h, "GetCalendarState", string(body))
	assert.NotEqual(t, http.StatusOK, rec.Code, "missing calendar should return error")
}

// TestAutomationOps_RequireRequiredFields locks in that DescribeAutomationStepExecutions,
// GetExecutionPreview, SendAutomationSignal, StartAutomationExecution and
// StartExecutionPreview all reject an empty body -- each has at least one
// required field on the real op (api_op_DescribeAutomationStepExecutions.go,
// api_op_GetExecutionPreview.go, api_op_SendAutomationSignal.go,
// api_op_StartAutomationExecution.go, api_op_StartExecutionPreview.go) that
// was previously unvalidated.
func TestAutomationOps_RequireRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		body string
	}{
		{name: "describe_automation_step_executions", op: "DescribeAutomationStepExecutions", body: `{}`},
		{name: "get_execution_preview", op: "GetExecutionPreview", body: `{}`},
		{name: "send_automation_signal_missing_id", op: "SendAutomationSignal", body: `{"SignalType":"Approve"}`},
		{
			name: "send_automation_signal_bad_type",
			op:   "SendAutomationSignal",
			body: `{"AutomationExecutionId":"auto-x","SignalType":"Bogus"}`,
		},
		{name: "start_automation_execution", op: "StartAutomationExecution", body: `{}`},
		{name: "start_execution_preview", op: "StartExecutionPreview", body: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "op=%s body=%s", tt.op, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}

// TestStopAutomationExecution_TypeSelectsTerminalStatus locks in that Type
// (Cancel/Complete, types.StopType) selects the terminal
// AutomationExecutionStatus -- Complete finishes successfully, Cancel (the
// default) cancels. Real AutomationExecutionStatus has no "Stopped" value.
func TestStopAutomationExecution_TypeSelectsTerminalStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stopType   string
		wantStatus string
	}{
		{name: "default_is_cancel", stopType: "", wantStatus: "Cancelled"},
		{name: "explicit_cancel", stopType: "Cancel", wantStatus: "Cancelled"},
		{name: "complete", stopType: "Complete", wantStatus: "Success"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ctx := context.Background()

			started, err := b.StartAutomationExecution(ctx, &ssm.StartAutomationExecutionInput{
				DocumentName: "AWS-RunShellScript",
			})
			require.NoError(t, err)

			_, err = b.StopAutomationExecution(ctx, &ssm.StopAutomationExecutionInput{
				AutomationExecutionID: started.AutomationExecutionID,
				Type:                  tt.stopType,
			})
			require.NoError(t, err)

			got, err := b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
				AutomationExecutionID: started.AutomationExecutionID,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.AutomationExecution.Status)
		})
	}
}

// TestStopAutomationExecution_NotFound locks in that stopping an unknown
// execution ID returns ErrAutomationExecutionNotFound rather than silently
// succeeding.
func TestStopAutomationExecution_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StopAutomationExecution(context.Background(), &ssm.StopAutomationExecutionInput{
		AutomationExecutionID: "auto-does-not-exist",
	})
	require.ErrorIs(t, err, ssm.ErrAutomationExecutionNotFound)
}

// TestSendAutomationSignal_NotFound locks in that signaling an unknown
// execution ID returns ErrAutomationExecutionNotFound rather than silently
// succeeding.
func TestSendAutomationSignal_NotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.SendAutomationSignal(context.Background(), &ssm.SendAutomationSignalInput{
		AutomationExecutionID: "auto-does-not-exist",
		SignalType:            "Approve",
	})
	require.ErrorIs(t, err, ssm.ErrAutomationExecutionNotFound)
}

// TestAutomationExecution_MaxConcurrencyMaxErrorsRoundTrip locks in that
// StartAutomationExecutionInput.MaxConcurrency/MaxErrors (accepted since
// before this fix) actually round-trip onto the execution record -- real
// AutomationExecution/AutomationExecutionMetadata both declare these members
// (types.go:727,730,917,920); previously they were parsed and then never
// stored anywhere.
func TestAutomationExecution_MaxConcurrencyMaxErrorsRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	ctx := context.Background()

	started, err := b.StartAutomationExecution(ctx, &ssm.StartAutomationExecutionInput{
		DocumentName:   "AWS-RunShellScript",
		MaxConcurrency: "5",
		MaxErrors:      "2",
	})
	require.NoError(t, err)

	got, err := b.GetAutomationExecution(ctx, &ssm.GetAutomationExecutionInput{
		AutomationExecutionID: started.AutomationExecutionID,
	})
	require.NoError(t, err)
	assert.Equal(t, "5", got.AutomationExecution.MaxConcurrency)
	assert.Equal(t, "2", got.AutomationExecution.MaxErrors)
}

// TestDescribeAutomationExecutions_TiedStartTimePageWalk proves
// DescribeAutomationExecutions sorts on StartTime alone, a field with no
// tiebreak, over automationExecutionsStore.All() (a store.Table map walk
// whose iteration order Go randomizes between calls). Several executions
// sharing one StartTime -- plausible any time two automations start in the
// same instant -- can therefore land in a different relative order on each
// call. paginateSlice pages by offset into that order, so a page boundary
// that fell between two tied executions on one call falls between two
// different tied executions on the next -- one gets dropped or duplicated
// across the page boundary with nothing else changed. Looped: a single walk
// can pass by luck since map iteration is randomized per-call.
func TestDescribeAutomationExecutions_TiedStartTimePageWalk(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	ctx := context.Background()

	const total = 12

	const tiedStart = 1_700_000_000.0

	want := make(map[string]bool, total)

	for i := range total {
		id := "auto-tied-" + strconv.Itoa(i)
		b.AddAutomationExecutionInternal(ssm.AutomationExecution{
			AutomationExecutionID: id,
			DocumentName:          "AWS-RunShellScript",
			Status:                "Success",
			StartTime:             tiedStart,
		})
		want[id] = true
	}

	pageSize := int32(5)

	for iter := range 30 {
		got := make(map[string]int, total)

		var token string
		for range total/int(pageSize) + 2 {
			out, err := b.DescribeAutomationExecutions(ctx, &ssm.DescribeAutomationExecutionsInput{
				MaxResults: &pageSize,
				NextToken:  token,
			})
			require.NoError(t, err)

			for _, e := range out.AutomationExecutionMetadataList {
				got[e.AutomationExecutionID]++
			}

			if out.NextToken == "" {
				break
			}

			token = out.NextToken
		}

		require.Len(
			t, got, total,
			"iteration %d: page walk produced %d distinct executions, want %d", iter, len(got), total,
		)

		for id := range want {
			require.Equalf(
				t,
				1,
				got[id],
				"iteration %d: execution %s appeared %d times across the page walk",
				iter,
				id,
				got[id],
			)
		}
	}
}
