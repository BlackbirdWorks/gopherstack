package stepfunctions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// ─── ARN Format ───────────────────────────────────────────────────────────────

func TestAudit_ARN_StateMachine(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	sm, err := b.CreateStateMachine(context.Background(), "my-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:states:us-east-1:123456789012:stateMachine:my-sm", sm.StateMachineArn)
}

func TestAudit_ARN_Execution(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	sm, err := b.CreateStateMachine(context.Background(), "my-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "my-exec", "{}")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:states:us-east-1:123456789012:execution:my-sm:my-exec", exec.ExecutionArn)
}

func TestAudit_ARN_Activity(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	act, err := b.CreateActivity(context.Background(), "my-activity")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:states:us-east-1:123456789012:activity:my-activity", act.ActivityArn)
}

func TestAudit_ARN_Version(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	sm, err := b.CreateStateMachine(context.Background(), "ver-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	v, err := b.PublishStateMachineVersion(sm.StateMachineArn, "v1", "")
	require.NoError(t, err)
	assert.Contains(t, v.StateMachineVersionArn, "arn:aws:states:us-east-1:123456789012:stateMachine:ver-sm:")
}

// ─── StateMachine CRUD ────────────────────────────────────────────────────────

func TestAudit_CreateStateMachine_DefaultTypeIsSTANDARD(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "t1", minimalDefinition, validRoleARN, "")
	require.NoError(t, err)
	assert.Equal(t, "STANDARD", sm.Type)
}

func TestAudit_CreateStateMachine_Express(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "exp1", minimalDefinition, validRoleARN, "EXPRESS")
	require.NoError(t, err)
	assert.Equal(t, "EXPRESS", sm.Type)
}

func TestAudit_CreateStateMachine_StatusIsActive(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "status-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", sm.Status)
}

func TestAudit_CreateStateMachine_CreationDateSet(t *testing.T) {
	t.Parallel()

	before := float64(time.Now().Unix())
	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "date-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, sm.CreationDate, before)
}

func TestAudit_CreateStateMachine_DuplicateNameDiffDefReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateStateMachine(context.Background(), "dup-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	altDef := `{"StartAt":"T","States":{"T":{"Type":"Succeed"}}}`
	_, err = b.CreateStateMachine(context.Background(), "dup-sm", altDef, validRoleARN, "STANDARD")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineAlreadyExists)
}

func TestAudit_CreateStateMachine_InvalidDefinition(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateStateMachine(context.Background(), "bad-def", `{"not":"valid-asl"}`, validRoleARN, "STANDARD")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidDefinition)
}

func TestAudit_DescribeStateMachine_NotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.DescribeStateMachine("arn:aws:states:us-east-1:123:stateMachine:none")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

func TestAudit_DeleteStateMachine_RemovesStateMachine(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "del-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	require.NoError(t, b.DeleteStateMachine(sm.StateMachineArn))

	_, err = b.DescribeStateMachine(sm.StateMachineArn)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

func TestAudit_DeleteStateMachine_NotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.DeleteStateMachine("arn:aws:states:us-east-1:123:stateMachine:ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

func TestAudit_UpdateStateMachine_UpdatesDefinition(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "upd-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	newDef := `{"StartAt":"S2","States":{"S2":{"Type":"Pass","End":true}}}`
	_, err = b.UpdateStateMachine(sm.StateMachineArn, newDef, "")
	require.NoError(t, err)

	updated, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	assert.Equal(t, newDef, updated.Definition)
}

func TestAudit_ListStateMachines_ReturnsSorted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	for _, name := range []string{"zz-sm", "aa-sm", "mm-sm"} {
		_, err := b.CreateStateMachine(context.Background(), name, minimalDefinition, validRoleARN, "STANDARD")
		require.NoError(t, err)
	}

	sms, next, err := b.ListStateMachines(context.Background(), "", 100)
	require.NoError(t, err)
	assert.Empty(t, next)
	require.Len(t, sms, 3)
	assert.Equal(t, "aa-sm", sms[0].Name)
	assert.Equal(t, "mm-sm", sms[1].Name)
	assert.Equal(t, "zz-sm", sms[2].Name)
}

func TestAudit_ListStateMachines_Pagination(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	for i := range 5 {
		name := fmt.Sprintf("pag-sm-%02d", i)
		_, err := b.CreateStateMachine(context.Background(), name, minimalDefinition, validRoleARN, "STANDARD")
		require.NoError(t, err)
	}

	page1, next, err := b.ListStateMachines(context.Background(), "", 2)
	require.NoError(t, err)
	require.NotEmpty(t, next)
	assert.Len(t, page1, 2)

	page2, next2, err := b.ListStateMachines(context.Background(), next, 2)
	require.NoError(t, err)
	require.NotEmpty(t, next2)
	assert.Len(t, page2, 2)

	page3, next3, err := b.ListStateMachines(context.Background(), next2, 2)
	require.NoError(t, err)
	assert.Empty(t, next3)
	assert.Len(t, page3, 1)
}

// ─── Name Validation ──────────────────────────────────────────────────────────

func TestAudit_Name_TooLong(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	longName := strings.Repeat("a", 81)
	_, err := b.CreateStateMachine(context.Background(), longName, minimalDefinition, validRoleARN, "STANDARD")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidName)
}

func TestAudit_Name_ExactMaxLength(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	name := strings.Repeat("a", 80)
	sm, err := b.CreateStateMachine(context.Background(), name, minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, name, sm.Name)
}

func TestAudit_Name_Empty(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateStateMachine(context.Background(), "", minimalDefinition, validRoleARN, "STANDARD")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidName)
}

func TestAudit_Name_InvalidChars(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	for _, badName := range []string{"has<angle", "has>angle", "has{brace", "has}brace"} {
		t.Run(badName, func(t *testing.T) {
			t.Parallel()

			_, err := b.CreateStateMachine(context.Background(), badName, minimalDefinition, validRoleARN, "STANDARD")
			require.Error(t, err)
			assert.ErrorIs(t, err, stepfunctions.ErrInvalidName)
		})
	}
}

func TestAudit_Name_ValidSpecialChars(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	for _, name := range []string{"has-dash", "has_underscore", "has.dot", "has+plus", "has=eq"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sm, err := b.CreateStateMachine(context.Background(), name, minimalDefinition, validRoleARN, "STANDARD")
			require.NoError(t, err)
			assert.Equal(t, name, sm.Name)
		})
	}
}

func TestAudit_ActivityName_TooLong(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	longName := strings.Repeat("x", 81)
	_, err := b.CreateActivity(context.Background(), longName)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidName)
}

func TestAudit_ExecutionName_TooLong(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "exec-name-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	longName := strings.Repeat("x", 81)
	_, err = b.StartExecution(sm.StateMachineArn, longName, "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidName)
}

// ─── Execution Lifecycle ──────────────────────────────────────────────────────

func TestAudit_Execution_StartAndDescribe(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "desc-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "desc-exec", `{"x":1}`)
	require.NoError(t, err)
	assert.NotEmpty(t, exec.ExecutionArn)
	assert.Equal(t, sm.StateMachineArn, exec.StateMachineArn)
	assert.Equal(t, "desc-exec", exec.Name)
	assert.Equal(t, `{"x":1}`, exec.Input)
}

func TestAudit_Execution_StartsRunning(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Use a Wait state to keep execution running long enough to observe RUNNING status.
	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	sm, err := b.CreateStateMachine(context.Background(), "run-sm", waitDef, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "run-exec", "{}")
	require.NoError(t, err)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", desc.Status)
}

func TestAudit_Execution_SucceedsAfterPass(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "succ-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "succ-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", desc.Status)
	assert.NotNil(t, desc.StopDate)
}

func TestAudit_Execution_FailStateProducesFailedStatus(t *testing.T) {
	t.Parallel()

	failDef := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"ErrFoo","Cause":"test cause","End":true}}}`
	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "fail-sm", failDef, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "fail-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "FAILED"
	}, 5*time.Second, 20*time.Millisecond)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", desc.Status)
	assert.Equal(t, "ErrFoo", desc.Error)
	assert.Equal(t, "test cause", desc.Cause)
}

func TestAudit_Execution_DuplicateNameReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "dup-exec-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	_, err = b.StartExecution(sm.StateMachineArn, "dup-exec", "{}")
	require.NoError(t, err)

	_, err = b.StartExecution(sm.StateMachineArn, "dup-exec", "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionAlreadyExists)
}

func TestAudit_Execution_NotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.DescribeExecution("arn:aws:states:us-east-1:123:execution:sm:none")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
}

func TestAudit_Execution_StateMachineNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.StartExecution("arn:aws:states:us-east-1:123:stateMachine:ghost", "e1", "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

// ─── StartExecution / StartSyncExecution type enforcement ────────────────────

func TestAudit_StartExecution_ExpressMachineReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "exp-async", minimalDefinition, validRoleARN, "EXPRESS")
	require.NoError(t, err)

	_, err = b.StartExecution(sm.StateMachineArn, "e1", "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidExecutionType)
}

func TestAudit_StartSyncExecution_StandardMachineReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "std-sync", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	_, err = b.StartSyncExecution(sm.StateMachineArn, "sync-e1", "{}")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidExecutionType)
}

func TestAudit_StartSyncExecution_Express_Succeeds(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "exp-sync", minimalDefinition, validRoleARN, "EXPRESS")
	require.NoError(t, err)

	result, err := b.StartSyncExecution(sm.StateMachineArn, "sync-e2", `{"ok":true}`)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", result.Status)
}

func TestAudit_StartSyncExecution_Express_InputPayload(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "exp-input", minimalDefinition, validRoleARN, "EXPRESS")
	require.NoError(t, err)

	result, err := b.StartSyncExecution(sm.StateMachineArn, "s1", `{"hello":"world"}`)
	require.NoError(t, err)
	assert.JSONEq(t, `{"hello":"world"}`, result.Input)
}

// ─── StopExecution ────────────────────────────────────────────────────────────

func TestAudit_StopExecution_SetsAborted(t *testing.T) {
	t.Parallel()

	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "stop-test-sm", waitDef, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "stop-e1", "{}")
	require.NoError(t, err)

	require.NoError(t, b.StopExecution(exec.ExecutionArn, "MyErr", "test cause"))

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "ABORTED", desc.Status)
	assert.Equal(t, "MyErr", desc.Error)
	assert.Equal(t, "test cause", desc.Cause)
	assert.NotNil(t, desc.StopDate)
}

func TestAudit_StopExecution_IdempotentOnTerminalState(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "idm-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "idm-exec", "{}")
	require.NoError(t, err)

	// Wait for execution to reach terminal state.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status != "RUNNING"
	}, 5*time.Second, 20*time.Millisecond)

	// Must not error and must not overwrite terminal status.
	err = b.StopExecution(exec.ExecutionArn, "ShouldNotOverwrite", "nope")
	require.NoError(t, err)

	desc, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", desc.Status, "terminal status must not be overwritten")
	assert.NotEqual(t, "ShouldNotOverwrite", desc.Error)
}

func TestAudit_StopExecution_NotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.StopExecution("arn:aws:states:us-east-1:123:execution:sm:ghost", "", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
}

// ─── GetExecutionHistory ──────────────────────────────────────────────────────

func TestAudit_GetExecutionHistory_HasExecutionStarted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "hist-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "hist-exec", "{}")
	require.NoError(t, err)

	// Allow time for history to populate.
	require.Eventually(t, func() bool {
		events, _, err2 := b.GetExecutionHistory(exec.ExecutionArn, "", 100, false)

		return err2 == nil && len(events) >= 1
	}, 5*time.Second, 20*time.Millisecond)

	events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, false)
	require.NoError(t, err)
	assert.Equal(t, "ExecutionStarted", events[0].Type)
}

func TestAudit_GetExecutionHistory_ReverseOrder(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "rev-hist-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "rev-hist-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)

	events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, true)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	// Last event in forward order should be first in reverse.
	assert.Equal(t, "ExecutionSucceeded", events[0].Type)
}

func TestAudit_GetExecutionHistory_Pagination(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "page-hist-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "page-hist-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)

	// Get all events first.
	all, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, false)
	require.NoError(t, err)
	require.NotEmpty(t, all)

	// Paginate with maxResults=1.
	var collected []stepfunctions.HistoryEvent
	tok := ""

	for {
		page, next, err2 := b.GetExecutionHistory(exec.ExecutionArn, tok, 1, false)
		require.NoError(t, err2)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		tok = next
	}

	assert.Len(t, collected, len(all))
}

func TestAudit_GetExecutionHistory_EventIDsMonotonicallyIncreasing(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "mono-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "mono-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)

	events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 100, false)
	require.NoError(t, err)

	for i := 1; i < len(events); i++ {
		assert.Greater(t, events[i].ID, events[i-1].ID, "event IDs must be monotonically increasing")
	}
}

// ─── ListExecutions ───────────────────────────────────────────────────────────

func TestAudit_ListExecutions_StatusFilter_RUNNING(t *testing.T) {
	t.Parallel()

	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "list-sm", waitDef, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "list-run-e", "{}")
	require.NoError(t, err)

	// Give the execution time to start.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "RUNNING"
	}, 5*time.Second, 20*time.Millisecond)

	running, _, err := b.ListExecutions(sm.StateMachineArn, "RUNNING", "", 100)
	require.NoError(t, err)
	assert.Len(t, running, 1)
	assert.Equal(t, exec.ExecutionArn, running[0].ExecutionArn)

	succeeded, _, err := b.ListExecutions(sm.StateMachineArn, "SUCCEEDED", "", 100)
	require.NoError(t, err)
	assert.Empty(t, succeeded)
}

func TestAudit_ListExecutions_Pagination(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "list-page-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	for i := range 4 {
		_, err = b.StartExecution(sm.StateMachineArn, fmt.Sprintf("pexec-%d", i), "{}")
		require.NoError(t, err)
	}

	page1, next, err := b.ListExecutions(sm.StateMachineArn, "", "", 2)
	require.NoError(t, err)
	require.NotEmpty(t, next)
	assert.Len(t, page1, 2)

	page2, next2, err := b.ListExecutions(sm.StateMachineArn, "", next, 2)
	require.NoError(t, err)
	assert.Empty(t, next2)
	assert.Len(t, page2, 2)
}

func TestAudit_ListExecutions_EmptyForNewSM(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "empty-list-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	execs, next, err := b.ListExecutions(sm.StateMachineArn, "", "", 100)
	require.NoError(t, err)
	assert.Empty(t, execs)
	assert.Empty(t, next)
}

// ─── DescribeStateMachineForExecution ─────────────────────────────────────────

func TestAudit_DescribeStateMachineForExecution(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(
		context.Background(),
		"dsm-for-exec-sm",
		minimalDefinition,
		validRoleARN,
		"STANDARD",
	)
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "dsm-exec", "{}")
	require.NoError(t, err)

	got, err := b.DescribeStateMachineForExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, sm.StateMachineArn, got.StateMachineArn)
	assert.Equal(t, "STANDARD", got.Type)
}

// ─── Activities ───────────────────────────────────────────────────────────────

func TestAudit_Activity_CreateAndDescribe(t *testing.T) {
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

func TestAudit_Activity_DuplicateReturnsError(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateActivity(context.Background(), "dup-act")
	require.NoError(t, err)

	_, err = b.CreateActivity(context.Background(), "dup-act")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityAlreadyExists)
}

func TestAudit_Activity_Delete(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	act, err := b.CreateActivity(context.Background(), "del-act")
	require.NoError(t, err)

	require.NoError(t, b.DeleteActivity(act.ActivityArn))

	_, err = b.DescribeActivity(act.ActivityArn)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityDoesNotExist)
}

func TestAudit_Activity_DeleteNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.DeleteActivity("arn:aws:states:us-east-1:123:activity:ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityDoesNotExist)
}

func TestAudit_Activity_ListAndPaginate(t *testing.T) {
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

func TestAudit_Activity_SendTaskSuccess(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	defer b.Destroy()

	act, err := b.CreateActivity(context.Background(), "send-act")
	require.NoError(t, err)

	actDef := fmt.Sprintf(`{"StartAt":"A","States":{"A":{"Type":"Task","Resource":%q,"End":true}}}`,
		act.ActivityArn)
	sm, err := b.CreateStateMachine(context.Background(), "act-sm", actDef, validRoleARN, "STANDARD")
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

func TestAudit_Activity_SendTaskFailure(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	defer b.Destroy()

	act, err := b.CreateActivity(context.Background(), "fail-act")
	require.NoError(t, err)

	actDef := fmt.Sprintf(`{"StartAt":"A","States":{"A":{"Type":"Task","Resource":%q,"End":true}}}`,
		act.ActivityArn)
	sm, err := b.CreateStateMachine(context.Background(), "act-fail-sm", actDef, validRoleARN, "STANDARD")
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

func TestAudit_Activity_SendTaskSuccessUnknownToken(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.SendTaskSuccess("unknown-token", `{}`)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrTaskTokenNotFound)
}

func TestAudit_Activity_SendTaskHeartbeat(t *testing.T) {
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

func TestAudit_Version_PublishAndDescribe(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "ver-pub-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	v, err := b.PublishStateMachineVersion(sm.StateMachineArn, "initial release", "")
	require.NoError(t, err)
	assert.NotEmpty(t, v.StateMachineVersionArn)
	assert.Equal(t, sm.StateMachineArn, v.StateMachineArn)
	assert.Equal(t, "initial release", v.Description)

	got, err := b.DescribeStateMachineVersion(v.StateMachineVersionArn)
	require.NoError(t, err)
	assert.Equal(t, v.StateMachineVersionArn, got.StateMachineVersionArn)
}

func TestAudit_Version_ListVersions(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "list-ver-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	for i := range 3 {
		_, err = b.PublishStateMachineVersion(sm.StateMachineArn, fmt.Sprintf("v%d", i+1), "")
		require.NoError(t, err)
	}

	versions, _, err := b.ListStateMachineVersions(sm.StateMachineArn, "", 100)
	require.NoError(t, err)
	assert.Len(t, versions, 3)
}

func TestAudit_Version_Delete(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "del-ver-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	v, err := b.PublishStateMachineVersion(sm.StateMachineArn, "", "")
	require.NoError(t, err)

	require.NoError(t, b.DeleteStateMachineVersion(v.StateMachineVersionArn))

	_, err = b.DescribeStateMachineVersion(v.StateMachineVersionArn)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineVersionDoesNotExist)
}

// ─── Aliases ──────────────────────────────────────────────────────────────────

func TestAudit_Alias_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "alias-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	v, err := b.PublishStateMachineVersion(sm.StateMachineArn, "", "")
	require.NoError(t, err)

	routing := []stepfunctions.AliasRoutingConfig{{StateMachineVersionArn: v.StateMachineVersionArn, Weight: 100}}
	alias, err := b.CreateStateMachineAlias(sm.StateMachineArn, "live", "prod alias", routing)
	require.NoError(t, err)
	assert.NotEmpty(t, alias.StateMachineAliasArn)
	assert.Equal(t, "live", alias.Name)

	got, err := b.DescribeStateMachineAlias(alias.StateMachineAliasArn)
	require.NoError(t, err)
	assert.Equal(t, "live", got.Name)
}

func TestAudit_Alias_ListAliases(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "list-alias-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	v, err := b.PublishStateMachineVersion(sm.StateMachineArn, "", "")
	require.NoError(t, err)

	routing := []stepfunctions.AliasRoutingConfig{{StateMachineVersionArn: v.StateMachineVersionArn, Weight: 100}}

	for _, name := range []string{"staging", "production"} {
		_, err = b.CreateStateMachineAlias(sm.StateMachineArn, name, "", routing)
		require.NoError(t, err)
	}

	aliases, _, err := b.ListStateMachineAliases(sm.StateMachineArn, "", 100)
	require.NoError(t, err)
	assert.Len(t, aliases, 2)
}

func TestAudit_Alias_Delete(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "del-alias-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	v, err := b.PublishStateMachineVersion(sm.StateMachineArn, "", "")
	require.NoError(t, err)

	routing := []stepfunctions.AliasRoutingConfig{{StateMachineVersionArn: v.StateMachineVersionArn, Weight: 100}}
	alias, err := b.CreateStateMachineAlias(sm.StateMachineArn, "del-alias", "", routing)
	require.NoError(t, err)

	require.NoError(t, b.DeleteStateMachineAlias(alias.StateMachineAliasArn))

	_, err = b.DescribeStateMachineAlias(alias.StateMachineAliasArn)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineAliasDoesNotExist)
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestAudit_Tags_CreateWithInlineTags(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body := map[string]any{
		"name":       "tagged-sm",
		"definition": validPassDef,
		"roleArn":    validRoleARN,
		"tags": []map[string]any{
			{"key": "env", "value": "prod"},
			{"key": "team", "value": "core"},
		},
	}

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(raw))
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	arnStr := out["stateMachineArn"].(string)
	listBody, err := json.Marshal(map[string]any{"resourceArn": arnStr})
	require.NoError(t, err)

	listRec := sfnPost(ctx, t, h, e, "ListTagsForResource", string(listBody))
	require.Equal(t, http.StatusOK, listRec.Code)

	var tags map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tags))

	tagList, _ := tags["tags"].([]any)
	assert.Len(t, tagList, 2)
}

func TestAudit_Tags_TagAndUntag(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	arnStr := createSM(ctx, t, h, e, "tag-sm")

	// Tag — this mock expects tags as a JSON object {"key":"value"}, not an AWS-style array.
	tagBody, err := json.Marshal(map[string]any{
		"resourceArn": arnStr,
		"tags":        map[string]string{"k1": "v1"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(tagBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify
	listBody, err := json.Marshal(map[string]any{"resourceArn": arnStr})
	require.NoError(t, err)

	listRec := sfnPost(ctx, t, h, e, "ListTagsForResource", string(listBody))
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagOut))

	tagList, _ := tagOut["tags"].([]any)
	assert.Len(t, tagList, 1)

	// Untag
	untagBody, err := json.Marshal(map[string]any{
		"resourceArn": arnStr,
		"tagKeys":     []string{"k1"},
	})
	require.NoError(t, err)

	untagRec := sfnPost(ctx, t, h, e, "UntagResource", string(untagBody))
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec2 := sfnPost(ctx, t, h, e, "ListTagsForResource", string(listBody))
	require.Equal(t, http.StatusOK, listRec2.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &after))

	tagList2, _ := after["tags"].([]any)
	assert.Empty(t, tagList2)
}

// ─── Logging / Tracing / Encryption configs ───────────────────────────────────

func TestAudit_Config_LoggingPersisted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "log-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	logging := &stepfunctions.LoggingConfiguration{
		Level:                "ALL",
		IncludeExecutionData: true,
		Destinations: []stepfunctions.LoggingDestination{{
			CloudWatchLogsLogGroup: &stepfunctions.CloudWatchLogsLogGroup{
				LogGroupArn: "arn:aws:logs:us-east-1:123:log-group:sfn:*",
			},
		}},
	}

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn, nil, logging, nil))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.LoggingConfiguration)
	assert.Equal(t, "ALL", got.LoggingConfiguration.Level)
	assert.True(t, got.LoggingConfiguration.IncludeExecutionData)
	require.Len(t, got.LoggingConfiguration.Destinations, 1)
	assert.Equal(t, "arn:aws:logs:us-east-1:123:log-group:sfn:*",
		got.LoggingConfiguration.Destinations[0].CloudWatchLogsLogGroup.LogGroupArn)
}

func TestAudit_Config_TracingPersisted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "trace-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn,
		&stepfunctions.TracingConfiguration{Enabled: true}, nil, nil))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.TracingConfiguration)
	assert.True(t, got.TracingConfiguration.Enabled)
}

func TestAudit_Config_EncryptionPersisted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "enc-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	enc := &stepfunctions.EncryptionConfiguration{
		KMSKeyID:                     "arn:aws:kms:us-east-1:123:key/abc",
		Type:                         "CUSTOMER_MANAGED_KMS_KEY",
		KMSDataKeyReusePeriodSeconds: 300,
	}

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn, nil, nil, enc))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.EncryptionConfiguration)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", got.EncryptionConfiguration.KMSKeyID)
	assert.Equal(t, 300, got.EncryptionConfiguration.KMSDataKeyReusePeriodSeconds)
}

func TestAudit_Config_NilArgDoesNotClearExisting(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "nil-cfg-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn,
		&stepfunctions.TracingConfiguration{Enabled: true}, nil, nil))

	// Passing nil logging must not clear existing tracing.
	require.NoError(t, b.SetStateMachineConfigurations(sm.StateMachineArn, nil,
		&stepfunctions.LoggingConfiguration{Level: "ERROR"}, nil))

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	require.NotNil(t, got.TracingConfiguration, "tracing should not be cleared by nil update")
	assert.True(t, got.TracingConfiguration.Enabled)
	require.NotNil(t, got.LoggingConfiguration)
	assert.Equal(t, "ERROR", got.LoggingConfiguration.Level)
}

// ─── ValidateStateMachineDefinition (HTTP) ────────────────────────────────────

func TestAudit_ValidateStateMachineDefinition_Valid(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body, err := json.Marshal(map[string]any{"definition": validPassDef})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "ValidateStateMachineDefinition", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_ValidateStateMachineDefinition_Invalid(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body, err := json.Marshal(map[string]any{"definition": `{"not":"valid-asl"}`})
	require.NoError(t, err)

	// AWS returns 200 with Result:"FAIL" and diagnostics — not a 4xx.
	rec := sfnPost(ctx, t, h, e, "ValidateStateMachineDefinition", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "FAIL", out["result"])
}

// ─── MapRun stubs ─────────────────────────────────────────────────────────────

func TestAudit_DescribeMapRun_ReturnsStub(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "DescribeMapRun", `{"mapRunArn":"arn:aws:states:us-east-1:123:mapRun:sm:exec:uuid"}`)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_ListMapRuns_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "ListMapRuns", `{"executionArn":"arn:aws:states:us-east-1:123:execution:sm:exec"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	mapRuns, _ := out["MapRuns"].([]any)
	assert.Empty(t, mapRuns)
}

// ─── RedriveExecution ─────────────────────────────────────────────────────────

func TestAudit_RedriveExecution_NotRedrivable(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "redrive-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	exec, err := b.StartExecution(sm.StateMachineArn, "rd-exec", "{}")
	require.NoError(t, err)

	// SUCCEEDED executions cannot be redriven.
	require.Eventually(t, func() bool {
		d, e := b.DescribeExecution(exec.ExecutionArn)

		return e == nil && d.Status == "SUCCEEDED"
	}, 5*time.Second, 20*time.Millisecond)

	_, err = b.RedriveExecution(exec.ExecutionArn)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrExecutionNotRedrivable)
}

// ─── roleArn validation ───────────────────────────────────────────────────────

func TestAudit_RoleARN_InvalidPrefix(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body := makeSMBody("role-bad-sm", validPassDef, "")
	body = strings.Replace(body, `"arn:role"`, `"not-an-arn"`, 1)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_RoleARN_WithWhitespace(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body := makeSMBody("role-ws-sm", validPassDef, "")
	body = strings.Replace(body, `"arn:role"`, `"arn:aws:iam:: 123:role/sfn"`, 1)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── InputPayload size validation ─────────────────────────────────────────────

func TestAudit_Input_ExactLimit_Succeeds(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "input-size-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)
	defer b.Destroy()

	// Build payload just under 256 KiB — account for JSON overhead.
	payload := `{"data":"` + strings.Repeat("x", 256*1024-12) + `"}`
	_, err = b.StartExecution(sm.StateMachineArn, "size-exec", payload)
	require.NoError(t, err)
}

func TestAudit_Input_OverLimit_Fails(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "input-over-sm", minimalDefinition, validRoleARN, "STANDARD")
	require.NoError(t, err)

	oversize := `{"data":"` + strings.Repeat("x", 256*1024+1) + `"}`
	_, err = b.StartExecution(sm.StateMachineArn, "over-exec", oversize)
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidExecutionInput)
}

// ─── Handler HTTP response codes ──────────────────────────────────────────────

func TestAudit_HTTP_StateMachineDoesNotExist_Returns404(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "DescribeStateMachine",
		`{"stateMachineArn":"arn:aws:states:us-east-1:123:stateMachine:ghost"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit_HTTP_ExecutionDoesNotExist_Returns404(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "DescribeExecution",
		`{"executionArn":"arn:aws:states:us-east-1:123:execution:sm:ghost"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit_HTTP_StateMachineAlreadyExists_Returns409(t *testing.T) {
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

func TestAudit_HTTP_InvalidDefinition_Returns400(t *testing.T) {
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

func TestAudit_HTTP_ActivityDoesNotExist_Returns404(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "DescribeActivity",
		`{"activityArn":"arn:aws:states:us-east-1:123:activity:ghost"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── CreateStateMachine HTTP response fields ──────────────────────────────────

func TestAudit_CreateStateMachine_ResponseContainsARNAndDate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine", makeSMBody("resp-sm", validPassDef, ""))
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.NotEmpty(t, out["stateMachineArn"])
	assert.Greater(t, out["creationDate"].(float64), float64(0))
}

// ─── StartExecution response fields ──────────────────────────────────────────

func TestAudit_StartExecution_ResponseContainsARNAndStartDate(t *testing.T) {
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
