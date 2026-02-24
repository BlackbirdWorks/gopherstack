package stepfunctions_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/stepfunctions"
)

func newBackend() *stepfunctions.InMemoryBackend {
	return stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

func TestCreateStateMachine(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, err := b.CreateStateMachine("my-sm", "{}", "arn:aws:iam::123456789012:role/role", "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, "my-sm", sm.Name)
	assert.Contains(t, sm.StateMachineArn, "my-sm")
	assert.Equal(t, "ACTIVE", sm.Status)
	assert.Equal(t, "STANDARD", sm.Type)
}

func TestCreateStateMachine_DefaultType(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, err := b.CreateStateMachine("typed-sm", "{}", "arn:role", "")
	require.NoError(t, err)
	assert.Equal(t, "STANDARD", sm.Type)
}

func TestCreateStateMachine_AlreadyExists(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateStateMachine("dup-sm", "{}", "arn:role", "STANDARD")
	require.NoError(t, err)

	_, err = b.CreateStateMachine("dup-sm", "{}", "arn:role", "STANDARD")
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineAlreadyExists)
}

func TestDescribeStateMachine(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, err := b.CreateStateMachine("desc-sm", `{"Comment":"test"}`, "arn:role", "EXPRESS")
	require.NoError(t, err)

	got, err := b.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	assert.Equal(t, "desc-sm", got.Name)
	assert.Equal(t, "EXPRESS", got.Type)
	assert.JSONEq(t, `{"Comment":"test"}`, got.Definition)
}

func TestDescribeStateMachine_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.DescribeStateMachine("arn:aws:states:us-east-1:123:stateMachine:nonexistent")
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

func TestListStateMachines(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateStateMachine("alpha-sm", "{}", "arn:role", "STANDARD")
	_, _ = b.CreateStateMachine("beta-sm", "{}", "arn:role", "STANDARD")

	sms, next, err := b.ListStateMachines("", 0)
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, sms, 2)
}

func TestListStateMachines_Pagination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	for i := range 5 {
		_, _ = b.CreateStateMachine(
			"sm-"+string(rune('a'+i)), "{}", "arn:role", "STANDARD",
		)
	}

	page1, next, err := b.ListStateMachines("", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, next)

	page2, next2, err := b.ListStateMachines(next, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, next2)

	page3, next3, err := b.ListStateMachines(next2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, next3)
}

func TestListStateMachines_EmptyToken(t *testing.T) {
	t.Parallel()
	b := newBackend()

	// nextToken beyond size returns empty
	sms, next, err := b.ListStateMachines("999", 0)
	require.NoError(t, err)
	assert.Empty(t, sms)
	assert.Empty(t, next)
}

func TestDeleteStateMachine(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, err := b.CreateStateMachine("to-delete", "{}", "arn:role", "STANDARD")
	require.NoError(t, err)

	err = b.DeleteStateMachine(sm.StateMachineArn)
	require.NoError(t, err)

	_, err = b.DescribeStateMachine(sm.StateMachineArn)
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

func TestDeleteStateMachine_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.DeleteStateMachine("arn:aws:states:us-east-1:123:stateMachine:nonexistent")
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

func TestStartExecution(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("exec-sm", "{}", "arn:role", "STANDARD")
	exec, err := b.StartExecution(sm.StateMachineArn, "exec1", `{"key":"value"}`)
	require.NoError(t, err)
	assert.Contains(t, exec.ExecutionArn, "exec1")
	assert.Equal(t, "SUCCEEDED", exec.Status)
	assert.JSONEq(t, `{"key":"value"}`, exec.Output)
	assert.NotNil(t, exec.StopDate)
}

func TestStartExecution_SMNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.StartExecution("arn:nonexistent", "exec1", "")
	require.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

func TestStartExecution_AlreadyExists(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("dup-exec-sm", "{}", "arn:role", "STANDARD")
	_, err := b.StartExecution(sm.StateMachineArn, "exec1", "")
	require.NoError(t, err)

	_, err = b.StartExecution(sm.StateMachineArn, "exec1", "")
	require.ErrorIs(t, err, stepfunctions.ErrExecutionAlreadyExists)
}

func TestDescribeExecution(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("desc-exec-sm", "{}", "arn:role", "STANDARD")
	exec, _ := b.StartExecution(sm.StateMachineArn, "exec1", `{"x":1}`)

	got, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", got.Status)
	assert.Equal(t, `{"x":1}`, got.Input)
}

func TestDescribeExecution_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.DescribeExecution("arn:nonexistent")
	require.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
}

func TestListExecutions(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("list-exec-sm", "{}", "arn:role", "STANDARD")
	_, _ = b.StartExecution(sm.StateMachineArn, "exec-a", "")
	_, _ = b.StartExecution(sm.StateMachineArn, "exec-b", "")

	execs, next, err := b.ListExecutions(sm.StateMachineArn, "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, execs, 2)
}

func TestListExecutions_StatusFilter(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("filter-sm", "{}", "arn:role", "STANDARD")
	_, _ = b.StartExecution(sm.StateMachineArn, "exec-a", "")

	// Filter by RUNNING - should return 0 (auto-SUCCEEDED)
	running, _, err := b.ListExecutions(sm.StateMachineArn, "RUNNING", "", 0)
	require.NoError(t, err)
	assert.Empty(t, running)

	// Filter by SUCCEEDED - should return 1
	succeeded, _, err := b.ListExecutions(sm.StateMachineArn, "SUCCEEDED", "", 0)
	require.NoError(t, err)
	assert.Len(t, succeeded, 1)
}

func TestGetExecutionHistory(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("hist-sm", "{}", "arn:role", "STANDARD")
	exec, _ := b.StartExecution(sm.StateMachineArn, "exec-h", "")

	events, next, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, events, 2)
	assert.Equal(t, "ExecutionStarted", events[0].Type)
	assert.Equal(t, "ExecutionSucceeded", events[1].Type)
}

func TestGetExecutionHistory_ReverseOrder(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("hist-rev-sm", "{}", "arn:role", "STANDARD")
	exec, _ := b.StartExecution(sm.StateMachineArn, "exec-rev", "")

	events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, true)
	require.NoError(t, err)
	assert.Equal(t, "ExecutionSucceeded", events[0].Type)
	assert.Equal(t, "ExecutionStarted", events[1].Type)
}

func TestGetExecutionHistory_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _, err := b.GetExecutionHistory("arn:nonexistent", "", 0, false)
	require.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
}

func TestStopExecution(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sm, _ := b.CreateStateMachine("stop-sm", "{}", "arn:role", "STANDARD")
	exec, _ := b.StartExecution(sm.StateMachineArn, "exec-stop", "")

	err := b.StopExecution(exec.ExecutionArn, "MyError", "stopped by test")
	require.NoError(t, err)

	got, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "ABORTED", got.Status)
	assert.Equal(t, "MyError", got.Error)
	assert.Equal(t, "stopped by test", got.Cause)
	assert.NotNil(t, got.StopDate)
}

func TestStopExecution_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.StopExecution("arn:nonexistent", "", "")
	require.ErrorIs(t, err, stepfunctions.ErrExecutionDoesNotExist)
}
