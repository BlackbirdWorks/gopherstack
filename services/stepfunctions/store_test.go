package stepfunctions_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ stepfunctions.StorageBackend = (*stepfunctions.InMemoryBackend)(nil)
}

// TestRefinement1_AccountID verifies that AccountID returns the configured value.
func TestAccountID(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("111222333444", "eu-west-1")
	assert.Equal(t, "111222333444", b.AccountID())
}

// TestRefinement1_Region verifies that Region returns the configured value.
func TestRegion(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("111222333444", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestRefinement1_StateMachineCount verifies the StateMachineCount export helper.
func TestStateMachineCount(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	assert.Equal(t, 0, b.StateMachineCount())

	_, err := b.CreateStateMachine(context.Background(), "sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, 1, b.StateMachineCount())
}

// TestRefinement1_ExecutionCount verifies the ExecutionCount export helper.
func TestExecutionCount(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine(context.Background(), "sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	assert.Equal(t, 0, b.ExecutionCount())

	_, err = b.StartExecution(sm.StateMachineArn, "exec1", `{}`)
	require.NoError(t, err)
	assert.Equal(t, 1, b.ExecutionCount())
}

// TestRefinement1_ActivityCount verifies the ActivityCount export helper.
func TestActivityCount(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	assert.Equal(t, 0, b.ActivityCount())

	_, err := b.CreateActivity(context.Background(), "my-activity")
	require.NoError(t, err)
	assert.Equal(t, 1, b.ActivityCount())
}

// TestRefinement1_Reset verifies that Reset() clears all backend state.
func TestReset(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(b)

	_, err := b.CreateStateMachine(context.Background(), "sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, 1, b.StateMachineCount())

	h.Reset()
	assert.Equal(t, 0, b.StateMachineCount())
}
