package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestStorageBackendInterface verifies the var _ StorageBackend assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ swf.StorageBackend = (*swf.InMemoryBackend)(nil)
}

// TestInMemoryBackend_AccountID verifies AccountID returns a non-empty string.
func TestInMemoryBackend_AccountID(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	assert.NotEmpty(t, b.AccountID())
}

// TestInMemoryBackend_Reset verifies Reset clears all state.
func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("d1", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("d1", "wf1", "1.0", "", swf.WorkflowTypeDefaults{}))
	b.AddActivityTypeInternal("d1", "act1", "1.0", "REGISTERED")
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "d1", WorkflowID: "wf-1", RunID: "run-1",
	})
	require.NoError(t, err)

	b.Reset()

	assert.Zero(t, swf.DomainCount(b))
	assert.Zero(t, swf.WorkflowTypeCount(b))
	assert.Zero(t, swf.ActivityTypeCount(b))
	assert.Zero(t, swf.ExecutionCount(b))
}

// TestInMemoryBackend_ExportCounts verifies the export helpers accurately
// reflect backend state across every resource family.
func TestInMemoryBackend_ExportCounts(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()

	assert.Zero(t, swf.DomainCount(b))
	assert.Zero(t, swf.WorkflowTypeCount(b))
	assert.Zero(t, swf.ActivityTypeCount(b))
	assert.Zero(t, swf.ExecutionCount(b))

	require.NoError(t, b.RegisterDomain("dom1", "", "NONE"))
	assert.Equal(t, 1, swf.DomainCount(b))

	require.NoError(t, b.RegisterWorkflowType("dom1", "wf1", "1.0", "", swf.WorkflowTypeDefaults{}))
	assert.Equal(t, 1, swf.WorkflowTypeCount(b))

	require.NoError(t, b.RegisterActivityType("dom1", "act1", "1.0", "", swf.ActivityTypeDefaults{}))
	assert.Equal(t, 1, swf.ActivityTypeCount(b))

	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom1", WorkflowID: "wf-1", RunID: "run-1",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, swf.ExecutionCount(b))

	assert.Equal(t, 39, swf.HandlerOpsLen(swf.NewHandler(b)))
}
