package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &swf.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &swf.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ swf.StorageBackend = (*swf.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	h := swf.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 39)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	h := swf.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_BackendReset verifies Reset clears all state.
func TestRefinement1_BackendReset(t *testing.T) {
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

// TestRefinement1_HandlerReset verifies Handler.Reset() delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("d1", "", "NONE"))
	h := swf.NewHandler(b)

	h.Reset()

	assert.Zero(t, swf.DomainCount(b))
}

// TestRefinement1_ExportCounts verifies export helpers are accurate.
func TestRefinement1_ExportCounts(t *testing.T) {
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

// TestRefinement1_ErrValidation_RegisterDomain verifies empty name returns ErrValidation.
func TestRefinement1_ErrValidation_RegisterDomain(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.RegisterDomain("", "desc", "NONE")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrValidation)
}

// TestRefinement1_ErrValidation_RegisterWorkflowType verifies validation of required fields.
func TestRefinement1_ErrValidation_RegisterWorkflowType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		domain  string
		wfName  string
		version string
	}{
		{name: "empty_domain", domain: "", wfName: "wf", version: "1.0"},
		{name: "empty_name", domain: "d1", wfName: "", version: "1.0"},
		{name: "empty_version", domain: "d1", wfName: "wf", version: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			err := b.RegisterWorkflowType(tt.domain, tt.wfName, tt.version, "", swf.WorkflowTypeDefaults{})

			require.Error(t, err)
			assert.ErrorIs(t, err, swf.ErrValidation)
		})
	}
}

// TestRefinement1_ErrValidation_StartWorkflowExecution validates required fields.
func TestRefinement1_ErrValidation_StartWorkflowExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		workflowID string
	}{
		{name: "empty_domain", domain: "", workflowID: "wf-1"},
		{name: "empty_workflowID", domain: "d1", workflowID: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     tt.domain,
				WorkflowID: tt.workflowID,
				RunID:      "run-1",
			})

			require.Error(t, err)
			assert.ErrorIs(t, err, swf.ErrValidation)
		})
	}
}

// TestRefinement1_DeprecateDomain_AlreadyDeprecated checks double-deprecate returns ErrDeprecated.
func TestRefinement1_DeprecateDomain_AlreadyDeprecated(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.DeprecateDomain("dom"))

	err := b.DeprecateDomain("dom")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrDeprecated)
}

// TestRefinement1_UndeprecateDomain verifies round-trip deprecate→undeprecate.
func TestRefinement1_UndeprecateDomain(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.DeprecateDomain("dom"))
	require.NoError(t, b.UndeprecateDomain("dom"))

	d, err := b.DescribeDomain("dom")
	require.NoError(t, err)
	assert.Equal(t, "REGISTERED", d.Status)
}

// TestRefinement1_UndeprecateDomain_NotFound returns ErrNotFound for missing domain.
func TestRefinement1_UndeprecateDomain_NotFound(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.UndeprecateDomain("missing")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNotFound)
}

// TestRefinement1_UndeprecateDomain_AlreadyRegistered returns ErrAlreadyExists if not deprecated.
func TestRefinement1_UndeprecateDomain_AlreadyRegistered(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))

	err := b.UndeprecateDomain("dom")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrAlreadyExists)
}

// TestRefinement1_RegisterWorkflowType_StoredDescription verifies description is stored.
func TestRefinement1_RegisterWorkflowType_StoredDescription(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "1.0", "my desc", swf.WorkflowTypeDefaults{}))

	wt, err := b.DescribeWorkflowType("dom", "wf", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "my desc", wt.Description)
}

// TestRefinement1_ListWorkflowTypes_FilterStatus verifies registrationStatus filter works.
func TestRefinement1_ListWorkflowTypes_FilterStatus(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf1", "1.0", "", swf.WorkflowTypeDefaults{}))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf2", "1.0", "", swf.WorkflowTypeDefaults{}))
	require.NoError(t, b.DeprecateWorkflowType("dom", "wf2", "1.0"))

	registered, err := b.ListWorkflowTypes("dom", "REGISTERED")
	require.NoError(t, err)
	deprecated, err := b.ListWorkflowTypes("dom", "DEPRECATED")
	require.NoError(t, err)
	all, err := b.ListWorkflowTypes("dom", "")
	require.NoError(t, err)

	assert.Len(t, registered, 1)
	assert.Equal(t, "wf1", registered[0].Name)
	assert.Len(t, deprecated, 1)
	assert.Equal(t, "wf2", deprecated[0].Name)
	assert.Len(t, all, 2)
}

// TestRefinement1_UndeprecateWorkflowType verifies round-trip deprecate→undeprecate.
func TestRefinement1_UndeprecateWorkflowType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "1.0", "", swf.WorkflowTypeDefaults{}))
	require.NoError(t, b.DeprecateWorkflowType("dom", "wf", "1.0"))
	require.NoError(t, b.UndeprecateWorkflowType("dom", "wf", "1.0"))

	wt, err := b.DescribeWorkflowType("dom", "wf", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "REGISTERED", wt.Status)
}

// TestRefinement1_UndeprecateWorkflowType_AlreadyRegistered returns ErrTypeAlreadyExists.
func TestRefinement1_UndeprecateWorkflowType_AlreadyRegistered(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "1.0", "", swf.WorkflowTypeDefaults{}))

	err := b.UndeprecateWorkflowType("dom", "wf", "1.0")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrTypeAlreadyExists)
}

// TestRefinement1_RegisterActivityType verifies creation and retrieval.
func TestRefinement1_RegisterActivityType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterActivityType("dom", "act", "2.0", "activity desc", swf.ActivityTypeDefaults{}))

	at, err := b.DescribeActivityType("dom", "act", "2.0")
	require.NoError(t, err)
	assert.Equal(t, "act", at.Name)
	assert.Equal(t, "REGISTERED", at.Status)
	assert.Equal(t, "activity desc", at.Description)
}

// TestRefinement1_RegisterActivityType_Duplicate verifies ErrTypeAlreadyExists.
func TestRefinement1_RegisterActivityType_Duplicate(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterActivityType("dom", "act", "1.0", "", swf.ActivityTypeDefaults{}))

	err := b.RegisterActivityType("dom", "act", "1.0", "", swf.ActivityTypeDefaults{})

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrTypeAlreadyExists)
}

// TestRefinement1_ListActivityTypes_FilterStatus verifies registrationStatus filter.
func TestRefinement1_ListActivityTypes_FilterStatus(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterActivityType("dom", "act1", "1.0", "", swf.ActivityTypeDefaults{}))
	require.NoError(t, b.RegisterActivityType("dom", "act2", "1.0", "", swf.ActivityTypeDefaults{}))
	require.NoError(t, b.DeprecateActivityType("dom", "act2", "1.0"))

	reg, err := b.ListActivityTypes("dom", "REGISTERED")
	require.NoError(t, err)
	dep, err := b.ListActivityTypes("dom", "DEPRECATED")
	require.NoError(t, err)
	all, err := b.ListActivityTypes("dom", "")
	require.NoError(t, err)

	assert.Len(t, reg, 1)
	assert.Len(t, dep, 1)
	assert.Len(t, all, 2)
}

// TestRefinement1_UndeprecateActivityType verifies round-trip deprecate→undeprecate.
func TestRefinement1_UndeprecateActivityType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterActivityType("dom", "act", "1.0", "", swf.ActivityTypeDefaults{}))
	require.NoError(t, b.DeprecateActivityType("dom", "act", "1.0"))
	require.NoError(t, b.UndeprecateActivityType("dom", "act", "1.0"))

	at, err := b.DescribeActivityType("dom", "act", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "REGISTERED", at.Status)
}

// TestRefinement1_TerminateWorkflowExecution verifies status change.
func TestRefinement1_TerminateWorkflowExecution(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", RunID: "run-1",
	})
	require.NoError(t, err)

	require.NoError(t, b.TerminateWorkflowExecution("dom", "wf-1", "", "", ""))

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
	require.NoError(t, err)
	assert.Equal(t, "TERMINATED", exec.Status)
	assert.Equal(t, "TERMINATED", exec.CloseStatus)
	assert.NotZero(t, exec.CloseTimestamp)
}

// TestRefinement1_TerminateWorkflowExecution_NotFound returns ErrNotFound.
func TestRefinement1_TerminateWorkflowExecution_NotFound(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.TerminateWorkflowExecution("dom", "missing", "", "", "")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNotFound)
}

// TestRefinement1_TerminateWorkflowExecution_AlreadyTerminated returns ErrNotFound.
func TestRefinement1_TerminateWorkflowExecution_AlreadyTerminated(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", RunID: "run-1",
	})
	require.NoError(t, err)

	require.NoError(t, b.TerminateWorkflowExecution("dom", "wf-1", "", "", ""))

	err = b.TerminateWorkflowExecution("dom", "wf-1", "", "", "")

	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNotFound)
}

// TestRefinement1_StartWorkflowExecution_SetsTimestamp verifies StartTimestamp is non-zero.
func TestRefinement1_StartWorkflowExecution_SetsTimestamp(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	exec, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", RunID: "run-1",
	})

	require.NoError(t, err)
	assert.NotZero(t, exec.StartTimestamp)
}

// TestRefinement1_CountTerminatedExecution_CountsClosed verifies terminated goes to closed.
func TestRefinement1_CountTerminatedExecution_CountsClosed(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", RunID: "run-1",
	})
	require.NoError(t, err)

	assert.Equal(t, 1, b.CountOpenWorkflowExecutions("dom", swf.ExecutionFilter{}))
	assert.Equal(t, 0, b.CountClosedWorkflowExecutions("dom", swf.ExecutionFilter{}))

	require.NoError(t, b.TerminateWorkflowExecution("dom", "wf-1", "", "", ""))

	assert.Equal(t, 0, b.CountOpenWorkflowExecutions("dom", swf.ExecutionFilter{}))
	assert.Equal(t, 1, b.CountClosedWorkflowExecutions("dom", swf.ExecutionFilter{}))
}

// TestRefinement1_SnapshotRestoreActivities verifies activities survive snapshot/restore.
func TestRefinement1_SnapshotRestoreActivities(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterActivityType("dom", "act", "1.0", "persisted", swf.ActivityTypeDefaults{}))

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := swf.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	at, err := b2.DescribeActivityType("dom", "act", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "persisted", at.Description)
}

// TestRefinement1_SnapshotRestoreWorkflowTypes verifies workflow types survive snapshot/restore.
func TestRefinement1_SnapshotRestoreWorkflowTypes(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "2.0", "wf desc", swf.WorkflowTypeDefaults{}))
	require.NoError(t, b.DeprecateWorkflowType("dom", "wf", "2.0"))

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := swf.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	wt, err := b2.DescribeWorkflowType("dom", "wf", "2.0")
	require.NoError(t, err)
	assert.Equal(t, "DEPRECATED", wt.Status)
	assert.Equal(t, "wf desc", wt.Description)
}

// TestRefinement1_AccountID verifies AccountID returns a non-empty string.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	assert.NotEmpty(t, b.AccountID())
}
