package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/swf"
)

func TestSWF_RegisterDomain(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.RegisterDomain("my-domain", "test domain")
	require.NoError(t, err)

	domains := b.ListDomains("REGISTERED")
	require.Len(t, domains, 1)
	assert.Equal(t, "my-domain", domains[0].Name)
	assert.Equal(t, "REGISTERED", domains[0].Status)
}

func TestSWF_RegisterDomain_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("my-domain", ""))

	err := b.RegisterDomain("my-domain", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrAlreadyExists)
}

func TestSWF_DeprecateDomain(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("my-domain", ""))

	err := b.DeprecateDomain("my-domain")
	require.NoError(t, err)

	domains := b.ListDomains("DEPRECATED")
	require.Len(t, domains, 1)
}

func TestSWF_DeprecateDomain_NotFound(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.DeprecateDomain("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNotFound)
}

func TestSWF_RegisterWorkflowType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("my-domain", ""))

	err := b.RegisterWorkflowType("my-domain", "my-workflow", "1.0")
	require.NoError(t, err)

	wts := b.ListWorkflowTypes("my-domain")
	require.Len(t, wts, 1)
	assert.Equal(t, "my-workflow", wts[0].Name)
}

func TestSWF_StartAndDescribeWorkflowExecution(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	exec, err := b.StartWorkflowExecution("my-domain", "wf-001", "run-001")
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", exec.Status)

	got, err := b.DescribeWorkflowExecution("my-domain", "wf-001")
	require.NoError(t, err)
	assert.Equal(t, "run-001", got.RunID)
}

func TestSWF_DescribeWorkflowExecution_NotFound(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	_, err := b.DescribeWorkflowExecution("my-domain", "nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNotFound)
}

func TestSWF_ListDomains_AllStatuses(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("d1", ""))
	require.NoError(t, b.RegisterDomain("d2", ""))

	all := b.ListDomains("")
	assert.Len(t, all, 2)
}
