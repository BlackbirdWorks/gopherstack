package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestCreateStackInstances_ProvisionsChildStacks(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("prov-ss", "desc", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	before := len(b.ListAll())

	_, err = b.CreateStackInstances(
		t.Context(),
		"prov-ss",
		[]string{"111111111111", "222222222222"},
		[]string{"us-east-1"},
	)
	require.NoError(t, err)

	instances, err := b.ListStackInstances("prov-ss", "")
	require.NoError(t, err)
	require.Len(t, instances.Data, 2)

	// Each instance must reference a real, describable child stack.
	for _, inst := range instances.Data {
		assert.Equal(t, "CURRENT", inst.Status)
		require.NotEmpty(t, inst.StackID)
		child, derr := b.DescribeStack(inst.StackID)
		require.NoError(t, derr, "instance stack %s must be a real stack", inst.StackID)
		assert.Equal(t, "CREATE_COMPLETE", child.StackStatus)
	}

	after := len(b.ListAll())
	assert.Equal(t, before+2, after, "two child stacks must have been provisioned")
}

func TestDeleteStackInstances_TearsDownChildStacks(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("teardown-ss", "desc", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	_, err = b.CreateStackInstances(t.Context(), "teardown-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)

	instances, err := b.ListStackInstances("teardown-ss", "")
	require.NoError(t, err)
	require.Len(t, instances.Data, 1)
	childID := instances.Data[0].StackID

	_, err = b.DeleteStackInstances(t.Context(), "teardown-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)

	remaining, err := b.ListStackInstances("teardown-ss", "")
	require.NoError(t, err)
	assert.Empty(t, remaining.Data)

	// The provisioned child stack must have been torn down.
	child, derr := b.DescribeStack(childID)
	if derr == nil {
		assert.Equal(t, "DELETE_COMPLETE", child.StackStatus)
	}
}
