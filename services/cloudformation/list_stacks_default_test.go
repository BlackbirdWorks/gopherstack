package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListStacks_NoFilter_IncludesDeletedStacks locks in
// ListStacksInput.StackStatusFilter's own doc comment
// (cloudformation@v1.76.1 api_op_ListStacks.go:14-16): "If no
// StackStatusFilter is specified, summary information for all stacks is
// returned (including existing stacks and stacks that have been deleted)."
// A wrong implementation would treat an empty filter as "active stacks
// only" and silently drop DELETE_COMPLETE entries -- the opposite of the ce
// ListCostCategoryDefinitions bug (empty date treated as no filter instead
// of "today"), but the same class: an absent optional filter still
// specifies behaviour, and that behaviour here is deliberately NOT plain
// "everything without regard to status" -- it explicitly promises deleted
// stacks stay visible.
func TestListStacks_NoFilter_IncludesDeletedStacks(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("list-default-active"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("list-default-deleted"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.DeleteStack(ctx, &cfnsdk.DeleteStackInput{
		StackName: aws.String("list-default-deleted"),
	})
	require.NoError(t, err)

	out, err := client.ListStacks(ctx, &cfnsdk.ListStacksInput{})
	require.NoError(t, err)

	byName := make(map[string]types.StackStatus, len(out.StackSummaries))
	for _, s := range out.StackSummaries {
		byName[*s.StackName] = s.StackStatus
	}

	assert.Contains(t, byName, "list-default-active", "an unfiltered ListStacks must still return live stacks")
	status, ok := byName["list-default-deleted"]
	require.True(
		t, ok, "an unfiltered ListStacks must return deleted stacks too, per StackStatusFilter's own doc comment",
	)
	assert.Equal(t, types.StackStatusDeleteComplete, status)
}
