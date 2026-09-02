package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteStackInstances_SurvivesFailedTeardown verifies that when a stack
// instance's provisioned child stack fails to delete -- here, blocked
// because another active stack still imports one of its exports, the same
// protection real DeleteStack enforces -- the instance is not silently
// dropped from the StackSet as if the delete had succeeded. Real
// CloudFormation documents exactly this outcome: "INOPERABLE: A
// DeleteStackInstances operation has failed and left the stack in an
// unstable state" (cloudformation@v1.76.1 types/types.go:1894,
// StackInstance.Status doc).
func TestDeleteStackInstances_SurvivesFailedTeardown(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("teardown-fail-ss"),
		TemplateBody: aws.String(exportTemplate),
	})
	require.NoError(t, err)

	_, err = client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String("teardown-fail-ss"),
		Accounts:     []string{"111111111111"},
		Regions:      []string{"us-east-1"},
	})
	require.NoError(t, err)

	_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("importer"),
		TemplateBody: aws.String(importTemplate),
	})
	require.NoError(t, err)

	deleteOut, err := client.DeleteStackInstances(t.Context(), &cfnsdk.DeleteStackInstancesInput{
		StackSetName: aws.String("teardown-fail-ss"),
		Accounts:     []string{"111111111111"},
		Regions:      []string{"us-east-1"},
		RetainStacks: aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotNil(t, deleteOut.OperationId)
	opID := *deleteOut.OperationId

	descOut, err := client.DescribeStackInstance(t.Context(), &cfnsdk.DescribeStackInstanceInput{
		StackSetName:         aws.String("teardown-fail-ss"),
		StackInstanceAccount: aws.String("111111111111"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.NoError(t, err, "instance must still be describable, not deleted")
	require.NotNil(t, descOut.StackInstance)
	assert.Equal(t, types.StackInstanceStatusInoperable, descOut.StackInstance.Status)

	listOut, err := client.ListStackInstances(t.Context(), &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String("teardown-fail-ss"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Summaries, 1, "instance must remain in the StackSet's instance list")
	assert.Equal(t, types.StackInstanceStatusInoperable, listOut.Summaries[0].Status)

	opOut, err := client.DescribeStackSetOperation(t.Context(), &cfnsdk.DescribeStackSetOperationInput{
		StackSetName: aws.String("teardown-fail-ss"),
		OperationId:  aws.String(opID),
	})
	require.NoError(t, err)
	require.NotNil(t, opOut.StackSetOperation)
	assert.Equal(t, types.StackSetOperationStatusFailed, opOut.StackSetOperation.Status)

	resultsOut, err := client.ListStackSetOperationResults(t.Context(), &cfnsdk.ListStackSetOperationResultsInput{
		StackSetName: aws.String("teardown-fail-ss"),
		OperationId:  aws.String(opID),
	})
	require.NoError(t, err)
	require.Len(t, resultsOut.Summaries, 1)
	assert.Equal(t, types.StackSetOperationResultStatusFailed, resultsOut.Summaries[0].Status)
	assert.Contains(t, aws.ToString(resultsOut.Summaries[0].StatusReason), "shared-bucket")

	inst, err := backend.DescribeStackInstance("teardown-fail-ss", "111111111111", "us-east-1")
	require.NoError(t, err)
	assert.Contains(t, inst.StatusReason, "shared-bucket")
	require.NotEmpty(t, inst.StackID)

	child, err := backend.DescribeStack(inst.StackID)
	require.NoError(t, err, "child stack must still exist since its teardown failed")
	assert.NotEqual(t, "DELETE_COMPLETE", child.StackStatus)
}
