package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_CloudFormation_StackLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudFormationClient(t)
	ctx := t.Context()

	stackName := "test-stack-" + uuid.NewString()[:8]
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {}
			}
		}
	}`

	// CreateStack
	createOut, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, createOut.StackId)

	// Wait for stack to reach CREATE_COMPLETE.
	var descOut *cloudformationsdk.DescribeStacksOutput
	require.Eventually(t, func() bool {
		var descErr error
		descOut, descErr = client.DescribeStacks(
			ctx, &cloudformationsdk.DescribeStacksInput{StackName: aws.String(stackName)},
		)

		return descErr == nil && len(descOut.Stacks) > 0 &&
			descOut.Stacks[0].StackStatus == cftypes.StackStatusCreateComplete
	}, 10*time.Second, 50*time.Millisecond)

	require.NotEmpty(t, descOut.Stacks)
	assert.Equal(t, stackName, *descOut.Stacks[0].StackName)

	// ListStacks
	listOut, err := client.ListStacks(ctx, &cloudformationsdk.ListStacksInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.StackSummaries)

	// DescribeStackEvents
	eventsOut, err := client.DescribeStackEvents(
		ctx,
		&cloudformationsdk.DescribeStackEventsInput{StackName: aws.String(stackName)},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, eventsOut.StackEvents)

	// DeleteStack
	_, err = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
}
