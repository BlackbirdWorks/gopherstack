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

const cfTestTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyBucket": {
			"Type": "AWS::S3::Bucket",
			"Properties": {}
		}
	}
}`

const (
	cfnWaitMax      = 30 * time.Second
	cfnWaitMinDelay = 50 * time.Millisecond
	cfnWaitMaxDelay = 200 * time.Millisecond
)

// waitStackCreated blocks until the stack is CREATE_COMPLETE; the SDK's default
// 30s poll delay is shortened because gopherstack's provisioning window is sub-second.
func waitStackCreated(t *testing.T, client *cloudformationsdk.Client, name string) {
	t.Helper()

	err := cloudformationsdk.NewStackCreateCompleteWaiter(client).Wait(
		t.Context(), &cloudformationsdk.DescribeStacksInput{StackName: aws.String(name)}, cfnWaitMax,
		func(o *cloudformationsdk.StackCreateCompleteWaiterOptions) {
			o.MinDelay, o.MaxDelay = cfnWaitMinDelay, cfnWaitMaxDelay
		},
	)
	require.NoError(t, err, "stack %s should reach CREATE_COMPLETE", name)
}

// waitStackUpdated blocks until the stack is UPDATE_COMPLETE.
func waitStackUpdated(t *testing.T, client *cloudformationsdk.Client, name string) {
	t.Helper()

	err := cloudformationsdk.NewStackUpdateCompleteWaiter(client).Wait(
		t.Context(), &cloudformationsdk.DescribeStacksInput{StackName: aws.String(name)}, cfnWaitMax,
		func(o *cloudformationsdk.StackUpdateCompleteWaiterOptions) {
			o.MinDelay, o.MaxDelay = cfnWaitMinDelay, cfnWaitMaxDelay
		},
	)
	require.NoError(t, err, "stack %s should reach UPDATE_COMPLETE", name)
}

// waitStackDeleted blocks until the stack is DELETE_COMPLETE or gone.
func waitStackDeleted(t *testing.T, client *cloudformationsdk.Client, name string) {
	t.Helper()

	err := cloudformationsdk.NewStackDeleteCompleteWaiter(client).Wait(
		t.Context(), &cloudformationsdk.DescribeStacksInput{StackName: aws.String(name)}, cfnWaitMax,
		func(o *cloudformationsdk.StackDeleteCompleteWaiterOptions) {
			o.MinDelay, o.MaxDelay = cfnWaitMinDelay, cfnWaitMaxDelay
		},
	)
	require.NoError(t, err, "stack %s should be deleted", name)
}

// TestIntegration_CloudFormation_StackCreateCompleteWaiter verifies that
// StackCreateCompleteWaiter succeeds immediately after CreateStack completes.
func TestIntegration_CloudFormation_StackCreateCompleteWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudFormationClient(t)
	ctx := t.Context()

	stackName := "waiter-create-" + uuid.NewString()[:8]

	_, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(cfTestTemplate),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStack(cleanupCtx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})

	first, err := client.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
	require.Len(t, first.Stacks, 1)
	assert.Contains(t,
		[]cftypes.StackStatus{cftypes.StackStatusCreateInProgress, cftypes.StackStatusCreateComplete},
		first.Stacks[0].StackStatus,
	)

	start := time.Now()
	waitStackCreated(t, client, stackName)
	assert.Less(t, time.Since(start), 5*time.Second, "StackCreateCompleteWaiter should complete quickly")
}

// TestIntegration_CloudFormation_StackUpdateCompleteWaiter verifies that
// StackUpdateCompleteWaiter succeeds after UpdateStack.
func TestIntegration_CloudFormation_StackUpdateCompleteWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudFormationClient(t)
	ctx := t.Context()

	stackName := "waiter-update-" + uuid.NewString()[:8]

	_, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(cfTestTemplate),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStack(cleanupCtx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})

	// Update the stack
	updatedTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {}
			},
			"MyBucket2": {
				"Type": "AWS::S3::Bucket",
				"Properties": {}
			}
		}
	}`
	waitStackCreated(t, client, stackName)

	_, err = client.UpdateStack(ctx, &cloudformationsdk.UpdateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(updatedTemplate),
	})
	require.NoError(t, err)

	start := time.Now()
	waitStackUpdated(t, client, stackName)
	assert.Less(t, time.Since(start), 5*time.Second, "StackUpdateCompleteWaiter should complete quickly")
}

// TestIntegration_CloudFormation_StackDeleteCompleteWaiter verifies that
// StackDeleteCompleteWaiter succeeds after DeleteStack.
func TestIntegration_CloudFormation_StackDeleteCompleteWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudFormationClient(t)
	ctx := t.Context()

	stackName := "waiter-delete-" + uuid.NewString()[:8]

	_, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(cfTestTemplate),
	})
	require.NoError(t, err)

	waitStackCreated(t, client, stackName)

	_, err = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	start := time.Now()
	waitStackDeleted(t, client, stackName)
	assert.Less(t, time.Since(start), 5*time.Second, "StackDeleteCompleteWaiter should complete quickly")
}
