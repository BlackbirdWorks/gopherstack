package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
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
		_, _ = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})

	waiter := cloudformationsdk.NewStackCreateCompleteWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &cloudformationsdk.DescribeStacksInput{StackName: aws.String(stackName)}, 5*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "StackCreateCompleteWaiter should succeed after CreateStack")
	assert.Less(t, elapsed, 3*time.Second, "StackCreateCompleteWaiter should complete quickly, took %v", elapsed)
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
		_, _ = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
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
	_, err = client.UpdateStack(ctx, &cloudformationsdk.UpdateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(updatedTemplate),
	})
	require.NoError(t, err)

	waiter := cloudformationsdk.NewStackUpdateCompleteWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &cloudformationsdk.DescribeStacksInput{StackName: aws.String(stackName)}, 5*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "StackUpdateCompleteWaiter should succeed after UpdateStack")
	assert.Less(t, elapsed, 3*time.Second, "StackUpdateCompleteWaiter should complete quickly, took %v", elapsed)
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

	// Delete the stack
	_, err = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	waiter := cloudformationsdk.NewStackDeleteCompleteWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &cloudformationsdk.DescribeStacksInput{StackName: aws.String(stackName)}, 5*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "StackDeleteCompleteWaiter should succeed after DeleteStack")
	assert.Less(t, elapsed, 3*time.Second, "StackDeleteCompleteWaiter should complete quickly, took %v", elapsed)
}
