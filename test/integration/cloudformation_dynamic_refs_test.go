package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForStackStatus polls DescribeStacks until the stack is no longer in any
// in-progress state, then returns the final status.  It fails the test after
// the given deadline.
func waitForStackStatus(
	t *testing.T,
	client *cloudformationsdk.Client,
	stackName string,
	deadline time.Duration,
) string {
	t.Helper()

	ctx := t.Context()
	cutoff := time.Now().Add(deadline)

	for {
		descOut, err := client.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{
			StackName: aws.String(stackName),
		})
		require.NoError(t, err)
		require.NotEmpty(t, descOut.Stacks)

		status := string(descOut.Stacks[0].StackStatus)

		// Terminal states – stop polling.
		switch types.StackStatus(status) {
		case types.StackStatusCreateComplete,
			types.StackStatusCreateFailed,
			types.StackStatusRollbackComplete,
			types.StackStatusRollbackFailed,
			types.StackStatusUpdateComplete,
			types.StackStatusUpdateFailed,
			types.StackStatusDeleteComplete,
			types.StackStatusDeleteFailed:
			return status
		default:
			// In-progress or other transient states — keep polling.
		}

		if time.Now().After(cutoff) {
			require.Fail(t, "timeout waiting for stack to reach a terminal state", "last status: %s", status)

			return status
		}

		time.Sleep(250 * time.Millisecond)
	}
}

func TestIntegration_CloudFormation_DynamicRefs_SSM(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	cfnClient := createCloudFormationClient(t)
	ssmClient := createSSMClient(t)
	ctx := t.Context()

	paramName := "/cfn-dynref-test/" + uuid.NewString()[:8]

	// Create the SSM parameter first.
	_, err := ssmClient.PutParameter(ctx, &ssmsdk.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("my-dynamic-queue"),
		Type:  "String",
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = ssmClient.DeleteParameter(t.Context(), &ssmsdk.DeleteParameterInput{Name: aws.String(paramName)})
	})

	stackName := "cfn-dynref-ssm-" + uuid.NewString()[:8]
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {
					"QueueName": "{{resolve:ssm:` + paramName + `}}"
				}
			}
		}
	}`

	createOut, err := cfnClient.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, createOut.StackId)

	waiter := cloudformationsdk.NewStackCreateCompleteWaiter(cfnClient)
	err = waiter.Wait(ctx, &cloudformationsdk.DescribeStacksInput{
		StackName: aws.String(stackName),
	}, 5*time.Minute)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = cfnClient.DeleteStack(t.Context(), &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})
}

func TestIntegration_CloudFormation_DynamicRefs_SSMMissing(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	cfnClient := createCloudFormationClient(t)
	ctx := t.Context()

	stackName := "cfn-dynref-ssm-missing-" + uuid.NewString()[:8]
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {
					"QueueName": "{{resolve:ssm:/nonexistent/parameter}}"
				}
			}
		}
	}`

	createOut, err := cfnClient.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, createOut.StackId)

	finalStatus := waitForStackStatus(t, cfnClient, stackName, 2*time.Minute)
	assert.Equal(t, "CREATE_FAILED", finalStatus)

	// Verify that stack events document the failure.
	eventsOut, err := cfnClient.DescribeStackEvents(ctx, &cloudformationsdk.DescribeStackEventsInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	var failureFound bool

	for _, ev := range eventsOut.StackEvents {
		if string(ev.ResourceStatus) == "CREATE_FAILED" && ev.ResourceStatusReason != nil {
			failureFound = true

			break
		}
	}

	assert.True(t, failureFound, "expected a CREATE_FAILED event with a status reason")

	t.Cleanup(func() {
		_, _ = cfnClient.DeleteStack(t.Context(), &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})
}

func TestIntegration_CloudFormation_DynamicRefs_SecretsManager(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	cfnClient := createCloudFormationClient(t)
	smClient := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "cfn-dynref-test-" + uuid.NewString()[:8]

	// Create the secret first.
	_, err := smClient.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("my-secret-queue-name"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = smClient.DeleteSecret(t.Context(), &secretsmanagersdk.DeleteSecretInput{
			SecretId:                   aws.String(secretName),
			ForceDeleteWithoutRecovery: aws.Bool(true),
		})
	})

	stackName := "cfn-dynref-sm-" + uuid.NewString()[:8]
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {
					"QueueName": "{{resolve:secretsmanager:` + secretName + `}}"
				}
			}
		}
	}`

	createOut, err := cfnClient.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, createOut.StackId)

	waiter := cloudformationsdk.NewStackCreateCompleteWaiter(cfnClient)
	err = waiter.Wait(ctx, &cloudformationsdk.DescribeStacksInput{
		StackName: aws.String(stackName),
	}, 5*time.Minute)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = cfnClient.DeleteStack(t.Context(), &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})
}

func TestIntegration_CloudFormation_DynamicRefs_SecretsManagerMissing(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	cfnClient := createCloudFormationClient(t)
	ctx := t.Context()

	stackName := "cfn-dynref-sm-missing-" + uuid.NewString()[:8]
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {
					"QueueName": "{{resolve:secretsmanager:nonexistent-secret-xyz}}"
				}
			}
		}
	}`

	createOut, err := cfnClient.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, createOut.StackId)

	finalStatus := waitForStackStatus(t, cfnClient, stackName, 2*time.Minute)
	assert.Equal(t, "CREATE_FAILED", finalStatus)

	t.Cleanup(func() {
		_, _ = cfnClient.DeleteStack(t.Context(), &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})
}
