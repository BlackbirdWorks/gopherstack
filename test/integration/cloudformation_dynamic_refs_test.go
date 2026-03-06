package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// Allow async processing.
	time.Sleep(500 * time.Millisecond)

	descOut, err := cfnClient.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.Stacks)
	assert.Equal(t, "CREATE_COMPLETE", string(descOut.Stacks[0].StackStatus))

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

	// Allow async processing.
	time.Sleep(500 * time.Millisecond)

	descOut, err := cfnClient.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.Stacks)
	assert.Equal(t, "CREATE_FAILED", string(descOut.Stacks[0].StackStatus))

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

	// Allow async processing.
	time.Sleep(500 * time.Millisecond)

	descOut, err := cfnClient.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.Stacks)
	assert.Equal(t, "CREATE_COMPLETE", string(descOut.Stacks[0].StackStatus))

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

	// Allow async processing.
	time.Sleep(500 * time.Millisecond)

	descOut, err := cfnClient.DescribeStacks(ctx, &cloudformationsdk.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.Stacks)
	assert.Equal(t, "CREATE_FAILED", string(descOut.Stacks[0].StackStatus))

	t.Cleanup(func() {
		_, _ = cfnClient.DeleteStack(t.Context(), &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	})
}
