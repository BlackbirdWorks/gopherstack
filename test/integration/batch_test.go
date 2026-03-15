package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchtypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Batch_ComputeEnvironmentLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBatchClient(t)
	ctx := t.Context()

	ceName := fmt.Sprintf("test-ce-%s", uuid.NewString()[:8])

	// CreateComputeEnvironment
	createOut, err := client.CreateComputeEnvironment(ctx, &batch.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   batchtypes.CETypeManaged,
		State:                  batchtypes.CEStateEnabled,
	})
	require.NoError(t, err)
	assert.Equal(t, ceName, aws.ToString(createOut.ComputeEnvironmentName))
	assert.NotEmpty(t, aws.ToString(createOut.ComputeEnvironmentArn))

	t.Cleanup(func() {
		_, _ = client.DeleteComputeEnvironment(ctx, &batch.DeleteComputeEnvironmentInput{
			ComputeEnvironment: aws.String(ceName),
		})
	})

	// DescribeComputeEnvironments
	descOut, err := client.DescribeComputeEnvironments(ctx, &batch.DescribeComputeEnvironmentsInput{
		ComputeEnvironments: []string{ceName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ComputeEnvironments, 1)
	assert.Equal(t, ceName, aws.ToString(descOut.ComputeEnvironments[0].ComputeEnvironmentName))

	// DescribeComputeEnvironments - list all
	listOut, err := client.DescribeComputeEnvironments(ctx, &batch.DescribeComputeEnvironmentsInput{})
	require.NoError(t, err)

	found := false

	for _, ce := range listOut.ComputeEnvironments {
		if aws.ToString(ce.ComputeEnvironmentName) == ceName {
			found = true

			break
		}
	}

	assert.True(t, found, "created compute environment should appear in list")

	// DeleteComputeEnvironment
	_, err = client.DeleteComputeEnvironment(ctx, &batch.DeleteComputeEnvironmentInput{
		ComputeEnvironment: aws.String(ceName),
	})
	require.NoError(t, err)

	// Verify deleted
	descOut2, err := client.DescribeComputeEnvironments(ctx, &batch.DescribeComputeEnvironmentsInput{
		ComputeEnvironments: []string{ceName},
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.ComputeEnvironments)
}

func TestIntegration_Batch_JobQueueLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBatchClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	ceName := fmt.Sprintf("jq-ce-%s", suffix)
	jqName := fmt.Sprintf("test-jq-%s", suffix)

	// Create compute environment first
	ceOut, err := client.CreateComputeEnvironment(ctx, &batch.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   batchtypes.CETypeManaged,
		State:                  batchtypes.CEStateEnabled,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteJobQueue(ctx, &batch.DeleteJobQueueInput{
			JobQueue: aws.String(jqName),
		})
		_, _ = client.DeleteComputeEnvironment(ctx, &batch.DeleteComputeEnvironmentInput{
			ComputeEnvironment: aws.String(ceName),
		})
	})

	// CreateJobQueue
	createOut, err := client.CreateJobQueue(ctx, &batch.CreateJobQueueInput{
		JobQueueName: aws.String(jqName),
		Priority:     aws.Int32(10),
		State:        batchtypes.JQStateEnabled,
		ComputeEnvironmentOrder: []batchtypes.ComputeEnvironmentOrder{
			{
				ComputeEnvironment: ceOut.ComputeEnvironmentArn,
				Order:              aws.Int32(1),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, jqName, aws.ToString(createOut.JobQueueName))
	assert.NotEmpty(t, aws.ToString(createOut.JobQueueArn))

	// DescribeJobQueues
	descOut, err := client.DescribeJobQueues(ctx, &batch.DescribeJobQueuesInput{
		JobQueues: []string{jqName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.JobQueues, 1)
	assert.Equal(t, jqName, aws.ToString(descOut.JobQueues[0].JobQueueName))

	// DeleteJobQueue
	_, err = client.DeleteJobQueue(ctx, &batch.DeleteJobQueueInput{
		JobQueue: aws.String(jqName),
	})
	require.NoError(t, err)

	// Verify deleted
	descOut2, err := client.DescribeJobQueues(ctx, &batch.DescribeJobQueuesInput{
		JobQueues: []string{jqName},
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.JobQueues)
}

func TestIntegration_Batch_JobDefinitionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBatchClient(t)
	ctx := t.Context()

	jdName := fmt.Sprintf("test-jd-%s", uuid.NewString()[:8])

	// RegisterJobDefinition
	registerOut, err := client.RegisterJobDefinition(ctx, &batch.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String(jdName),
		Type:              batchtypes.JobDefinitionTypeContainer,
	})
	require.NoError(t, err)
	assert.Equal(t, jdName, aws.ToString(registerOut.JobDefinitionName))
	assert.NotEmpty(t, aws.ToString(registerOut.JobDefinitionArn))
	assert.Equal(t, int32(1), aws.ToInt32(registerOut.Revision))

	jdARN := aws.ToString(registerOut.JobDefinitionArn)

	t.Cleanup(func() {
		_, _ = client.DeregisterJobDefinition(ctx, &batch.DeregisterJobDefinitionInput{
			JobDefinition: aws.String(jdARN),
		})
	})

	// DescribeJobDefinitions
	descOut, err := client.DescribeJobDefinitions(ctx, &batch.DescribeJobDefinitionsInput{
		JobDefinitionName: aws.String(jdName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.JobDefinitions)
	assert.Equal(t, jdName, aws.ToString(descOut.JobDefinitions[0].JobDefinitionName))

	// DeregisterJobDefinition
	_, err = client.DeregisterJobDefinition(ctx, &batch.DeregisterJobDefinitionInput{
		JobDefinition: aws.String(jdARN),
	})
	require.NoError(t, err)

	// Verify inactive after deregister — status filter is not supported by the handler,
	// so query by name and check the status field directly.
	descOut2, err := client.DescribeJobDefinitions(ctx, &batch.DescribeJobDefinitionsInput{
		JobDefinitionName: aws.String(jdName),
	})
	require.NoError(t, err)
	require.Len(t, descOut2.JobDefinitions, 1)
	assert.Equal(t, "INACTIVE", aws.ToString(descOut2.JobDefinitions[0].Status))
}
