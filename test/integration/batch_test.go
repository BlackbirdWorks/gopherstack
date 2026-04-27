package integration_test

import (
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

	ceName := "test-ce-" + uuid.NewString()[:8]

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
	ceName := "jq-ce-" + suffix
	jqName := "test-jq-" + suffix

	// Create compute environment first
	ceOut, err := client.CreateComputeEnvironment(ctx, &batch.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   batchtypes.CETypeManaged,
		State:                  batchtypes.CEStateEnabled,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.UpdateJobQueue(ctx, &batch.UpdateJobQueueInput{
			JobQueue: aws.String(jqName),
			State:    batchtypes.JQStateDisabled,
		})
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

	// Disable before delete (required)
	_, err = client.UpdateJobQueue(ctx, &batch.UpdateJobQueueInput{
		JobQueue: aws.String(jqName),
		State:    batchtypes.JQStateDisabled,
	})
	require.NoError(t, err)

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

	jdName := "test-jd-" + uuid.NewString()[:8]

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

func TestIntegration_Batch_ConsumableResourceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBatchClient(t)
	ctx := t.Context()

	crName := "test-cr-" + uuid.NewString()[:8]

	// Create
	createOut, err := client.CreateConsumableResource(ctx, &batch.CreateConsumableResourceInput{
		ConsumableResourceName: aws.String(crName),
		TotalQuantity:          aws.Int64(100),
	})
	require.NoError(t, err)
	assert.Equal(t, crName, aws.ToString(createOut.ConsumableResourceName))
	assert.NotEmpty(t, aws.ToString(createOut.ConsumableResourceArn))

	t.Cleanup(func() {
		_, _ = client.DeleteConsumableResource(ctx, &batch.DeleteConsumableResourceInput{
			ConsumableResource: aws.String(crName),
		})
	})

	// Describe
	descOut, err := client.DescribeConsumableResource(ctx, &batch.DescribeConsumableResourceInput{
		ConsumableResource: aws.String(crName),
	})
	require.NoError(t, err)
	assert.Equal(t, crName, aws.ToString(descOut.ConsumableResourceName))

	// Delete
	_, err = client.DeleteConsumableResource(ctx, &batch.DeleteConsumableResourceInput{
		ConsumableResource: aws.String(crName),
	})
	require.NoError(t, err)
}

func TestIntegration_Batch_SchedulingPolicyLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBatchClient(t)
	ctx := t.Context()

	spName := "test-sp-" + uuid.NewString()[:8]

	// Create
	createOut, err := client.CreateSchedulingPolicy(ctx, &batch.CreateSchedulingPolicyInput{
		Name: aws.String(spName),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createOut.Arn))

	spArn := aws.ToString(createOut.Arn)

	t.Cleanup(func() {
		_, _ = client.DeleteSchedulingPolicy(ctx, &batch.DeleteSchedulingPolicyInput{
			Arn: aws.String(spArn),
		})
	})

	// ListSchedulingPolicies
	listOut, err := client.ListSchedulingPolicies(ctx, &batch.ListSchedulingPoliciesInput{})
	require.NoError(t, err)

	found := false

	for _, sp := range listOut.SchedulingPolicies {
		if aws.ToString(sp.Arn) == spArn {
			found = true

			break
		}
	}

	assert.True(t, found, "created scheduling policy should appear in list")

	// Delete
	_, err = client.DeleteSchedulingPolicy(ctx, &batch.DeleteSchedulingPolicyInput{
		Arn: aws.String(spArn),
	})
	require.NoError(t, err)
}

func TestIntegration_Batch_ServiceEnvironmentLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBatchClient(t)
	ctx := t.Context()

	seName := "test-se-" + uuid.NewString()[:8]

	// Create
	createOut, err := client.CreateServiceEnvironment(ctx, &batch.CreateServiceEnvironmentInput{
		ServiceEnvironmentName: aws.String(seName),
		ServiceEnvironmentType: batchtypes.ServiceEnvironmentTypeSagemakerTraining,
		State:                  batchtypes.ServiceEnvironmentStateEnabled,
	})
	require.NoError(t, err)
	assert.Equal(t, seName, aws.ToString(createOut.ServiceEnvironmentName))
	assert.NotEmpty(t, aws.ToString(createOut.ServiceEnvironmentArn))

	t.Cleanup(func() {
		_, _ = client.DeleteServiceEnvironment(ctx, &batch.DeleteServiceEnvironmentInput{
			ServiceEnvironment: aws.String(seName),
		})
	})

	// Describe
	descOut, err := client.DescribeServiceEnvironments(ctx, &batch.DescribeServiceEnvironmentsInput{
		ServiceEnvironments: []string{seName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ServiceEnvironments, 1)
	assert.Equal(t, seName, aws.ToString(descOut.ServiceEnvironments[0].ServiceEnvironmentName))

	// Delete
	_, err = client.DeleteServiceEnvironment(ctx, &batch.DeleteServiceEnvironmentInput{
		ServiceEnvironment: aws.String(seName),
	})
	require.NoError(t, err)
}
