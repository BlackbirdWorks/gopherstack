package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for batch's four flagged List ops, verified via
// cmd/structfielddiff against batch@v1.68.4. ListConsumableResources and
// ListJobsByConsumableResource already matched their real summary types
// (ConsumableResourceSummary/ListJobsByConsumableResourceSummary) exactly.
// ListJobs had a real gap: JobSummary.jobDefinition/shareIdentifier/
// arrayProperties are sourced in the Job model but were never emitted.
// ListServiceJobs is unchanged: ServiceJobSummary's CapacityUsage/
// LatestAttempt have no backing state in the ServiceJob model (see
// PARITY.md items_still_open).
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("list jobs gains job definition share identifier and array properties", func(t *testing.T) {
		t.Parallel()

		h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
		client := newTestBatchClient(t, h)
		ctx := t.Context()

		ceName := "lss-ce-" + uuid.NewString()[:8]
		_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
			ComputeEnvironmentName: aws.String(ceName),
			Type:                   types.CETypeManaged,
		})
		require.NoError(t, err)

		qName := "lss-queue-" + uuid.NewString()[:8]
		_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
			JobQueueName: aws.String(qName),
			Priority:     aws.Int32(1),
			ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
				{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
			},
		})
		require.NoError(t, err)

		jdName := "lss-jd-" + uuid.NewString()[:8]
		_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
			JobDefinitionName: aws.String(jdName),
			Type:              types.JobDefinitionTypeContainer,
			ContainerProperties: &types.ContainerProperties{
				Image: aws.String("busybox"),
			},
		})
		require.NoError(t, err)

		_, err = client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
			JobName:         aws.String("lss-job-" + uuid.NewString()[:8]),
			JobQueue:        aws.String(qName),
			JobDefinition:   aws.String(jdName),
			ShareIdentifier: aws.String("shareA"),
			ArrayProperties: &types.ArrayProperties{Size: aws.Int32(3)},
		})
		require.NoError(t, err)

		listOut, err := client.ListJobs(ctx, &batchsdk.ListJobsInput{
			JobQueue:  aws.String(qName),
			JobStatus: types.JobStatusSubmitted,
		})
		require.NoError(t, err)
		require.Len(t, listOut.JobSummaryList, 1)

		s := listOut.JobSummaryList[0]
		assert.Contains(t, aws.ToString(s.JobDefinition), jdName)
		assert.Equal(t, "shareA", aws.ToString(s.ShareIdentifier))
		require.NotNil(t, s.ArrayProperties)
		assert.Equal(t, int32(3), aws.ToInt32(s.ArrayProperties.Size))
	})

	t.Run("list consumable resources exact", func(t *testing.T) {
		t.Parallel()

		h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
		client := newTestBatchClient(t, h)
		ctx := t.Context()

		_, err := client.CreateConsumableResource(ctx, &batchsdk.CreateConsumableResourceInput{
			ConsumableResourceName: aws.String("lss-res-" + uuid.NewString()[:8]),
			TotalQuantity:          aws.Int64(10),
			ResourceType:           aws.String("REPLENISHABLE"),
		})
		require.NoError(t, err)

		out, err := client.ListConsumableResources(ctx, &batchsdk.ListConsumableResourcesInput{})
		require.NoError(t, err)
		require.Len(t, out.ConsumableResources, 1)
		r := out.ConsumableResources[0]
		assert.NotEmpty(t, aws.ToString(r.ConsumableResourceArn))
		assert.NotEmpty(t, aws.ToString(r.ConsumableResourceName))
		assert.Equal(t, "REPLENISHABLE", aws.ToString(r.ResourceType))
		assert.Equal(t, int64(10), aws.ToInt64(r.TotalQuantity))
	})

	t.Run("list jobs by consumable resource exact", func(t *testing.T) {
		t.Parallel()

		h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
		client := newTestBatchClient(t, h)
		ctx := t.Context()

		ceName := "lss-ce-" + uuid.NewString()[:8]
		_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
			ComputeEnvironmentName: aws.String(ceName),
			Type:                   types.CETypeManaged,
		})
		require.NoError(t, err)

		qName := "lss-queue-" + uuid.NewString()[:8]
		_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
			JobQueueName: aws.String(qName),
			Priority:     aws.Int32(1),
			ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
				{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
			},
		})
		require.NoError(t, err)

		jdName := "lss-jd-" + uuid.NewString()[:8]
		_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
			JobDefinitionName: aws.String(jdName),
			Type:              types.JobDefinitionTypeContainer,
			ContainerProperties: &types.ContainerProperties{
				Image: aws.String("busybox"),
			},
		})
		require.NoError(t, err)

		resName := "lss-cr-" + uuid.NewString()[:8]
		_, err = client.CreateConsumableResource(ctx, &batchsdk.CreateConsumableResourceInput{
			ConsumableResourceName: aws.String(resName),
			TotalQuantity:          aws.Int64(5),
		})
		require.NoError(t, err)

		_, err = client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
			JobName:       aws.String("lss-job-" + uuid.NewString()[:8]),
			JobQueue:      aws.String(qName),
			JobDefinition: aws.String(jdName),
			ConsumableResourcePropertiesOverride: &types.ConsumableResourceProperties{
				ConsumableResourceList: []types.ConsumableResourceRequirement{
					{ConsumableResource: aws.String(resName), Quantity: aws.Int64(2)},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListJobsByConsumableResource(ctx, &batchsdk.ListJobsByConsumableResourceInput{
			ConsumableResource: aws.String(resName),
		})
		require.NoError(t, err)
		require.Len(t, out.Jobs, 1)
		j := out.Jobs[0]
		assert.NotEmpty(t, aws.ToString(j.JobArn))
		assert.NotEmpty(t, aws.ToString(j.JobQueueArn))
		assert.Equal(t, int64(2), aws.ToInt64(j.Quantity))
		require.NotNil(t, j.ConsumableResourceProperties)
		require.Len(t, j.ConsumableResourceProperties.ConsumableResourceList, 1)
		gotResource := j.ConsumableResourceProperties.ConsumableResourceList[0].ConsumableResource
		assert.Equal(t, resName, aws.ToString(gotResource))
	})

	t.Run("list service jobs exact", func(t *testing.T) {
		t.Parallel()

		h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
		client := newTestBatchClient(t, h)
		ctx := t.Context()

		ceName := "lss-sjce-" + uuid.NewString()[:8]
		_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
			ComputeEnvironmentName: aws.String(ceName),
			Type:                   types.CETypeManaged,
		})
		require.NoError(t, err)

		qName := "lss-sjqueue-" + uuid.NewString()[:8]
		_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
			JobQueueName: aws.String(qName),
			Priority:     aws.Int32(1),
			ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
				{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
			},
		})
		require.NoError(t, err)

		_, err = client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
			JobName:               aws.String("lss-sj-" + uuid.NewString()[:8]),
			JobQueue:              aws.String(qName),
			ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
			ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
			ShareIdentifier:       aws.String("shareB"),
		})
		require.NoError(t, err)

		out, err := client.ListServiceJobs(ctx, &batchsdk.ListServiceJobsInput{
			JobQueue:  aws.String(qName),
			JobStatus: types.ServiceJobStatusSubmitted,
		})
		require.NoError(t, err)
		require.Len(t, out.JobSummaryList, 1)
		s := out.JobSummaryList[0]
		assert.NotEmpty(t, aws.ToString(s.JobArn))
		assert.Equal(t, types.ServiceJobTypeSagemakerTraining, s.ServiceJobType)
		assert.Equal(t, "shareB", aws.ToString(s.ShareIdentifier))
	})
}
