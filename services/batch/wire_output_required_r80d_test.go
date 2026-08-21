package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// Test_SDKRoundTrip_DescribeJobs_StartedAt proves JobDetail.StartedAt --
// required unconditionally on the real API (api_op_DescribeJobs.go /
// types.JobDetail) -- is never dropped for a freshly submitted job. A real
// client can call DescribeJobs immediately after SubmitJob, well before this
// backend's opt-in janitor (never started here) would advance the job past
// SUBMITTED, so StartedAt is nil internally; the wire struct's prior
// `omitempty` tag dropped the required key entirely in that state instead of
// emitting the documented zero value.
func Test_SDKRoundTrip_DescribeJobs_StartedAt(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "sa-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "sa-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	jdName := "sa-jd-" + uuid.NewString()[:8]
	_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String(jdName),
		Type:              types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{
			Image: aws.String("busybox"),
		},
	})
	require.NoError(t, err)

	submitOut, err := client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
		JobName:       aws.String("sa-job-" + uuid.NewString()[:8]),
		JobQueue:      aws.String(qName),
		JobDefinition: aws.String(jdName),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeJobs(ctx, &batchsdk.DescribeJobsInput{Jobs: []string{aws.ToString(submitOut.JobId)}})
	require.NoError(t, err)
	require.Len(t, descOut.Jobs, 1)
	require.NotNil(
		t,
		descOut.Jobs[0].StartedAt,
		"JobDetail.StartedAt is required and must decode non-nil even before the job starts",
	)
	require.Equal(t, int64(0), *descOut.Jobs[0].StartedAt)
}

// Test_SDKRoundTrip_DescribeServiceJob_StartedAt is the ServiceJob analog of
// Test_SDKRoundTrip_DescribeJobs_StartedAt: DescribeServiceJobOutput.StartedAt
// is likewise required unconditionally (api_op_DescribeServiceJob.go).
func Test_SDKRoundTrip_DescribeServiceJob_StartedAt(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "sasj-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "sasj-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	submitOut, err := client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("sasj-job-" + uuid.NewString()[:8]),
		JobQueue:              aws.String(qName),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeServiceJob(ctx, &batchsdk.DescribeServiceJobInput{JobId: submitOut.JobId})
	require.NoError(t, err)
	require.NotNil(
		t,
		descOut.StartedAt,
		"DescribeServiceJobOutput.StartedAt is required and must decode non-nil even before the job starts",
	)
	require.Equal(t, int64(0), *descOut.StartedAt)
}

// Test_SDKRoundTrip_ComputeResource_MaxvCpus proves ComputeResource.MaxvCpus
// -- required on ComputeEnvironmentDetail (types/types.go) -- survives a
// real client's explicit zero value. The real SDK's own client-side
// validateComputeResource (validators.go) only rejects a nil MaxvCpus
// pointer, not zero, so aws.Int32(0) is a state a real client can reach
// without bypassing any validation.
func Test_SDKRoundTrip_ComputeResource_MaxvCpus(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "mvc-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
		ComputeResources: &types.ComputeResource{
			Type:          types.CRTypeEc2,
			MaxvCpus:      aws.Int32(0),
			MinvCpus:      aws.Int32(0),
			InstanceRole:  aws.String("arn:aws:iam::000000000000:instance-profile/ecsInstanceRole"),
			InstanceTypes: []string{"optimal"},
			Subnets:       []string{"subnet-1"},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeComputeEnvironments(ctx, &batchsdk.DescribeComputeEnvironmentsInput{
		ComputeEnvironments: []string{ceName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ComputeEnvironments, 1)
	cr := descOut.ComputeEnvironments[0].ComputeResources
	require.NotNil(t, cr)
	require.NotNil(
		t,
		cr.MaxvCpus,
		"ComputeResource.MaxvCpus is required and must decode non-nil even when explicitly 0",
	)
	require.Equal(t, int32(0), *cr.MaxvCpus)
}

// Test_SDKRoundTrip_JobQueue_ComputeEnvironmentOrder proves
// JobQueueDetail.ComputeEnvironmentOrder -- required unconditionally
// (types/types.go) -- is emitted as an empty array, not omitted, for a job
// queue built purely from ServiceEnvironmentOrder. CreateJobQueueInput
// itself declares the two mutually exclusive (api_op_CreateJobQueue.go:
// "A job queue can't have both a serviceEnvironmentOrder and a
// computeEnvironmentOrder field"), so a service-environment-only queue is a
// state a real client reaches routinely, not an edge case.
func Test_SDKRoundTrip_JobQueue_ComputeEnvironmentOrder(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	seName := "ceo-se-" + uuid.NewString()[:8]
	_, err := client.CreateServiceEnvironment(ctx, &batchsdk.CreateServiceEnvironmentInput{
		ServiceEnvironmentName: aws.String(seName),
		ServiceEnvironmentType: types.ServiceEnvironmentTypeSagemakerTraining,
		CapacityLimits: []types.CapacityLimit{
			{MaxCapacity: aws.Int32(1), CapacityUnit: aws.String("NUM_INSTANCES")},
		},
	})
	require.NoError(t, err)

	qName := "ceo-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ServiceEnvironmentOrder: []types.ServiceEnvironmentOrder{
			{Order: aws.Int32(1), ServiceEnvironment: aws.String(seName)},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeJobQueues(ctx, &batchsdk.DescribeJobQueuesInput{JobQueues: []string{qName}})
	require.NoError(t, err)
	require.Len(t, descOut.JobQueues, 1)
	require.NotNil(
		t,
		descOut.JobQueues[0].ComputeEnvironmentOrder,
		"JobQueueDetail.ComputeEnvironmentOrder is required and must decode non-nil (empty) for a "+
			"serviceEnvironmentOrder-only queue",
	)
	require.Empty(t, descOut.JobQueues[0].ComputeEnvironmentOrder)
}

// Test_SDKRoundTrip_QuotaShare_MaxCapacity proves
// QuotaShareCapacityLimit.MaxCapacity -- required (types/types.go) -- is
// never dropped when explicitly 0. The real SDK's own client-side
// validateQuotaShareCapacityLimit (validators.go) only rejects a nil
// MaxCapacity pointer, not zero.
func Test_SDKRoundTrip_QuotaShare_MaxCapacity(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "qs-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "qs-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	createOut, err := client.CreateQuotaShare(ctx, &batchsdk.CreateQuotaShareInput{
		QuotaShareName: aws.String("qs-" + uuid.NewString()[:8]),
		JobQueue:       aws.String(qName),
		CapacityLimits: []types.QuotaShareCapacityLimit{
			{CapacityUnit: aws.String("ml.m5.large"), MaxCapacity: aws.Int32(0)},
		},
		PreemptionConfiguration: &types.QuotaSharePreemptionConfiguration{
			InSharePreemption: types.QuotaShareInSharePreemptionStateEnabled,
		},
		ResourceSharingConfiguration: &types.QuotaShareResourceSharingConfiguration{
			Strategy: types.QuotaShareResourceSharingStrategyReserve,
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeQuotaShare(
		ctx,
		&batchsdk.DescribeQuotaShareInput{QuotaShareArn: createOut.QuotaShareArn},
	)
	require.NoError(t, err)
	require.Len(t, descOut.CapacityLimits, 1)
	require.NotNil(
		t,
		descOut.CapacityLimits[0].MaxCapacity,
		"QuotaShareCapacityLimit.MaxCapacity is required and must decode non-nil even when explicitly 0",
	)
	require.Equal(t, int32(0), *descOut.CapacityLimits[0].MaxCapacity)
}

// Test_SDKRoundTrip_ServiceJobRetryStrategy_Attempts proves
// ServiceJobRetryStrategy.Attempts -- required (types/types.go) -- is never
// dropped when explicitly 0. The real SDK's own client-side
// validateServiceJobRetryStrategy (validators.go) only rejects a nil
// Attempts pointer, not zero (the documented 1-10 range is not enforced
// client-side).
func Test_SDKRoundTrip_ServiceJobRetryStrategy_Attempts(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "srs-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "srs-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	submitOut, err := client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("srs-job-" + uuid.NewString()[:8]),
		JobQueue:              aws.String(qName),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
		RetryStrategy: &types.ServiceJobRetryStrategy{
			Attempts: aws.Int32(0),
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeServiceJob(ctx, &batchsdk.DescribeServiceJobInput{JobId: submitOut.JobId})
	require.NoError(t, err)
	require.NotNil(t, descOut.RetryStrategy)
	require.NotNil(
		t,
		descOut.RetryStrategy.Attempts,
		"ServiceJobRetryStrategy.Attempts is required and must decode non-nil even when explicitly 0",
	)
	require.Equal(t, int32(0), *descOut.RetryStrategy.Attempts)
}
