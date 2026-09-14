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

// TestRealClient_JobAndResourceLifecycle drives the 10 ops that a real aws-sdk-go-v2 batch
// client had never exercised before (gopherstack-n3zi): CancelJob,
// DeleteQuotaShare, GetJobQueueSnapshot, ListJobsByConsumableResource,
// ListQuotaShares, TerminateJob, TerminateServiceJob, UpdateConsumableResource,
// UpdateQuotaShare, UpdateServiceJob.
func TestRealClient_JobAndResourceLifecycle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "job_lifecycle_cancel_terminate_snapshot", run: func(t *testing.T) {
			t.Helper()

			testJobLifecycleRealClient(t)
		}},
		{name: "consumable_resource", run: func(t *testing.T) {
			t.Helper()

			testConsumableResourceRealClient(t)
		}},
		{name: "quota_share", run: func(t *testing.T) {
			t.Helper()

			testQuotaShareRealClient(t)
		}},
		{name: "service_job", run: func(t *testing.T) {
			t.Helper()

			testServiceJobRealClient(t)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func newRealClient(t *testing.T) *batchsdk.Client {
	t.Helper()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1"))

	return newTestBatchClient(t, h)
}

func testJobLifecycleRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	ceName := "s19-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "s19-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	jdName := "s19-jd-" + uuid.NewString()[:8]
	_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String(jdName),
		Type:              types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{
			Image: aws.String("busybox"),
		},
	})
	require.NoError(t, err)

	// GetJobQueueSnapshot.
	snap, err := client.GetJobQueueSnapshot(ctx, &batchsdk.GetJobQueueSnapshotInput{JobQueue: aws.String(qName)})
	require.NoError(t, err)
	require.NotNil(t, snap.FrontOfQueue, "GetJobQueueSnapshotOutput.FrontOfQueue must decode")

	// CancelJob.
	cancelSubmit, err := client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
		JobName:       aws.String("s19-cancel-" + uuid.NewString()[:8]),
		JobQueue:      aws.String(qName),
		JobDefinition: aws.String(jdName),
	})
	require.NoError(t, err)
	_, err = client.CancelJob(ctx, &batchsdk.CancelJobInput{
		JobId:  cancelSubmit.JobId,
		Reason: aws.String("slice19 test"),
	})
	require.NoError(t, err)
	descCancel, err := client.DescribeJobs(
		ctx,
		&batchsdk.DescribeJobsInput{Jobs: []string{aws.ToString(cancelSubmit.JobId)}},
	)
	require.NoError(t, err)
	require.Len(t, descCancel.Jobs, 1)
	require.True(t, aws.ToBool(descCancel.Jobs[0].IsCancelled))

	// TerminateJob.
	termSubmit, err := client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
		JobName:       aws.String("s19-term-" + uuid.NewString()[:8]),
		JobQueue:      aws.String(qName),
		JobDefinition: aws.String(jdName),
	})
	require.NoError(t, err)
	_, err = client.TerminateJob(ctx, &batchsdk.TerminateJobInput{
		JobId:  termSubmit.JobId,
		Reason: aws.String("slice19 test"),
	})
	require.NoError(t, err)
	descTerm, err := client.DescribeJobs(
		ctx,
		&batchsdk.DescribeJobsInput{Jobs: []string{aws.ToString(termSubmit.JobId)}},
	)
	require.NoError(t, err)
	require.Len(t, descTerm.Jobs, 1)
	require.True(t, aws.ToBool(descTerm.Jobs[0].IsTerminated))
}

func testConsumableResourceRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	crName := "s19-cr-" + uuid.NewString()[:8]
	created, err := client.CreateConsumableResource(ctx, &batchsdk.CreateConsumableResourceInput{
		ConsumableResourceName: aws.String(crName),
		ResourceType:           aws.String("REPLENISHABLE"),
		TotalQuantity:          aws.Int64(10),
	})
	require.NoError(t, err)
	crID := aws.ToString(created.ConsumableResourceArn)
	require.NotEmpty(t, crID)

	// UpdateConsumableResource.
	updated, err := client.UpdateConsumableResource(ctx, &batchsdk.UpdateConsumableResourceInput{
		ConsumableResource: aws.String(crName),
		Operation:          aws.String("SET"),
		Quantity:           aws.Int64(20),
	})
	require.NoError(t, err)
	require.Equal(t, int64(20), aws.ToInt64(updated.TotalQuantity))

	// ListJobsByConsumableResource.
	ceName := "s19-cr-ce-" + uuid.NewString()[:8]
	_, err = client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)
	qName := "s19-cr-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)
	jdName := "s19-cr-jd-" + uuid.NewString()[:8]
	_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String(jdName),
		Type:              types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{
			Image: aws.String("busybox"),
		},
	})
	require.NoError(t, err)
	jobName := "s19-cr-job-" + uuid.NewString()[:8]
	_, err = client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
		JobName:       aws.String(jobName),
		JobQueue:      aws.String(qName),
		JobDefinition: aws.String(jdName),
		ConsumableResourcePropertiesOverride: &types.ConsumableResourceProperties{
			ConsumableResourceList: []types.ConsumableResourceRequirement{
				{ConsumableResource: aws.String(crName), Quantity: aws.Int64(4)},
			},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListJobsByConsumableResource(ctx, &batchsdk.ListJobsByConsumableResourceInput{
		ConsumableResource: aws.String(crName),
	})
	require.NoError(t, err)
	require.Len(t, listed.Jobs, 1)
	require.Equal(t, jobName, aws.ToString(listed.Jobs[0].JobName))
	require.Equal(t, int64(4), aws.ToInt64(listed.Jobs[0].Quantity))
	require.NotEmpty(t, aws.ToString(listed.Jobs[0].JobQueueArn))
}

func testQuotaShareRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	ceName := "s19-qs-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)
	qName := "s19-qs-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	qsName := "s19-qs-" + uuid.NewString()[:8]
	created, err := client.CreateQuotaShare(ctx, &batchsdk.CreateQuotaShareInput{
		QuotaShareName: aws.String(qsName),
		JobQueue:       aws.String(qName),
		CapacityLimits: []types.QuotaShareCapacityLimit{
			{CapacityUnit: aws.String("vCPU"), MaxCapacity: aws.Int32(10)},
		},
		PreemptionConfiguration: &types.QuotaSharePreemptionConfiguration{
			InSharePreemption: types.QuotaShareInSharePreemptionStateEnabled,
		},
		ResourceSharingConfiguration: &types.QuotaShareResourceSharingConfiguration{
			Strategy: types.QuotaShareResourceSharingStrategyReserve,
		},
	})
	require.NoError(t, err)
	qsArn := aws.ToString(created.QuotaShareArn)
	require.NotEmpty(t, qsArn)

	// UpdateQuotaShare.
	updated, err := client.UpdateQuotaShare(ctx, &batchsdk.UpdateQuotaShareInput{
		QuotaShareArn: aws.String(qsArn),
		CapacityLimits: []types.QuotaShareCapacityLimit{
			{CapacityUnit: aws.String("vCPU"), MaxCapacity: aws.Int32(25)},
		},
	})
	require.NoError(t, err)
	require.Equal(t, qsArn, aws.ToString(updated.QuotaShareArn))

	descAfterUpdate, err := client.DescribeQuotaShare(
		ctx,
		&batchsdk.DescribeQuotaShareInput{QuotaShareArn: aws.String(qsArn)},
	)
	require.NoError(t, err)
	require.Len(t, descAfterUpdate.CapacityLimits, 1)
	require.Equal(t, int32(25), aws.ToInt32(descAfterUpdate.CapacityLimits[0].MaxCapacity))

	// ListQuotaShares.
	listed, err := client.ListQuotaShares(ctx, &batchsdk.ListQuotaSharesInput{JobQueue: aws.String(qName)})
	require.NoError(t, err)
	found := false
	for _, qs := range listed.QuotaShares {
		if aws.ToString(qs.QuotaShareArn) == qsArn {
			found = true
		}
	}
	require.True(t, found, "ListQuotaShares must include the created quota share")

	// DeleteQuotaShare requires the quota share be DISABLED first (real AWS
	// behavior, correctly enforced by this backend).
	_, err = client.UpdateQuotaShare(ctx, &batchsdk.UpdateQuotaShareInput{
		QuotaShareArn: aws.String(qsArn),
		State:         types.QuotaShareStateDisabled,
	})
	require.NoError(t, err)
	_, err = client.DeleteQuotaShare(ctx, &batchsdk.DeleteQuotaShareInput{QuotaShareArn: aws.String(qsArn)})
	require.NoError(t, err)
	_, err = client.DescribeQuotaShare(ctx, &batchsdk.DescribeQuotaShareInput{QuotaShareArn: aws.String(qsArn)})
	require.Error(t, err, "quota share must be gone after DeleteQuotaShare")
}

func testServiceJobRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	ceName := "s19-sj-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)
	qName := "s19-sj-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	// UpdateServiceJob.
	submitForUpdate, err := client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("s19-sj-upd-" + uuid.NewString()[:8]),
		JobQueue:              aws.String(qName),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
	})
	require.NoError(t, err)
	updated, err := client.UpdateServiceJob(ctx, &batchsdk.UpdateServiceJobInput{
		JobId:              submitForUpdate.JobId,
		SchedulingPriority: aws.Int32(42),
	})
	require.NoError(t, err)
	require.NotNil(t, updated)
	descUpdated, err := client.DescribeServiceJob(ctx, &batchsdk.DescribeServiceJobInput{JobId: submitForUpdate.JobId})
	require.NoError(t, err)
	require.Equal(t, int32(42), aws.ToInt32(descUpdated.SchedulingPriority))

	// TerminateServiceJob.
	submitForTerm, err := client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("s19-sj-term-" + uuid.NewString()[:8]),
		JobQueue:              aws.String(qName),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{"foo":"bar"}`),
	})
	require.NoError(t, err)
	_, err = client.TerminateServiceJob(ctx, &batchsdk.TerminateServiceJobInput{
		JobId:  submitForTerm.JobId,
		Reason: aws.String("slice19 test"),
	})
	require.NoError(t, err)
	descTerm, err := client.DescribeServiceJob(ctx, &batchsdk.DescribeServiceJobInput{JobId: submitForTerm.JobId})
	require.NoError(t, err)
	require.Equal(t, types.ServiceJobStatusFailed, descTerm.Status)
}
