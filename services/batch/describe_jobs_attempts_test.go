package batch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestDescribeJobs_Attempts_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantStatus   types.JobStatus
		name         string
		wantAttempts int
		retryAttempt int32
	}{
		{name: "no_retry_one_attempt_recorded", retryAttempt: 1, wantAttempts: 1, wantStatus: types.JobStatusFailed},
		{name: "retry_records_each_attempt", retryAttempt: 2, wantAttempts: 2, wantStatus: types.JobStatusFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bk := batch.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestBatchClient(t, batch.NewHandler(bk))

			_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
				ComputeEnvironmentName: aws.String("ce"),
				Type:                   types.CETypeManaged,
			})
			require.NoError(t, err)

			_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
				JobQueueName: aws.String("q"),
				Priority:     aws.Int32(1),
				ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
					{Order: aws.Int32(1), ComputeEnvironment: aws.String("ce")},
				},
			})
			require.NoError(t, err)

			_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
				JobDefinitionName: aws.String("jd"),
				Type:              types.JobDefinitionTypeContainer,
				ContainerProperties: &types.ContainerProperties{
					Image: aws.String("busybox"),
					ResourceRequirements: []types.ResourceRequirement{
						{Type: types.ResourceTypeVcpu, Value: aws.String("1")},
						{Type: types.ResourceTypeMemory, Value: aws.String("128")},
					},
				},
			})
			require.NoError(t, err)

			sub, err := client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
				JobName:       aws.String("job"),
				JobQueue:      aws.String("q"),
				JobDefinition: aws.String("jd"),
				RetryStrategy: &types.RetryStrategy{Attempts: aws.Int32(tt.retryAttempt)},
				Timeout:       &types.JobTimeout{AttemptDurationSeconds: aws.Int32(1)},
			})
			require.NoError(t, err)

			jan := batch.NewJanitor(bk, time.Minute, 24*time.Hour, 24*time.Hour)

			for range tt.wantAttempts {
				jan.SweepOnce(ctx)
				bk.SetJobStartedAtForTest(aws.ToString(sub.JobId), time.Now().Add(-2*time.Second))
				jan.SweepOnce(ctx)
			}

			out, err := client.DescribeJobs(ctx, &batchsdk.DescribeJobsInput{Jobs: []string{aws.ToString(sub.JobId)}})
			require.NoError(t, err)
			require.Len(t, out.Jobs, 1)

			job := out.Jobs[0]
			assert.Equal(t, tt.wantStatus, job.Status)
			require.Len(t, job.Attempts, tt.wantAttempts)

			for _, a := range job.Attempts {
				assert.Equal(t, "job attempt duration exceeded timeout", aws.ToString(a.StatusReason))
				require.NotNil(t, a.StartedAt)
				require.NotNil(t, a.StoppedAt)
				assert.GreaterOrEqual(t, *a.StoppedAt, *a.StartedAt)
			}
		})
	}
}
