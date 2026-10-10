package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestRequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(t *testing.T, c *batchsdk.Client) error
		name    string
		wantMsg string
	}{
		{
			name: "compute environment name",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.CreateComputeEnvironment(t.Context(), &batchsdk.CreateComputeEnvironmentInput{
					ComputeEnvironmentName: aws.String("bad name!"), Type: types.CETypeUnmanaged,
				})

				return err
			},
			wantMsg: "computeEnvironmentName must match",
		},
		{
			name: "min above max vcpus",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.CreateComputeEnvironment(t.Context(), &batchsdk.CreateComputeEnvironmentInput{
					ComputeEnvironmentName: aws.String("ce"), Type: types.CETypeManaged,
					ComputeResources: &types.ComputeResource{
						Type: types.CRTypeEc2, MinvCpus: aws.Int32(8), MaxvCpus: aws.Int32(4),
					},
				})

				return err
			},
			wantMsg: "minvCpus must not exceed maxvCpus",
		},
		{
			name: "job queue name",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.CreateJobQueue(t.Context(), &batchsdk.CreateJobQueueInput{
					JobQueueName: aws.String("bad name"), Priority: aws.Int32(1),
				})

				return err
			},
			wantMsg: "jobQueueName must match",
		},
		{
			name: "retry attempts above ten",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.RegisterJobDefinition(t.Context(), &batchsdk.RegisterJobDefinitionInput{
					JobDefinitionName: aws.String("jd"), Type: types.JobDefinitionTypeContainer,
					RetryStrategy: &types.RetryStrategy{Attempts: aws.Int32(11)},
				})

				return err
			},
			wantMsg: "retryStrategy.attempts must be between 1 and 10",
		},
		{
			name: "timeout below sixty",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.RegisterJobDefinition(t.Context(), &batchsdk.RegisterJobDefinitionInput{
					JobDefinitionName: aws.String("jd"), Type: types.JobDefinitionTypeContainer,
					Timeout: &types.JobTimeout{AttemptDurationSeconds: aws.Int32(5)},
				})

				return err
			},
			wantMsg: "attemptDurationSeconds must be at least 60",
		},
		{
			name: "job name",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.SubmitJob(t.Context(), &batchsdk.SubmitJobInput{
					JobName: aws.String("bad name"), JobQueue: aws.String("q"), JobDefinition: aws.String("jd"),
				})

				return err
			},
			wantMsg: "jobName must match",
		},
		{
			name: "garbage next token",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.DescribeJobQueues(t.Context(), &batchsdk.DescribeJobQueuesInput{
					NextToken: aws.String("garbage"),
				})

				return err
			},
			wantMsg: "invalid nextToken",
		},
		{
			name: "terminate unknown job message",
			call: func(t *testing.T, c *batchsdk.Client) error {
				t.Helper()
				_, err := c.TerminateJob(t.Context(), &batchsdk.TerminateJobInput{
					JobId: aws.String("nope"), Reason: aws.String("x"),
				})

				return err
			},
			wantMsg: "job nope not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestBatchClient(t, batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1")))
			err := tt.call(t, c)
			require.Error(t, err)

			var ce *types.ClientException
			require.ErrorAs(t, err, &ce)
			assert.Contains(t, ce.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, ce.ErrorMessage(), "ClientException:")
		})
	}
}
