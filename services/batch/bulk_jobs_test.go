package batch_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const bulkMaxJobs = 50

func newBulkJobsFixture(t *testing.T) (*batchsdk.Client, string, string) {
	t.Helper()

	ctx := t.Context()
	client := newRealClient(t)

	ceName := "bulk-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "bulk-queue-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	jdName := "bulk-jd-" + uuid.NewString()[:8]
	_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName:   aws.String(jdName),
		Type:                types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{Image: aws.String("busybox")},
	})
	require.NoError(t, err)

	return client, qName, jdName
}

func submitBulkJob(t *testing.T, client *batchsdk.Client, queue, jd string) string {
	t.Helper()

	out, err := client.SubmitJob(t.Context(), &batchsdk.SubmitJobInput{
		JobName:       aws.String("bulk-" + uuid.NewString()[:8]),
		JobQueue:      aws.String(queue),
		JobDefinition: aws.String(jd),
	})
	require.NoError(t, err)

	return aws.ToString(out.JobId)
}

func TestSDKRoundTrip_CancelJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prepare    func(t *testing.T, c *batchsdk.Client, q, jd string) []string
		name       string
		wantErrors int
		wantOK     int
		wantFailed int
	}{
		{
			name: "submitted_jobs_cancelled",
			prepare: func(t *testing.T, c *batchsdk.Client, q, jd string) []string {
				t.Helper()

				return []string{submitBulkJob(t, c, q, jd), submitBulkJob(t, c, q, jd)}
			},
			wantOK:     2,
			wantFailed: 2,
		},
		{
			name: "unknown_job_counts_as_successful",
			prepare: func(_ *testing.T, _ *batchsdk.Client, _, _ string) []string {
				return []string{"00000000-0000-0000-0000-000000000000"}
			},
			wantOK: 1,
		},
		{
			name: "already_terminal_job_reports_error",
			prepare: func(t *testing.T, c *batchsdk.Client, q, jd string) []string {
				t.Helper()

				id := submitBulkJob(t, c, q, jd)
				_, err := c.CancelJob(t.Context(), &batchsdk.CancelJobInput{
					JobId: aws.String(id), Reason: aws.String("first"),
				})
				require.NoError(t, err)

				return []string{id}
			},
			wantErrors: 1,
			wantFailed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, q, jd := newBulkJobsFixture(t)
			ids := tt.prepare(t, client, q, jd)

			out, err := client.CancelJobs(t.Context(), &batchsdk.CancelJobsInput{
				Jobs: ids, Reason: aws.String("bulk-cancel"),
			})
			require.NoError(t, err)
			assert.Len(t, out.Successful, tt.wantOK)
			require.Len(t, out.Errors, tt.wantErrors)

			for _, e := range out.Errors {
				assert.Equal(t, "ClientException", aws.ToString(e.Code))
				assert.Equal(t, ids[0], aws.ToString(e.Job))
				assert.NotEmpty(t, aws.ToString(e.Message))
			}

			desc, err := client.DescribeJobs(t.Context(), &batchsdk.DescribeJobsInput{Jobs: ids})
			require.NoError(t, err)

			failed := 0

			for _, j := range desc.Jobs {
				if j.Status == types.JobStatusFailed {
					failed++
				}
			}

			assert.Equal(t, tt.wantFailed, failed)
		})
	}
}

func TestSDKRoundTrip_CancelJobs_Reason(t *testing.T) {
	t.Parallel()

	client, q, jd := newBulkJobsFixture(t)
	id := submitBulkJob(t, client, q, jd)

	_, err := client.CancelJobs(t.Context(), &batchsdk.CancelJobsInput{
		Jobs: []string{id}, Reason: aws.String("because"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeJobs(t.Context(), &batchsdk.DescribeJobsInput{Jobs: []string{id}})
	require.NoError(t, err)
	require.Len(t, desc.Jobs, 1)
	assert.Equal(t, "because", aws.ToString(desc.Jobs[0].StatusReason))
	assert.True(t, aws.ToBool(desc.Jobs[0].IsCancelled))
}

func TestSDKRoundTrip_TerminateJobs(t *testing.T) {
	t.Parallel()

	client, q, jd := newBulkJobsFixture(t)
	live := submitBulkJob(t, client, q, jd)
	done := submitBulkJob(t, client, q, jd)

	_, err := client.TerminateJob(t.Context(), &batchsdk.TerminateJobInput{
		JobId: aws.String(done), Reason: aws.String("earlier"),
	})
	require.NoError(t, err)

	missing := "00000000-0000-0000-0000-000000000000"
	out, err := client.TerminateJobs(t.Context(), &batchsdk.TerminateJobsInput{
		Jobs: []string{live, done, missing}, Reason: aws.String("bulk-term"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{live, missing}, out.Successful)
	require.Len(t, out.Errors, 1)
	assert.Equal(t, done, aws.ToString(out.Errors[0].Job))
	assert.Equal(t, "ClientException", aws.ToString(out.Errors[0].Code))

	desc, err := client.DescribeJobs(t.Context(), &batchsdk.DescribeJobsInput{Jobs: []string{live}})
	require.NoError(t, err)
	require.Len(t, desc.Jobs, 1)
	assert.Equal(t, types.JobStatusFailed, desc.Jobs[0].Status)
	assert.Equal(t, "bulk-term", aws.ToString(desc.Jobs[0].StatusReason))
	assert.True(t, aws.ToBool(desc.Jobs[0].IsTerminated))
}

func TestSDKRoundTrip_TerminateServiceJobs(t *testing.T) {
	t.Parallel()

	client, q, _ := newBulkJobsFixture(t)

	out, err := client.SubmitServiceJob(t.Context(), &batchsdk.SubmitServiceJobInput{
		JobName:               aws.String("bulk-sj"),
		JobQueue:              aws.String(q),
		ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
		ServiceRequestPayload: aws.String(`{}`),
	})
	require.NoError(t, err)

	id := aws.ToString(out.JobId)
	missing := "00000000-0000-0000-0000-000000000000"

	res, err := client.TerminateServiceJobs(t.Context(), &batchsdk.TerminateServiceJobsInput{
		Jobs: []string{id, missing}, Reason: aws.String("bulk-sj-term"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{id, missing}, res.Successful)
	assert.Empty(t, res.Errors)

	desc, err := client.DescribeServiceJob(t.Context(), &batchsdk.DescribeServiceJobInput{JobId: aws.String(id)})
	require.NoError(t, err)
	assert.Equal(t, types.ServiceJobStatusFailed, desc.Status)
	assert.Equal(t, "bulk-sj-term", aws.ToString(desc.StatusReason))
}

func TestSDKRoundTrip_BulkJobs_Validation(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, bulkMaxJobs+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("job-%d", i)
	}

	ops := map[string]func(c *batchsdk.Client, jobs []string) error{
		"cancel": func(c *batchsdk.Client, jobs []string) error {
			_, err := c.CancelJobs(t.Context(), &batchsdk.CancelJobsInput{Jobs: jobs, Reason: aws.String("r")})

			return err
		},
		"terminate": func(c *batchsdk.Client, jobs []string) error {
			_, err := c.TerminateJobs(t.Context(), &batchsdk.TerminateJobsInput{Jobs: jobs, Reason: aws.String("r")})

			return err
		},
		"terminate_service": func(c *batchsdk.Client, jobs []string) error {
			_, err := c.TerminateServiceJobs(t.Context(), &batchsdk.TerminateServiceJobsInput{
				Jobs: jobs, Reason: aws.String("r"),
			})

			return err
		},
	}

	for opName, op := range ops {
		for name, jobs := range map[string][]string{"empty": {}, "too_many": tooMany} {
			t.Run(opName+"_"+name, func(t *testing.T) {
				t.Parallel()

				client := newRealClient(t)
				err := op(client, jobs)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ClientException")
			})
		}
	}
}
