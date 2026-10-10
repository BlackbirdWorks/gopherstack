package mediaconvert_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	mediaconverttypes "github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const realismRole = "arn:aws:iam::123456789012:role/r"

func TestRequestRealism_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *mediaconvertsdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "job not found",
			call: func(c *mediaconvertsdk.Client) error {
				_, err := c.GetJob(t.Context(), &mediaconvertsdk.GetJobInput{Id: aws.String("nope")})

				return err
			},
			wantCode: "NotFoundException",
		},
		{
			name: "unknown job template",
			call: func(c *mediaconvertsdk.Client) error {
				_, err := c.CreateJob(t.Context(), &mediaconvertsdk.CreateJobInput{
					Role: aws.String(
						realismRole,
					),
					Settings:    &mediaconverttypes.JobSettings{},
					JobTemplate: aws.String("missing"),
				})

				return err
			},
			wantCode: "NotFoundException",
		},
		{
			name: "priority out of range",
			call: func(c *mediaconvertsdk.Client) error {
				_, err := c.CreateJob(t.Context(), &mediaconvertsdk.CreateJobInput{
					Role: aws.String(realismRole), Settings: &mediaconverttypes.JobSettings{}, Priority: aws.Int32(99),
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "bad next token",
			call: func(c *mediaconvertsdk.Client) error {
				_, err := c.ListJobs(t.Context(), &mediaconvertsdk.ListJobsInput{NextToken: aws.String("zzz")})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "max results too large",
			call: func(c *mediaconvertsdk.Client) error {
				_, err := c.ListQueues(t.Context(), &mediaconvertsdk.ListQueuesInput{MaxResults: aws.Int32(21)})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "delete default queue",
			call: func(c *mediaconvertsdk.Client) error {
				_, err := c.DeleteQueue(t.Context(), &mediaconvertsdk.DeleteQueueInput{Name: aws.String("Default")})

				return err
			},
			wantCode: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newSDKTestClient(t, newTestHandler(t)))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode, apiErr.ErrorMessage())
		})
	}
}

func TestDefaultQueue(t *testing.T) {
	t.Parallel()

	client := newSDKTestClient(t, newTestHandler(t))

	q, err := client.GetQueue(t.Context(), &mediaconvertsdk.GetQueueInput{Name: aws.String("Default")})
	require.NoError(t, err)
	assert.Equal(t, "SYSTEM", string(q.Queue.Type))
	assert.Equal(t, "ACTIVE", string(q.Queue.Status))

	job, err := client.CreateJob(t.Context(), &mediaconvertsdk.CreateJobInput{
		Role: aws.String(realismRole), Settings: &mediaconverttypes.JobSettings{},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(q.Queue.Arn), aws.ToString(job.Job.Queue))
}
