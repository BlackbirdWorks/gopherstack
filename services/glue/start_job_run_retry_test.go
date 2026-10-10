package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartJobRun_PreviousRunID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		retry bool
	}{
		{name: "fresh_run"},
		{name: "retry_of_previous", retry: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
				Name: aws.String("job"), Role: aws.String("r"),
				Command: &types.JobCommand{Name: aws.String("glueetl")},
			})
			require.NoError(t, err)

			first, err := client.StartJobRun(ctx, &gluesdk.StartJobRunInput{JobName: aws.String("job")})
			require.NoError(t, err)

			in := &gluesdk.StartJobRunInput{JobName: aws.String("job")}
			if tt.retry {
				in.JobRunId = first.JobRunId
			}

			second, err := client.StartJobRun(ctx, in)
			require.NoError(t, err)

			got, err := client.GetJobRun(ctx, &gluesdk.GetJobRunInput{
				JobName: aws.String("job"), RunId: second.JobRunId,
			})
			require.NoError(t, err)

			if tt.retry {
				assert.Equal(t, aws.ToString(first.JobRunId), aws.ToString(got.JobRun.PreviousRunId))

				return
			}

			assert.Nil(t, got.JobRun.PreviousRunId)
		})
	}
}
