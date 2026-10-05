package mediaconvert_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateJob_InheritsTemplateQueue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		explicit bool
	}{
		{name: "explicit queue wins", explicit: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaConvertClient(t, newTestHandler(t))
			ctx := t.Context()

			tplQueue, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{Name: aws.String("tpl-queue")})
			require.NoError(t, err)
			other, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{Name: aws.String("other-queue")})
			require.NoError(t, err)

			_, err = client.CreateJobTemplate(ctx, &mediaconvertsdk.CreateJobTemplateInput{
				Name: aws.String("tpl"), Queue: tplQueue.Queue.Arn, Settings: &types.JobTemplateSettings{},
			})
			require.NoError(t, err)

			in := &mediaconvertsdk.CreateJobInput{
				Role: aws.String("arn:aws:iam::000000000000:role/mc"), JobTemplate: aws.String("tpl"),
				Settings: &types.JobSettings{},
			}
			want := tplQueue.Queue.Arn
			if tc.explicit {
				in.Queue, want = other.Queue.Arn, other.Queue.Arn
			}

			created, err := client.CreateJob(ctx, in)
			require.NoError(t, err)

			got, err := client.GetJob(ctx, &mediaconvertsdk.GetJobInput{Id: created.Job.Id})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(want), aws.ToString(got.Job.Queue))
		})
	}
}
