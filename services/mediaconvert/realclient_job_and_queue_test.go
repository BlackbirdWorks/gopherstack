package mediaconvert_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_JobAndQueue drives every remaining uncovered op through
// a real aws-sdk-go-v2 client: CancelJob, DeleteJobTemplate, DescribeEndpoints,
// GetJobTemplate, ListJobs, ListVersions, Probe, SearchJobs, StartJobsQuery,
// TagResource, UntagResource, UpdateJobTemplate, UpdateQueue.
func TestRealClient_JobAndQueue(t *testing.T) {
	t.Parallel()

	role := "arn:aws:iam::123456789012:role/MediaConvert_Default_Role"

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "job lifecycle: list, cancel, search, jobsquery",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestMediaConvertClient(t, h)
				ctx := t.Context()

				created, err := client.CreateJob(ctx, &mediaconvertsdk.CreateJobInput{
					Role:     aws.String(role),
					Settings: &types.JobSettings{},
				})
				require.NoError(t, err)
				jobID := aws.ToString(created.Job.Id)
				require.NotEmpty(t, jobID)

				listOut, err := client.ListJobs(ctx, &mediaconvertsdk.ListJobsInput{})
				require.NoError(t, err)
				assert.NotEmpty(t, listOut.Jobs)

				var found bool
				for _, j := range listOut.Jobs {
					if aws.ToString(j.Id) == jobID {
						found = true
					}
				}
				assert.True(t, found, "created job must appear in ListJobs")

				searchOut, err := client.SearchJobs(ctx, &mediaconvertsdk.SearchJobsInput{})
				require.NoError(t, err)
				found = false
				for _, j := range searchOut.Jobs {
					if aws.ToString(j.Id) == jobID {
						found = true
					}
				}
				assert.True(t, found, "created job must appear in SearchJobs")

				startOut, err := client.StartJobsQuery(ctx, &mediaconvertsdk.StartJobsQueryInput{})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(startOut.Id))

				queryOut, err := client.GetJobsQueryResults(
					ctx,
					&mediaconvertsdk.GetJobsQueryResultsInput{Id: startOut.Id},
				)
				require.NoError(t, err)
				assert.Equal(t, types.JobsQueryStatusComplete, queryOut.Status)

				_, err = client.CancelJob(ctx, &mediaconvertsdk.CancelJobInput{Id: created.Job.Id})
				require.NoError(t, err)

				got, err := client.GetJob(ctx, &mediaconvertsdk.GetJobInput{Id: created.Job.Id})
				require.NoError(t, err)
				assert.Equal(t, types.JobStatusCanceled, got.Job.Status)
			},
		},
		{
			name: "job template lifecycle: get, update, delete",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestMediaConvertClient(t, h)
				ctx := t.Context()

				name := "tmpl-slice25"
				_, err := client.CreateJobTemplate(ctx, &mediaconvertsdk.CreateJobTemplateInput{
					Name:     aws.String(name),
					Settings: &types.JobTemplateSettings{},
					Priority: aws.Int32(1),
				})
				require.NoError(t, err)

				got, err := client.GetJobTemplate(ctx, &mediaconvertsdk.GetJobTemplateInput{Name: aws.String(name)})
				require.NoError(t, err)
				require.NotNil(t, got.JobTemplate)
				assert.Equal(t, name, aws.ToString(got.JobTemplate.Name))
				assert.EqualValues(t, 1, aws.ToInt32(got.JobTemplate.Priority))

				updated, err := client.UpdateJobTemplate(ctx, &mediaconvertsdk.UpdateJobTemplateInput{
					Name:        aws.String(name),
					Description: aws.String("updated via typed client"),
					Priority:    aws.Int32(5),
				})
				require.NoError(t, err)
				require.NotNil(t, updated.JobTemplate)
				assert.Equal(t, "updated via typed client", aws.ToString(updated.JobTemplate.Description))
				assert.EqualValues(t, 5, aws.ToInt32(updated.JobTemplate.Priority))

				_, err = client.DeleteJobTemplate(ctx, &mediaconvertsdk.DeleteJobTemplateInput{Name: aws.String(name)})
				require.NoError(t, err)

				_, err = client.GetJobTemplate(ctx, &mediaconvertsdk.GetJobTemplateInput{Name: aws.String(name)})
				require.Error(t, err)
			},
		},
		{
			name: "queue update",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestMediaConvertClient(t, h)
				ctx := t.Context()

				name := "queue-slice25"
				_, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{Name: aws.String(name)})
				require.NoError(t, err)

				updated, err := client.UpdateQueue(ctx, &mediaconvertsdk.UpdateQueueInput{
					Name:           aws.String(name),
					ConcurrentJobs: aws.Int32(3),
					Description:    aws.String("updated queue"),
				})
				require.NoError(t, err)
				require.NotNil(t, updated.Queue)
				assert.Equal(t, "updated queue", aws.ToString(updated.Queue.Description))
				require.NotNil(t, updated.Queue.ConcurrentJobs)
				assert.EqualValues(t, 3, aws.ToInt32(updated.Queue.ConcurrentJobs))
			},
		},
		{
			name: "tags: tag and untag a resource",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestMediaConvertClient(t, h)
				ctx := t.Context()

				created, err := client.CreateQueue(
					ctx, &mediaconvertsdk.CreateQueueInput{Name: aws.String("queue-tags-slice25")},
				)
				require.NoError(t, err)
				arn := aws.ToString(created.Queue.Arn)
				require.NotEmpty(t, arn)

				_, err = client.TagResource(ctx, &mediaconvertsdk.TagResourceInput{
					Arn:  aws.String(arn),
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				tagsOut, err := client.ListTagsForResource(
					ctx,
					&mediaconvertsdk.ListTagsForResourceInput{Arn: aws.String(arn)},
				)
				require.NoError(t, err)
				assert.Equal(t, "test", tagsOut.ResourceTags.Tags["env"])

				_, err = client.UntagResource(ctx, &mediaconvertsdk.UntagResourceInput{
					Arn:     aws.String(arn),
					TagKeys: []string{"env"},
				})
				require.NoError(t, err)

				tagsOut, err = client.ListTagsForResource(
					ctx,
					&mediaconvertsdk.ListTagsForResourceInput{Arn: aws.String(arn)},
				)
				require.NoError(t, err)
				assert.NotContains(t, tagsOut.ResourceTags.Tags, "env")
			},
		},
		{
			name: "DescribeEndpoints",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestMediaConvertClient(t, h)
				ctx := t.Context()

				//nolint:staticcheck // deprecated but still a real, routed op
				out, err := client.DescribeEndpoints(ctx, &mediaconvertsdk.DescribeEndpointsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, out.Endpoints)
				assert.NotEmpty(t, aws.ToString(out.Endpoints[0].Url))
			},
		},
		{
			name: "ListVersions",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestMediaConvertClient(t, h)
				ctx := t.Context()

				out, err := client.ListVersions(ctx, &mediaconvertsdk.ListVersionsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, out.Versions)
				assert.NotEmpty(t, aws.ToString(out.Versions[0].Version))
			},
		},
		{
			name: "Probe",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestMediaConvertClient(t, h)
				ctx := t.Context()

				out, err := client.Probe(ctx, &mediaconvertsdk.ProbeInput{
					InputFiles: []types.ProbeInputFile{{FileUrl: aws.String("s3://bucket/input.mp4")}},
				})
				require.NoError(t, err)
				require.Len(t, out.ProbeResults, 1)
				require.NotNil(t, out.ProbeResults[0].Container)
				assert.Equal(t, types.Format("mp4"), out.ProbeResults[0].Container.Format)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
