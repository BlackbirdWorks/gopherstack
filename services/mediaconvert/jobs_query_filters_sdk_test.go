package mediaconvert_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobsQuery_FilterKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		want   []string
		filter types.JobsQueryFilter
	}{
		{
			name:   "queue_by_name",
			filter: types.JobsQueryFilter{Key: "queue", Values: []string{"q1"}},
			want:   []string{"a"},
		},
		{
			name:   "queue_by_arn",
			filter: types.JobsQueryFilter{Key: "queue", Values: []string{"@q1arn"}},
			want:   []string{"a"},
		},
		{
			name:   "queue_or_values",
			filter: types.JobsQueryFilter{Key: "queue", Values: []string{"q1", "q2"}},
			want:   []string{"a", "b"},
		},
		{
			name:   "file_input_partial",
			filter: types.JobsQueryFilter{Key: "fileInput", Values: []string{"movie-b"}},
			want:   []string{"b"},
		},
		{
			name:   "file_input_no_match",
			filter: types.JobsQueryFilter{Key: "fileInput", Values: []string{"nothing"}},
			want:   nil,
		},
		{
			name:   "video_codec",
			filter: types.JobsQueryFilter{Key: "videoCodec", Values: []string{"H_265"}},
			want:   []string{"b"},
		},
		{
			name:   "video_codec_or",
			filter: types.JobsQueryFilter{Key: "videoCodec", Values: []string{"H_264", "H_265"}},
			want:   []string{"a", "b"},
		},
		{
			name:   "audio_codec",
			filter: types.JobsQueryFilter{Key: "audioCodec", Values: []string{"AAC"}},
			want:   []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaConvertClient(t, newTestHandler(t))
			ctx := t.Context()
			role := "arn:aws:iam::123456789012:role/MediaConvert_Default_Role"

			queues := map[string]string{}
			for _, n := range []string{"q1", "q2"} {
				q, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{Name: aws.String(n)})
				require.NoError(t, err)

				queues[n] = aws.ToString(q.Queue.Arn)
			}

			settings := func(file, codec string) *types.JobSettings {
				js := &types.JobSettings{
					Inputs: []types.Input{{FileInput: aws.String(file)}},
					OutputGroups: []types.OutputGroup{{Outputs: []types.Output{{
						VideoDescription: &types.VideoDescription{CodecSettings: &types.VideoCodecSettings{
							Codec: types.VideoCodec(codec),
						}},
					}}}},
				}

				return js
			}

			jobs := map[string]string{}
			specs := []struct{ tag, queue, file, codec string }{
				{"a", "q1", "s3://bkt/movie-a.mp4", "H_264"},
				{"b", "q2", "s3://bkt/movie-b.mp4", "H_265"},
			}

			for _, sp := range specs {
				js := settings(sp.file, sp.codec)
				if sp.tag == "a" {
					js.OutputGroups[0].Outputs[0].AudioDescriptions = []types.AudioDescription{{
						CodecSettings: &types.AudioCodecSettings{Codec: types.AudioCodecAac},
					}}
				}

				out, err := client.CreateJob(ctx, &mediaconvertsdk.CreateJobInput{
					Role: aws.String(role), Queue: aws.String(queues[sp.queue]), Settings: js,
				})
				require.NoError(t, err)

				jobs[aws.ToString(out.Job.Id)] = sp.tag
			}

			f := tt.filter
			for i, v := range f.Values {
				if v == "@q1arn" {
					f.Values[i] = queues["q1"]
				}
			}

			start, err := client.StartJobsQuery(ctx, &mediaconvertsdk.StartJobsQueryInput{
				FilterList: []types.JobsQueryFilter{f},
			})
			require.NoError(t, err)

			res, err := client.GetJobsQueryResults(ctx, &mediaconvertsdk.GetJobsQueryResultsInput{Id: start.Id})
			require.NoError(t, err)

			var got []string
			for _, j := range res.Jobs {
				got = append(got, jobs[aws.ToString(j.Id)])
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
