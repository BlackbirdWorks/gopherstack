package mediatailor_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestTagResource_VisibleOnDescribe reproduces gopherstack-vdrs item 3:
// PrefetchSchedule/Program/LiveSource store tags on their own struct at
// creation time, and their Describe/Get response reads that struct field
// directly rather than the ARN-keyed tag map that TagResource/UntagResource
// mutate. A TagResource call against one of these ARNs is invisible to a
// subsequent Describe/Get of the resource, even though ListTagsForResource
// (which reads the ARN-keyed map) sees it immediately.
func TestTagResource_VisibleOnDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *mediatailorsdk.Client) string
		describe func(t *testing.T, client *mediatailorsdk.Client, arn string) map[string]string
		name     string
	}{
		{
			name: "prefetch_schedule",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()

				_, err := client.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
					Name:                  aws.String("pc-divergence"),
					AdDecisionServerUrl:   aws.String("https://ads.example.com"),
					VideoContentSourceUrl: aws.String("https://video.example.com"),
				})
				require.NoError(t, err)

				out, err := client.CreatePrefetchSchedule(t.Context(), &mediatailorsdk.CreatePrefetchScheduleInput{
					PlaybackConfigurationName: aws.String("pc-divergence"),
					Name:                      aws.String("ps-divergence"),
					Retrieval: &types.PrefetchRetrieval{
						EndTime: aws.Time(time.Now().Add(time.Hour)),
					},
					Consumption: &types.PrefetchConsumption{
						EndTime: aws.Time(time.Now().Add(time.Hour)),
					},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
			describe: func(t *testing.T, client *mediatailorsdk.Client, resourceARN string) map[string]string {
				t.Helper()

				out, err := client.GetPrefetchSchedule(t.Context(), &mediatailorsdk.GetPrefetchScheduleInput{
					PlaybackConfigurationName: aws.String("pc-divergence"),
					Name:                      aws.String("ps-divergence"),
				})
				require.NoError(t, err)
				require.Equal(t, resourceARN, aws.ToString(out.Arn))

				return out.Tags
			},
		},
		{
			name: "program",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()

				_, err := client.CreateChannel(t.Context(), &mediatailorsdk.CreateChannelInput{
					ChannelName:  aws.String("channel-divergence"),
					PlaybackMode: types.PlaybackModeLoop,
					Outputs: []types.RequestOutputItem{
						{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
					},
				})
				require.NoError(t, err)

				_, err = client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String("sl-divergence"),
					HttpConfiguration: &types.HttpConfiguration{
						BaseUrl: aws.String("https://example.com"),
					},
				})
				require.NoError(t, err)

				_, err = client.CreateVodSource(t.Context(), &mediatailorsdk.CreateVodSourceInput{
					SourceLocationName: aws.String("sl-divergence"),
					VodSourceName:      aws.String("vs-divergence"),
					HttpPackageConfigurations: []types.HttpPackageConfiguration{
						{Path: aws.String("/vod"), SourceGroup: aws.String("default"), Type: types.TypeHls},
					},
				})
				require.NoError(t, err)

				out, err := client.CreateProgram(t.Context(), &mediatailorsdk.CreateProgramInput{
					ChannelName:        aws.String("channel-divergence"),
					ProgramName:        aws.String("program-divergence"),
					SourceLocationName: aws.String("sl-divergence"),
					VodSourceName:      aws.String("vs-divergence"),
					ScheduleConfiguration: &types.ScheduleConfiguration{
						Transition: &types.Transition{
							Type:                     aws.String("ABSOLUTE"),
							RelativePosition:         types.RelativePositionAfterProgram,
							ScheduledStartTimeMillis: aws.Int64(time.Now().UnixMilli()),
							DurationMillis:           aws.Int64(30000),
						},
					},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
			describe: func(t *testing.T, client *mediatailorsdk.Client, resourceARN string) map[string]string {
				t.Helper()

				out, err := client.DescribeProgram(t.Context(), &mediatailorsdk.DescribeProgramInput{
					ChannelName: aws.String("channel-divergence"),
					ProgramName: aws.String("program-divergence"),
				})
				require.NoError(t, err)
				require.Equal(t, resourceARN, aws.ToString(out.Arn))

				return out.Tags
			},
		},
		{
			name: "live_source",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()

				_, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String("sl-live-divergence"),
					HttpConfiguration: &types.HttpConfiguration{
						BaseUrl: aws.String("https://example.com"),
					},
				})
				require.NoError(t, err)

				out, err := client.CreateLiveSource(t.Context(), &mediatailorsdk.CreateLiveSourceInput{
					SourceLocationName: aws.String("sl-live-divergence"),
					LiveSourceName:     aws.String("live-divergence"),
					HttpPackageConfigurations: []types.HttpPackageConfiguration{
						{Path: aws.String("/live"), SourceGroup: aws.String("default"), Type: types.TypeHls},
					},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
			describe: func(t *testing.T, client *mediatailorsdk.Client, resourceARN string) map[string]string {
				t.Helper()

				out, err := client.DescribeLiveSource(t.Context(), &mediatailorsdk.DescribeLiveSourceInput{
					SourceLocationName: aws.String("sl-live-divergence"),
					LiveSourceName:     aws.String("live-divergence"),
				})
				require.NoError(t, err)
				require.Equal(t, resourceARN, aws.ToString(out.Arn))

				return out.Tags
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
			client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

			resourceARN := tc.setup(t, client)
			require.NotEmpty(t, resourceARN)

			_, err := client.TagResource(t.Context(), &mediatailorsdk.TagResourceInput{
				ResourceArn: aws.String(resourceARN),
				Tags:        map[string]string{"team": "video"},
			})
			require.NoError(t, err)

			listed, err := client.ListTagsForResource(t.Context(), &mediatailorsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceARN),
			})
			require.NoError(t, err)
			assert.Equal(t, "video", listed.Tags["team"], "ListTagsForResource must see the TagResource call")

			described := tc.describe(t, client, resourceARN)
			assert.Equal(
				t, "video", described["team"],
				"Describe/Get must reflect a TagResource call against the same ARN, matching ListTagsForResource",
			)
		})
	}
}

// TestPutFunction_TagsVisibleOnListTagsForResource reproduces the same
// architectural split from the opposite direction: PutFunction never wrote
// the caller's Tags into the ARN-keyed tag map at all (unlike
// CreatePrefetchSchedule/CreateProgram/CreateLiveSource, which write both),
// so ListTagsForResource against a freshly tagged function's ARN returned
// empty even though GetFunction showed the tags.
func TestPutFunction_TagsVisibleOnListTagsForResource(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	out, err := client.PutFunction(t.Context(), &mediatailorsdk.PutFunctionInput{
		FunctionId:   aws.String("fn-divergence"),
		FunctionType: types.FunctionTypeHttpRequest,
		Tags:         map[string]string{"team": "video"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.Arn))

	listed, err := client.ListTagsForResource(t.Context(), &mediatailorsdk.ListTagsForResourceInput{
		ResourceArn: out.Arn,
	})
	require.NoError(t, err)
	assert.Equal(
		t, "video", listed.Tags["team"],
		"ListTagsForResource must see tags supplied to PutFunction, matching GetFunction",
	)
}
