package mediatailor_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

const mediatailorTagsRTRegion = "us-east-1"

// newTestMediaTailorClient stands up the real aws-sdk-go-v2 MediaTailor
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestMediaTailorClient(t *testing.T, h *mediatailor.Handler) *mediatailorsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(mediatailorTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return mediatailorsdk.NewFromConfig(cfg, func(o *mediatailorsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every mediatailor Create* op whose
// real Input struct accepts Tags (mediatailor@v1.63.4: api_op_CreateChannel.go,
// api_op_CreateLiveSource.go, api_op_CreatePrefetchSchedule.go,
// api_op_CreateProgram.go, api_op_CreateSourceLocation.go,
// api_op_CreateVodSource.go, all `Tags map[string]string`) through the real
// SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *mediatailorsdk.Client) string
		name  string
	}{
		{
			name: "channel",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()
				out, err := client.CreateChannel(t.Context(), &mediatailorsdk.CreateChannelInput{
					ChannelName:  aws.String("tagged-channel"),
					PlaybackMode: types.PlaybackModeLoop,
					Outputs: []types.RequestOutputItem{
						{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "source location",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()
				out, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String("tagged-sl"),
					HttpConfiguration: &types.HttpConfiguration{
						BaseUrl: aws.String("https://example.com"),
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "live source",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()
				_, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String("sl-for-live"),
					HttpConfiguration: &types.HttpConfiguration{
						BaseUrl: aws.String("https://example.com"),
					},
				})
				require.NoError(t, err)

				out, err := client.CreateLiveSource(t.Context(), &mediatailorsdk.CreateLiveSourceInput{
					SourceLocationName: aws.String("sl-for-live"),
					LiveSourceName:     aws.String("tagged-live"),
					HttpPackageConfigurations: []types.HttpPackageConfiguration{
						{Path: aws.String("/live"), SourceGroup: aws.String("default"), Type: types.TypeHls},
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "vod source",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()
				_, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String("sl-for-vod"),
					HttpConfiguration: &types.HttpConfiguration{
						BaseUrl: aws.String("https://example.com"),
					},
				})
				require.NoError(t, err)

				out, err := client.CreateVodSource(t.Context(), &mediatailorsdk.CreateVodSourceInput{
					SourceLocationName: aws.String("sl-for-vod"),
					VodSourceName:      aws.String("tagged-vod"),
					HttpPackageConfigurations: []types.HttpPackageConfiguration{
						{Path: aws.String("/vod"), SourceGroup: aws.String("default"), Type: types.TypeHls},
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "program",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()
				_, err := client.CreateChannel(t.Context(), &mediatailorsdk.CreateChannelInput{
					ChannelName:  aws.String("channel-for-program"),
					PlaybackMode: types.PlaybackModeLoop,
					Outputs: []types.RequestOutputItem{
						{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
					},
				})
				require.NoError(t, err)

				_, err = client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String("sl-for-program"),
					HttpConfiguration: &types.HttpConfiguration{
						BaseUrl: aws.String("https://example.com"),
					},
				})
				require.NoError(t, err)

				_, err = client.CreateVodSource(t.Context(), &mediatailorsdk.CreateVodSourceInput{
					SourceLocationName: aws.String("sl-for-program"),
					VodSourceName:      aws.String("vs-for-program"),
					HttpPackageConfigurations: []types.HttpPackageConfiguration{
						{Path: aws.String("/vod"), SourceGroup: aws.String("default"), Type: types.TypeHls},
					},
				})
				require.NoError(t, err)

				out, err := client.CreateProgram(t.Context(), &mediatailorsdk.CreateProgramInput{
					ChannelName:        aws.String("channel-for-program"),
					ProgramName:        aws.String("tagged-program"),
					SourceLocationName: aws.String("sl-for-program"),
					VodSourceName:      aws.String("vs-for-program"),
					ScheduleConfiguration: &types.ScheduleConfiguration{
						Transition: &types.Transition{
							Type:                     aws.String("ABSOLUTE"),
							RelativePosition:         types.RelativePositionAfterProgram,
							ScheduledStartTimeMillis: aws.Int64(time.Now().UnixMilli()),
							DurationMillis:           aws.Int64(30000),
						},
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "prefetch schedule",
			setup: func(t *testing.T, client *mediatailorsdk.Client) string {
				t.Helper()
				_, err := client.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
					Name:                  aws.String("pc-for-prefetch"),
					AdDecisionServerUrl:   aws.String("https://ads.example.com"),
					VideoContentSourceUrl: aws.String("https://video.example.com"),
				})
				require.NoError(t, err)

				out, err := client.CreatePrefetchSchedule(t.Context(), &mediatailorsdk.CreatePrefetchScheduleInput{
					PlaybackConfigurationName: aws.String("pc-for-prefetch"),
					Name:                      aws.String("tagged-prefetch"),
					Retrieval: &types.PrefetchRetrieval{
						EndTime: aws.Time(time.Now().Add(time.Hour)),
					},
					Consumption: &types.PrefetchConsumption{
						EndTime: aws.Time(time.Now().Add(time.Hour)),
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
			client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &mediatailorsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got.Tags)
		})
	}
}

// TestGetChannelSchedule_AudienceFilter proves GetChannelScheduleInput.Audience
// (api_op_GetChannelSchedule.go: "The single audience for
// GetChannelScheduleRequest") constrains the returned schedule entries to
// those whose Audiences list contains the requested value. Before the fix,
// the handler read only MaxResults/NextToken from the query string; Audience
// was never read, and ScheduleEntry.Audiences was never even populated from
// CreateProgramInput.AudienceMedia.
func TestGetChannelSchedule_AudienceFilter(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	_, err := client.CreateChannel(t.Context(), &mediatailorsdk.CreateChannelInput{
		ChannelName:  aws.String("audience-channel"),
		PlaybackMode: types.PlaybackModeLoop,
		Outputs: []types.RequestOutputItem{
			{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
		SourceLocationName: aws.String("sl-for-audience"),
		HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
	})
	require.NoError(t, err)

	_, err = client.CreateVodSource(t.Context(), &mediatailorsdk.CreateVodSourceInput{
		SourceLocationName: aws.String("sl-for-audience"),
		VodSourceName:      aws.String("vs-for-audience"),
		HttpPackageConfigurations: []types.HttpPackageConfiguration{
			{Path: aws.String("/vod"), SourceGroup: aws.String("default"), Type: types.TypeHls},
		},
	})
	require.NoError(t, err)

	base := time.Now().UnixMilli()

	_, err = client.CreateProgram(t.Context(), &mediatailorsdk.CreateProgramInput{
		ChannelName:        aws.String("audience-channel"),
		ProgramName:        aws.String("prog-family"),
		SourceLocationName: aws.String("sl-for-audience"),
		VodSourceName:      aws.String("vs-for-audience"),
		ScheduleConfiguration: &types.ScheduleConfiguration{
			Transition: &types.Transition{
				Type:                     aws.String("ABSOLUTE"),
				RelativePosition:         types.RelativePositionAfterProgram,
				ScheduledStartTimeMillis: aws.Int64(base),
				DurationMillis:           aws.Int64(30000),
			},
		},
		AudienceMedia: []types.AudienceMedia{{Audience: aws.String("FAMILY")}},
	})
	require.NoError(t, err)

	_, err = client.CreateProgram(t.Context(), &mediatailorsdk.CreateProgramInput{
		ChannelName:        aws.String("audience-channel"),
		ProgramName:        aws.String("prog-adult"),
		SourceLocationName: aws.String("sl-for-audience"),
		VodSourceName:      aws.String("vs-for-audience"),
		ScheduleConfiguration: &types.ScheduleConfiguration{
			Transition: &types.Transition{
				Type:                     aws.String("ABSOLUTE"),
				RelativePosition:         types.RelativePositionAfterProgram,
				ScheduledStartTimeMillis: aws.Int64(base + 60000),
				DurationMillis:           aws.Int64(30000),
			},
		},
		AudienceMedia: []types.AudienceMedia{{Audience: aws.String("ADULT")}},
	})
	require.NoError(t, err)

	all, err := client.GetChannelSchedule(t.Context(), &mediatailorsdk.GetChannelScheduleInput{
		ChannelName: aws.String("audience-channel"),
	})
	require.NoError(t, err)
	require.Len(t, all.Items, 2)
	assert.ElementsMatch(t, []string{"FAMILY"}, all.Items[0].Audiences)

	family, err := client.GetChannelSchedule(t.Context(), &mediatailorsdk.GetChannelScheduleInput{
		ChannelName: aws.String("audience-channel"),
		Audience:    aws.String("FAMILY"),
	})
	require.NoError(t, err)
	require.Len(t, family.Items, 1)
	assert.Equal(t, "prog-family", aws.ToString(family.Items[0].ProgramName))
}
