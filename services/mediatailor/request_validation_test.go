package mediatailor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestRequestValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(c *mediatailorsdk.Client) error
		name string
	}{
		{func(c *mediatailorsdk.Client) error {
			_, err := c.CreateChannel(t.Context(), &mediatailorsdk.CreateChannelInput{
				ChannelName: aws.String("bad name"), PlaybackMode: types.PlaybackModeLoop,
				Outputs: []types.RequestOutputItem{{ManifestName: aws.String("m"), SourceGroup: aws.String("g")}},
			})

			return err
		}, "channel_name_space"},
		{func(c *mediatailorsdk.Client) error {
			_, err := c.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
				Name: aws.String("with/slash"),
			})

			return err
		}, "playback_name_slash"},
		{func(c *mediatailorsdk.Client) error {
			_, err := c.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
				Name:             aws.String("pc"),
				AvailSuppression: &types.AvailSuppression{Mode: types.Mode("BOGUS")},
			})

			return err
		}, "avail_suppression_mode"},
		{func(c *mediatailorsdk.Client) error {
			_, err := c.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
				Name:             aws.String("pc"),
				AvailSuppression: &types.AvailSuppression{Mode: types.ModeOff, FillPolicy: types.FillPolicy("BOGUS")},
			})

			return err
		}, "avail_suppression_fill"},
		{func(c *mediatailorsdk.Client) error {
			_, err := c.ListChannels(t.Context(), &mediatailorsdk.ListChannelsInput{MaxResults: aws.Int32(0)})

			return err
		}, "max_results_zero"},
		{func(c *mediatailorsdk.Client) error {
			_, err := c.ListSourceLocations(
				t.Context(),
				&mediatailorsdk.ListSourceLocationsInput{NextToken: aws.String("%%bad")},
			)

			return err
		}, "bad_token"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.run(
				newTestMediaTailorClient(
					t,
					mediatailor.NewHandler(mediatailor.NewInMemoryBackend("123456789012", "us-east-1")),
				),
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "BadRequestException")
		})
	}
}

func TestProgramOmitsUnsetSourceName(t *testing.T) {
	t.Parallel()

	client := newTestMediaTailorClient(
		t,
		mediatailor.NewHandler(mediatailor.NewInMemoryBackend("123456789012", "us-east-1")),
	)
	ctx := t.Context()

	_, err := client.CreateSourceLocation(ctx, &mediatailorsdk.CreateSourceLocationInput{
		SourceLocationName: aws.String("sl"),
		HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
	})
	require.NoError(t, err)
	_, err = client.CreateVodSource(ctx, &mediatailorsdk.CreateVodSourceInput{
		SourceLocationName: aws.String("sl"), VodSourceName: aws.String("v"),
		HttpPackageConfigurations: []types.HttpPackageConfiguration{
			{Path: aws.String("/p"), SourceGroup: aws.String("g"), Type: types.TypeHls},
		},
	})
	require.NoError(t, err)
	_, err = client.CreateChannel(ctx, &mediatailorsdk.CreateChannelInput{
		ChannelName: aws.String("c"), PlaybackMode: types.PlaybackModeLinear,
		Outputs: []types.RequestOutputItem{{ManifestName: aws.String("m"), SourceGroup: aws.String("g")}},
	})
	require.NoError(t, err)

	out, err := client.CreateProgram(ctx, &mediatailorsdk.CreateProgramInput{
		ChannelName: aws.String("c"), ProgramName: aws.String("p"),
		SourceLocationName: aws.String("sl"), VodSourceName: aws.String("v"),
		ScheduleConfiguration: &types.ScheduleConfiguration{Transition: &types.Transition{
			Type: aws.String("ABSOLUTE"), RelativePosition: types.RelativePositionAfterProgram,
			ScheduledStartTimeMillis: aws.Int64(1893456000000),
		}},
	})
	require.NoError(t, err)
	assert.Nil(t, out.LiveSourceName)
	assert.Equal(t, "v", aws.ToString(out.VodSourceName))
}
