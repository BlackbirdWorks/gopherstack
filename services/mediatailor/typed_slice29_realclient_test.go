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

// Test_Slice29_RealClient drives every gopherstack-n3zi typed-slice-29
// uncovered mediatailor op through the real aws-sdk-go-v2 client.
func Test_Slice29_RealClient(t *testing.T) {
	t.Parallel()

	newHandler := func() *mediatailor.Handler {
		return mediatailor.NewHandler(mediatailor.NewInMemoryBackend("123456789012", "us-east-1"))
	}

	t.Run("channel_lifecycle", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestMediaTailorClient(t, h)
		ctx := t.Context()

		_, err := client.CreateChannel(ctx, &mediatailorsdk.CreateChannelInput{
			ChannelName:  aws.String("chan1"),
			PlaybackMode: types.PlaybackModeLoop,
			Outputs: []types.RequestOutputItem{
				{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
			},
		})
		require.NoError(t, err)

		descOut, err := client.DescribeChannel(ctx, &mediatailorsdk.DescribeChannelInput{
			ChannelName: aws.String("chan1"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ChannelStateStopped, descOut.ChannelState)

		logsOut, err := client.ConfigureLogsForChannel(ctx, &mediatailorsdk.ConfigureLogsForChannelInput{
			ChannelName: aws.String("chan1"),
			LogTypes:    []types.LogType{types.LogTypeAsRun},
		})
		require.NoError(t, err)
		assert.Equal(t, "chan1", aws.ToString(logsOut.ChannelName))
		require.Len(t, logsOut.LogTypes, 1)

		updOut, err := client.UpdateChannel(ctx, &mediatailorsdk.UpdateChannelInput{
			ChannelName: aws.String("chan1"),
			Outputs: []types.RequestOutputItem{
				{ManifestName: aws.String("index2"), SourceGroup: aws.String("default")},
			},
		})
		require.NoError(t, err)
		require.Len(t, updOut.Outputs, 1)
		assert.Equal(t, "index2", aws.ToString(updOut.Outputs[0].ManifestName))

		_, err = client.StartChannel(ctx, &mediatailorsdk.StartChannelInput{ChannelName: aws.String("chan1")})
		require.NoError(t, err)

		descOut2, err := client.DescribeChannel(ctx, &mediatailorsdk.DescribeChannelInput{
			ChannelName: aws.String("chan1"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ChannelStateRunning, descOut2.ChannelState)

		_, err = client.StopChannel(ctx, &mediatailorsdk.StopChannelInput{ChannelName: aws.String("chan1")})
		require.NoError(t, err)

		descOut3, err := client.DescribeChannel(ctx, &mediatailorsdk.DescribeChannelInput{
			ChannelName: aws.String("chan1"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ChannelStateStopped, descOut3.ChannelState)

		_, err = client.DeleteChannel(ctx, &mediatailorsdk.DeleteChannelInput{ChannelName: aws.String("chan1")})
		require.NoError(t, err)

		_, err = client.DescribeChannel(ctx, &mediatailorsdk.DescribeChannelInput{ChannelName: aws.String("chan1")})
		require.Error(t, err, "channel was deleted")
	})

	t.Run("channel_policy_lifecycle", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestMediaTailorClient(t, h)
		ctx := t.Context()

		_, err := client.CreateChannel(ctx, &mediatailorsdk.CreateChannelInput{
			ChannelName:  aws.String("chan-pol"),
			PlaybackMode: types.PlaybackModeLoop,
			Outputs: []types.RequestOutputItem{
				{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
			},
		})
		require.NoError(t, err)

		_, err = client.PutChannelPolicy(ctx, &mediatailorsdk.PutChannelPolicyInput{
			ChannelName: aws.String("chan-pol"),
			Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)

		getOut, err := client.GetChannelPolicy(ctx, &mediatailorsdk.GetChannelPolicyInput{
			ChannelName: aws.String("chan-pol"),
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(getOut.Policy))

		_, err = client.DeleteChannelPolicy(ctx, &mediatailorsdk.DeleteChannelPolicyInput{
			ChannelName: aws.String("chan-pol"),
		})
		require.NoError(t, err)

		_, err = client.GetChannelPolicy(ctx, &mediatailorsdk.GetChannelPolicyInput{
			ChannelName: aws.String("chan-pol"),
		})
		require.Error(t, err, "policy was deleted")
	})

	t.Run("function_delete", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestMediaTailorClient(t, h)
		ctx := t.Context()

		putOut, err := client.PutFunction(ctx, &mediatailorsdk.PutFunctionInput{
			FunctionId:   aws.String("fn1"),
			FunctionType: types.FunctionTypeCustomOutput,
			CustomOutputConfiguration: &types.CustomOutputConfiguration{
				Runtime: types.RuntimeTypeJsonata,
				Output:  map[string]string{"result": "1"},
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteFunction(ctx, &mediatailorsdk.DeleteFunctionInput{
			FunctionId: putOut.FunctionId,
		})
		require.NoError(t, err)

		_, err = client.GetFunction(ctx, &mediatailorsdk.GetFunctionInput{
			FunctionId: putOut.FunctionId,
		})
		require.Error(t, err, "function was deleted")
	})

	t.Run("source_location_and_live_source_and_vod_source", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestMediaTailorClient(t, h)
		ctx := t.Context()

		_, err := client.CreateSourceLocation(ctx, &mediatailorsdk.CreateSourceLocationInput{
			SourceLocationName: aws.String("sl1"),
			HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
		})
		require.NoError(t, err)

		updSLOut, err := client.UpdateSourceLocation(ctx, &mediatailorsdk.UpdateSourceLocationInput{
			SourceLocationName: aws.String("sl1"),
			HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://updated.example.com")},
		})
		require.NoError(t, err)
		assert.Equal(t, "https://updated.example.com", aws.ToString(updSLOut.HttpConfiguration.BaseUrl))

		_, err = client.CreateLiveSource(ctx, &mediatailorsdk.CreateLiveSourceInput{
			SourceLocationName: aws.String("sl1"),
			LiveSourceName:     aws.String("live1"),
			HttpPackageConfigurations: []types.HttpPackageConfiguration{
				{Path: aws.String("/live"), SourceGroup: aws.String("default"), Type: types.TypeHls},
			},
		})
		require.NoError(t, err)

		updLiveOut, err := client.UpdateLiveSource(ctx, &mediatailorsdk.UpdateLiveSourceInput{
			SourceLocationName: aws.String("sl1"),
			LiveSourceName:     aws.String("live1"),
			HttpPackageConfigurations: []types.HttpPackageConfiguration{
				{Path: aws.String("/live2"), SourceGroup: aws.String("default"), Type: types.TypeHls},
			},
		})
		require.NoError(t, err)
		require.Len(t, updLiveOut.HttpPackageConfigurations, 1)
		assert.Equal(t, "/live2", aws.ToString(updLiveOut.HttpPackageConfigurations[0].Path))

		_, err = client.DeleteLiveSource(ctx, &mediatailorsdk.DeleteLiveSourceInput{
			SourceLocationName: aws.String("sl1"),
			LiveSourceName:     aws.String("live1"),
		})
		require.NoError(t, err)

		_, err = client.CreateVodSource(ctx, &mediatailorsdk.CreateVodSourceInput{
			SourceLocationName: aws.String("sl1"),
			VodSourceName:      aws.String("vod1"),
			HttpPackageConfigurations: []types.HttpPackageConfiguration{
				{Path: aws.String("/vod"), SourceGroup: aws.String("default"), Type: types.TypeHls},
			},
		})
		require.NoError(t, err)

		updVodOut, err := client.UpdateVodSource(ctx, &mediatailorsdk.UpdateVodSourceInput{
			SourceLocationName: aws.String("sl1"),
			VodSourceName:      aws.String("vod1"),
			HttpPackageConfigurations: []types.HttpPackageConfiguration{
				{Path: aws.String("/vod2"), SourceGroup: aws.String("default"), Type: types.TypeHls},
			},
		})
		require.NoError(t, err)
		require.Len(t, updVodOut.HttpPackageConfigurations, 1)
		assert.Equal(t, "/vod2", aws.ToString(updVodOut.HttpPackageConfigurations[0].Path))

		_, err = client.DeleteVodSource(ctx, &mediatailorsdk.DeleteVodSourceInput{
			SourceLocationName: aws.String("sl1"),
			VodSourceName:      aws.String("vod1"),
		})
		require.NoError(t, err)

		_, err = client.DescribeVodSource(ctx, &mediatailorsdk.DescribeVodSourceInput{
			SourceLocationName: aws.String("sl1"),
			VodSourceName:      aws.String("vod1"),
		})
		require.Error(t, err, "vod source was deleted")
	})

	t.Run("program_lifecycle", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestMediaTailorClient(t, h)
		ctx := t.Context()

		_, err := client.CreateChannel(ctx, &mediatailorsdk.CreateChannelInput{
			ChannelName:  aws.String("chan-prog"),
			PlaybackMode: types.PlaybackModeLoop,
			Outputs: []types.RequestOutputItem{
				{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
			},
		})
		require.NoError(t, err)
		_, err = client.CreateSourceLocation(ctx, &mediatailorsdk.CreateSourceLocationInput{
			SourceLocationName: aws.String("sl-prog"),
			HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
		})
		require.NoError(t, err)
		_, err = client.CreateVodSource(ctx, &mediatailorsdk.CreateVodSourceInput{
			SourceLocationName: aws.String("sl-prog"),
			VodSourceName:      aws.String("vs-prog"),
			HttpPackageConfigurations: []types.HttpPackageConfiguration{
				{Path: aws.String("/vod"), SourceGroup: aws.String("default"), Type: types.TypeHls},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateProgram(ctx, &mediatailorsdk.CreateProgramInput{
			ChannelName:        aws.String("chan-prog"),
			ProgramName:        aws.String("prog1"),
			SourceLocationName: aws.String("sl-prog"),
			VodSourceName:      aws.String("vs-prog"),
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

		updOut, err := client.UpdateProgram(ctx, &mediatailorsdk.UpdateProgramInput{
			ChannelName: aws.String("chan-prog"),
			ProgramName: aws.String("prog1"),
			ScheduleConfiguration: &types.UpdateProgramScheduleConfiguration{
				Transition: &types.UpdateProgramTransition{
					DurationMillis: aws.Int64(60000),
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(60000), aws.ToInt64(updOut.DurationMillis))

		_, err = client.DeleteProgram(ctx, &mediatailorsdk.DeleteProgramInput{
			ChannelName: aws.String("chan-prog"),
			ProgramName: aws.String("prog1"),
		})
		require.NoError(t, err)

		_, err = client.DescribeProgram(ctx, &mediatailorsdk.DescribeProgramInput{
			ChannelName: aws.String("chan-prog"),
			ProgramName: aws.String("prog1"),
		})
		require.Error(t, err, "program was deleted")
	})

	t.Run("playback_configuration_and_prefetch_and_alerts", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestMediaTailorClient(t, h)
		ctx := t.Context()

		putOut, err := client.PutPlaybackConfiguration(ctx, &mediatailorsdk.PutPlaybackConfigurationInput{
			Name:                  aws.String("pc1"),
			AdDecisionServerUrl:   aws.String("https://ads.example.com"),
			VideoContentSourceUrl: aws.String("https://video.example.com"),
		})
		require.NoError(t, err)

		getOut, err := client.GetPlaybackConfiguration(ctx, &mediatailorsdk.GetPlaybackConfigurationInput{
			Name: aws.String("pc1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "pc1", aws.ToString(getOut.Name))
		assert.Equal(t, aws.ToString(putOut.PlaybackConfigurationArn), aws.ToString(getOut.PlaybackConfigurationArn))

		_, err = client.CreatePrefetchSchedule(ctx, &mediatailorsdk.CreatePrefetchScheduleInput{
			PlaybackConfigurationName: aws.String("pc1"),
			Name:                      aws.String("prefetch1"),
			Retrieval:                 &types.PrefetchRetrieval{EndTime: aws.Time(time.Now().Add(time.Hour))},
			Consumption:               &types.PrefetchConsumption{EndTime: aws.Time(time.Now().Add(time.Hour))},
		})
		require.NoError(t, err)

		listOut, err := client.ListPrefetchSchedules(ctx, &mediatailorsdk.ListPrefetchSchedulesInput{
			PlaybackConfigurationName: aws.String("pc1"),
		})
		require.NoError(t, err)
		require.Len(t, listOut.Items, 1)
		assert.Equal(t, "prefetch1", aws.ToString(listOut.Items[0].Name))

		alertsOut, err := client.ListAlerts(ctx, &mediatailorsdk.ListAlertsInput{
			ResourceArn: putOut.PlaybackConfigurationArn,
		})
		require.NoError(t, err)
		assert.Empty(t, alertsOut.Items)

		_, err = client.DeletePrefetchSchedule(ctx, &mediatailorsdk.DeletePrefetchScheduleInput{
			PlaybackConfigurationName: aws.String("pc1"),
			Name:                      aws.String("prefetch1"),
		})
		require.NoError(t, err)

		listOut2, err := client.ListPrefetchSchedules(ctx, &mediatailorsdk.ListPrefetchSchedulesInput{
			PlaybackConfigurationName: aws.String("pc1"),
		})
		require.NoError(t, err)
		assert.Empty(t, listOut2.Items)

		_, err = client.DeletePlaybackConfiguration(ctx, &mediatailorsdk.DeletePlaybackConfigurationInput{
			Name: aws.String("pc1"),
		})
		require.NoError(t, err)

		_, err = client.GetPlaybackConfiguration(ctx, &mediatailorsdk.GetPlaybackConfigurationInput{
			Name: aws.String("pc1"),
		})
		require.Error(t, err, "playback configuration was deleted")
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		client := newTestMediaTailorClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateChannel(ctx, &mediatailorsdk.CreateChannelInput{
			ChannelName:  aws.String("chan-tags"),
			PlaybackMode: types.PlaybackModeLoop,
			Outputs: []types.RequestOutputItem{
				{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
			},
			Tags: map[string]string{"env": "test"},
		})
		require.NoError(t, err)

		_, err = client.UntagResource(ctx, &mediatailorsdk.UntagResourceInput{
			ResourceArn: createOut.Arn,
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		listOut, err := client.ListTagsForResource(ctx, &mediatailorsdk.ListTagsForResourceInput{
			ResourceArn: createOut.Arn,
		})
		require.NoError(t, err)
		assert.Empty(t, listOut.Tags)
	})
}
