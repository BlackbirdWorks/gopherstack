package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestScheduleAction_SettingsRoundTrip_RealClient drives BatchUpdateSchedule
// then DescribeSchedule through the real SDK client and asserts
// ScheduleActionSettings/ScheduleActionStartSettings round-trip.
// types.ScheduleAction.ScheduleActionSettings and .ScheduleActionStartSettings
// are both "This member is required" (medialive@v1.101.4 types/types.go:
// 7282, 7287), but the pre-fix handler only ever read/stored/emitted
// ActionName, silently dropping both required members --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestScheduleAction_SettingsRoundTrip_RealClient(t *testing.T) {
	t.Parallel()

	h := medialive.NewHandler(medialive.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestMediaLiveClient(t, h)
	ctx := t.Context()

	channelID := createTestChannelSDK(t, client)

	action := types.ScheduleAction{
		ActionName: aws.String("pause-at-start"),
		ScheduleActionSettings: &types.ScheduleActionSettings{
			PauseStateSettings: &types.PauseStateScheduleActionSettings{
				Pipelines: []types.PipelinePauseStateSettings{
					{PipelineId: types.PipelineIdPipeline0},
				},
			},
		},
		ScheduleActionStartSettings: &types.ScheduleActionStartSettings{
			ImmediateModeScheduleActionStartSettings: &types.ImmediateModeScheduleActionStartSettings{},
		},
	}

	created, err := client.BatchUpdateSchedule(ctx, &medialivesdk.BatchUpdateScheduleInput{
		ChannelId: aws.String(channelID),
		Creates: &types.BatchScheduleActionCreateRequest{
			ScheduleActions: []types.ScheduleAction{action},
		},
	})
	require.NoError(t, err)
	require.Len(t, created.Creates.ScheduleActions, 1)
	assertPauseStateRoundTrip(t, created.Creates.ScheduleActions[0])

	described, err := client.DescribeSchedule(ctx, &medialivesdk.DescribeScheduleInput{
		ChannelId: aws.String(channelID),
	})
	require.NoError(t, err)
	require.Len(t, described.ScheduleActions, 1)
	assertPauseStateRoundTrip(t, described.ScheduleActions[0])

	deleted, err := client.BatchUpdateSchedule(ctx, &medialivesdk.BatchUpdateScheduleInput{
		ChannelId: aws.String(channelID),
		Deletes:   &types.BatchScheduleActionDeleteRequest{ActionNames: []string{"pause-at-start"}},
	})
	require.NoError(t, err)
	require.Len(t, deleted.Deletes.ScheduleActions, 1,
		"BatchScheduleActionDeleteResult must echo the full deleted ScheduleAction, not just its name")
	assertPauseStateRoundTrip(t, deleted.Deletes.ScheduleActions[0])
}

func assertPauseStateRoundTrip(t *testing.T, a types.ScheduleAction) {
	t.Helper()

	require.NotNil(t, a.ScheduleActionSettings, "ScheduleActionSettings dropped from response")
	require.NotNil(t, a.ScheduleActionSettings.PauseStateSettings)
	require.Len(t, a.ScheduleActionSettings.PauseStateSettings.Pipelines, 1)
	assert.Equal(t, types.PipelineIdPipeline0, a.ScheduleActionSettings.PauseStateSettings.Pipelines[0].PipelineId)

	require.NotNil(t, a.ScheduleActionStartSettings, "ScheduleActionStartSettings dropped from response")
	assert.NotNil(t, a.ScheduleActionStartSettings.ImmediateModeScheduleActionStartSettings)
}

// createTestChannelSDK creates a minimal channel via the real SDK client and
// returns its ID.
func createTestChannelSDK(t *testing.T, client *medialivesdk.Client) string {
	t.Helper()

	out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
		Name:         aws.String("schedule-settings-channel"),
		ChannelClass: types.ChannelClassStandard,
	})
	require.NoError(t, err)

	return aws.ToString(out.Channel.Id)
}
