package medialive_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestRealClient_DescribeScheduleHonoursPaging covers maxResults/nextToken (query) on
// DescribeSchedule and rejection of a malformed token on the alert lists.
func TestRealClient_DescribeScheduleHonoursPaging(t *testing.T) {
	t.Parallel()

	client := newTestMediaLiveClient(t, medialive.NewHandler(medialive.NewInMemoryBackend("000000000000", "us-east-1")))
	channelID := createTestChannelSDK(t, client)

	actions := make([]types.ScheduleAction, 0, 3)
	for i := range 3 {
		actions = append(actions, types.ScheduleAction{
			ActionName: aws.String(fmt.Sprintf("a%d", i)),
			ScheduleActionSettings: &types.ScheduleActionSettings{
				PauseStateSettings: &types.PauseStateScheduleActionSettings{
					Pipelines: []types.PipelinePauseStateSettings{{PipelineId: types.PipelineIdPipeline0}},
				},
			},
			ScheduleActionStartSettings: &types.ScheduleActionStartSettings{
				ImmediateModeScheduleActionStartSettings: &types.ImmediateModeScheduleActionStartSettings{},
			},
		})
	}

	_, err := client.BatchUpdateSchedule(t.Context(), &medialivesdk.BatchUpdateScheduleInput{
		ChannelId: aws.String(channelID), Creates: &types.BatchScheduleActionCreateRequest{ScheduleActions: actions},
	})
	require.NoError(t, err)

	var token *string

	got, pages := 0, 0

	for {
		out, descErr := client.DescribeSchedule(t.Context(), &medialivesdk.DescribeScheduleInput{
			ChannelId: aws.String(channelID), MaxResults: aws.Int32(2), NextToken: token,
		})
		require.NoError(t, descErr)

		got += len(out.ScheduleActions)
		pages++

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	require.Equal(t, 3, got)
	require.Equal(t, 2, pages)

	_, alertErr := client.ListAlerts(t.Context(), &medialivesdk.ListAlertsInput{
		ChannelId: aws.String(channelID), NextToken: aws.String("%%%"),
	})

	var apiErr smithy.APIError

	require.ErrorAs(t, alertErr, &apiErr)
	require.Equal(t, "BadRequestException", apiErr.ErrorCode())
}
