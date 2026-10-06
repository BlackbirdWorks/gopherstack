package pinpoint_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

func TestSDK_RenamedAndDroppedMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *pinpoint.Handler, c *pinpointsdk.Client)
		name string
	}{
		{
			name: "activity_metrics_journey_activity_id",
			run: func(t *testing.T, h *pinpoint.Handler, c *pinpointsdk.Client) {
				t.Helper()

				appID := createTestApp(t, h, "rt-app")
				j, err := c.CreateJourney(t.Context(), &pinpointsdk.CreateJourneyInput{
					ApplicationId:       aws.String(appID),
					WriteJourneyRequest: &types.WriteJourneyRequest{Name: aws.String("j")},
				})
				require.NoError(t, err)

				in := &pinpointsdk.GetJourneyExecutionActivityMetricsInput{
					ApplicationId:     aws.String(appID),
					JourneyId:         j.JourneyResponse.Id,
					JourneyActivityId: aws.String("act-1"),
				}

				out, err := c.GetJourneyExecutionActivityMetrics(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, "act-1", aws.ToString(out.JourneyExecutionActivityMetricsResponse.JourneyActivityId))
			},
		},
		{
			name: "phone_number_validate_national",
			run: func(t *testing.T, _ *pinpoint.Handler, c *pinpointsdk.Client) {
				t.Helper()

				out, err := c.PhoneNumberValidate(t.Context(), &pinpointsdk.PhoneNumberValidateInput{
					NumberValidateRequest: &types.NumberValidateRequest{PhoneNumber: aws.String("+12125551234")},
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(out.NumberValidateResponse.CleansedPhoneNumberNational))
			},
		},
		{
			name: "application_settings_core_members",
			run: func(t *testing.T, h *pinpoint.Handler, c *pinpointsdk.Client) {
				t.Helper()

				appID := createTestApp(t, h, "settings-app")
				_, err := c.UpdateApplicationSettings(t.Context(), &pinpointsdk.UpdateApplicationSettingsInput{
					ApplicationId: aws.String(appID),
					WriteApplicationSettingsRequest: &types.WriteApplicationSettingsRequest{
						CloudWatchMetricsEnabled: aws.Bool(true),
						Limits:                   &types.CampaignLimits{Daily: aws.Int32(100)},
					},
				})
				require.NoError(t, err)

				got, err := c.GetApplicationSettings(t.Context(), &pinpointsdk.GetApplicationSettingsInput{
					ApplicationId: aws.String(appID),
				})
				require.NoError(t, err)
				assert.Equal(t, appID, aws.ToString(got.ApplicationSettingsResource.ApplicationId))
				require.NotNil(t, got.ApplicationSettingsResource.Limits)
				assert.Equal(t, int32(100), aws.ToInt32(got.ApplicationSettingsResource.Limits.Daily))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			tc.run(t, h, newTestPinpointClient(t, h))
		})
	}
}
