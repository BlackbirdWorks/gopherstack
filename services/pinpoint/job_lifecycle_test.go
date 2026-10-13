package pinpoint_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

func TestBackend_JobProgression(t *testing.T) {
	t.Parallel()

	const delay = 10 * time.Second

	cases := []struct {
		name    string
		want    string
		advance time.Duration
	}{
		{name: "created", advance: 0, want: "CREATED"},
		{name: "processing", advance: delay / 2, want: "PROCESSING"},
		{name: "completed", advance: delay, want: "COMPLETED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newTestBackend()
				b.SetLifecycleDelay(delay)

				app, err := b.CreateApp("us-east-1", "123456789012", "a", nil)
				require.NoError(t, err)
				require.NoError(t, pinpoint.UpdateEndpointForTest(b, app.ID, "ep1", "a@example.com"))

				exp, err := b.CreateExportJob("us-east-1", "123456789012", app.ID,
					pinpoint.ExportedCreateExportJobRequest{RoleArn: "r", S3UrlPrefix: "s3://b/p"})
				require.NoError(t, err)
				imp, err := b.CreateImportJob("us-east-1", "123456789012", app.ID,
					pinpoint.ExportedCreateImportJobRequest{RoleArn: "r", S3Url: "s3://b/k", Format: "CSV"})
				require.NoError(t, err)

				time.Sleep(tc.advance)

				gotExp, err := b.GetExportJob(app.ID, exp.ID)
				require.NoError(t, err)
				assert.Equal(t, tc.want, gotExp.JobStatus)

				gotImp, err := b.GetImportJob(app.ID, imp.ID)
				require.NoError(t, err)
				assert.Equal(t, tc.want, gotImp.JobStatus)

				if tc.want == "COMPLETED" {
					assert.NotEmpty(t, gotExp.CompletionDate)
					assert.Equal(t, 1, gotExp.CompletedPieces)
					assert.Equal(t, 1, gotExp.TotalProcessed)
				}
			})
		})
	}
}

func TestSDK_JobAndCampaignValidation(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::000000000000:role/r"

	cases := []struct {
		name string
		call func(t *testing.T, c *pinpointsdk.Client, appID, segID string) error
		code string
	}{
		{"import_bad_format", func(t *testing.T, c *pinpointsdk.Client, appID, _ string) error {
			t.Helper()

			_, err := c.CreateImportJob(t.Context(), &pinpointsdk.CreateImportJobInput{
				ApplicationId: aws.String(appID),
				ImportJobRequest: &types.ImportJobRequest{
					Format: types.Format("XML"), RoleArn: aws.String(role), S3Url: aws.String("s3://b/k"),
				},
			})

			return err
		}, "BadRequestException"},
		{"export_bad_prefix", func(t *testing.T, c *pinpointsdk.Client, appID, _ string) error {
			t.Helper()

			_, err := c.CreateExportJob(t.Context(), &pinpointsdk.CreateExportJobInput{
				ApplicationId: aws.String(appID),
				ExportJobRequest: &types.ExportJobRequest{
					RoleArn:     aws.String(role),
					S3UrlPrefix: aws.String("http://x"),
				},
			})

			return err
		}, "BadRequestException"},
		{"campaign_bad_frequency", func(t *testing.T, c *pinpointsdk.Client, appID, segID string) error {
			t.Helper()

			_, err := c.CreateCampaign(t.Context(), &pinpointsdk.CreateCampaignInput{
				ApplicationId: aws.String(appID),
				WriteCampaignRequest: &types.WriteCampaignRequest{
					SegmentId: aws.String(segID),
					Schedule: &types.Schedule{
						StartTime: aws.String("IMMEDIATE"),
						Frequency: types.Frequency("SOMETIMES"),
					},
				},
			})

			return err
		}, "BadRequestException"},
		{"campaign_bad_start", func(t *testing.T, c *pinpointsdk.Client, appID, segID string) error {
			t.Helper()

			_, err := c.CreateCampaign(t.Context(), &pinpointsdk.CreateCampaignInput{
				ApplicationId: aws.String(appID),
				WriteCampaignRequest: &types.WriteCampaignRequest{
					SegmentId: aws.String(segID),
					Schedule:  &types.Schedule{StartTime: aws.String("tomorrow")},
				},
			})

			return err
		}, "BadRequestException"},
		{"journey_bad_state", func(t *testing.T, c *pinpointsdk.Client, appID, _ string) error {
			t.Helper()

			_, err := c.CreateJourney(t.Context(), &pinpointsdk.CreateJourneyInput{
				ApplicationId:       aws.String(appID),
				WriteJourneyRequest: &types.WriteJourneyRequest{Name: aws.String("j"), State: types.State("NOPE")},
			})

			return err
		}, "BadRequestException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestPinpointClient(t, newHandlerForTest(t))
			app, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
				CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("a")},
			})
			require.NoError(t, err)
			seg, err := client.CreateSegment(t.Context(), &pinpointsdk.CreateSegmentInput{
				ApplicationId:       app.ApplicationResponse.Id,
				WriteSegmentRequest: &types.WriteSegmentRequest{Name: aws.String("s")},
			})
			require.NoError(t, err)

			err = tc.call(t, client, aws.ToString(app.ApplicationResponse.Id), aws.ToString(seg.SegmentResponse.Id))

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.code, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), tc.code+":")
		})
	}
}

func TestSDK_JourneyCreatedActive(t *testing.T) {
	t.Parallel()

	client := newTestPinpointClient(t, newHandlerForTest(t))
	app, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("a")},
	})
	require.NoError(t, err)

	out, err := client.CreateJourney(t.Context(), &pinpointsdk.CreateJourneyInput{
		ApplicationId:       app.ApplicationResponse.Id,
		WriteJourneyRequest: &types.WriteJourneyRequest{Name: aws.String("j"), State: types.StateActive},
	})
	require.NoError(t, err)
	assert.Equal(t, types.StateActive, out.JourneyResponse.State)

	runs, err := client.GetJourneyRuns(t.Context(), &pinpointsdk.GetJourneyRunsInput{
		ApplicationId: app.ApplicationResponse.Id, JourneyId: out.JourneyResponse.Id,
	})
	require.NoError(t, err)
	assert.Len(t, runs.JourneyRunsResponse.Item, 1)
}
