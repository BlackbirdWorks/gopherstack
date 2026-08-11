package pinpoint_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// newTestPinpointClient stands up the real aws-sdk-go-v2 Pinpoint client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestPinpointClient(t *testing.T, h *pinpoint.Handler) *pinpointsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return pinpointsdk.NewFromConfig(cfg, func(o *pinpointsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every Pinpoint Create op that accepts
// Tags in the real SDK (pinpoint@v1.42.4: CreateApp, CreateCampaign,
// CreateEmailTemplate, CreateInAppTemplate, CreatePushTemplate,
// CreateSegment, CreateSmsTemplate, CreateVoiceTemplate) through a real SDK
// client and asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). CreateExportJob/CreateImportJob/CreateJourney/
// CreateRecommenderConfiguration take no Tags field in the real SDK
// (verified: no Tags field on ExportJobRequest/ImportJobRequest/
// WriteJourneyRequest/CreateRecommenderConfigurationShape) so are excluded.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := map[string]string{"env": "prod"}

	newClient := func(t *testing.T) *pinpointsdk.Client {
		t.Helper()

		h := pinpoint.NewHandler(pinpoint.NewInMemoryBackend("us-east-1", "123456789012"))

		return newTestPinpointClient(t, h)
	}

	requireTags := func(t *testing.T, client *pinpointsdk.Client, resourceARN string) {
		t.Helper()

		out, err := client.ListTagsForResource(t.Context(), &pinpointsdk.ListTagsForResourceInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		require.Len(t, out.TagsModel.Tags, 1)
		assert.Equal(t, "prod", out.TagsModel.Tags["env"])
	}

	t.Run("createapp", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{
				Name: aws.String("tagged-app"),
				Tags: tags,
			},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.ApplicationResponse.Arn))
	})

	t.Run("createcampaign", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		app, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("campaign-app")},
		})
		require.NoError(t, err)

		seg, err := client.CreateSegment(t.Context(), &pinpointsdk.CreateSegmentInput{
			ApplicationId:       app.ApplicationResponse.Id,
			WriteSegmentRequest: &types.WriteSegmentRequest{Name: aws.String("campaign-segment")},
		})
		require.NoError(t, err)

		out, err := client.CreateCampaign(t.Context(), &pinpointsdk.CreateCampaignInput{
			ApplicationId: app.ApplicationResponse.Id,
			WriteCampaignRequest: &types.WriteCampaignRequest{
				Name:      aws.String("tagged-campaign"),
				SegmentId: seg.SegmentResponse.Id,
				Tags:      tags,
			},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.CampaignResponse.Arn))
	})

	t.Run("createemailtemplate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateEmailTemplate(t.Context(), &pinpointsdk.CreateEmailTemplateInput{
			TemplateName: aws.String("tagged-email-template"),
			EmailTemplateRequest: &types.EmailTemplateRequest{
				Subject: aws.String("hi"),
				Tags:    tags,
			},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.CreateTemplateMessageBody.Arn))
	})

	t.Run("createinapptemplate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateInAppTemplate(t.Context(), &pinpointsdk.CreateInAppTemplateInput{
			TemplateName:         aws.String("tagged-inapp-template"),
			InAppTemplateRequest: &types.InAppTemplateRequest{Tags: tags},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.TemplateCreateMessageBody.Arn))
	})

	t.Run("createpushtemplate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreatePushTemplate(t.Context(), &pinpointsdk.CreatePushTemplateInput{
			TemplateName: aws.String("tagged-push-template"),
			PushNotificationTemplateRequest: &types.PushNotificationTemplateRequest{
				Tags: tags,
			},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.CreateTemplateMessageBody.Arn))
	})

	t.Run("createsegment", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		app, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("segment-app")},
		})
		require.NoError(t, err)

		out, err := client.CreateSegment(t.Context(), &pinpointsdk.CreateSegmentInput{
			ApplicationId: app.ApplicationResponse.Id,
			WriteSegmentRequest: &types.WriteSegmentRequest{
				Name: aws.String("tagged-segment"),
				Tags: tags,
			},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.SegmentResponse.Arn))
	})

	t.Run("createsmstemplate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateSmsTemplate(t.Context(), &pinpointsdk.CreateSmsTemplateInput{
			TemplateName:       aws.String("tagged-sms-template"),
			SMSTemplateRequest: &types.SMSTemplateRequest{Tags: tags},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.CreateTemplateMessageBody.Arn))
	})

	t.Run("createvoicetemplate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateVoiceTemplate(t.Context(), &pinpointsdk.CreateVoiceTemplateInput{
			TemplateName:         aws.String("tagged-voice-template"),
			VoiceTemplateRequest: &types.VoiceTemplateRequest{Tags: tags},
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.CreateTemplateMessageBody.Arn))
	})
}
