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

// TestGetInAppMessages_RealClient covers gopherstack-ipmu: GetInAppMessages
// had zero test coverage, and its handler fabricated a "campaign" out of
// every in-app template in the account rather than returning real campaigns
// that target one. Driven through the real SDK client so a typed decode
// proves the wire shape (flat InAppMessageCampaigns, per
// pinpoint@v1.42.4 deserializers.go:10412 -- see
// messages_wrapper_fix_test.go's package doc comment on pinpoint's
// flat-response convention) as well as the content.
func TestGetInAppMessages_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("gim-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.CreateInAppTemplate(t.Context(), &pinpointsdk.CreateInAppTemplateInput{
		TemplateName: aws.String("gim-template"),
		InAppTemplateRequest: &types.InAppTemplateRequest{
			Layout: types.LayoutBottomBanner,
			Content: []types.InAppMessageContent{
				{BackgroundColor: aws.String("#FFFFFF")},
			},
		},
	})
	require.NoError(t, err)

	campOut, err := client.CreateCampaign(t.Context(), &pinpointsdk.CreateCampaignInput{
		ApplicationId: aws.String(appID),
		WriteCampaignRequest: &types.WriteCampaignRequest{
			Name:      aws.String("gim-campaign"),
			SegmentId: aws.String("seg-001"),
			Priority:  aws.Int32(5),
			Schedule:  &types.Schedule{StartTime: aws.String("IMMEDIATE")},
			TemplateConfiguration: &types.TemplateConfiguration{
				InAppTemplate: &types.Template{Name: aws.String("gim-template")},
			},
		},
	})
	require.NoError(t, err)
	campaignID := aws.ToString(campOut.CampaignResponse.Id)

	out, err := client.GetInAppMessages(t.Context(), &pinpointsdk.GetInAppMessagesInput{
		ApplicationId: aws.String(appID),
		EndpointId:    aws.String("gim-endpoint"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.InAppMessagesResponse)
	require.Len(t, out.InAppMessagesResponse.InAppMessageCampaigns, 1)

	got := out.InAppMessagesResponse.InAppMessageCampaigns[0]
	assert.Equal(t, campaignID, aws.ToString(got.CampaignId))
	assert.EqualValues(t, 5, aws.ToInt32(got.Priority))

	require.NotNil(t, got.InAppMessage, "InAppMessage must carry the template's real content")
	assert.Equal(t, types.LayoutBottomBanner, got.InAppMessage.Layout)
	require.Len(t, got.InAppMessage.Content, 1)
	assert.Equal(t, "#FFFFFF", aws.ToString(got.InAppMessage.Content[0].BackgroundColor))
}

// TestGetInAppMessages_UnknownApp_RealClient proves the exact NotFoundException
// error code (pinpoint's generic error shape across almost every op --
// pinpoint@v1.42.4 types/errors.go's NotFoundException) surfaces through the
// real client rather than a generic smithy API error.
func TestGetInAppMessages_UnknownApp_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	_, err := client.GetInAppMessages(t.Context(), &pinpointsdk.GetInAppMessagesInput{
		ApplicationId: aws.String("does-not-exist"),
		EndpointId:    aws.String("gim-endpoint"),
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe, "must surface as the real NotFoundException type, not a generic API error")
}
