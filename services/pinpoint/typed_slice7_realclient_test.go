package pinpoint_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice7RealClient drives pinpoint's typed-coverage-blind ops
// (gopherstack-n3zi slice 7) through the real aws-sdk-go-v2 pinpoint
// client, one subtest per named priority family, asserting decoded values.
func TestTypedSlice7RealClient(t *testing.T) {
	t.Parallel()

	t.Run("campaigns", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("s7-camp-app")},
		})
		require.NoError(t, err)
		appID := aws.ToString(appOut.ApplicationResponse.Id)

		segOut, err := client.CreateSegment(t.Context(), &pinpointsdk.CreateSegmentInput{
			ApplicationId:       aws.String(appID),
			WriteSegmentRequest: &types.WriteSegmentRequest{Name: aws.String("s7-camp-seg")},
		})
		require.NoError(t, err)
		segID := aws.ToString(segOut.SegmentResponse.Id)

		createOut, err := client.CreateCampaign(t.Context(), &pinpointsdk.CreateCampaignInput{
			ApplicationId: aws.String(appID),
			WriteCampaignRequest: &types.WriteCampaignRequest{
				Name:      aws.String("s7-campaign"),
				SegmentId: aws.String(segID),
				Schedule:  &types.Schedule{StartTime: aws.String("IMMEDIATE")},
			},
		})
		require.NoError(t, err)
		campID := aws.ToString(createOut.CampaignResponse.Id)

		getOut, err := client.GetCampaign(t.Context(), &pinpointsdk.GetCampaignInput{
			ApplicationId: aws.String(appID),
			CampaignId:    aws.String(campID),
		})
		require.NoError(t, err)
		assert.Equal(t, "s7-campaign", aws.ToString(getOut.CampaignResponse.Name))

		listOut, err := client.GetCampaigns(t.Context(), &pinpointsdk.GetCampaignsInput{
			ApplicationId: aws.String(appID),
		})
		require.NoError(t, err)
		require.Len(t, listOut.CampaignsResponse.Item, 1)

		verOut, err := client.GetCampaignVersion(t.Context(), &pinpointsdk.GetCampaignVersionInput{
			ApplicationId: aws.String(appID),
			CampaignId:    aws.String(campID),
			Version:       aws.String("1"),
		})
		require.NoError(t, err)
		assert.Equal(t, campID, aws.ToString(verOut.CampaignResponse.Id))

		versionsOut, err := client.GetCampaignVersions(t.Context(), &pinpointsdk.GetCampaignVersionsInput{
			ApplicationId: aws.String(appID),
			CampaignId:    aws.String(campID),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, versionsOut.CampaignsResponse.Item)

		updOut, err := client.UpdateCampaign(t.Context(), &pinpointsdk.UpdateCampaignInput{
			ApplicationId: aws.String(appID),
			CampaignId:    aws.String(campID),
			WriteCampaignRequest: &types.WriteCampaignRequest{
				Name:      aws.String("s7-campaign-renamed"),
				SegmentId: aws.String(segID),
				Schedule:  &types.Schedule{StartTime: aws.String("IMMEDIATE")},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "s7-campaign-renamed", aws.ToString(updOut.CampaignResponse.Name))

		actOut, err := client.GetCampaignActivities(t.Context(), &pinpointsdk.GetCampaignActivitiesInput{
			ApplicationId: aws.String(appID),
			CampaignId:    aws.String(campID),
		})
		require.NoError(t, err)
		assert.NotNil(t, actOut.ActivitiesResponse.Item)

		kpiOut, err := client.GetCampaignDateRangeKpi(t.Context(), &pinpointsdk.GetCampaignDateRangeKpiInput{
			ApplicationId: aws.String(appID),
			CampaignId:    aws.String(campID),
			KpiName:       aws.String("email-open-rate"),
		})
		require.NoError(t, err)
		assert.Equal(t, campID, aws.ToString(kpiOut.CampaignDateRangeKpiResponse.CampaignId))

		delOut, err := client.DeleteCampaign(t.Context(), &pinpointsdk.DeleteCampaignInput{
			ApplicationId: aws.String(appID),
			CampaignId:    aws.String(campID),
		})
		require.NoError(t, err)
		assert.Equal(t, campID, aws.ToString(delOut.CampaignResponse.Id))
	})

	t.Run("segments", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("s7-seg-app")},
		})
		require.NoError(t, err)
		appID := aws.ToString(appOut.ApplicationResponse.Id)

		createOut, err := client.CreateSegment(t.Context(), &pinpointsdk.CreateSegmentInput{
			ApplicationId:       aws.String(appID),
			WriteSegmentRequest: &types.WriteSegmentRequest{Name: aws.String("s7-segment")},
		})
		require.NoError(t, err)
		segID := aws.ToString(createOut.SegmentResponse.Id)

		getOut, err := client.GetSegment(t.Context(), &pinpointsdk.GetSegmentInput{
			ApplicationId: aws.String(appID),
			SegmentId:     aws.String(segID),
		})
		require.NoError(t, err)
		assert.Equal(t, "s7-segment", aws.ToString(getOut.SegmentResponse.Name))

		listOut, err := client.GetSegments(t.Context(), &pinpointsdk.GetSegmentsInput{
			ApplicationId: aws.String(appID),
		})
		require.NoError(t, err)
		require.Len(t, listOut.SegmentsResponse.Item, 1)

		verOut, err := client.GetSegmentVersion(t.Context(), &pinpointsdk.GetSegmentVersionInput{
			ApplicationId: aws.String(appID),
			SegmentId:     aws.String(segID),
			Version:       aws.String("1"),
		})
		require.NoError(t, err)
		assert.Equal(t, segID, aws.ToString(verOut.SegmentResponse.Id))

		versionsOut, err := client.GetSegmentVersions(t.Context(), &pinpointsdk.GetSegmentVersionsInput{
			ApplicationId: aws.String(appID),
			SegmentId:     aws.String(segID),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, versionsOut.SegmentsResponse.Item)

		updOut, err := client.UpdateSegment(t.Context(), &pinpointsdk.UpdateSegmentInput{
			ApplicationId:       aws.String(appID),
			SegmentId:           aws.String(segID),
			WriteSegmentRequest: &types.WriteSegmentRequest{Name: aws.String("s7-segment-renamed")},
		})
		require.NoError(t, err)
		assert.Equal(t, "s7-segment-renamed", aws.ToString(updOut.SegmentResponse.Name))

		expOut, err := client.GetSegmentExportJobs(t.Context(), &pinpointsdk.GetSegmentExportJobsInput{
			ApplicationId: aws.String(appID),
			SegmentId:     aws.String(segID),
		})
		require.NoError(t, err)
		assert.NotNil(t, expOut.ExportJobsResponse.Item)

		impOut, err := client.GetSegmentImportJobs(t.Context(), &pinpointsdk.GetSegmentImportJobsInput{
			ApplicationId: aws.String(appID),
			SegmentId:     aws.String(segID),
		})
		require.NoError(t, err)
		assert.NotNil(t, impOut.ImportJobsResponse.Item)

		delOut, err := client.DeleteSegment(t.Context(), &pinpointsdk.DeleteSegmentInput{
			ApplicationId: aws.String(appID),
			SegmentId:     aws.String(segID),
		})
		require.NoError(t, err)
		assert.Equal(t, segID, aws.ToString(delOut.SegmentResponse.Id))
	})

	t.Run("endpoints", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("s7-ep-app")},
		})
		require.NoError(t, err)
		appID := aws.ToString(appOut.ApplicationResponse.Id)

		_, err = client.UpdateEndpoint(t.Context(), &pinpointsdk.UpdateEndpointInput{
			ApplicationId: aws.String(appID),
			EndpointId:    aws.String("s7-endpoint"),
			EndpointRequest: &types.EndpointRequest{
				Address:        aws.String("+12065550100"),
				ChannelType:    types.ChannelTypeSms,
				OptOut:         aws.String("NONE"),
				Attributes:     map[string][]string{"Interests": {"golf"}},
				EndpointStatus: aws.String("ACTIVE"),
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetEndpoint(t.Context(), &pinpointsdk.GetEndpointInput{
			ApplicationId: aws.String(appID),
			EndpointId:    aws.String("s7-endpoint"),
		})
		require.NoError(t, err)
		assert.Equal(t, "+12065550100", aws.ToString(getOut.EndpointResponse.Address))
		assert.Equal(t, types.ChannelTypeSms, getOut.EndpointResponse.ChannelType)

		batchOut, err := client.UpdateEndpointsBatch(t.Context(), &pinpointsdk.UpdateEndpointsBatchInput{
			ApplicationId: aws.String(appID),
			EndpointBatchRequest: &types.EndpointBatchRequest{
				Item: []types.EndpointBatchItem{
					{
						Id:          aws.String("s7-endpoint-2"),
						Address:     aws.String("+12065550111"),
						ChannelType: types.ChannelTypeSms,
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, batchOut.MessageBody)

		removeOut, err := client.RemoveAttributes(t.Context(), &pinpointsdk.RemoveAttributesInput{
			ApplicationId: aws.String(appID),
			AttributeType: aws.String("endpoint-custom-attributes"),
			UpdateAttributesRequest: &types.UpdateAttributesRequest{
				Blacklist: []string{"Interests"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, removeOut.AttributesResource)
		assert.Equal(t, appID, aws.ToString(removeOut.AttributesResource.ApplicationId))

		delOut, err := client.DeleteEndpoint(t.Context(), &pinpointsdk.DeleteEndpointInput{
			ApplicationId: aws.String(appID),
			EndpointId:    aws.String("s7-endpoint"),
		})
		require.NoError(t, err)
		assert.Equal(t, "+12065550100", aws.ToString(delOut.EndpointResponse.Address))
	})

	t.Run("journeys", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("s7-journey-app")},
		})
		require.NoError(t, err)
		appID := aws.ToString(appOut.ApplicationResponse.Id)

		createOut, err := client.CreateJourney(t.Context(), &pinpointsdk.CreateJourneyInput{
			ApplicationId:       aws.String(appID),
			WriteJourneyRequest: &types.WriteJourneyRequest{Name: aws.String("s7-journey")},
		})
		require.NoError(t, err)
		journeyID := aws.ToString(createOut.JourneyResponse.Id)

		getOut, err := client.GetJourney(t.Context(), &pinpointsdk.GetJourneyInput{
			ApplicationId: aws.String(appID),
			JourneyId:     aws.String(journeyID),
		})
		require.NoError(t, err)
		assert.Equal(t, "s7-journey", aws.ToString(getOut.JourneyResponse.Name))

		listOut, err := client.ListJourneys(t.Context(), &pinpointsdk.ListJourneysInput{
			ApplicationId: aws.String(appID),
		})
		require.NoError(t, err)
		require.Len(t, listOut.JourneysResponse.Item, 1)

		metricsOut, err := client.GetJourneyRunExecutionMetrics(
			t.Context(),
			&pinpointsdk.GetJourneyRunExecutionMetricsInput{
				ApplicationId: aws.String(appID),
				JourneyId:     aws.String(journeyID),
				RunId:         aws.String("s7-run"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s7-run", aws.ToString(metricsOut.JourneyRunExecutionMetricsResponse.RunId))

		actMetricsOut, err := client.GetJourneyRunExecutionActivityMetrics(
			t.Context(),
			&pinpointsdk.GetJourneyRunExecutionActivityMetricsInput{
				ApplicationId:     aws.String(appID),
				JourneyId:         aws.String(journeyID),
				RunId:             aws.String("s7-run"),
				JourneyActivityId: aws.String("s7-activity"),
			},
		)
		require.NoError(t, err)
		assert.Equal(
			t, "s7-activity", aws.ToString(actMetricsOut.JourneyRunExecutionActivityMetricsResponse.JourneyActivityId),
		)

		delOut, err := client.DeleteJourney(t.Context(), &pinpointsdk.DeleteJourneyInput{
			ApplicationId: aws.String(appID),
			JourneyId:     aws.String(journeyID),
		})
		require.NoError(t, err)
		assert.Equal(t, journeyID, aws.ToString(delOut.JourneyResponse.Id))
	})

	t.Run("templates_email_sms", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		_, err := client.CreateEmailTemplate(t.Context(), &pinpointsdk.CreateEmailTemplateInput{
			TemplateName: aws.String("s7-email-tmpl"),
			EmailTemplateRequest: &types.EmailTemplateRequest{
				Subject:  aws.String("hello"),
				HtmlPart: aws.String("<p>hi</p>"),
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetEmailTemplate(t.Context(), &pinpointsdk.GetEmailTemplateInput{
			TemplateName: aws.String("s7-email-tmpl"),
		})
		require.NoError(t, err)
		assert.Equal(t, "hello", aws.ToString(getOut.EmailTemplateResponse.Subject))

		_, err = client.UpdateEmailTemplate(t.Context(), &pinpointsdk.UpdateEmailTemplateInput{
			TemplateName: aws.String("s7-email-tmpl"),
			EmailTemplateRequest: &types.EmailTemplateRequest{
				Subject:  aws.String("hello-v2"),
				HtmlPart: aws.String("<p>hi v2</p>"),
			},
		})
		require.NoError(t, err)

		getOut, err = client.GetEmailTemplate(t.Context(), &pinpointsdk.GetEmailTemplateInput{
			TemplateName: aws.String("s7-email-tmpl"),
		})
		require.NoError(t, err)
		assert.Equal(t, "hello-v2", aws.ToString(getOut.EmailTemplateResponse.Subject))

		_, err = client.CreateSmsTemplate(t.Context(), &pinpointsdk.CreateSmsTemplateInput{
			TemplateName:       aws.String("s7-sms-tmpl"),
			SMSTemplateRequest: &types.SMSTemplateRequest{Body: aws.String("hi there")},
		})
		require.NoError(t, err)

		smsOut, err := client.GetSmsTemplate(t.Context(), &pinpointsdk.GetSmsTemplateInput{
			TemplateName: aws.String("s7-sms-tmpl"),
		})
		require.NoError(t, err)
		assert.Equal(t, "hi there", aws.ToString(smsOut.SMSTemplateResponse.Body))

		_, err = client.UpdateSmsTemplate(t.Context(), &pinpointsdk.UpdateSmsTemplateInput{
			TemplateName:       aws.String("s7-sms-tmpl"),
			SMSTemplateRequest: &types.SMSTemplateRequest{Body: aws.String("hi there v2")},
			CreateNewVersion:   aws.Bool(true),
		})
		require.NoError(t, err)

		listOut, err := client.ListTemplates(t.Context(), &pinpointsdk.ListTemplatesInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, listOut.TemplatesResponse.Item)

		versionsOut, err := client.ListTemplateVersions(t.Context(), &pinpointsdk.ListTemplateVersionsInput{
			TemplateName: aws.String("s7-sms-tmpl"),
			TemplateType: aws.String("SMS"),
		})
		require.NoError(t, err)
		require.Len(t, versionsOut.TemplateVersionsResponse.Item, 2)

		_, err = client.UpdateTemplateActiveVersion(t.Context(), &pinpointsdk.UpdateTemplateActiveVersionInput{
			TemplateName:                 aws.String("s7-sms-tmpl"),
			TemplateType:                 aws.String("SMS"),
			TemplateActiveVersionRequest: &types.TemplateActiveVersionRequest{Version: aws.String("1")},
		})
		require.NoError(t, err)

		_, err = client.DeleteEmailTemplate(t.Context(), &pinpointsdk.DeleteEmailTemplateInput{
			TemplateName: aws.String("s7-email-tmpl"),
		})
		require.NoError(t, err)

		_, err = client.DeleteSmsTemplate(t.Context(), &pinpointsdk.DeleteSmsTemplateInput{
			TemplateName: aws.String("s7-sms-tmpl"),
		})
		require.NoError(t, err)
	})

	t.Run("templates_push_voice_inapp", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		_, err := client.CreatePushTemplate(t.Context(), &pinpointsdk.CreatePushTemplateInput{
			TemplateName: aws.String("s7-push-tmpl"),
			PushNotificationTemplateRequest: &types.PushNotificationTemplateRequest{
				Default: &types.DefaultPushNotificationTemplate{Body: aws.String("push body")},
			},
		})
		require.NoError(t, err)

		pushOut, err := client.GetPushTemplate(t.Context(), &pinpointsdk.GetPushTemplateInput{
			TemplateName: aws.String("s7-push-tmpl"),
		})
		require.NoError(t, err)
		require.NotNil(t, pushOut.PushNotificationTemplateResponse.Default)
		assert.Equal(t, "push body", aws.ToString(pushOut.PushNotificationTemplateResponse.Default.Body))

		_, err = client.UpdatePushTemplate(t.Context(), &pinpointsdk.UpdatePushTemplateInput{
			TemplateName: aws.String("s7-push-tmpl"),
			PushNotificationTemplateRequest: &types.PushNotificationTemplateRequest{
				Default: &types.DefaultPushNotificationTemplate{Body: aws.String("push body v2")},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateVoiceTemplate(t.Context(), &pinpointsdk.CreateVoiceTemplateInput{
			TemplateName:         aws.String("s7-voice-tmpl"),
			VoiceTemplateRequest: &types.VoiceTemplateRequest{Body: aws.String("voice body")},
		})
		require.NoError(t, err)

		voiceOut, err := client.GetVoiceTemplate(t.Context(), &pinpointsdk.GetVoiceTemplateInput{
			TemplateName: aws.String("s7-voice-tmpl"),
		})
		require.NoError(t, err)
		assert.Equal(t, "voice body", aws.ToString(voiceOut.VoiceTemplateResponse.Body))

		_, err = client.UpdateVoiceTemplate(t.Context(), &pinpointsdk.UpdateVoiceTemplateInput{
			TemplateName:         aws.String("s7-voice-tmpl"),
			VoiceTemplateRequest: &types.VoiceTemplateRequest{Body: aws.String("voice body v2")},
		})
		require.NoError(t, err)

		_, err = client.CreateInAppTemplate(t.Context(), &pinpointsdk.CreateInAppTemplateInput{
			TemplateName: aws.String("s7-inapp-tmpl"),
			InAppTemplateRequest: &types.InAppTemplateRequest{
				Layout: types.LayoutBottomBanner,
			},
		})
		require.NoError(t, err)

		inAppOut, err := client.GetInAppTemplate(t.Context(), &pinpointsdk.GetInAppTemplateInput{
			TemplateName: aws.String("s7-inapp-tmpl"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.LayoutBottomBanner, inAppOut.InAppTemplateResponse.Layout)

		_, err = client.UpdateInAppTemplate(t.Context(), &pinpointsdk.UpdateInAppTemplateInput{
			TemplateName:         aws.String("s7-inapp-tmpl"),
			InAppTemplateRequest: &types.InAppTemplateRequest{Layout: types.LayoutMiddleBanner},
		})
		require.NoError(t, err)

		_, err = client.DeletePushTemplate(t.Context(), &pinpointsdk.DeletePushTemplateInput{
			TemplateName: aws.String("s7-push-tmpl"),
		})
		require.NoError(t, err)

		_, err = client.DeleteVoiceTemplate(t.Context(), &pinpointsdk.DeleteVoiceTemplateInput{
			TemplateName: aws.String("s7-voice-tmpl"),
		})
		require.NoError(t, err)

		_, err = client.DeleteInAppTemplate(t.Context(), &pinpointsdk.DeleteInAppTemplateInput{
			TemplateName: aws.String("s7-inapp-tmpl"),
		})
		require.NoError(t, err)
	})

	t.Run("channels", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("s7-channels-app")},
		})
		require.NoError(t, err)
		appID := aws.ToString(appOut.ApplicationResponse.Id)

		_, err = client.UpdateEmailChannel(t.Context(), &pinpointsdk.UpdateEmailChannelInput{
			ApplicationId: aws.String(appID),
			EmailChannelRequest: &types.EmailChannelRequest{
				Enabled:     aws.Bool(true),
				FromAddress: aws.String("a@b.com"),
				Identity:    aws.String("arn:aws:ses:us-east-1:123456789012:identity/example.com"),
			},
		})
		require.NoError(t, err)

		emailOut, err := client.GetEmailChannel(
			t.Context(), &pinpointsdk.GetEmailChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.Equal(t, "a@b.com", aws.ToString(emailOut.EmailChannelResponse.FromAddress))

		channelsOut, err := client.GetChannels(
			t.Context(), &pinpointsdk.GetChannelsInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		require.NotNil(t, channelsOut.ChannelsResponse.Channels)
		emailChannel, ok := channelsOut.ChannelsResponse.Channels["EMAIL"]
		require.True(t, ok, "GetChannels must include the EMAIL channel")
		assert.True(t, aws.ToBool(emailChannel.Enabled))

		_, err = client.UpdateSmsChannel(t.Context(), &pinpointsdk.UpdateSmsChannelInput{
			ApplicationId:     aws.String(appID),
			SMSChannelRequest: &types.SMSChannelRequest{Enabled: aws.Bool(true), SenderId: aws.String("MYAPP")},
		})
		require.NoError(t, err)

		smsOut, err := client.GetSmsChannel(
			t.Context(),
			&pinpointsdk.GetSmsChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.Equal(t, "MYAPP", aws.ToString(smsOut.SMSChannelResponse.SenderId))

		_, err = client.UpdateVoiceChannel(t.Context(), &pinpointsdk.UpdateVoiceChannelInput{
			ApplicationId:       aws.String(appID),
			VoiceChannelRequest: &types.VoiceChannelRequest{Enabled: aws.Bool(true)},
		})
		require.NoError(t, err)

		voiceOut, err := client.GetVoiceChannel(
			t.Context(),
			&pinpointsdk.GetVoiceChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(voiceOut.VoiceChannelResponse.Enabled))

		_, err = client.UpdateApnsSandboxChannel(t.Context(), &pinpointsdk.UpdateApnsSandboxChannelInput{
			ApplicationId: aws.String(appID),
			APNSSandboxChannelRequest: &types.APNSSandboxChannelRequest{
				Enabled: aws.Bool(true), BundleId: aws.String("com.s7.app"),
			},
		})
		require.NoError(t, err)

		apnsSandboxOut, err := client.GetApnsSandboxChannel(
			t.Context(), &pinpointsdk.GetApnsSandboxChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(apnsSandboxOut.APNSSandboxChannelResponse.Enabled))

		_, err = client.UpdateApnsVoipChannel(t.Context(), &pinpointsdk.UpdateApnsVoipChannelInput{
			ApplicationId: aws.String(appID),
			APNSVoipChannelRequest: &types.APNSVoipChannelRequest{
				Enabled: aws.Bool(true), BundleId: aws.String("com.s7.voip"),
			},
		})
		require.NoError(t, err)

		apnsVoipOut, err := client.GetApnsVoipChannel(
			t.Context(), &pinpointsdk.GetApnsVoipChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(apnsVoipOut.APNSVoipChannelResponse.Enabled))

		_, err = client.UpdateApnsVoipSandboxChannel(t.Context(), &pinpointsdk.UpdateApnsVoipSandboxChannelInput{
			ApplicationId: aws.String(appID),
			APNSVoipSandboxChannelRequest: &types.APNSVoipSandboxChannelRequest{
				Enabled: aws.Bool(true), BundleId: aws.String("com.s7.voipsandbox"),
			},
		})
		require.NoError(t, err)

		apnsVoipSandboxOut, err := client.GetApnsVoipSandboxChannel(
			t.Context(), &pinpointsdk.GetApnsVoipSandboxChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(apnsVoipSandboxOut.APNSVoipSandboxChannelResponse.Enabled))

		_, err = client.DeleteApnsSandboxChannel(
			t.Context(), &pinpointsdk.DeleteApnsSandboxChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteApnsVoipChannel(
			t.Context(), &pinpointsdk.DeleteApnsVoipChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteApnsVoipSandboxChannel(
			t.Context(), &pinpointsdk.DeleteApnsVoipSandboxChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteApnsChannel(
			t.Context(), &pinpointsdk.DeleteApnsChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteGcmChannel(
			t.Context(), &pinpointsdk.DeleteGcmChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteAdmChannel(
			t.Context(), &pinpointsdk.DeleteAdmChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteBaiduChannel(
			t.Context(), &pinpointsdk.DeleteBaiduChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteEmailChannel(
			t.Context(), &pinpointsdk.DeleteEmailChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteSmsChannel(
			t.Context(), &pinpointsdk.DeleteSmsChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		_, err = client.DeleteVoiceChannel(
			t.Context(), &pinpointsdk.DeleteVoiceChannelInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
	})

	t.Run("export_import_jobs", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("s7-jobs-app")},
		})
		require.NoError(t, err)
		appID := aws.ToString(appOut.ApplicationResponse.Id)

		_, err = client.CreateExportJob(t.Context(), &pinpointsdk.CreateExportJobInput{
			ApplicationId: aws.String(appID),
			ExportJobRequest: &types.ExportJobRequest{
				RoleArn:     aws.String("arn:aws:iam::123456789012:role/export"),
				S3UrlPrefix: aws.String("s3://s7-bucket/export"),
			},
		})
		require.NoError(t, err)

		expJobsOut, err := client.GetExportJobs(
			t.Context(),
			&pinpointsdk.GetExportJobsInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		require.Len(t, expJobsOut.ExportJobsResponse.Item, 1)
		assert.Equal(
			t,
			"s3://s7-bucket/export",
			aws.ToString(expJobsOut.ExportJobsResponse.Item[0].Definition.S3UrlPrefix),
		)

		_, err = client.CreateImportJob(t.Context(), &pinpointsdk.CreateImportJobInput{
			ApplicationId: aws.String(appID),
			ImportJobRequest: &types.ImportJobRequest{
				RoleArn: aws.String("arn:aws:iam::123456789012:role/import"),
				S3Url:   aws.String("s3://s7-bucket/import.csv"),
				Format:  types.FormatCsv,
			},
		})
		require.NoError(t, err)

		impJobsOut, err := client.GetImportJobs(
			t.Context(),
			&pinpointsdk.GetImportJobsInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		require.Len(t, impJobsOut.ImportJobsResponse.Item, 1)
		assert.Equal(
			t,
			"s3://s7-bucket/import.csv",
			aws.ToString(impJobsOut.ImportJobsResponse.Item[0].Definition.S3Url),
		)
	})

	t.Run("event_streams", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
			CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("s7-events-app")},
		})
		require.NoError(t, err)
		appID := aws.ToString(appOut.ApplicationResponse.Id)

		putOut, err := client.PutEventStream(t.Context(), &pinpointsdk.PutEventStreamInput{
			ApplicationId: aws.String(appID),
			WriteEventStream: &types.WriteEventStream{
				DestinationStreamArn: aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/s7-stream"),
				RoleArn:              aws.String("arn:aws:iam::123456789012:role/event-stream"),
			},
		})
		require.NoError(t, err)
		assert.Equal(
			t,
			"arn:aws:kinesis:us-east-1:123456789012:stream/s7-stream",
			aws.ToString(putOut.EventStream.DestinationStreamArn),
		)

		getOut, err := client.GetEventStream(
			t.Context(),
			&pinpointsdk.GetEventStreamInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.Equal(t, appID, aws.ToString(getOut.EventStream.ApplicationId))

		delOut, err := client.DeleteEventStream(
			t.Context(),
			&pinpointsdk.DeleteEventStreamInput{ApplicationId: aws.String(appID)},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"arn:aws:kinesis:us-east-1:123456789012:stream/s7-stream",
			aws.ToString(delOut.EventStream.DestinationStreamArn),
		)
	})

	t.Run("recommender_configurations", func(t *testing.T) {
		t.Parallel()

		h := newHandlerForTest(t)
		client := newTestPinpointClient(t, h)

		createOut, err := client.CreateRecommenderConfiguration(
			t.Context(),
			&pinpointsdk.CreateRecommenderConfigurationInput{
				CreateRecommenderConfiguration: &types.CreateRecommenderConfigurationShape{
					RecommendationProviderRoleArn: aws.String("arn:aws:iam::123456789012:role/recommender"),
					RecommendationProviderUri:     aws.String("arn:aws:personalize:us-east-1:123456789012:campaign/s7"),
				},
			},
		)
		require.NoError(t, err)
		recID := aws.ToString(createOut.RecommenderConfigurationResponse.Id)

		getOut, err := client.GetRecommenderConfiguration(
			t.Context(), &pinpointsdk.GetRecommenderConfigurationInput{RecommenderId: aws.String(recID)},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"arn:aws:personalize:us-east-1:123456789012:campaign/s7",
			aws.ToString(getOut.RecommenderConfigurationResponse.RecommendationProviderUri),
		)

		listOut, err := client.GetRecommenderConfigurations(
			t.Context(), &pinpointsdk.GetRecommenderConfigurationsInput{},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, listOut.ListRecommenderConfigurationsResponse.Item)

		updOut, err := client.UpdateRecommenderConfiguration(
			t.Context(),
			&pinpointsdk.UpdateRecommenderConfigurationInput{
				RecommenderId: aws.String(recID),
				UpdateRecommenderConfiguration: &types.UpdateRecommenderConfigurationShape{
					RecommendationProviderRoleArn: aws.String("arn:aws:iam::123456789012:role/recommender-v2"),
					RecommendationProviderUri:     aws.String("arn:aws:personalize:us-east-1:123456789012:campaign/s7"),
				},
			},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"arn:aws:iam::123456789012:role/recommender-v2",
			aws.ToString(updOut.RecommenderConfigurationResponse.RecommendationProviderRoleArn),
		)

		delOut, err := client.DeleteRecommenderConfiguration(
			t.Context(), &pinpointsdk.DeleteRecommenderConfigurationInput{RecommenderId: aws.String(recID)},
		)
		require.NoError(t, err)
		assert.Equal(t, recID, aws.ToString(delOut.RecommenderConfigurationResponse.Id))
	})
}
