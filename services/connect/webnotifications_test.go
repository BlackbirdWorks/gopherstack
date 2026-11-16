package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendOutboundWebNotification(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "webnotif"))

	view, err := f.client.CreateView(t.Context(), &sdk.CreateViewInput{
		InstanceId: inst, Name: aws.String("promo"), Status: types.ViewStatusPublished,
		Content: &types.ViewInputContent{Template: aws.String(viewTemplate), Actions: []string{"Go"}},
	})
	require.NoError(t, err)

	base := func() *sdk.SendOutboundWebNotificationInput {
		return &sdk.SendOutboundWebNotificationInput{
			InstanceId: inst, BrowserId: aws.String("browser-1"), SessionId: aws.String("session-1"),
			ExpiresAt: aws.Time(f.clock.now().Add(time.Hour)),
			Content:   &types.WebNotificationContent{Type: types.NotificationTypeWidgetView, ViewArn: view.View.Arn},
			Destination: &types.WidgetDestination{
				WidgetId: aws.String("widget-1"), ProfileId: aws.String("profile-1"),
			},
			Source: &types.WebNotificationSource{SourceCampaign: &types.SourceCampaign{
				CampaignId: aws.String("camp-1"), OutboundRequestId: aws.String("req-1"),
			}},
		}
	}

	tests := []struct {
		mut  func(*sdk.SendOutboundWebNotificationInput)
		name string
		code string
	}{
		{name: "ok", mut: func(*sdk.SendOutboundWebNotificationInput) {}},
		{name: "noview", mut: func(in *sdk.SendOutboundWebNotificationInput) { in.Content.ViewArn = nil }},
		{name: "expired", code: "InvalidRequestException", mut: func(in *sdk.SendOutboundWebNotificationInput) {
			in.ExpiresAt = aws.Time(f.clock.now().Add(-time.Minute))
		}},
		{name: "unknownview", code: "ResourceNotFoundException", mut: func(in *sdk.SendOutboundWebNotificationInput) {
			in.Content.ViewArn = aws.String("arn:aws:connect:us-east-1:123456789012:instance/x/view/ghost")
		}},
		{name: "longbrowser", code: "InvalidRequestException", mut: func(in *sdk.SendOutboundWebNotificationInput) {
			in.BrowserId = aws.String("0123456789012345678901234567890123456")
		}},
		{name: "badtype", code: "InvalidRequestException", mut: func(in *sdk.SendOutboundWebNotificationInput) {
			in.Content.Type = "SMS"
		}},
		{name: "noinstance", code: "ResourceNotFoundException", mut: func(in *sdk.SendOutboundWebNotificationInput) {
			in.InstanceId = aws.String("ghost")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := base()
			tt.mut(in)
			_, serr := f.client.SendOutboundWebNotification(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(serr))
		})
	}

	t.Run("persisted", func(t *testing.T) {
		t.Parallel()

		_, serr := f.client.SendOutboundWebNotification(t.Context(), base())
		require.NoError(t, serr)
		assert.Contains(t, string(f.handler.Snapshot(t.Context())), "webNotifications")
		assert.Contains(t, string(f.handler.Snapshot(t.Context())), "widget-1")
	})
}
