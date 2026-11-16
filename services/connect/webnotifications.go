package connect

import (
	"context"
	"encoding/json/jsontext"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	labelWebNotification = "OutboundWebNotification"
	maxBrowserSessionLen = 36
	webNotifRecorded     = "RECORDED"
)

type webNotificationRec struct {
	Sent              awstime.EpochTime `json:"sent"`
	ExpiresAt         awstime.EpochTime `json:"expiresAt"`
	ID                string            `json:"id"`
	InstanceID        string            `json:"instanceId"`
	BrowserID         string            `json:"browserId"`
	SessionID         string            `json:"sessionId"`
	Type              string            `json:"type"`
	ViewArn           string            `json:"viewArn,omitempty"`
	WidgetID          string            `json:"widgetId"`
	ProfileID         string            `json:"profileId"`
	CampaignID        string            `json:"campaignId"`
	OutboundRequestID string            `json:"outboundRequestId"`
	Status            string            `json:"status"`
	Attributes        jsontext.Value    `json:"attributes,omitempty"`
}

// SendOutboundWebNotificationInput is the SendOutboundWebNotification request.
type SendOutboundWebNotificationInput struct {
	Content *struct {
		Type       string         `json:"Type"`
		ViewArn    string         `json:"ViewArn"`
		Attributes jsontext.Value `json:"Attributes"`
	} `json:"Content"`
	Destination *struct {
		ProfileID string `json:"ProfileId"`
		WidgetID  string `json:"WidgetId"`
	} `json:"Destination"`
	Source *struct {
		SourceCampaign *struct {
			CampaignID        string `json:"CampaignId"`
			OutboundRequestID string `json:"OutboundRequestId"`
		} `json:"SourceCampaign"`
	} `json:"Source"`
	ExpiresAt   awstime.EpochTime `json:"ExpiresAt"`
	BrowserID   string            `json:"BrowserId"`
	ClientToken string            `json:"ClientToken"`
	SessionID   string            `json:"SessionId"`
	instPath
}

func (in *SendOutboundWebNotificationInput) validate() error {
	if in.Content == nil || in.Destination == nil || in.Source == nil || in.Source.SourceCampaign == nil {
		return invalidRequestErr("Content, Destination and Source.SourceCampaign are required")
	}

	for name, v := range map[string]string{"BrowserId": in.BrowserID, "SessionId": in.SessionID} {
		if v == "" || len(v) > maxBrowserSessionLen {
			return invalidRequestErr("%s must be 1-%d characters", name, maxBrowserSessionLen)
		}
	}

	if in.Content.Type != "WIDGET_VIEW" && in.Content.Type != "WIDGET_ACTION" {
		return invalidRequestErr("Content.Type must be WIDGET_VIEW or WIDGET_ACTION")
	}

	return firstError(
		requireAll("Destination.WidgetId, Destination.ProfileId", in.Destination.WidgetID, in.Destination.ProfileID),
		requireAll("SourceCampaign.CampaignId, SourceCampaign.OutboundRequestId",
			in.Source.SourceCampaign.CampaignID, in.Source.SourceCampaign.OutboundRequestID),
	)
}

func requireAll(names string, values ...string) error {
	if slices.Contains(values, "") {
		return invalidRequestErr("%s are required", names)
	}

	return nil
}

// SendOutboundWebNotification records a campaign web notification for a widget; no browser exists to receive it.
func (b *Backend) SendOutboundWebNotification(
	ctx context.Context,
	in *SendOutboundWebNotificationInput,
) (*EmptyOutput, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	return writeInstance(b, "SendOutboundWebNotification", in.InstanceID, func(inst string) (*EmptyOutput, error) {
		if !in.ExpiresAt.Time.After(b.now()) {
			return nil, invalidRequestErr("ExpiresAt must be in the future")
		}

		if in.Content.ViewArn != "" {
			if _, _, err := b.viewByRef(inst, in.Content.ViewArn); err != nil {
				return nil, err
			}
		}

		rec := &webNotificationRec{
			ID: newID(), InstanceID: inst, Sent: awstime.NewEpochTime(b.now()), ExpiresAt: in.ExpiresAt,
			Attributes: in.Content.Attributes, BrowserID: in.BrowserID, SessionID: in.SessionID,
			Type: in.Content.Type, ViewArn: in.Content.ViewArn, WidgetID: in.Destination.WidgetID,
			ProfileID: in.Destination.ProfileID, CampaignID: in.Source.SourceCampaign.CampaignID,
			OutboundRequestID: in.Source.SourceCampaign.OutboundRequestID, Status: webNotifRecorded,
		}
		b.webNotifications.put(rec)
		logger.Load(ctx).
			InfoContext(ctx, "connect: would deliver the web notification to the customer browser (no widget)",
				"widget", rec.WidgetID, "browser", rec.BrowserID, "campaign", rec.CampaignID)

		return &EmptyOutput{}, nil
	})
}
