package pinpoint

import (
	"encoding/json"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) extractChannelOp(method, channelType string) string {
	if channelType == "" {
		return unknownOperation
	}

	switch method {
	case http.MethodGet:
		return "Get" + channelTypeOpName(channelType) + "Channel"
	case http.MethodPut:
		return "Update" + channelTypeOpName(channelType) + "Channel"
	case http.MethodDelete:
		return "Delete" + channelTypeOpName(channelType) + "Channel"
	}

	return unknownOperation
}

// channelTypeOpName converts a URL channel type segment to the AWS op suffix.
// e.g. "adm" → "Adm", "apns" → "Apns", "apns_sandbox" → "ApnsSandbox".
func channelTypeOpName(channelType string) string {
	switch strings.ToLower(channelType) {
	case "adm":
		return "Adm"
	case "apns":
		return "Apns"
	case "apns_sandbox":
		return "ApnsSandbox"
	case "apns_voip":
		return "ApnsVoip"
	case "apns_voip_sandbox":
		return "ApnsVoipSandbox"
	case "baidu":
		return "Baidu"
	case templateTypeEmail:
		return "Email"
	case "gcm":
		return "Gcm"
	case templateTypeSMS:
		return "Sms"
	case templateTypeVoice:
		return "Voice"
	}

	return channelType
}

func (h *Handler) dispatchChannelByType(c *echo.Context, appID, channelType string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetChannel(c, appID, channelType)
	case http.MethodPut:
		return h.handleUpdateChannel(c, appID, channelType)
	case http.MethodDelete:
		return h.handleDeleteChannel(c, appID, channelType)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

// toChannelResponse converts a Channel to its wire format including per-type extra fields.
func toChannelResponse(ch *Channel) map[string]any {
	resp := map[string]any{
		"ApplicationId":    ch.ApplicationID,
		"ChannelType":      ch.ChannelType,
		"Platform":         ch.Platform,
		"Enabled":          ch.Enabled,
		"IsArchived":       ch.IsArchived,
		"Version":          ch.Version,
		"CreationDate":     ch.CreationDate,
		"LastModifiedDate": ch.LastModifiedDate,
	}

	if ch.HasCredential {
		resp["HasCredential"] = true
	}

	if ch.HasTokenKey {
		resp["HasTokenKey"] = true
	}

	if ch.MessagesPerSecond > 0 {
		resp["MessagesPerSecond"] = ch.MessagesPerSecond
	}

	maps.Copy(resp, ch.ExtraData)

	return resp
}

// handleGetChannel handles GET /v1/apps/{appId}/channels/{channelType}.
func (h *Handler) handleGetChannel(c *echo.Context, appID, channelType string) error {
	ch := h.Backend.GetChannel(appID, channelType)
	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toChannelResponse(ch))

	return nil
}

// handleGetChannels handles GET /v1/apps/{appId}/channels.
func (h *Handler) handleGetChannels(c *echo.Context, appID string) error {
	channels := h.Backend.GetAllChannels(appID)
	chMap := make(map[string]map[string]any, len(channels))

	for _, ch := range channels {
		chMap[ch.ChannelType] = toChannelResponse(ch)
	}

	return c.JSON(http.StatusOK, map[string]any{"Channels": chMap})
}

func parseGCMChannelExtra(body []byte) (bool, map[string]any) {
	var req updateGCMChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return false, nil
	}

	extra := map[string]any{"DefaultAuthenticationMethod": req.DefaultAuthenticationMethod}

	if req.APIKey != "" {
		extra["ApiKey"] = req.APIKey
	}

	if req.ServiceJSON != "" {
		extra["ServiceJson"] = req.ServiceJSON
	}

	return req.Enabled, extra
}

func parseAPNSChannelExtra(body []byte) (bool, map[string]any) {
	var req updateAPNSChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return false, nil
	}

	extra := map[string]any{"DefaultAuthenticationMethod": req.DefaultAuthenticationMethod}

	for k, v := range map[string]string{
		"BundleId": req.BundleID, "Certificate": req.Certificate,
		"TeamId": req.TeamID, "TokenKey": req.TokenKey, "TokenKeyId": req.TokenKeyID,
	} {
		if v != "" {
			extra[k] = v
		}
	}

	return req.Enabled, extra
}

func parseEmailChannelExtra(body []byte) (bool, map[string]any) {
	var req updateEmailChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return false, nil
	}

	extra := map[string]any{}

	for k, v := range map[string]string{
		"FromAddress": req.FromAddress, "Identity": req.Identity,
		"RoleArn": req.RoleArn, "ConfigurationSet": req.ConfigurationSet,
		"OrchestrationSendingRoleArn": req.OrchestrationSendingRoleArn,
	} {
		if v != "" {
			extra[k] = v
		}
	}

	return req.Enabled, extra
}

func parseSMSChannelExtra(body []byte) (bool, map[string]any) {
	var req updateSMSChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return false, nil
	}

	extra := map[string]any{}

	if req.SenderID != "" {
		extra["SenderId"] = req.SenderID
	}

	if req.ShortCode != "" {
		extra["ShortCode"] = req.ShortCode
	}

	return req.Enabled, extra
}

// parseChannelExtra extracts per-channel extra fields from the request body.
func parseChannelExtra(channelType string, body []byte) (bool, map[string]any) {
	switch strings.ToLower(channelType) {
	case "gcm":
		return parseGCMChannelExtra(body)
	case "apns", "apns_sandbox", "apns_voip", "apns_voip_sandbox":
		return parseAPNSChannelExtra(body)
	case "email":
		return parseEmailChannelExtra(body)
	case "sms":
		return parseSMSChannelExtra(body)
	case "adm":
		var req updateADMChannelRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return false, nil
		}

		extra := map[string]any{}

		if req.ClientID != "" {
			extra["ClientId"] = req.ClientID
		}

		if req.ClientSecret != "" {
			extra["ClientSecret"] = req.ClientSecret
		}

		return req.Enabled, extra
	case "baidu":
		var req updateBaiduChannelRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return false, nil
		}

		extra := map[string]any{}

		if req.APIKey != "" {
			extra["ApiKey"] = req.APIKey
		}

		if req.SecretKey != "" {
			extra["SecretKey"] = req.SecretKey
		}

		return req.Enabled, extra
	default:
		var req updateChannelRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return false, nil
		}

		return req.Enabled, nil
	}
}

// handleUpdateChannel handles PUT /v1/apps/{appId}/channels/{channelType}.
func (h *Handler) handleUpdateChannel(c *echo.Context, appID, channelType string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	enabled, extra := parseChannelExtra(channelType, body)
	ch := h.Backend.UpsertChannel(appID, channelType, enabled, extra)
	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toChannelResponse(ch))

	return nil
}

// handleDeleteChannel handles DELETE /v1/apps/{appId}/channels/{channelType}.
func (h *Handler) handleDeleteChannel(c *echo.Context, appID, channelType string) error {
	ch := h.Backend.DeleteChannel(appID, channelType)
	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toChannelResponse(ch))

	return nil
}

// ──────────────────────────────────────────────────
// Campaign handlers
// ──────────────────────────────────────────────────
