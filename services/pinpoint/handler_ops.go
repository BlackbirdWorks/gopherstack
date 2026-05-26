package pinpoint

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// ──────────────────────────────────────────────────
// Channel handlers
// ──────────────────────────────────────────────────

// toChannelResponse converts a Channel to its wire format.
func toChannelResponse(ch *Channel) channelResponse {
	return channelResponse{
		ApplicationID:     ch.ApplicationID,
		ChannelType:       ch.ChannelType,
		Platform:          ch.Platform,
		Enabled:           ch.Enabled,
		IsArchived:        ch.IsArchived,
		HasCredential:     ch.HasCredential,
		HasTokenKey:       ch.HasTokenKey,
		Version:           ch.Version,
		CreationDate:      ch.CreationDate,
		LastModifiedDate:  ch.LastModifiedDate,
		MessagesPerSecond: ch.MessagesPerSecond,
	}
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
	resp := channelsResponse{Channels: make(map[string]channelResponse)}

	for _, ch := range channels {
		resp.Channels[ch.ChannelType] = toChannelResponse(ch)
	}

	return c.JSON(http.StatusOK, resp)
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

	extra := map[string]any{"DefaultAuthMethod": req.DefaultAuthMethod}

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

// handleGetCampaign handles GET /v1/apps/{appId}/campaigns/{campaignId}.
func (h *Handler) handleGetCampaign(c *echo.Context, appID, campaignID string) error {
	campaign, err := h.Backend.GetCampaign(appID, campaignID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	resp := toCampaignResponse(campaign)
	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetCampaigns handles GET /v1/apps/{appId}/campaigns.
func (h *Handler) handleGetCampaigns(c *echo.Context, appID string) error {
	campaigns, err := h.Backend.GetCampaigns(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]campaignResponse, 0, len(campaigns))

	for _, c2 := range campaigns {
		items = append(items, toCampaignResponse(c2))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, campaignsListResponse{Item: items})

	return nil
}

// unmarshalBody reads the HTTP request body and unmarshals it into dst.
// If reading or parsing fails, it writes an error response to c and returns false.
func unmarshalBody(c *echo.Context, dst any) bool {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		_ = writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")

		return false
	}

	if jsonErr := json.Unmarshal(body, dst); jsonErr != nil {
		_ = writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")

		return false
	}

	return true
}

// handleUpdateCampaign handles PUT /v1/apps/{appId}/campaigns/{campaignId}.
func (h *Handler) handleUpdateCampaign(c *echo.Context, appID, campaignID string) error {
	var req updateCampaignRequest
	if !unmarshalBody(c, &req) {
		return nil
	}

	campaign, backendErr := h.Backend.UpdateCampaign(appID, campaignID, req)
	if backendErr != nil {
		return writeNotFoundOrInternal(c, backendErr)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toCampaignResponse(campaign))

	return nil
}

// handleDeleteCampaign handles DELETE /v1/apps/{appId}/campaigns/{campaignId}.
func (h *Handler) handleDeleteCampaign(c *echo.Context, appID, campaignID string) error {
	campaign, err := h.Backend.DeleteCampaign(appID, campaignID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toCampaignResponse(campaign))

	return nil
}

// handleGetCampaignActivities handles GET /v1/apps/{appId}/campaigns/{campaignId}/activities.
func (h *Handler) handleGetCampaignActivities(c *echo.Context, appID, campaignID string) error {
	resp, err := h.Backend.GetCampaignActivities(appID, campaignID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetCampaignDateRangeKpi handles GET /v1/apps/{appId}/campaigns/{campaignId}/kpis/daterange/{kpiName}.
func (h *Handler) handleGetCampaignDateRangeKpi(c *echo.Context, appID, campaignID, kpiName string) error {
	resp, err := h.Backend.GetCampaignDateRangeKpi(appID, campaignID, kpiName)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetCampaignVersion handles GET /v1/apps/{appId}/campaigns/{campaignId}/versions/{version}.
func (h *Handler) handleGetCampaignVersion(c *echo.Context, appID, campaignID string, version int) error {
	campaign, err := h.Backend.GetCampaignVersion(appID, campaignID, version)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toCampaignResponse(campaign))

	return nil
}

// handleGetCampaignVersions handles GET /v1/apps/{appId}/campaigns/{campaignId}/versions.
func (h *Handler) handleGetCampaignVersions(c *echo.Context, appID, campaignID string) error {
	campaigns, err := h.Backend.GetCampaignVersions(appID, campaignID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]campaignResponse, 0, len(campaigns))

	for _, c2 := range campaigns {
		items = append(items, toCampaignResponse(c2))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, campaignVersionsResponse{Item: items})

	return nil
}

// ──────────────────────────────────────────────────
// Segment handlers
// ──────────────────────────────────────────────────

// handleGetSegment handles GET /v1/apps/{appId}/segments/{segmentId}.
func (h *Handler) handleGetSegment(c *echo.Context, appID, segmentID string) error {
	segment, err := h.Backend.GetSegment(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleGetSegments handles GET /v1/apps/{appId}/segments.
func (h *Handler) handleGetSegments(c *echo.Context, appID string) error {
	segments, err := h.Backend.GetSegments(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]segmentResponse, 0, len(segments))

	for _, s := range segments {
		items = append(items, toSegmentResponse(s))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, segmentsListResponse{Item: items})

	return nil
}

// handleUpdateSegment handles PUT /v1/apps/{appId}/segments/{segmentId}.
func (h *Handler) handleUpdateSegment(c *echo.Context, appID, segmentID string) error {
	var req updateSegmentRequest
	if !unmarshalBody(c, &req) {
		return nil
	}

	segment, backendErr := h.Backend.UpdateSegment(appID, segmentID, req)
	if backendErr != nil {
		return writeNotFoundOrInternal(c, backendErr)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleDeleteSegment handles DELETE /v1/apps/{appId}/segments/{segmentId}.
func (h *Handler) handleDeleteSegment(c *echo.Context, appID, segmentID string) error {
	segment, err := h.Backend.DeleteSegment(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleGetSegmentVersion handles GET /v1/apps/{appId}/segments/{segmentId}/versions/{version}.
func (h *Handler) handleGetSegmentVersion(c *echo.Context, appID, segmentID string, version int) error {
	segment, err := h.Backend.GetSegmentVersion(appID, segmentID, version)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleGetSegmentVersions handles GET /v1/apps/{appId}/segments/{segmentId}/versions.
func (h *Handler) handleGetSegmentVersions(c *echo.Context, appID, segmentID string) error {
	segments, err := h.Backend.GetSegmentVersions(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]segmentResponse, 0, len(segments))

	for _, s := range segments {
		items = append(items, toSegmentResponse(s))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, segmentVersionsResponse{Item: items})

	return nil
}

// handleGetSegmentExportJobs handles GET /v1/apps/{appId}/segments/{segmentId}/jobs/export.
func (h *Handler) handleGetSegmentExportJobs(c *echo.Context, appID, segmentID string) error {
	jobs, err := h.Backend.GetSegmentExportJobs(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]exportJobResponse, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, toExportJobResponse(j))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, exportJobsListResponse{Item: items})

	return nil
}

// handleGetSegmentImportJobs handles GET /v1/apps/{appId}/segments/{segmentId}/jobs/import.
func (h *Handler) handleGetSegmentImportJobs(c *echo.Context, appID, segmentID string) error {
	jobs, err := h.Backend.GetSegmentImportJobs(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]importJobResponse, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, toImportJobResponse(j))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, importJobsListResponse{Item: items})

	return nil
}

// ──────────────────────────────────────────────────
// Journey handlers
// ──────────────────────────────────────────────────

// handleGetJourney handles GET /v1/apps/{appId}/journeys/{journeyId}.
func (h *Handler) handleGetJourney(c *echo.Context, appID, journeyID string) error {
	journey, err := h.Backend.GetJourney(appID, journeyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toJourneyResponse(journey))

	return nil
}

// handleListJourneys handles GET /v1/apps/{appId}/journeys.
func (h *Handler) handleListJourneys(c *echo.Context, appID string) error {
	journeys, err := h.Backend.GetJourneys(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]journeyResponse, 0, len(journeys))

	for _, j := range journeys {
		items = append(items, toJourneyResponse(j))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, journeysListResponse{Item: items})

	return nil
}

// handleUpdateJourney handles PUT /v1/apps/{appId}/journeys/{journeyId}.
func (h *Handler) handleUpdateJourney(c *echo.Context, appID, journeyID string) error {
	var req updateJourneyRequest
	if !unmarshalBody(c, &req) {
		return nil
	}

	journey, backendErr := h.Backend.UpdateJourney(appID, journeyID, req)
	if backendErr != nil {
		if errors.Is(backendErr, awserr.ErrInvalidParameter) {
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", backendErr.Error())
		}

		return writeNotFoundOrInternal(c, backendErr)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toJourneyResponse(journey))

	return nil
}

// handleUpdateJourneyState handles PUT /v1/apps/{appId}/journeys/{journeyId}/state.
func (h *Handler) handleUpdateJourneyState(c *echo.Context, appID, journeyID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req updateJourneyStateRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	journey, backendErr := h.Backend.UpdateJourneyState(appID, journeyID, req.State)
	if backendErr != nil {
		if errors.Is(backendErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", backendErr.Error())
		}

		if errors.Is(backendErr, awserr.ErrInvalidParameter) {
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", backendErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toJourneyResponse(journey))

	return nil
}

// handleDeleteJourney handles DELETE /v1/apps/{appId}/journeys/{journeyId}.
func (h *Handler) handleDeleteJourney(c *echo.Context, appID, journeyID string) error {
	journey, err := h.Backend.DeleteJourney(appID, journeyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toJourneyResponse(journey))

	return nil
}

// handleGetJourneyDateRangeKpi handles GET /v1/apps/{appId}/journeys/{journeyId}/kpis/daterange/{kpiName}.
func (h *Handler) handleGetJourneyDateRangeKpi(c *echo.Context, appID, journeyID, kpiName string) error {
	resp, err := h.Backend.GetJourneyDateRangeKpi(appID, journeyID, kpiName)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetJourneyExecutionMetrics handles GET /v1/apps/{appId}/journeys/{journeyId}/execution/metrics.
func (h *Handler) handleGetJourneyExecutionMetrics(c *echo.Context, appID, journeyID string) error {
	resp, err := h.Backend.GetJourneyExecutionMetrics(appID, journeyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetJourneyExecutionActivityMetrics handles GET journey activity execution metrics.
func (h *Handler) handleGetJourneyExecutionActivityMetrics(c *echo.Context, appID, journeyID, activityID string) error {
	resp, err := h.Backend.GetJourneyExecutionActivityMetrics(appID, journeyID, activityID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetJourneyRuns handles GET /v1/apps/{appId}/journeys/{journeyId}/runs.
func (h *Handler) handleGetJourneyRuns(c *echo.Context, appID, journeyID string) error {
	resp, err := h.Backend.GetJourneyRuns(appID, journeyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetJourneyRunExecutionMetrics handles GET /v1/apps/{appId}/journeys/{journeyId}/runs/{runId}/execution/metrics.
func (h *Handler) handleGetJourneyRunExecutionMetrics(c *echo.Context, appID, journeyID, runID string) error {
	resp, err := h.Backend.GetJourneyRunExecutionMetrics(appID, journeyID, runID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetJourneyRunExecutionActivityMetrics handles GET journey run activity execution metrics.
func (h *Handler) handleGetJourneyRunExecutionActivityMetrics(
	c *echo.Context,
	appID, journeyID, runID, activityID string,
) error {
	resp, err := h.Backend.GetJourneyRunExecutionActivityMetrics(appID, journeyID, runID, activityID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// ──────────────────────────────────────────────────
// Template GET/UPDATE/DELETE handlers (non-create)
// ──────────────────────────────────────────────────

// handleGetTemplate handles GET for any template type.
func (h *Handler) handleGetTemplate(c *echo.Context, templateName, templateType string) error {
	switch templateType {
	case templateTypeEmail:
		t, err := h.Backend.GetEmailTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, cloneEmailTemplateToResponse(t))

		return nil
	case templateTypeInApp:
		t, err := h.Backend.GetInAppTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	case templateTypePush:
		t, err := h.Backend.GetPushTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	case templateTypeSMS:
		t, err := h.Backend.GetSmsTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, t)

		return nil
	case templateTypeVoice:
		t, err := h.Backend.GetVoiceTemplate(templateName)
		if err != nil {
			return writeNotFoundOrInternal(c, err)
		}

		httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, voiceTemplateResponse{
			ARN:          t.ARN,
			TemplateName: t.TemplateName,
			Body:         t.Body,
			Tags:         t.Tags,
			CreationDate: t.CreationDate,
		})

		return nil
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "unknown template type")
}

// handleUpdateTemplate handles PUT for any template type.
func (h *Handler) handleUpdateTemplate(c *echo.Context, templateName, templateType string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	if updateErr := h.applyTemplateUpdate(c, body, templateName, templateType); updateErr != nil {
		return updateErr
	}

	httputils.WriteJSON(
		c.Request().Context(),
		c.Response(),
		http.StatusAccepted,
		messageBodyResponse{Message: acceptedMessage},
	)

	return nil
}

// applyTemplateUpdate applies the update for the given template type.
func (h *Handler) applyTemplateUpdate(c *echo.Context, body []byte, templateName, templateType string) error {
	switch templateType {
	case templateTypeEmail:
		return h.updateEmailTemplateFromBody(c, body, templateName)
	case templateTypeInApp:
		return h.updateInAppTemplateFromBody(c, body, templateName)
	case templateTypePush:
		return h.updatePushTemplateFromBody(c, body, templateName)
	case templateTypeSMS:
		return h.updateSMSTemplateFromBody(c, body, templateName)
	case templateTypeVoice:
		return h.updateVoiceTemplateFromBody(c, body, templateName)
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "unknown template type")
}

// updateEmailTemplateFromBody parses and applies an email template update.
func (h *Handler) updateEmailTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createEmailTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateEmailTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updateInAppTemplateFromBody parses and applies an in-app template update.
func (h *Handler) updateInAppTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createInAppTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateInAppTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updatePushTemplateFromBody parses and applies a push template update.
func (h *Handler) updatePushTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createPushTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdatePushTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updateSMSTemplateFromBody parses and applies an SMS template update.
func (h *Handler) updateSMSTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createSmsTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateSmsTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// updateVoiceTemplateFromBody parses and applies a voice template update.
func (h *Handler) updateVoiceTemplateFromBody(c *echo.Context, body []byte, name string) error {
	var req createVoiceTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if _, err := h.Backend.UpdateVoiceTemplate(name, req); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	return nil
}

// handleDeleteTemplateByType handles DELETE for any template type.
func (h *Handler) handleDeleteTemplateByType(c *echo.Context, templateName, templateType string) error {
	switch templateType {
	case templateTypeEmail:
		if _, err := h.Backend.DeleteEmailTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypeInApp:
		if _, err := h.Backend.DeleteInAppTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypePush:
		if _, err := h.Backend.DeletePushTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypeSMS:
		if _, err := h.Backend.DeleteSmsTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	case templateTypeVoice:
		if _, err := h.Backend.DeleteVoiceTemplate(templateName); err != nil {
			return writeNotFoundOrInternal(c, err)
		}
	default:
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "unknown template type")
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, messageBodyResponse{Message: "Deleted"})

	return nil
}

// handleListTemplates handles GET /v1/templates.
func (h *Handler) handleListTemplates(c *echo.Context) error {
	items, err := h.Backend.ListTemplates()
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	prefix := c.QueryParam("prefix")
	templateType := strings.ToUpper(c.QueryParam("template-type"))

	resp := templatesListResponse{Item: make([]templateListItem, 0, len(items))}

	for _, item := range items {
		if prefix != "" && !strings.HasPrefix(item.TemplateName, prefix) {
			continue
		}

		if templateType != "" && !strings.EqualFold(item.TemplateType, templateType) {
			continue
		}

		resp.Item = append(resp.Item, *item)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleListTemplateVersions handles GET /v1/templates/{templateName}/{type}/versions.
func (h *Handler) handleListTemplateVersions(c *echo.Context, templateName, templateType string) error {
	items, err := h.Backend.ListTemplateVersions(templateName, templateType)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	resp := templateVersionsListResponse{Item: make([]templateVersionItem, 0, len(items))}

	for _, item := range items {
		resp.Item = append(resp.Item, *item)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleUpdateTemplateActiveVersion handles PUT /v1/templates/{templateName}/{type}/active-version.
func (h *Handler) handleUpdateTemplateActiveVersion(c *echo.Context, templateName, templateType string) error {
	_, _ = httputils.ReadBody(c.Request())

	if err := h.Backend.UpdateTemplateActiveVersion(templateName, templateType); err != nil {
		return writeNotFoundOrInternal(c, err)
	}

	httputils.WriteJSON(
		c.Request().Context(),
		c.Response(),
		http.StatusAccepted,
		messageBodyResponse{Message: acceptedMessage},
	)

	return nil
}

// ──────────────────────────────────────────────────
// Endpoint handlers
// ──────────────────────────────────────────────────

// toEndpointResponse converts an Endpoint to its wire format.
func toEndpointResponse(e *Endpoint) endpointResponse {
	return endpointResponse{
		ApplicationID:  e.ApplicationID,
		ID:             e.ID,
		ChannelType:    e.ChannelType,
		Address:        e.Address,
		EffectiveDate:  e.EffectiveDate,
		CreationDate:   e.CreationDate,
		EndpointStatus: e.EndpointStatus,
		OptOut:         e.OptOut,
		RequestID:      e.RequestID,
		Attributes:     e.Attributes,
		Metrics:        e.Metrics,
		Demographic:    e.Demographic,
		Location:       e.Location,
		User: endpointUserResponse{
			UserID:         e.UserID,
			UserAttributes: e.UserAttributes,
		},
	}
}

// handleGetEndpoint handles GET /v1/apps/{appId}/endpoints/{endpointId}.
func (h *Handler) handleGetEndpoint(c *echo.Context, appID, endpointID string) error {
	e, err := h.Backend.GetEndpoint(appID, endpointID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toEndpointResponse(e))

	return nil
}

// handleUpdateEndpoint handles PUT /v1/apps/{appId}/endpoints/{endpointId}.
func (h *Handler) handleUpdateEndpoint(c *echo.Context, appID, endpointID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req updateEndpointRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	e, backendErr := h.Backend.UpdateEndpoint(appID, endpointID, req)
	if backendErr != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusAccepted, toEndpointResponse(e))

	return nil
}

// handleDeleteEndpoint handles DELETE /v1/apps/{appId}/endpoints/{endpointId}.
func (h *Handler) handleDeleteEndpoint(c *echo.Context, appID, endpointID string) error {
	e, err := h.Backend.DeleteEndpoint(appID, endpointID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toEndpointResponse(e))

	return nil
}

// handleGetUserEndpoints handles GET /v1/apps/{appId}/users/{userId}.
func (h *Handler) handleGetUserEndpoints(c *echo.Context, appID, userID string) error {
	endpoints, err := h.Backend.GetUserEndpoints(appID, userID)
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]endpointResponse, 0, len(endpoints))

	for _, e := range endpoints {
		items = append(items, toEndpointResponse(e))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, endpointsResponse{Item: items})

	return nil
}

// handleDeleteUserEndpoints handles DELETE /v1/apps/{appId}/users/{userId}.
func (h *Handler) handleDeleteUserEndpoints(c *echo.Context, appID, userID string) error {
	if err := h.Backend.DeleteUserEndpoints(appID, userID); err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// handleUpdateEndpointsBatch handles PUT /v1/apps/{appId}/endpoints.
func (h *Handler) handleUpdateEndpointsBatch(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req updateEndpointsBatchRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if backendErr := h.Backend.UpdateEndpointsBatch(appID, req.Item); backendErr != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(
		c.Request().Context(),
		c.Response(),
		http.StatusAccepted,
		messageBodyResponse{Message: acceptedMessage},
	)

	return nil
}

// ──────────────────────────────────────────────────
// EventStream handlers
// ──────────────────────────────────────────────────

// handleGetEventStream handles GET /v1/apps/{appId}/eventstream.
func (h *Handler) handleGetEventStream(c *echo.Context, appID string) error {
	es, err := h.Backend.GetEventStream(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, eventStreamResponse{
		ApplicationID:        es.ApplicationID,
		DestinationStreamArn: es.DestinationStreamArn,
		RoleArn:              es.RoleArn,
		LastModifiedDate:     es.LastModifiedDate,
	})

	return nil
}

// handlePutEventStream handles POST /v1/apps/{appId}/eventstream.
func (h *Handler) handlePutEventStream(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req putEventStreamRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	es, backendErr := h.Backend.PutEventStream(appID, req)
	if backendErr != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, eventStreamResponse{
		ApplicationID:        es.ApplicationID,
		DestinationStreamArn: es.DestinationStreamArn,
		RoleArn:              es.RoleArn,
		LastModifiedDate:     es.LastModifiedDate,
	})

	return nil
}

// handleDeleteEventStream handles DELETE /v1/apps/{appId}/eventstream.
func (h *Handler) handleDeleteEventStream(c *echo.Context, appID string) error {
	es, err := h.Backend.DeleteEventStream(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, eventStreamResponse{
		ApplicationID:        es.ApplicationID,
		DestinationStreamArn: es.DestinationStreamArn,
		RoleArn:              es.RoleArn,
	})

	return nil
}

// ──────────────────────────────────────────────────
// Messaging / analytics handlers
// ──────────────────────────────────────────────────

// handleSendMessages handles POST /v1/apps/{appId}/messages.
func (h *Handler) handleSendMessages(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req sendMessagesRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	resp, backendErr := h.Backend.SendMessages(appID, req)
	if backendErr != nil {
		if errors.Is(backendErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", backendErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleSendUsersMessages handles POST /v1/apps/{appId}/users-messages.
func (h *Handler) handleSendUsersMessages(c *echo.Context, appID string) error {
	_, _ = httputils.ReadBody(c.Request())

	resp, err := h.Backend.SendUsersMessages(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleSendOTPMessage handles POST /v1/apps/{appId}/otp.
func (h *Handler) handleSendOTPMessage(c *echo.Context, appID string) error {
	_, _ = httputils.ReadBody(c.Request())

	resp, err := h.Backend.SendOTPMessage(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleVerifyOTPMessage handles POST /v1/apps/{appId}/verify-otp.
func (h *Handler) handleVerifyOTPMessage(c *echo.Context, appID string) error {
	_, _ = httputils.ReadBody(c.Request())

	resp, err := h.Backend.VerifyOTPMessage(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handlePutEvents handles POST /v1/apps/{appId}/events.
func (h *Handler) handlePutEvents(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req putEventsRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if backendErr := h.Backend.PutEvents(appID, req); backendErr != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(
		c.Request().Context(),
		c.Response(),
		http.StatusAccepted,
		messageBodyResponse{Message: acceptedMessage},
	)

	return nil
}

// handleGetApplicationDateRangeKpi handles GET /v1/apps/{appId}/kpis/daterange/{kpiName}.
func (h *Handler) handleGetApplicationDateRangeKpi(c *echo.Context, appID, kpiName string) error {
	resp, err := h.Backend.GetApplicationDateRangeKpi(appID, kpiName)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handlePhoneNumberValidate handles POST /v1/phone/number/validate.
func (h *Handler) handlePhoneNumberValidate(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req phoneNumberValidateRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	resp, backendErr := h.Backend.PhoneNumberValidate(req.NumberValidateRequest.PhoneNumber)
	if backendErr != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleRemoveAttributes handles PUT /v1/apps/{appId}/attributes/{attributeType}.
func (h *Handler) handleRemoveAttributes(c *echo.Context, appID, attributeType string) error {
	_, _ = httputils.ReadBody(c.Request())

	resp, err := h.Backend.RemoveAttributes(appID, attributeType)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetInAppMessages handles GET /v1/apps/{appId}/endpoints/{endpointId}/inappmessages.
func (h *Handler) handleGetInAppMessages(c *echo.Context, appID, endpointID string) error {
	resp, err := h.Backend.GetInAppMessages(appID, endpointID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// ──────────────────────────────────────────────────
// Recommender additional handlers
// ──────────────────────────────────────────────────

// handleGetRecommenderConfiguration handles GET /v1/recommenders/{recommenderId}.
func (h *Handler) handleGetRecommenderConfiguration(c *echo.Context, recommenderID string) error {
	r, err := h.Backend.GetRecommenderConfiguration(recommenderID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toRecommenderConfigResponse(r))

	return nil
}

// handleGetRecommenderConfigurations handles GET /v1/recommenders.
func (h *Handler) handleGetRecommenderConfigurations(c *echo.Context) error {
	recommenders, err := h.Backend.GetRecommenderConfigurations()
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]recommenderConfigResponse, 0, len(recommenders))

	for _, r := range recommenders {
		items = append(items, toRecommenderConfigResponse(r))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, recommenderConfigsListResponse{Item: items})

	return nil
}

// handleUpdateRecommenderConfiguration handles PUT /v1/recommenders/{recommenderId}.
func (h *Handler) handleUpdateRecommenderConfiguration(c *echo.Context, recommenderID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createRecommenderConfigRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	r, backendErr := h.Backend.UpdateRecommenderConfiguration(recommenderID, req)
	if backendErr != nil {
		if errors.Is(backendErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", backendErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toRecommenderConfigResponse(r))

	return nil
}

// handleDeleteRecommenderConfiguration handles DELETE /v1/recommenders/{recommenderId}.
func (h *Handler) handleDeleteRecommenderConfiguration(c *echo.Context, recommenderID string) error {
	r, err := h.Backend.DeleteRecommenderConfiguration(recommenderID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toRecommenderConfigResponse(r))

	return nil
}

// ──────────────────────────────────────────────────
// Job list handlers
// ──────────────────────────────────────────────────

// handleGetExportJob handles GET /v1/apps/{appId}/jobs/export/{jobId}.
func (h *Handler) handleGetExportJob(c *echo.Context, appID, jobID string) error {
	job, err := h.Backend.GetExportJob(appID, jobID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toExportJobResponse(job))

	return nil
}

// handleGetExportJobs handles GET /v1/apps/{appId}/jobs/export.
func (h *Handler) handleGetExportJobs(c *echo.Context, appID string) error {
	jobs, err := h.Backend.GetExportJobs(appID)
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]exportJobResponse, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, toExportJobResponse(j))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, exportJobsListResponse{Item: items})

	return nil
}

// handleGetImportJob handles GET /v1/apps/{appId}/jobs/import/{jobId}.
func (h *Handler) handleGetImportJob(c *echo.Context, appID, jobID string) error {
	job, err := h.Backend.GetImportJob(appID, jobID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toImportJobResponse(job))

	return nil
}

// handleGetImportJobs handles GET /v1/apps/{appId}/jobs/import.
func (h *Handler) handleGetImportJobs(c *echo.Context, appID string) error {
	jobs, err := h.Backend.GetImportJobs(appID)
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]importJobResponse, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, toImportJobResponse(j))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, importJobsListResponse{Item: items})

	return nil
}

// ──────────────────────────────────────────────────
// Helper: writeNotFoundOrInternal
// ──────────────────────────────────────────────────

func writeNotFoundOrInternal(c *echo.Context, err error) error {
	if errors.Is(err, awserr.ErrNotFound) {
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
	}

	return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
}

// ──────────────────────────────────────────────────
// Conversion helpers
// ──────────────────────────────────────────────────

func toCampaignResponse(c *Campaign) campaignResponse {
	status := c.Status
	if status == "" {
		status = campaignStatusScheduled
	}

	return campaignResponse{
		ApplicationID:               c.ApplicationID,
		ARN:                         c.ARN,
		ID:                          c.ID,
		Name:                        c.Name,
		SegmentID:                   c.SegmentID,
		SegmentVersion:              c.SegmentVersion,
		Tags:                        c.Tags,
		MessageConfiguration:        c.MessageConfiguration,
		Schedule:                    c.Schedule,
		Hook:                        c.Hook,
		Limits:                      c.Limits,
		TemplateConfiguration:       c.TemplateConfiguration,
		CustomDeliveryConfiguration: c.CustomDeliveryConfiguration,
		TreatmentDescription:        c.TreatmentDescription,
		TreatmentName:               c.TreatmentName,
		AdditionalTreatments:        c.AdditionalTreatments,
		Priority:                    c.Priority,
		IsPaused:                    c.IsPaused,
		Version:                     c.Version,
		CreationDate:                c.CreationDate,
		LastModifiedDate:            c.LastModifiedDate,
		State:                       campaignState{CampaignStatus: status},
	}
}

func toSegmentResponse(s *Segment) segmentResponse {
	return segmentResponse{
		ApplicationID:    s.ApplicationID,
		ARN:              s.ARN,
		ID:               s.ID,
		Name:             s.Name,
		SegmentType:      s.SegmentType,
		Tags:             s.Tags,
		Dimensions:       s.Dimensions,
		SegmentGroups:    s.SegmentGroups,
		ImportDefinition: s.ImportDefinition,
		CreationDate:     s.CreationDate,
		LastModifiedDate: s.LastModifiedDate,
		Version:          s.Version,
	}
}

func toJourneyResponse(j *Journey) journeyResponse {
	return journeyResponse{
		ApplicationID:          j.ApplicationID,
		ARN:                    j.ARN,
		ID:                     j.ID,
		Name:                   j.Name,
		State:                  j.State,
		Tags:                   j.Tags,
		Activities:             j.Activities,
		StartCondition:         j.StartCondition,
		Schedule:               j.Schedule,
		Limits:                 j.Limits,
		QuietTime:              j.QuietTime,
		OpenHours:              j.OpenHours,
		ClosedDays:             j.ClosedDays,
		StartActivity:          j.StartActivity,
		RefreshFrequency:       j.RefreshFrequency,
		LocalTime:              j.LocalTime,
		WaitForQuietTime:       j.WaitForQuietTime,
		RefreshOnSegmentUpdate: j.RefreshOnSegmentUpdate,
		CreationDate:           j.CreationDate,
		LastModifiedDate:       j.LastModifiedDate,
	}
}

func toRecommenderConfigResponse(r *RecommenderConfiguration) recommenderConfigResponse {
	return recommenderConfigResponse{
		Attributes:                    r.Attributes,
		ID:                            r.ID,
		Name:                          r.Name,
		Description:                   r.Description,
		RecommendationProviderIDType:  r.RecommendationProviderIDType,
		RecommendationProviderRoleArn: r.RecommendationProviderRoleARN,
		RecommendationProviderURI:     r.RecommendationProviderURI,
		RecommendationsPerMessage:     r.RecommendationsPerMessage,
		CreationDate:                  r.CreationDate,
		LastModifiedDate:              r.LastModifiedDate,
	}
}

func toExportJobResponse(j *ExportJob) exportJobResponse {
	return exportJobResponse{
		ARN:           j.ARN,
		ApplicationID: j.ApplicationID,
		ID:            j.ID,
		RoleArn:       j.RoleArn,
		S3UrlPrefix:   j.S3UrlPrefix,
		JobStatus:     j.JobStatus,
		Type:          exportJobType,
		CreationDate:  j.CreationDate,
	}
}

func toImportJobResponse(j *ImportJob) importJobResponse {
	return importJobResponse{
		ARN:           j.ARN,
		ApplicationID: j.ApplicationID,
		ID:            j.ID,
		RoleArn:       j.RoleArn,
		S3Url:         j.S3Url,
		Format:        j.Format,
		JobStatus:     j.JobStatus,
		Type:          importJobType,
		CreationDate:  j.CreationDate,
	}
}

func cloneEmailTemplateToResponse(t *EmailTemplate) map[string]any {
	resp := map[string]any{
		"Arn":              t.ARN,
		"TemplateName":     t.TemplateName,
		"CreationDate":     t.CreationDate,
		"LastModifiedDate": t.LastModifiedDate,
		"Version":          t.Version,
		"tags":             t.Tags,
	}

	if t.Subject != "" {
		resp["Subject"] = t.Subject
	}

	if t.HTMLPart != "" {
		resp["HtmlPart"] = t.HTMLPart
	}

	if t.TextPart != "" {
		resp["TextPart"] = t.TextPart
	}

	if t.TemplateDescription != "" {
		resp["TemplateDescription"] = t.TemplateDescription
	}

	if t.RecommenderID != "" {
		resp["RecommenderId"] = t.RecommenderID
	}

	if len(t.DefaultSubstitutions) > 0 {
		resp["DefaultSubstitutions"] = t.DefaultSubstitutions
	}

	return resp
}

// parseVersionParam parses a version integer from a path segment.
func parseVersionParam(s string) (int, error) {
	return strconv.Atoi(s)
}
