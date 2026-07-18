package pinpoint

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// extractSettingsOp returns the settings operation name.
func extractSettingsOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetApplicationSettings"
	case http.MethodPut:
		return "UpdateApplicationSettings"
	}

	return unknownOperation
}

// dispatchAppSettings handles GET/PUT /v1/apps/{appId}/settings.
func (h *Handler) dispatchAppSettings(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetApplicationSettings(c, appID)
	case http.MethodPut:
		return h.handleUpdateApplicationSettings(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

// handleGetApplicationSettings handles GET /v1/apps/{appId}/settings.
func (h *Handler) handleGetApplicationSettings(c *echo.Context, appID string) error {
	settings, err := h.Backend.GetApplicationSettings(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	resp := appSettingsResponse{
		ApplicationID:            appID,
		LastModifiedDate:         settings.LastModifiedDate,
		CampaignHook:             settings.CampaignHook,
		Limits:                   settings.Limits,
		QuietTime:                settings.QuietTime,
		CloudWatchMetricsEnabled: settings.CloudWatchMetrics,
		EventTaggingEnabled:      settings.EventTaggingEnabled,
	}

	if resp.CampaignHook == nil {
		resp.CampaignHook = map[string]any{}
	}

	if resp.Limits == nil {
		resp.Limits = map[string]any{}
	}

	if resp.QuietTime == nil {
		resp.QuietTime = map[string]any{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleUpdateApplicationSettings handles PUT /v1/apps/{appId}/settings.
func (h *Handler) handleUpdateApplicationSettings(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var incoming struct {
		CampaignHook        map[string]any `json:"CampaignHook"`
		Limits              map[string]any `json:"Limits"`
		QuietTime           map[string]any `json:"QuietTime"`
		CloudWatchMetrics   bool           `json:"CloudWatchMetricsEnabled"`
		EventTaggingEnabled bool           `json:"EventTaggingEnabled"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &incoming); jsonErr != nil {
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
		}
	}

	settingsToStore := &storedAppSettings{
		CampaignHook:        incoming.CampaignHook,
		Limits:              incoming.Limits,
		QuietTime:           incoming.QuietTime,
		CloudWatchMetrics:   incoming.CloudWatchMetrics,
		EventTaggingEnabled: incoming.EventTaggingEnabled,
	}

	settings, updateErr := h.Backend.UpdateApplicationSettings(appID, settingsToStore)
	if updateErr != nil {
		if errors.Is(updateErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", updateErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", updateErr.Error())
	}

	resp := appSettingsResponse{
		ApplicationID:            appID,
		LastModifiedDate:         settings.LastModifiedDate,
		CampaignHook:             settings.CampaignHook,
		Limits:                   settings.Limits,
		QuietTime:                settings.QuietTime,
		CloudWatchMetricsEnabled: settings.CloudWatchMetrics,
		EventTaggingEnabled:      settings.EventTaggingEnabled,
	}

	if resp.CampaignHook == nil {
		resp.CampaignHook = map[string]any{}
	}

	if resp.Limits == nil {
		resp.Limits = map[string]any{}
	}

	if resp.QuietTime == nil {
		resp.QuietTime = map[string]any{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

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
