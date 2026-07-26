package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opCreateCisScanConfiguration = "CreateCisScanConfiguration"
	opDeleteCisScanConfiguration = "DeleteCisScanConfiguration"
	opUpdateCisScanConfiguration = "UpdateCisScanConfiguration"
	opListCisScanConfigurations  = "ListCisScanConfigurations"

	opStartCisSession                              = "StartCisSession"
	opStopCisSession                               = "StopCisSession"
	opSendCisSessionHealth                         = "SendCisSessionHealth"
	opSendCisSessionTelemetry                      = "SendCisSessionTelemetry"
	opGetCisScanReport                             = "GetCisScanReport"
	opGetCisScanResultDetails                      = "GetCisScanResultDetails"
	opListCisScans                                 = "ListCisScans"
	opListCisScanResultsAggregatedByChecks         = "ListCisScanResultsAggregatedByChecks"
	opListCisScanResultsAggregatedByTargetResource = "ListCisScanResultsAggregatedByTargetResource"

	pathCisScanConfigCreate = "/cis/scan-configuration/create"
	pathCisScanConfigDelete = "/cis/scan-configuration/delete"
	pathCisScanConfigUpdate = "/cis/scan-configuration/update"
	pathCisScanConfigList   = "/cis/scan-configuration/list"

	pathCisSessionStart           = "/cissession/start"
	pathCisSessionStop            = "/cissession/stop"
	pathCisSessionHealthSend      = "/cissession/health/send"
	pathCisSessionTelemetrySend   = "/cissession/telemetry/send"
	pathCisScanReportGet          = "/cis/scan/report/get"
	pathCisScanResultDetailsGet   = "/cis/scan-result/details/get"
	pathCisScanList               = "/cis/scan/list"
	pathCisScanResultCheckList    = "/cis/scan-result/check/list"
	pathCisScanResultResourceList = "/cis/scan-result/resource/list"

	keyScanConfigurationArn = "scanConfigurationArn"
)

func (h *Handler) handleCreateCisScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Schedule map[string]any    `json:"schedule"`
		Targets  map[string]any    `json:"targets"`
		Tags     map[string]string `json:"tags"`
		ScanName string            `json:"scanName"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, createErr := h.Backend.CreateCisScanConfiguration(
		req.ScanName,
		req.Schedule,
		req.Targets,
		req.Tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyScanConfigurationArn: cfg.Arn},
	)
}

func (h *Handler) handleDeleteCisScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if deleteErr := h.Backend.DeleteCisScanConfiguration(req.ScanConfigurationArn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: req.ScanConfigurationArn})
}

func (h *Handler) handleUpdateCisScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Schedule             map[string]any `json:"schedule"`
		Targets              map[string]any `json:"targets"`
		ScanConfigurationArn string         `json:"scanConfigurationArn"`
		ScanName             string         `json:"scanName"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, updateErr := h.Backend.UpdateCisScanConfiguration(
		req.ScanConfigurationArn,
		req.ScanName,
		req.Schedule,
		req.Targets,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: cfg.Arn})
}

func (h *Handler) handleListCisScanConfigurations(c *echo.Context) error {
	cfgs, err := h.Backend.ListCisScanConfigurations()
	if err != nil {
		return h.mapError(c, err)
	}

	if cfgs == nil {
		cfgs = []*CisScanConfiguration{}
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurations: cfgs})
}

func (h *Handler) handleStartCisSession(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Message   map[string]any `json:"message"`
		ScanJobID string         `json:"scanJobId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	sessionToken := ""
	if req.Message != nil {
		if st, ok := req.Message["sessionToken"].(string); ok {
			sessionToken = st
		}
	}

	if _, startErr := h.Backend.StartCisSession(req.ScanJobID, sessionToken); startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStopCisSession(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanJobID string `json:"scanJobId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if stopErr := h.Backend.StopCisSession(req.ScanJobID); stopErr != nil {
		return h.mapError(c, stopErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleSendCisSessionHealth(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanJobID string `json:"scanJobId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if healthErr := h.Backend.SendCisSessionHealth(req.ScanJobID); healthErr != nil {
		return h.mapError(c, healthErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleSendCisSessionTelemetry(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Messages  map[string]any `json:"messages"`
		ScanJobID string         `json:"scanJobId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if telErr := h.Backend.SendCisSessionTelemetry(req.ScanJobID, req.Messages); telErr != nil {
		return h.mapError(c, telErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetCisScanReport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	report, err := h.Backend.GetCisScanReport(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, report)
}

func (h *Handler) handleGetCisScanResultDetails(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	details, err := h.Backend.GetCisScanResultDetails(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, details)
}

func (h *Handler) handleListCisScans(c *echo.Context) error {
	scans, err := h.Backend.ListCisScans()
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"cisScans": scans})
}

func (h *Handler) handleListCisScanResultsAggregatedByChecks(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	results, err := h.Backend.ListCisScanResultsAggregatedByChecks(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"checkAggregations": results})
}

func (h *Handler) handleListCisScanResultsAggregatedByTargetResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	results, err := h.Backend.ListCisScanResultsAggregatedByTargetResource(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"targetResourceAggregations": results})
}
