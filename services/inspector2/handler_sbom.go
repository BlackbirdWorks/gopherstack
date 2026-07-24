package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opCreateSbomExport = "CreateSbomExport"
	opCancelSbomExport = "CancelSbomExport"
	opGetSbomExport    = "GetSbomExport"

	pathSbomExportCreate = "/sbomexport/create"
	pathSbomExportCancel = "/sbomexport/cancel"
	pathSbomExportGet    = "/sbomexport/get"
)

func (h *Handler) handleCreateSbomExport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	// Real CreateSbomExportInput fields are reportFormat/resourceFilterCriteria/
	// s3Destination -- the prior "sbomFormat" key does not exist on the real
	// request shape (real key is "reportFormat"), so every real client's
	// report format was silently dropped.
	var req struct {
		S3Destination          map[string]any `json:"s3Destination"`
		ResourceFilterCriteria map[string]any `json:"resourceFilterCriteria"`
		ReportFormat           string         `json:"reportFormat"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	export, createErr := h.Backend.CreateSbomExport(req.S3Destination, req.ResourceFilterCriteria, req.ReportFormat)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyReportID: export.ReportID})
}

func (h *Handler) handleCancelSbomExport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if cancelErr := h.Backend.CancelSbomExport(req.ReportID); cancelErr != nil {
		return h.mapError(c, cancelErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyReportID: req.ReportID})
}

func (h *Handler) handleGetSbomExport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	export, getErr := h.Backend.GetSbomExport(req.ReportID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	resp := map[string]any{
		keyReportID:  export.ReportID,
		keyStatus:    export.Status,
		keyErrorCode: export.ErrorCode,
	}

	if export.ErrorMessage != "" {
		resp["errorMessage"] = export.ErrorMessage
	}

	if export.Destination != nil {
		resp["s3Destination"] = export.Destination
	}

	if export.FilterCriteria != nil {
		resp["filterCriteria"] = export.FilterCriteria
	}

	if export.Format != "" {
		resp["format"] = export.Format
	}

	return c.JSON(http.StatusOK, resp)
}
