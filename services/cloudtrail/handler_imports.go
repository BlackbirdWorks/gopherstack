package cloudtrail

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- StartImport ---

type startImportBody struct {
	ImportSource struct {
		S3 *struct {
			S3LocationURI string `json:"S3LocationUri"`
		} `json:"S3"`
	} `json:"ImportSource"`
	Destinations []string `json:"Destinations"`
}

func (h *Handler) handleStartImport(c *echo.Context, body []byte) error {
	var in startImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	var src string
	if in.ImportSource.S3 != nil {
		src = in.ImportSource.S3.S3LocationURI
	}

	imp, err := h.Backend.StartImport(in.Destinations, src)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportID:         imp.ImportID,
		keyImportStatus:     imp.ImportStatus,
		keyDestinations:     imp.Destinations,
		keyCreatedTimestamp: float64(imp.CreatedTimestamp.Unix()),
		keyUpdatedTimestamp: float64(imp.UpdatedTimestamp.Unix()),
	})
}

// --- GetImport ---

type getImportBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleGetImport(c *echo.Context, body []byte) error {
	var in getImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	imp, err := h.Backend.GetImport(in.ImportID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportID:         imp.ImportID,
		keyImportStatus:     imp.ImportStatus,
		keyDestinations:     imp.Destinations,
		keyCreatedTimestamp: float64(imp.CreatedTimestamp.Unix()),
		keyUpdatedTimestamp: float64(imp.UpdatedTimestamp.Unix()),
	})
}

// --- ListImports ---

func (h *Handler) handleListImports(c *echo.Context, _ []byte) error {
	list := h.Backend.ListImports()
	items := make([]map[string]any, 0, len(list))
	for _, imp := range list {
		items = append(items, map[string]any{
			keyImportID:     imp.ImportID,
			keyImportStatus: imp.ImportStatus,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"Imports": items})
}

// --- StopImport ---

type stopImportBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleStopImport(c *echo.Context, body []byte) error {
	var in stopImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	imp, err := h.Backend.StopImport(in.ImportID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportID:         imp.ImportID,
		keyImportStatus:     imp.ImportStatus,
		keyCreatedTimestamp: float64(imp.CreatedTimestamp.Unix()),
		keyUpdatedTimestamp: float64(imp.UpdatedTimestamp.Unix()),
	})
}

// --- ListImportFailures ---

type listImportFailuresBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleListImportFailures(c *echo.Context, body []byte) error {
	var in listImportFailuresBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	failures := h.Backend.ListImportFailures(in.ImportID)

	return c.JSON(http.StatusOK, map[string]any{"Failures": failures})
}
