package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleDataSources handles /v1/apis/{apiId}/datasources[/{name}].
func (h *Handler) handleDataSources(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	method := c.Request().Method

	if len(segs) == pathSegsAPISubresource {
		// /v1/apis/{apiId}/datasources
		switch method {
		case http.MethodPost:
			return h.createDataSource(ctx, c, apiID)
		case http.MethodGet:
			return h.listDataSources(ctx, c, apiID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/datasources/{name}
	dsName := segs[4]

	switch method {
	case http.MethodGet:
		return h.getDataSource(ctx, c, apiID, dsName)
	case http.MethodDelete:
		return h.deleteDataSource(ctx, c, apiID, dsName)
	case http.MethodPost, http.MethodPut:
		return h.updateDataSource(ctx, c, apiID, dsName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createDataSource handles POST /v1/apis/{apiId}/datasources.
func (h *Handler) createDataSource(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var ds DataSource
	if jsonErr := json.Unmarshal(body, &ds); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	created, createErr := h.Backend.CreateDataSource(apiID, &ds)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateDataSource", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyDataSource: created})
}

// getDataSource handles GET /v1/apis/{apiId}/datasources/{name}.
func (h *Handler) getDataSource(ctx context.Context, c *echo.Context, apiID, name string) error {
	ds, err := h.Backend.GetDataSource(apiID, name)
	if err != nil {
		return h.handleError(ctx, c, "GetDataSource", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDataSource: ds})
}

// listDataSources handles GET /v1/apis/{apiId}/datasources.
func (h *Handler) listDataSources(ctx context.Context, c *echo.Context, apiID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	dss, err := h.Backend.ListDataSources(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListDataSources", err)
	}

	page, tok := appsyncPaginate(dss, nextToken, maxResults)
	out := map[string]any{"dataSources": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// deleteDataSource handles DELETE /v1/apis/{apiId}/datasources/{name}.
func (h *Handler) deleteDataSource(ctx context.Context, c *echo.Context, apiID, name string) error {
	if err := h.Backend.DeleteDataSource(apiID, name); err != nil {
		return h.handleError(ctx, c, "DeleteDataSource", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateDataSource handles PUT /v1/apis/{apiId}/datasources/{name}.
func (h *Handler) updateDataSource(ctx context.Context, c *echo.Context, apiID, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var ds DataSource
	if jsonErr := json.Unmarshal(body, &ds); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateDataSource(apiID, name, &ds)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateDataSource", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDataSource: updated})
}

// handleDataSourceIntrospections handles the legacy convenience alias
// /v1/dataSource-introspections[/{introspectionId}]. The real AWS SDK endpoint is
// /v1/datasources/introspections instead (see handleRealDataSourceIntrospections) --
// this alias is kept working for non-SDK/manual callers, now routed to the same
// corrected rdsDataApiConfig-based backend contract.
func (h *Handler) handleDataSourceIntrospections(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsAPIs:
		// POST /v1/dataSource-introspections → StartDataSourceIntrospection
		return h.requireMethod(c, http.MethodPost, func() error {
			return h.startDataSourceIntrospection(ctx, c)
		})
	case pathSegsAPIID:
		// GET /v1/dataSource-introspections/{introspectionId}
		return h.requireMethod(c, http.MethodGet, func() error {
			return h.getDataSourceIntrospection(ctx, c, segs[2])
		})
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// handleRealDataSourceIntrospections handles the real AWS SDK endpoint
// /v1/datasources/introspections[/{introspectionId}] (POST for
// StartDataSourceIntrospection, GET for GetDataSourceIntrospection). Distinct from the
// legacy /v1/dataSource-introspections alias above.
func (h *Handler) handleRealDataSourceIntrospections(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) < pathSegsAPIID || segs[2] != "introspections" {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	switch len(segs) {
	case pathSegsAPIID:
		// POST /v1/datasources/introspections → StartDataSourceIntrospection
		return h.requireMethod(c, http.MethodPost, func() error {
			return h.startDataSourceIntrospection(ctx, c)
		})
	case pathSegsAPISubresource:
		// GET /v1/datasources/introspections/{introspectionId}
		return h.requireMethod(c, http.MethodGet, func() error {
			return h.getDataSourceIntrospection(ctx, c, segs[3])
		})
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// startDataSourceIntrospection handles POST /v1/datasources/introspections (and the
// legacy /v1/dataSource-introspections alias).
func (h *Handler) startDataSourceIntrospection(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		RDSDataAPIConfig *RDSDataAPIConfig `json:"rdsDataApiConfig"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	rec, startErr := h.Backend.StartDataSourceIntrospection(input.RDSDataAPIConfig)
	if startErr != nil {
		return h.handleError(ctx, c, opStartDataSourceIntrospection, startErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"introspectionId":           rec.IntrospectionID,
		"introspectionStatus":       rec.IntrospectionStatus,
		"introspectionStatusDetail": rec.IntrospectionStatusDetail,
	})
}

// getDataSourceIntrospection handles GET /v1/datasources/introspections/{introspectionId}
// (and the legacy /v1/dataSource-introspections/{introspectionId} alias).
func (h *Handler) getDataSourceIntrospection(ctx context.Context, c *echo.Context, introspectionID string) error {
	rec, err := h.Backend.GetDataSourceIntrospection(introspectionID)
	if err != nil {
		return h.handleError(ctx, c, opGetDataSourceIntrospection, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"introspectionId":           rec.IntrospectionID,
		"introspectionResult":       rec.IntrospectionResult,
		"introspectionStatus":       rec.IntrospectionStatus,
		"introspectionStatusDetail": rec.IntrospectionStatusDetail,
	})
}
