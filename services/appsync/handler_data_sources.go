package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
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

// handleDataSourceIntrospections handles /v1/dataSource-introspections[/{introspectionId}].
func (h *Handler) handleDataSourceIntrospections(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsAPIs:
		// POST /v1/dataSource-introspections → StartDataSourceIntrospection
		if c.Request().Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}

		return h.startDataSourceIntrospection(ctx, c)
	case pathSegsAPIID:
		// GET /v1/dataSource-introspections/{introspectionId}
		if c.Request().Method != http.MethodGet {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}

		return h.getDataSourceIntrospection(ctx, c, segs[2])
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// startDataSourceIntrospection handles POST /v1/dataSource-introspections.
func (h *Handler) startDataSourceIntrospection(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		APIID          string `json:"apiId"`
		DataSourceName string `json:"dataSourceName"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	id, startErr := h.Backend.StartDataSourceIntrospection(input.APIID, input.DataSourceName)
	if startErr != nil {
		return h.handleError(ctx, c, "StartDataSourceIntrospection", startErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"introspectionId": id})
}

// getDataSourceIntrospection handles GET /v1/dataSource-introspections/{introspectionId}.
func (h *Handler) getDataSourceIntrospection(ctx context.Context, c *echo.Context, introspectionID string) error {
	result, err := h.Backend.GetDataSourceIntrospection(introspectionID)
	if err != nil {
		return h.handleError(ctx, c, "GetDataSourceIntrospection", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"introspectionResult": result})
}
