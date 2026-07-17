package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleSchemaCreation handles /v1/apis/{apiId}/schemacreation.
func (h *Handler) handleSchemaCreation(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.startSchemaCreation(ctx, c, apiID)
	case http.MethodGet:
		return h.getSchemaCreationStatus(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// startSchemaCreation handles POST /v1/apis/{apiId}/schemacreation.
func (h *Handler) startSchemaCreation(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Definition string `json:"definition"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	// AWS SDK sends the definition as base64-encoded bytes.
	sdl := input.Definition
	if decoded, decErr := base64.StdEncoding.DecodeString(sdl); decErr == nil {
		sdl = string(decoded)
	}

	schema, schemaErr := h.Backend.StartSchemaCreation(apiID, sdl)
	if schemaErr != nil {
		return h.handleError(ctx, c, "StartSchemaCreation", schemaErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStatus: schema.Status,
		"details": schema.Details,
	})
}

// getSchemaCreationStatus handles GET /v1/apis/{apiId}/schemacreation.
func (h *Handler) getSchemaCreationStatus(ctx context.Context, c *echo.Context, apiID string) error {
	schema, err := h.Backend.GetSchemaCreationStatus(apiID)
	if err != nil {
		return h.handleError(ctx, c, "GetSchemaCreationStatus", err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStatus: schema.Status,
		"details": schema.Details,
	})
}

// getIntrospectionSchema handles GET /v1/apis/{apiId}/schema.
func (h *Handler) getIntrospectionSchema(ctx context.Context, c *echo.Context, apiID string) error {
	format := c.Request().URL.Query().Get("format")
	if format == "" {
		format = "SDL"
	}

	sdl, err := h.Backend.GetIntrospectionSchema(apiID, format)
	if err != nil {
		return h.handleError(ctx, c, "GetIntrospectionSchema", err)
	}

	c.Response().Header().Set("Content-Type", "application/octet-stream")

	return c.Blob(http.StatusOK, "application/octet-stream", sdl)
}

// handleDataplaneEvalsSchemaMerge handles POST /v1/apis/{apiId}/schemaMerge.
func (h *Handler) handleSchemaMerge(ctx context.Context, c *echo.Context, apiID string) error {
	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	status, err := h.Backend.StartSchemaMerge(apiID)
	if err != nil {
		return h.handleError(ctx, c, "StartSchemaMerge", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"sourceApiSchemaMetadata": []any{}, keyStatus: status})
}
