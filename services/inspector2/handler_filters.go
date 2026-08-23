package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleCreateFilter handles POST /filters/create.
func (h *Handler) handleCreateFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any    `json:"filterCriteria"`
		Tags           map[string]string `json:"tags"`
		Name           string            `json:"name"`
		Action         string            `json:"action"`
		Description    string            `json:"description"`
		Reason         string            `json:"reason"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "name is required"),
		)
	}

	if req.Action == "" {
		req.Action = "NONE"
	}

	f, createErr := h.Backend.CreateFilter(
		req.Name,
		req.Action,
		req.Description,
		req.Reason,
		req.FilterCriteria,
		req.Tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyArn: f.Arn})
}

// handleUpdateFilter handles POST /filters/update.
func (h *Handler) handleUpdateFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
		FilterArn      string         `json:"filterArn"`
		Action         string         `json:"action"`
		Description    string         `json:"description"`
		Reason         string         `json:"reason"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.FilterArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "filterArn is required"),
		)
	}

	f, updateErr := h.Backend.UpdateFilter(
		req.FilterArn,
		req.Action,
		req.Description,
		req.Reason,
		req.FilterCriteria,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyArn: f.Arn})
}

// handleDeleteFilter handles POST /filters/delete.
func (h *Handler) handleDeleteFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Arn string `json:"arn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.Arn == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "arn is required"),
		)
	}

	if deleteErr := h.Backend.DeleteFilter(req.Arn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyArn: req.Arn})
}

// handleListFilters handles POST /filters/list.
func (h *Handler) handleListFilters(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Action string   `json:"action"`
		Arns   []string `json:"arns"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	filters, listErr := h.Backend.ListFilters(req.Arns, req.Action)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	result := make([]map[string]any, 0, len(filters))
	for _, f := range filters {
		entry := map[string]any{
			keyArn:       f.Arn,
			keyName:      f.Name,
			"action":     f.Action,
			"ownerId":    f.OwnerID,
			keyCreatedAt: awstime.Epoch(f.CreatedAt),
			keyUpdatedAt: awstime.Epoch(f.UpdatedAt),
		}

		if f.Description != "" {
			entry["description"] = f.Description
		}

		if f.Reason != "" {
			entry["reason"] = f.Reason
		}

		if f.Criteria != nil {
			// Real Filter's member is "criteria", not "filterCriteria" --
			// that name belongs to CreateFilterInput's request parameter
			// (api_op_CreateFilter.go), a different Smithy member entirely.
			entry["criteria"] = f.Criteria
		}

		if len(f.Tags) > 0 {
			entry["tags"] = f.Tags
		}

		result = append(result, entry)
	}

	return c.JSON(http.StatusOK, map[string]any{"filters": result})
}
