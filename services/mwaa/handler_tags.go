package mwaa

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(h.contextWithRegion(c), resourceARN)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	if tags == nil {
		tags = map[string]string{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{
		"Tags": tags,
	})

	return nil
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", "failed to read request body")
	}

	var req struct {
		Tags map[string]string `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if tagErr := h.Backend.TagResource(h.contextWithRegion(c), resourceARN, req.Tags); tagErr != nil {
		if errors.Is(tagErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", tagErr.Error())
		}

		if errors.Is(tagErr, awserr.ErrInvalidParameter) {
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", tagErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", tagErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(h.contextWithRegion(c), resourceARN, tagKeys); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{})

	return nil
}
