package mwaa

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleInvokeRestAPI(c *echo.Context, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", "failed to read request body")
	}

	var req invokeRestAPIRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	resp, err := h.Backend.InvokeRestAPI(h.contextWithRegion(c), name, &req)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}
