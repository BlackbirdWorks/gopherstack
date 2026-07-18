package mwaa

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateCliToken(c *echo.Context, name string) error {
	token, hostname, err := h.Backend.CreateCliToken(h.contextWithRegion(c), name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"CliToken":          token,
		"WebServerHostname": hostname,
	})

	return nil
}
