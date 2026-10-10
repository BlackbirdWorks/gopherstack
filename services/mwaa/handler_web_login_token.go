package mwaa

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// iamIdentity turns a caller ARN into the resource form the SDK documents ("assumed-role/Admin/name").
func iamIdentity(callerARN string) string {
	const arnFields = 6

	parts := strings.SplitN(callerARN, ":", arnFields)
	if len(parts) < arnFields {
		return ""
	}

	return parts[arnFields-1]
}

func (h *Handler) handleCreateWebLoginToken(c *echo.Context, name string) error {
	token, hostname, err := h.Backend.CreateWebLoginToken(h.contextWithRegion(c), name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	out := map[string]string{
		"WebToken":          token,
		"WebServerHostname": hostname,
	}
	if id := iamIdentity(awsmeta.CallerArn(c.Request().Context())); id != "" {
		out["IamIdentity"] = id
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, out)

	return nil
}
