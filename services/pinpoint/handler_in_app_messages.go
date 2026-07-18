package pinpoint

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleGetInAppMessages handles GET /v1/apps/{appId}/endpoints/{endpointId}/inappmessages.
func (h *Handler) handleGetInAppMessages(c *echo.Context, appID, endpointID string) error {
	resp, err := h.Backend.GetInAppMessages(appID, endpointID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// ──────────────────────────────────────────────────
// Recommender additional handlers
// ──────────────────────────────────────────────────
