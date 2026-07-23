package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// updateScheduledActionRequest is the JSON request body for
// UpdateScheduledAction (types.UpdateScheduledActionInput -- DomainName comes
// from the URL path, not the body).
type updateScheduledActionRequest struct {
	ActionID         string `json:"ActionID"`
	ActionType       string `json:"ActionType"`
	ScheduleAt       string `json:"ScheduleAt"`
	DesiredStartTime int64  `json:"DesiredStartTime"`
}

// handleUpdateScheduledAction handles
// PUT /2021-01-01/opensearch/domain/{DomainName}/scheduledAction/update.
func (h *Handler) handleUpdateScheduledAction(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req updateScheduledActionRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	action, updateErr := h.Backend.UpdateScheduledAction(
		domainName, req.ActionID, req.ActionType, req.ScheduleAt, req.DesiredStartTime,
	)
	if updateErr != nil {
		if errors.Is(updateErr, ErrScheduledActionNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", updateErr.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{"ScheduledAction": action})
}
