package opensearch

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleScheduledActionsRoutes handles scheduled action routes.
func (h *Handler) handleScheduledActionsRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchScheduledActionsPath)

	switch {
	// GET /scheduledActions → ListScheduledActions
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		domainName := r.URL.Query().Get("DomainName")
		actions := h.Backend.ListScheduledActions(domainName)
		if actions == nil {
			actions = []*ScheduledAction{}
		}
		h.writeJSON(r, w, map[string]any{"ScheduledActions": actions})
	// PUT /scheduledActions/update → UpdateScheduledAction
	case rest == "/update" && r.Method == http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			ScheduledAction *ScheduledAction `json:"ScheduledAction"`
			DomainName      string           `json:"DomainName"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		if req.ScheduledAction == nil {
			req.ScheduledAction = &ScheduledAction{}
		}
		action, _ := h.Backend.UpdateScheduledAction(req.DomainName, req.ScheduledAction)
		h.writeJSON(r, w, map[string]any{"ScheduledAction": action})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}
