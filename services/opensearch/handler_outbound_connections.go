package opensearch

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleCCOutboundRoutes handles outbound cross-cluster connection sub-routes.
func (h *Handler) handleCCOutboundRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	const prefix = "/outboundConnection/"

	switch {
	// GET /outboundConnection → DescribeOutboundConnections
	case (rest == "/outboundConnection" || rest == "/outboundConnection/") &&
		r.Method == http.MethodGet:
		conns := h.Backend.DescribeOutboundConnections()
		h.writeJSON(r, w, map[string]any{"Connections": conns})
	// POST /outboundConnection → CreateOutboundConnection
	case (rest == "/outboundConnection" || rest == "/outboundConnection/") &&
		r.Method == http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			LocalDomainInfo  map[string]any `json:"LocalDomainInfo"`
			RemoteDomainInfo map[string]any `json:"RemoteDomainInfo"`
			ConnectionAlias  string         `json:"ConnectionAlias"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		conn, createErr := h.Backend.CreateOutboundConnection(
			req.ConnectionAlias,
			req.LocalDomainInfo,
			req.RemoteDomainInfo,
		)
		if createErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"ConnectionId":     conn.ConnectionID,
			"ConnectionAlias":  conn.ConnectionAlias,
			"ConnectionStatus": map[string]any{jsonKeyStatusCode: conn.Status},
		})
	// DELETE /outboundConnection/{id} → DeleteOutboundConnection
	case strings.HasPrefix(rest, prefix) && r.Method == http.MethodDelete:
		connID := strings.TrimPrefix(rest, prefix)
		conn, err := h.Backend.DeleteOutboundConnection(connID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyConnection: map[string]any{
			jsonKeyConnectionID:     conn.ConnectionID,
			jsonKeyConnectionStatus: map[string]any{jsonKeyStatusCode: conn.Status},
		}})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}
