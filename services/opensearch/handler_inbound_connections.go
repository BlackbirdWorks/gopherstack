package opensearch

import (
	"errors"
	"net/http"
	"strings"
)

// handleCCRoutes handles cross-cluster connection routes.
func (h *Handler) handleCCRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchCCPath)

	if strings.HasPrefix(rest, "/inboundConnection") {
		h.handleCCInboundRoutes(w, r, rest)

		return
	}

	if strings.HasPrefix(rest, "/outboundConnection") {
		h.handleCCOutboundRoutes(w, r, rest)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleCCInboundRoutes handles inbound cross-cluster connection sub-routes.
func (h *Handler) handleCCInboundRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	const prefix = "/inboundConnection/"

	switch {
	// GET /inboundConnection → DescribeInboundConnections
	case (rest == "/inboundConnection" || rest == "/inboundConnection/") && r.Method == http.MethodGet:
		conns := h.Backend.DescribeInboundConnections()
		h.writeJSON(r, w, map[string]any{"Connections": conns})
	// PUT /inboundConnection/{id}/accept → AcceptInboundConnection
	case strings.HasPrefix(rest, prefix) && strings.HasSuffix(rest, "/accept") &&
		r.Method == http.MethodPut:
		connID := strings.TrimSuffix(strings.TrimPrefix(rest, prefix), "/accept")
		h.handleAcceptInboundConnection(w, r, connID)
	// PUT /inboundConnection/{id}/reject → RejectInboundConnection
	case strings.HasPrefix(rest, prefix) && strings.HasSuffix(rest, "/reject") &&
		r.Method == http.MethodPut:
		connID := strings.TrimSuffix(strings.TrimPrefix(rest, prefix), "/reject")
		conn, err := h.Backend.RejectInboundConnection(connID)
		if err != nil {
			conn = &InboundConnection{ConnectionID: connID, Status: "REJECTED"}
		}
		h.writeJSON(r, w, map[string]any{jsonKeyConnection: map[string]any{
			jsonKeyConnectionID:     conn.ConnectionID,
			jsonKeyConnectionStatus: map[string]any{jsonKeyStatusCode: conn.Status},
		}})
	// DELETE /inboundConnection/{id} → DeleteInboundConnection
	case strings.HasPrefix(rest, prefix) && r.Method == http.MethodDelete:
		connID := strings.TrimPrefix(rest, prefix)
		conn, err := h.Backend.DeleteInboundConnection(connID)
		if err != nil {
			conn = &InboundConnection{ConnectionID: connID, Status: statusDeleted}
		}
		h.writeJSON(r, w, map[string]any{jsonKeyConnection: map[string]any{
			jsonKeyConnectionID:     conn.ConnectionID,
			jsonKeyConnectionStatus: map[string]any{jsonKeyStatusCode: conn.Status},
		}})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// acceptInboundConnectionOutput is the JSON response for AcceptInboundConnection.
type acceptInboundConnectionOutput struct {
	Connection inboundConnectionJSON `json:"Connection"`
}

// inboundConnectionJSON is the JSON representation of an inbound connection.
type inboundConnectionJSON struct {
	ConnectionID     string                `json:"ConnectionId"`
	ConnectionStatus inboundConnStatusJSON `json:"ConnectionStatus"`
}

// inboundConnStatusJSON is the JSON representation of a connection status.
type inboundConnStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

func (h *Handler) handleAcceptInboundConnection(
	w http.ResponseWriter,
	r *http.Request,
	connectionID string,
) {
	conn, err := h.Backend.AcceptInboundConnection(connectionID)
	if err != nil {
		if errors.Is(err, ErrInvalidParameter) {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		} else {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, acceptInboundConnectionOutput{
		Connection: inboundConnectionJSON{
			ConnectionID: conn.ConnectionID,
			ConnectionStatus: inboundConnStatusJSON{
				StatusCode: conn.Status,
			},
		},
	})
}
