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

// domainInfoJSON renders a DomainInformation as the wire-shape
// DomainInformationContainer, nesting it under the AWSDomainInformation key
// (types.DomainInformationContainer / types.AWSDomainInformation).
func domainInfoJSON(d DomainInformation) map[string]any {
	info := map[string]any{"DomainName": d.DomainName}
	if d.OwnerID != "" {
		info["OwnerId"] = d.OwnerID
	}

	if d.Region != "" {
		info["Region"] = d.Region
	}

	return map[string]any{"AWSDomainInformation": info}
}

// connectionStatusJSON renders a status code/message pair as the wire-shape
// InboundConnectionStatus / OutboundConnectionStatus object.
func connectionStatusJSON(statusCode, message string) map[string]any {
	out := map[string]any{jsonKeyStatusCode: statusCode}
	if message != "" {
		out["Message"] = message
	}

	return out
}

// inboundConnectionJSON renders an InboundConnection as the wire-shape
// types.InboundConnection object.
func inboundConnectionJSON(c *InboundConnection) map[string]any {
	return map[string]any{
		jsonKeyConnectionID:     c.ConnectionID,
		"ConnectionMode":        c.ConnectionMode,
		jsonKeyConnectionStatus: connectionStatusJSON(c.Status, c.StatusMessage),
		"LocalDomainInfo":       domainInfoJSON(c.LocalDomainInfo),
		"RemoteDomainInfo":      domainInfoJSON(c.RemoteDomainInfo),
	}
}

// handleCCInboundRoutes handles inbound cross-cluster connection sub-routes.
func (h *Handler) handleCCInboundRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	const prefix = "/inboundConnection/"

	switch {
	// GET /inboundConnection → DescribeInboundConnections
	case (rest == "/inboundConnection" || rest == "/inboundConnection/") && r.Method == http.MethodGet:
		conns := h.Backend.DescribeInboundConnections()
		items := make([]map[string]any, 0, len(conns))
		for _, c := range conns {
			items = append(items, inboundConnectionJSON(c))
		}
		h.writeJSON(r, w, map[string]any{"Connections": items})
	// PUT /inboundConnection/{id}/accept → AcceptInboundConnection
	case strings.HasPrefix(rest, prefix) && strings.HasSuffix(rest, "/accept") &&
		r.Method == http.MethodPut:
		connID := strings.TrimSuffix(strings.TrimPrefix(rest, prefix), "/accept")
		h.handleAcceptInboundConnection(w, r, connID)
	// PUT /inboundConnection/{id}/reject → RejectInboundConnection
	case strings.HasPrefix(rest, prefix) && strings.HasSuffix(rest, "/reject") &&
		r.Method == http.MethodPut:
		connID := strings.TrimSuffix(strings.TrimPrefix(rest, prefix), "/reject")
		h.handleRejectInboundConnection(w, r, connID)
	// DELETE /inboundConnection/{id} → DeleteInboundConnection
	case strings.HasPrefix(rest, prefix) && r.Method == http.MethodDelete:
		connID := strings.TrimPrefix(rest, prefix)
		h.handleDeleteInboundConnection(w, r, connID)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// writeConnectionNotFoundOrValidation classifies a connection-lookup error
// and writes the appropriate error response. It returns true if an error was
// written.
func (h *Handler) writeConnectionNotFoundOrValidation(r *http.Request, w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrInvalidParameter) {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

		return true
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

	return true
}

func (h *Handler) handleAcceptInboundConnection(
	w http.ResponseWriter,
	r *http.Request,
	connectionID string,
) {
	conn, err := h.Backend.AcceptInboundConnection(connectionID)
	if h.writeConnectionNotFoundOrValidation(r, w, err) {
		return
	}

	h.writeJSON(r, w, map[string]any{jsonKeyConnection: inboundConnectionJSON(conn)})
}

func (h *Handler) handleRejectInboundConnection(
	w http.ResponseWriter,
	r *http.Request,
	connectionID string,
) {
	conn, err := h.Backend.RejectInboundConnection(connectionID)
	if h.writeConnectionNotFoundOrValidation(r, w, err) {
		return
	}

	h.writeJSON(r, w, map[string]any{jsonKeyConnection: inboundConnectionJSON(conn)})
}

func (h *Handler) handleDeleteInboundConnection(
	w http.ResponseWriter,
	r *http.Request,
	connectionID string,
) {
	conn, err := h.Backend.DeleteInboundConnection(connectionID)
	if h.writeConnectionNotFoundOrValidation(r, w, err) {
		return
	}

	h.writeJSON(r, w, map[string]any{jsonKeyConnection: inboundConnectionJSON(conn)})
}
