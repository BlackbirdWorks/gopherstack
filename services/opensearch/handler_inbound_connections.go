package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// describeConnectionsRequest is the shared request body shape for
// DescribeInboundConnections and DescribeOutboundConnections
// (api_op_DescribeInboundConnections.go / api_op_DescribeOutboundConnections.go).
type describeConnectionsRequest struct {
	NextToken string `json:"NextToken"`
	Filters   []struct {
		Name   string   `json:"Name"`
		Values []string `json:"Values"`
	} `json:"Filters"`
	MaxResults int32 `json:"MaxResults"`
}

// connectionIDFilters extracts the Values of every Filter named
// "connection-id" -- the only Filter Name documented for these operations.
func (req describeConnectionsRequest) connectionIDFilters() []string {
	var ids []string

	for _, f := range req.Filters {
		if f.Name == "connection-id" {
			ids = append(ids, f.Values...)
		}
	}

	return ids
}

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
	// POST /inboundConnection/search → DescribeInboundConnections. Real clients
	// always POST here (api_op_DescribeInboundConnections.go, opensearch@v1.75.4
	// serializers.go); a bare GET on /inboundConnection is never sent -- gopherstack-l5ir.
	case rest == "/inboundConnection/search" && r.Method == http.MethodPost:
		h.handleDescribeInboundConnections(w, r)
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

// handleDescribeInboundConnections serves POST /inboundConnection/search.
func (h *Handler) handleDescribeInboundConnections(w http.ResponseWriter, r *http.Request) {
	req, ok := h.readDescribeConnectionsRequest(w, r)
	if !ok {
		return
	}

	p := h.Backend.DescribeInboundConnections(req.connectionIDFilters(), req.NextToken, int(req.MaxResults))
	items := make([]map[string]any, 0, len(p.Data))

	for _, c := range p.Data {
		items = append(items, inboundConnectionJSON(c))
	}

	h.writeConnectionsResponse(r, w, items, p.Next)
}

// readDescribeConnectionsRequest parses the shared DescribeInboundConnections/
// DescribeOutboundConnections request body, writing a ValidationException and
// returning ok=false on a read or parse failure.
func (h *Handler) readDescribeConnectionsRequest(
	w http.ResponseWriter, r *http.Request,
) (describeConnectionsRequest, bool) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return describeConnectionsRequest{}, false
	}

	var req describeConnectionsRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to parse body")

			return describeConnectionsRequest{}, false
		}
	}

	return req, true
}

// writeConnectionsResponse writes the shared {Connections, NextToken} wire
// shape both DescribeInboundConnections and DescribeOutboundConnections
// return.
func (h *Handler) writeConnectionsResponse(
	r *http.Request, w http.ResponseWriter, items []map[string]any, next string,
) {
	out := map[string]any{"Connections": items}
	if next != "" {
		out["NextToken"] = next
	}

	h.writeJSON(r, w, out)
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
