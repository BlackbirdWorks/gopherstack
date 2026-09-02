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
	// POST /outboundConnection/search → DescribeOutboundConnections. Real clients
	// always POST here (api_op_DescribeOutboundConnections.go, opensearch@v1.75.4
	// serializers.go); a bare GET on /outboundConnection is never sent -- gopherstack-l5ir.
	case rest == "/outboundConnection/search" && r.Method == http.MethodPost:
		h.handleDescribeOutboundConnections(w, r)
	// POST /outboundConnection → CreateOutboundConnection
	case (rest == "/outboundConnection" || rest == "/outboundConnection/") &&
		r.Method == http.MethodPost:
		h.handleCreateOutboundConnection(w, r)
	// DELETE /outboundConnection/{id} → DeleteOutboundConnection
	case strings.HasPrefix(rest, prefix) && r.Method == http.MethodDelete:
		connID := strings.TrimPrefix(rest, prefix)
		conn, err := h.Backend.DeleteOutboundConnection(connID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyConnection: outboundConnectionJSON(conn)})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// domainInformationContainerReq is the request-body shape for
// LocalDomainInfo/RemoteDomainInfo on CreateOutboundConnection
// (types.DomainInformationContainer / types.AWSDomainInformation).
type domainInformationContainerReq struct {
	AWSDomainInformation struct {
		DomainName string `json:"DomainName"`
		OwnerID    string `json:"OwnerId"`
		Region     string `json:"Region"`
	} `json:"AWSDomainInformation"`
}

func (d domainInformationContainerReq) toDomainInformation() DomainInformation {
	return DomainInformation{
		DomainName: d.AWSDomainInformation.DomainName,
		OwnerID:    d.AWSDomainInformation.OwnerID,
		Region:     d.AWSDomainInformation.Region,
	}
}

// createOutboundConnectionRequest is the request body for
// CreateOutboundConnection (types.CreateOutboundConnectionInput).
type createOutboundConnectionRequest struct {
	ConnectionAlias      string                        `json:"ConnectionAlias"`
	ConnectionMode       string                        `json:"ConnectionMode"`
	LocalDomainInfo      domainInformationContainerReq `json:"LocalDomainInfo"`
	RemoteDomainInfo     domainInformationContainerReq `json:"RemoteDomainInfo"`
	ConnectionProperties struct {
		Endpoint           string `json:"Endpoint"`
		CrossClusterSearch struct {
			SkipUnavailable string `json:"SkipUnavailable"`
		} `json:"CrossClusterSearch"`
	} `json:"ConnectionProperties"`
}

// outboundConnectionJSON renders an OutboundConnection as the wire-shape
// types.OutboundConnection object (also used verbatim, sans wrapper, for
// CreateOutboundConnectionOutput, whose members are unnested top-level
// fields of the same shape).
func outboundConnectionJSON(c *OutboundConnection) map[string]any {
	return map[string]any{
		"ConnectionAlias":       c.ConnectionAlias,
		jsonKeyConnectionID:     c.ConnectionID,
		"ConnectionMode":        c.ConnectionMode,
		"ConnectionProperties":  connectionPropertiesJSON(c.SkipUnavailable, c.Endpoint),
		jsonKeyConnectionStatus: connectionStatusJSON(c.Status, c.StatusMessage),
		"LocalDomainInfo":       domainInfoJSON(c.LocalDomainInfo),
		"RemoteDomainInfo":      domainInfoJSON(c.RemoteDomainInfo),
	}
}

// connectionPropertiesJSON renders the wire-shape types.ConnectionProperties
// object.
func connectionPropertiesJSON(skipUnavailable, endpoint string) map[string]any {
	props := map[string]any{}
	if skipUnavailable != "" {
		props["CrossClusterSearch"] = map[string]any{"SkipUnavailable": skipUnavailable}
	}

	if endpoint != "" {
		props["Endpoint"] = endpoint
	}

	return props
}

// handleDescribeOutboundConnections serves POST /outboundConnection/search.
func (h *Handler) handleDescribeOutboundConnections(w http.ResponseWriter, r *http.Request) {
	req, ok := h.readDescribeConnectionsRequest(w, r)
	if !ok {
		return
	}

	p := h.Backend.DescribeOutboundConnections(req.connectionIDFilters(), req.NextToken, int(req.MaxResults))
	items := make([]map[string]any, 0, len(p.Data))

	for _, c := range p.Data {
		items = append(items, outboundConnectionJSON(c))
	}

	h.writeConnectionsResponse(r, w, items, p.Next)
}

func (h *Handler) handleCreateOutboundConnection(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createOutboundConnectionRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to parse body")

			return
		}
	}

	conn, createErr := h.Backend.CreateOutboundConnection(
		req.ConnectionAlias,
		req.ConnectionMode,
		req.LocalDomainInfo.toDomainInformation(),
		req.RemoteDomainInfo.toDomainInformation(),
		req.ConnectionProperties.CrossClusterSearch.SkipUnavailable,
		req.ConnectionProperties.Endpoint,
	)
	if createErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

		return
	}

	h.writeJSON(r, w, outboundConnectionJSON(conn))
}
