package elasticsearch

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// outboundConnectionJSON is the JSON representation of an outbound cross-cluster connection.
type outboundConnectionJSON struct {
	CrossClusterSearchConnectionID string                       `json:"CrossClusterSearchConnectionId"`
	ConnectionAlias                string                       `json:"ConnectionAlias"`
	ConnectionStatus               outboundConnectionStatusJSON `json:"ConnectionStatus"`
	SourceDomainInfo               crossClusterDomainInfoJSON   `json:"SourceDomainInfo"`
	DestinationDomainInfo          crossClusterDomainInfoJSON   `json:"DestinationDomainInfo"`
}

type outboundConnectionStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

// createOutboundConnectionRequest is the JSON body for CreateOutboundCrossClusterSearchConnection.
type createOutboundConnectionRequest struct {
	SourceDomainInfo      crossClusterDomainInfoJSON `json:"SourceDomainInfo"`
	DestinationDomainInfo crossClusterDomainInfoJSON `json:"DestinationDomainInfo"`
	ConnectionAlias       string                     `json:"ConnectionAlias"`
}

func (h *Handler) handleCreateOutboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createOutboundConnectionRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	localDomain := CrossClusterDomainInfo{
		OwnerID:    req.SourceDomainInfo.OwnerID,
		DomainName: req.SourceDomainInfo.DomainName,
		Region:     req.SourceDomainInfo.Region,
	}
	remoteDomain := CrossClusterDomainInfo{
		OwnerID:    req.DestinationDomainInfo.OwnerID,
		DomainName: req.DestinationDomainInfo.DomainName,
		Region:     req.DestinationDomainInfo.Region,
	}

	conn, createErr := h.Backend.CreateOutboundCrossClusterSearchConnection(
		h.reqContext(r),
		localDomain,
		remoteDomain,
		req.ConnectionAlias,
	)
	if createErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

		return
	}

	// CreateOutboundCrossClusterSearchConnectionOutput is flat -- unlike
	// Delete/Accept/Reject, it has no CrossClusterSearchConnection wrapper
	// (deserializers.go:1253's case list is ConnectionAlias/ConnectionStatus/
	// CrossClusterSearchConnectionId/SourceDomainInfo/DestinationDomainInfo
	// directly at the response root).
	h.writeJSON(r, w, toOutboundConnectionJSON(conn))
}

func toOutboundConnectionJSON(c *OutboundConnection) outboundConnectionJSON {
	return outboundConnectionJSON{
		CrossClusterSearchConnectionID: c.ConnectionID,
		ConnectionAlias:                c.ConnectionAlias,
		ConnectionStatus:               outboundConnectionStatusJSON{StatusCode: c.ConnectionStatus},
		SourceDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.LocalDomainInfo.OwnerID,
			DomainName: c.LocalDomainInfo.DomainName,
			Region:     c.LocalDomainInfo.Region,
		},
		DestinationDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.RemoteDomainInfo.OwnerID,
			DomainName: c.RemoteDomainInfo.DomainName,
			Region:     c.RemoteDomainInfo.Region,
		},
	}
}

func (h *Handler) handleDescribeOutboundCrossClusterSearchConnections(w http.ResponseWriter, r *http.Request) {
	describeCrossClusterConnections(
		h, w, r,
		h.Backend.DescribeOutboundCrossClusterSearchConnections,
		outboundConnectionFilterValue,
		func(c *OutboundConnection) any { return toOutboundConnectionJSON(c) },
	)
}

// outboundConnectionFilterValue resolves the five real Filternames
// DescribeOutboundCrossClusterSearchConnections documents (api_op_
// DescribeOutboundCrossClusterSearchConnections.go's Input doc comment)
// against one connection.
func outboundConnectionFilterValue(c *OutboundConnection) func(string) (string, bool) {
	return func(name string) (string, bool) {
		switch name {
		case "cross-cluster-search-connection-id":
			return c.ConnectionID, true
		case "destination-domain-info.domain-name":
			return c.RemoteDomainInfo.DomainName, true
		case "destination-domain-info.owner-id":
			return c.RemoteDomainInfo.OwnerID, true
		case "destination-domain-info.region":
			return c.RemoteDomainInfo.Region, true
		case "source-domain-info.domain-name":
			return c.LocalDomainInfo.DomainName, true
		default:
			return "", false
		}
	}
}

func (h *Handler) handleDeleteOutboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchCCSOutbound+"/")
	connection, err := h.Backend.DeleteOutboundCrossClusterSearchConnection(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{keyCrossClusterSearchConnection: toOutboundConnectionJSON(connection)})
}
