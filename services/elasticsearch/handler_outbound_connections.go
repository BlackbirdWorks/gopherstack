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
	LocalDomainInfo                crossClusterDomainInfoJSON   `json:"LocalDomainInfo"`
	RemoteDomainInfo               crossClusterDomainInfoJSON   `json:"RemoteDomainInfo"`
}

type outboundConnectionStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

// createOutboundConnectionRequest is the JSON body for CreateOutboundCrossClusterSearchConnection.
type createOutboundConnectionRequest struct {
	LocalDomainInfo  crossClusterDomainInfoJSON `json:"LocalDomainInfo"`
	RemoteDomainInfo crossClusterDomainInfoJSON `json:"RemoteDomainInfo"`
	ConnectionAlias  string                     `json:"ConnectionAlias"`
}

// createOutboundConnectionOutput wraps the new outbound connection.
type createOutboundConnectionOutput struct {
	CrossClusterSearchConnection outboundConnectionJSON `json:"CrossClusterSearchConnection"`
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
		OwnerID:    req.LocalDomainInfo.OwnerID,
		DomainName: req.LocalDomainInfo.DomainName,
		Region:     req.LocalDomainInfo.Region,
	}
	remoteDomain := CrossClusterDomainInfo{
		OwnerID:    req.RemoteDomainInfo.OwnerID,
		DomainName: req.RemoteDomainInfo.DomainName,
		Region:     req.RemoteDomainInfo.Region,
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

	h.writeJSON(r, w, createOutboundConnectionOutput{
		CrossClusterSearchConnection: toOutboundConnectionJSON(conn),
	})
}

func toOutboundConnectionJSON(c *OutboundConnection) outboundConnectionJSON {
	return outboundConnectionJSON{
		CrossClusterSearchConnectionID: c.ConnectionID,
		ConnectionAlias:                c.ConnectionAlias,
		ConnectionStatus:               outboundConnectionStatusJSON{StatusCode: c.ConnectionStatus},
		LocalDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.LocalDomainInfo.OwnerID,
			DomainName: c.LocalDomainInfo.DomainName,
			Region:     c.LocalDomainInfo.Region,
		},
		RemoteDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.RemoteDomainInfo.OwnerID,
			DomainName: c.RemoteDomainInfo.DomainName,
			Region:     c.RemoteDomainInfo.Region,
		},
	}
}

func (h *Handler) handleDescribeOutboundCrossClusterSearchConnections(w http.ResponseWriter, r *http.Request) {
	connections := h.Backend.DescribeOutboundCrossClusterSearchConnections(h.reqContext(r))
	result := make([]outboundConnectionJSON, 0, len(connections))
	for _, connection := range connections {
		result = append(result, toOutboundConnectionJSON(connection))
	}

	h.writeJSON(r, w, map[string]any{"CrossClusterSearchConnections": result})
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
