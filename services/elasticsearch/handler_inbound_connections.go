package elasticsearch

import (
	"errors"
	"net/http"
	"strings"
)

// inboundConnectionJSON is the JSON representation of an inbound cross-cluster connection.
type inboundConnectionJSON struct {
	CrossClusterSearchConnectionID string                      `json:"CrossClusterSearchConnectionId"`
	ConnectionStatus               inboundConnectionStatusJSON `json:"ConnectionStatus"`
	SourceDomainInfo               crossClusterDomainInfoJSON  `json:"SourceDomainInfo"`
	DestinationDomainInfo          crossClusterDomainInfoJSON  `json:"DestinationDomainInfo"`
}

type inboundConnectionStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

type crossClusterDomainInfoJSON struct {
	OwnerID    string `json:"OwnerId"`
	DomainName string `json:"DomainName"`
	Region     string `json:"Region"`
}

// acceptInboundConnectionOutput wraps the accepted connection.
type acceptInboundConnectionOutput struct {
	CrossClusterSearchConnection inboundConnectionJSON `json:"CrossClusterSearchConnection"`
}

func (h *Handler) handleAcceptInboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	// Path: /2015-01-01/es/ccs/inboundConnection/{connectionId}/accept
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchCCSInbound+"/")
	connectionID, _ := strings.CutSuffix(rest, "/accept")

	conn, err := h.Backend.AcceptInboundCrossClusterSearchConnection(h.reqContext(r), connectionID)
	if err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, acceptInboundConnectionOutput{
		CrossClusterSearchConnection: toInboundConnectionJSON(conn),
	})
}

func toInboundConnectionJSON(c *InboundConnection) inboundConnectionJSON {
	return inboundConnectionJSON{
		CrossClusterSearchConnectionID: c.ConnectionID,
		ConnectionStatus:               inboundConnectionStatusJSON{StatusCode: c.ConnectionStatus},
		SourceDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.SourceDomainInfo.OwnerID,
			DomainName: c.SourceDomainInfo.DomainName,
			Region:     c.SourceDomainInfo.Region,
		},
		DestinationDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.DestDomainInfo.OwnerID,
			DomainName: c.DestDomainInfo.DomainName,
			Region:     c.DestDomainInfo.Region,
		},
	}
}

func (h *Handler) handleDescribeInboundCrossClusterSearchConnections(w http.ResponseWriter, r *http.Request) {
	connections := h.Backend.DescribeInboundCrossClusterSearchConnections(h.reqContext(r))
	result := make([]inboundConnectionJSON, 0, len(connections))
	for _, connection := range connections {
		result = append(result, toInboundConnectionJSON(connection))
	}

	h.writeJSON(r, w, map[string]any{"CrossClusterSearchConnections": result})
}

func (h *Handler) handleDeleteInboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchCCSInbound+"/")
	connection, err := h.Backend.DeleteInboundCrossClusterSearchConnection(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{keyCrossClusterSearchConnection: toInboundConnectionJSON(connection)})
}

func (h *Handler) handleRejectInboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path, elasticsearchCCSInbound+"/", "/reject")
	connection, err := h.Backend.RejectInboundCrossClusterSearchConnection(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{keyCrossClusterSearchConnection: toInboundConnectionJSON(connection)})
}
