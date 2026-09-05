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
	describeCrossClusterConnections(
		h, w, r,
		h.Backend.DescribeInboundCrossClusterSearchConnections,
		inboundConnectionFilterValue,
		func(c *InboundConnection) any { return toInboundConnectionJSON(c) },
	)
}

// inboundConnectionFilterValue resolves the five real Filternames
// DescribeInboundCrossClusterSearchConnections documents (api_op_
// DescribeInboundCrossClusterSearchConnections.go's Input doc comment)
// against one connection.
func inboundConnectionFilterValue(c *InboundConnection) func(string) (string, bool) {
	return func(name string) (string, bool) {
		switch name {
		case "cross-cluster-search-connection-id":
			return c.ConnectionID, true
		case "source-domain-info.domain-name":
			return c.SourceDomainInfo.DomainName, true
		case "source-domain-info.owner-id":
			return c.SourceDomainInfo.OwnerID, true
		case "source-domain-info.region":
			return c.SourceDomainInfo.Region, true
		case "destination-domain-info.domain-name":
			return c.DestDomainInfo.DomainName, true
		default:
			return "", false
		}
	}
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
