package networkmanager

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// connectPeersRoutes wires PARITY.md family K (4 ops).
func (h *Handler) connectPeersRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segConnectPeers},
			op:      "CreateConnectPeer",
			fn:      h.dispatchCreateConnectPeer,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segConnectPeers, paramConnectPeerID},
			op:      "DeleteConnectPeer",
			fn:      h.dispatchDeleteConnectPeer,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segConnectPeers, paramConnectPeerID},
			op:      "GetConnectPeer",
			fn:      h.dispatchGetConnectPeer,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segConnectPeers},
			op:      "ListConnectPeers",
			fn:      h.dispatchListConnectPeers,
		},
	}
}

func (h *Handler) dispatchCreateConnectPeer(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createConnectPeerReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var bgp *BgpOptions
	if req.BgpOptions != nil {
		bgp = &BgpOptions{PeerAsn: req.BgpOptions.PeerAsn}
	}

	c, err := h.Backend.CreateConnectPeer(
		req.ConnectAttachmentID, req.PeerAddress, bgp, req.CoreNetworkAddress, req.SubnetArn, req.InsideCidrBlocks,
		tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectPeerEnvelope{ConnectPeer: toConnectPeerWire(c)})
}

func (h *Handler) dispatchDeleteConnectPeer(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	c, err := h.Backend.DeleteConnectPeer(params["ConnectPeerId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectPeerEnvelope{ConnectPeer: toConnectPeerWire(c)})
}

func (h *Handler) dispatchGetConnectPeer(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	c, err := h.Backend.GetConnectPeer(params["ConnectPeerId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectPeerEnvelope{ConnectPeer: toConnectPeerWire(c)})
}

func (h *Handler) dispatchListConnectPeers(
	_ context.Context,
	r *http.Request,
	_ routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.ListConnectPeers(
		q.Get("connectAttachmentId"),
		q.Get("coreNetworkId"),
		queryNextToken(q),
		queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]connectPeerSummaryWire, len(p.Data))
	for i, c := range p.Data {
		out[i] = toConnectPeerSummaryWire(c)
	}

	return marshalResponse(listConnectPeersResponse{ConnectPeers: out, NextToken: p.Next})
}
