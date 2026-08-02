package networkmanager

import (
	"context"
	"net/http"
)

// associationsRoutes wires PARITY.md families G-J (12 ops).
func (h *Handler) associationsRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segCustomerGWAssn},
			op:      "AssociateCustomerGateway",
			fn:      h.dispatchAssociateCustomerGateway,
		},
		{
			method: http.MethodDelete,
			pattern: []string{
				segGlobalNetworks,
				paramGlobalNetworkID,
				segCustomerGWAssn,
				":CustomerGatewayArn",
			},
			op: "DisassociateCustomerGateway",
			fn: h.dispatchDisassociateCustomerGateway,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segCustomerGWAssn},
			op:      "GetCustomerGatewayAssociations",
			fn:      h.dispatchGetCustomerGatewayAssociations,
		},

		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segTgwRegistrations},
			op:      "RegisterTransitGateway",
			fn:      h.dispatchRegisterTransitGateway,
		},
		{
			method: http.MethodDelete,
			pattern: []string{
				segGlobalNetworks,
				paramGlobalNetworkID,
				segTgwRegistrations,
				":TransitGatewayArn",
			},
			op: "DeregisterTransitGateway",
			fn: h.dispatchDeregisterTransitGateway,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segTgwRegistrations},
			op:      "GetTransitGatewayRegistrations",
			fn:      h.dispatchGetTransitGatewayRegistrations,
		},

		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segTgwConnectPeerAssn},
			op:      "AssociateTransitGatewayConnectPeer",
			fn:      h.dispatchAssociateTransitGatewayConnectPeer,
		},
		{
			method: http.MethodDelete,
			pattern: []string{
				segGlobalNetworks,
				paramGlobalNetworkID,
				segTgwConnectPeerAssn,
				":TransitGatewayConnectPeerArn",
			},
			op: "DisassociateTransitGatewayConnectPeer",
			fn: h.dispatchDisassociateTransitGatewayConnectPeer,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segTgwConnectPeerAssn},
			op:      "GetTransitGatewayConnectPeerAssociations",
			fn:      h.dispatchGetTransitGatewayConnectPeerAssociations,
		},

		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segConnectPeerAssn},
			op:      "AssociateConnectPeer",
			fn:      h.dispatchAssociateConnectPeer,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segConnectPeerAssn, paramConnectPeerID},
			op:      "DisassociateConnectPeer",
			fn:      h.dispatchDisassociateConnectPeer,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segConnectPeerAssn},
			op:      "GetConnectPeerAssociations",
			fn:      h.dispatchGetConnectPeerAssociations,
		},
	}
}

// ---- Customer Gateway Association ----

func (h *Handler) dispatchAssociateCustomerGateway(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req associateCustomerGatewayReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.AssociateCustomerGateway(
		params["GlobalNetworkId"],
		req.CustomerGatewayArn,
		req.DeviceID,
		req.LinkID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		customerGatewayAssociationEnvelope{CustomerGatewayAssociation: toCustomerGatewayAssociationWire(a)},
	)
}

func (h *Handler) dispatchDisassociateCustomerGateway(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.DisassociateCustomerGateway(params["GlobalNetworkId"], params["CustomerGatewayArn"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		customerGatewayAssociationEnvelope{CustomerGatewayAssociation: toCustomerGatewayAssociationWire(a)},
	)
}

func (h *Handler) dispatchGetCustomerGatewayAssociations(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetCustomerGatewayAssociations(
		params["GlobalNetworkId"], q["customerGatewayArns"], queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]customerGatewayAssociationWire, len(p.Data))
	for i, a := range p.Data {
		out[i] = *toCustomerGatewayAssociationWire(a)
	}

	return marshalResponse(getCustomerGatewayAssociationsResponse{CustomerGatewayAssociations: out, NextToken: p.Next})
}

// ---- Transit Gateway Registration ----

func (h *Handler) dispatchRegisterTransitGateway(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req registerTransitGatewayReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	r, err := h.Backend.RegisterTransitGateway(params["GlobalNetworkId"], req.TransitGatewayArn)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		transitGatewayRegistrationEnvelope{TransitGatewayRegistration: toTransitGatewayRegistrationWire(r)},
	)
}

func (h *Handler) dispatchDeregisterTransitGateway(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	r, err := h.Backend.DeregisterTransitGateway(params["GlobalNetworkId"], params["TransitGatewayArn"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		transitGatewayRegistrationEnvelope{TransitGatewayRegistration: toTransitGatewayRegistrationWire(r)},
	)
}

func (h *Handler) dispatchGetTransitGatewayRegistrations(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetTransitGatewayRegistrations(
		params["GlobalNetworkId"], q["transitGatewayArns"], queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]transitGatewayRegistrationWire, len(p.Data))
	for i, v := range p.Data {
		out[i] = *toTransitGatewayRegistrationWire(v)
	}

	return marshalResponse(getTransitGatewayRegistrationsResponse{TransitGatewayRegistrations: out, NextToken: p.Next})
}

// ---- Transit Gateway Connect Peer Association ----

func (h *Handler) dispatchAssociateTransitGatewayConnectPeer(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req associateTransitGatewayConnectPeerReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.AssociateTransitGatewayConnectPeer(
		params["GlobalNetworkId"], req.DeviceID, req.TransitGatewayConnectPeerArn, req.LinkID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(transitGatewayConnectPeerAssociationEnvelope{
		TransitGatewayConnectPeerAssociation: toTransitGatewayConnectPeerAssociationWire(a),
	})
}

func (h *Handler) dispatchDisassociateTransitGatewayConnectPeer(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.DisassociateTransitGatewayConnectPeer(
		params["GlobalNetworkId"], params["TransitGatewayConnectPeerArn"],
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(transitGatewayConnectPeerAssociationEnvelope{
		TransitGatewayConnectPeerAssociation: toTransitGatewayConnectPeerAssociationWire(a),
	})
}

func (h *Handler) dispatchGetTransitGatewayConnectPeerAssociations(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetTransitGatewayConnectPeerAssociations(
		params["GlobalNetworkId"], q["transitGatewayConnectPeerArns"], queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]transitGatewayConnectPeerAssociationWire, len(p.Data))
	for i, v := range p.Data {
		out[i] = *toTransitGatewayConnectPeerAssociationWire(v)
	}

	return marshalResponse(getTransitGatewayConnectPeerAssociationsResponse{
		TransitGatewayConnectPeerAssociations: out, NextToken: p.Next,
	})
}

// ---- Connect Peer <-> Global Network association ----

func (h *Handler) dispatchAssociateConnectPeer(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req associateConnectPeerReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.AssociateConnectPeer(params["GlobalNetworkId"], req.ConnectPeerID, req.DeviceID, req.LinkID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectPeerAssociationEnvelope{ConnectPeerAssociation: toConnectPeerAssociationWire(a)})
}

func (h *Handler) dispatchDisassociateConnectPeer(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.DisassociateConnectPeer(params["GlobalNetworkId"], params["ConnectPeerId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectPeerAssociationEnvelope{ConnectPeerAssociation: toConnectPeerAssociationWire(a)})
}

func (h *Handler) dispatchGetConnectPeerAssociations(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetConnectPeerAssociations(
		params["GlobalNetworkId"],
		q["connectPeerIds"],
		queryNextToken(q),
		queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]connectPeerAssociationWire, len(p.Data))
	for i, v := range p.Data {
		out[i] = *toConnectPeerAssociationWire(v)
	}

	return marshalResponse(getConnectPeerAssociationsResponse{ConnectPeerAssociations: out, NextToken: p.Next})
}
