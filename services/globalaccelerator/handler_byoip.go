package globalaccelerator

import "context"

func registerByoipOps(ops map[string]opFunc) {
	ops["ProvisionByoipCidr"] = bind((*Handler).provisionByoip)
	ops["AdvertiseByoipCidr"] = bind((*Handler).advertiseByoip)
	ops["WithdrawByoipCidr"] = bind((*Handler).withdrawByoip)
	ops["DeprovisionByoipCidr"] = bind((*Handler).deprovisionByoip)
	ops["ListByoipCidrs"] = bind((*Handler).listByoip)
}

func byoipResult(c *ByoipCidr, err error) (*byoipResp, error) {
	if err != nil {
		return nil, err
	}

	return &byoipResp{ByoipCidr: byoipToWire(c)}, nil
}

func (h *Handler) provisionByoip(_ context.Context, in *provisionByoipReq) (*byoipResp, error) {
	var message, signature string
	if in.AuthorizationContext != nil {
		message, signature = in.AuthorizationContext.Message, in.AuthorizationContext.Signature
	}

	return byoipResult(h.Backend.ProvisionByoipCidr(in.Cidr, message, signature))
}

func (h *Handler) advertiseByoip(_ context.Context, in *cidrReq) (*byoipResp, error) {
	return byoipResult(h.Backend.AdvertiseByoipCidr(in.Cidr))
}

func (h *Handler) withdrawByoip(_ context.Context, in *cidrReq) (*byoipResp, error) {
	return byoipResult(h.Backend.WithdrawByoipCidr(in.Cidr))
}

func (h *Handler) deprovisionByoip(_ context.Context, in *cidrReq) (*byoipResp, error) {
	return byoipResult(h.Backend.DeprovisionByoipCidr(in.Cidr))
}

func (h *Handler) listByoip(_ context.Context, in *listPageReq) (*listByoipResp, error) {
	pg, err := h.Backend.ListByoipCidrs(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listByoipResp{NextToken: pg.Next, ByoipCidrs: mapPage(pg, byoipToWire)}, nil
}
