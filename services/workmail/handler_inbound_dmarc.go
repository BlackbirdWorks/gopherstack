package workmail

import (
	"context"
)

// ---- Inbound DMARC Settings ----

type putInboundDmarcSettingsReq struct {
	OrganizationID string `json:"OrganizationId"`
	Enforced       bool   `json:"Enforced"`
}

func (h *Handler) handlePutInboundDmarcSettings(
	_ context.Context, req *putInboundDmarcSettingsReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.PutInboundDmarcSettings(req.OrganizationID, req.Enforced)
}

type describeInboundDmarcSettingsReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeInboundDmarcSettingsResp struct {
	Enforced bool `json:"Enforced"`
}

func (h *Handler) handleDescribeInboundDmarcSettings(
	_ context.Context, req *describeInboundDmarcSettingsReq,
) (*describeInboundDmarcSettingsResp, error) {
	enforced, err := h.Backend.DescribeInboundDmarcSettings(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	return &describeInboundDmarcSettingsResp{Enforced: enforced}, nil
}
