package workmail

import (
	"context"
)

// ---- Mail Domains ----

type registerMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleRegisterMailDomain(_ context.Context, req *registerMailDomainReq) (*emptyResp, error) {
	if err := h.Backend.RegisterMailDomain(req.OrganizationID, req.DomainName); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deregisterMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleDeregisterMailDomain(_ context.Context, req *deregisterMailDomainReq) (*emptyResp, error) {
	if err := h.Backend.DeregisterMailDomain(req.OrganizationID, req.DomainName); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type getMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

type getMailDomainResp struct {
	DomainName                  string `json:"DomainName,omitempty"`
	OwnershipVerificationStatus string `json:"OwnershipVerificationStatus,omitempty"`
	IsDefault                   bool   `json:"IsDefault"`
	IsTestDomain                bool   `json:"IsTestDomain"`
}

func (h *Handler) handleGetMailDomain(_ context.Context, req *getMailDomainReq) (*getMailDomainResp, error) {
	d, err := h.Backend.GetMailDomain(req.OrganizationID, req.DomainName)
	if err != nil {
		return nil, err
	}

	return &getMailDomainResp{
		DomainName:                  d.DomainName,
		IsDefault:                   d.IsDefault,
		IsTestDomain:                d.IsTestDomain,
		OwnershipVerificationStatus: d.OwnershipVerificationStatus,
	}, nil
}

type listMailDomainsReq struct {
	OrganizationID string `json:"OrganizationId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

// mailDomainSummaryResp mirrors aws-sdk-go-v2/service/workmail/types.
// MailDomainSummary (the ListMailDomains item shape), which is a distinct,
// narrower type from GetMailDomainOutput: the wire key is "DefaultDomain",
// not "IsDefault", and there is no IsTestDomain field at all.
type mailDomainSummaryResp struct {
	DomainName    string `json:"DomainName"`
	DefaultDomain bool   `json:"DefaultDomain"`
}

type listMailDomainsResp struct {
	NextToken   string                  `json:"NextToken,omitempty"`
	MailDomains []mailDomainSummaryResp `json:"MailDomains"`
}

func (h *Handler) handleListMailDomains(_ context.Context, req *listMailDomainsReq) (*listMailDomainsResp, error) {
	domains, next, err := h.Backend.ListMailDomains(req.OrganizationID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	dresps := make([]mailDomainSummaryResp, 0, len(domains))
	for _, d := range domains {
		dresps = append(dresps, mailDomainSummaryResp{
			DomainName:    d.DomainName,
			DefaultDomain: d.IsDefault,
		})
	}

	return &listMailDomainsResp{MailDomains: dresps, NextToken: next}, nil
}

type updateDefaultMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleUpdateDefaultMailDomain(
	_ context.Context,
	req *updateDefaultMailDomainReq,
) (*emptyResp, error) {
	if err := h.Backend.UpdateDefaultMailDomain(req.OrganizationID, req.DomainName); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}
