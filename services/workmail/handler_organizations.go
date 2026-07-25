package workmail

import (
	"context"
)

// ---- request/response types ----

// domainReq mirrors the real Domain shape the SDK sends for
// CreateOrganizationInput.Domains: a list of objects, not bare strings (see
// aws-sdk-go-v2/service/workmail/types.Domain). HostedZoneId is Route53-only
// and has no meaning for the in-memory backend, so it is accepted but
// discarded.
type domainReq struct {
	DomainName   string `json:"DomainName"`
	HostedZoneID string `json:"HostedZoneId"`
}

type createOrgReq struct {
	Alias   string      `json:"Alias"`
	Domains []domainReq `json:"Domains"`
}

type createOrgResp struct {
	OrganizationID string `json:"OrganizationId"`
}

func (h *Handler) handleCreateOrganization(ctx context.Context, req *createOrgReq) (*createOrgResp, error) {
	domains := make([]string, 0, len(req.Domains))
	for _, d := range req.Domains {
		domains = append(domains, d.DomainName)
	}

	org, err := h.Backend.CreateOrganization(ctx, req.Alias, domains)
	if err != nil {
		return nil, err
	}

	return &createOrgResp{OrganizationID: org.OrgID}, nil
}

type describeOrgReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeOrgResp struct {
	OrganizationID          string `json:"OrganizationId"`
	Alias                   string `json:"Alias"`
	State                   string `json:"State"`
	ARN                     string `json:"ARN"`
	DirectoryID             string `json:"DirectoryId"`
	DirectoryType           string `json:"DirectoryType"`
	DefaultMailDomain       string `json:"DefaultMailDomain"`
	ErrorMessage            string `json:"ErrorMessage,omitempty"`
	MigrationAdmin          string `json:"MigrationAdmin,omitempty"`
	CompletedDate           int64  `json:"CompletedDate"`
	InteroperabilityEnabled bool   `json:"InteroperabilityEnabled"`
}

func (h *Handler) handleDescribeOrganization(_ context.Context, req *describeOrgReq) (*describeOrgResp, error) {
	org, err := h.Backend.DescribeOrganization(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	return &describeOrgResp{
		OrganizationID:    org.OrgID,
		Alias:             org.Alias,
		State:             org.State,
		ARN:               org.ARN,
		DirectoryID:       org.DirectoryID,
		DirectoryType:     org.DirectoryType,
		DefaultMailDomain: org.DefaultMailDomain,
		ErrorMessage:      org.ErrorMessage,
		MigrationAdmin:    org.MigrationAdmin,
		CompletedDate:     org.CompletedDate.Unix(),
	}, nil
}

type deleteOrgReq struct {
	OrganizationID  string `json:"OrganizationId"`
	DeleteDirectory bool   `json:"DeleteDirectory"`
}

type deleteOrgResp struct {
	OrganizationID string `json:"OrganizationId"`
	State          string `json:"State"`
}

func (h *Handler) handleDeleteOrganization(_ context.Context, req *deleteOrgReq) (*deleteOrgResp, error) {
	if err := h.Backend.DeleteOrganization(req.OrganizationID, req.DeleteDirectory); err != nil {
		return nil, err
	}

	return &deleteOrgResp{OrganizationID: req.OrganizationID, State: "DELETED"}, nil
}

type listOrgsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type orgSummaryResp struct {
	OrganizationID    string `json:"OrganizationId"`
	Alias             string `json:"Alias"`
	DefaultMailDomain string `json:"DefaultMailDomain,omitempty"`
	State             string `json:"State"`
	ErrorMessage      string `json:"ErrorMessage,omitempty"`
}

type listOrgsResp struct {
	NextToken             string           `json:"NextToken,omitempty"`
	OrganizationSummaries []orgSummaryResp `json:"OrganizationSummaries"`
}

func (h *Handler) handleListOrganizations(ctx context.Context, req *listOrgsReq) (*listOrgsResp, error) {
	orgs, next, err := h.Backend.ListOrganizations(ctx, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]orgSummaryResp, 0, len(orgs))
	for _, o := range orgs {
		summaries = append(summaries, orgSummaryResp{
			OrganizationID:    o.OrgID,
			Alias:             o.Alias,
			DefaultMailDomain: o.DefaultMailDomain,
			State:             o.State,
			ErrorMessage:      o.ErrorMessage,
		})
	}

	return &listOrgsResp{OrganizationSummaries: summaries, NextToken: next}, nil
}
