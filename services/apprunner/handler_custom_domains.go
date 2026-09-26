package apprunner

import (
	"context"
	"fmt"
)

type associateCustomDomainInput struct {
	EnableWWWSubdomain *bool  `json:"EnableWWWSubdomain"`
	ServiceArn         string `json:"ServiceArn"`
	DomainName         string `json:"DomainName"`
}

type certificateValidationRecordOutput struct {
	Name   string `json:"Name"`
	Type   string `json:"Type"`
	Value  string `json:"Value"`
	Status string `json:"Status"`
}

type customDomainOutput struct {
	DomainName                   string                              `json:"DomainName"`
	Status                       string                              `json:"Status"`
	CertificateValidationRecords []certificateValidationRecordOutput `json:"CertificateValidationRecords,omitempty"`
	EnableWWWSubdomain           bool                                `json:"EnableWWWSubdomain"`
}

type associateCustomDomainOutput struct {
	DNSTarget     string             `json:"DNSTarget"`
	ServiceArn    string             `json:"ServiceArn"`
	VpcDNSTargets []any              `json:"VpcDNSTargets"`
	CustomDomain  customDomainOutput `json:"CustomDomain"`
}

func toCustomDomainOutput(cd *CustomDomain) customDomainOutput {
	records := make([]certificateValidationRecordOutput, len(cd.CertificateValidationRecords))
	for i, r := range cd.CertificateValidationRecords {
		records[i] = certificateValidationRecordOutput(r)
	}

	return customDomainOutput{
		DomainName:                   cd.DomainName,
		Status:                       cd.Status,
		CertificateValidationRecords: records,
		EnableWWWSubdomain:           cd.EnableWWWSubdomain,
	}
}

func (h *Handler) handleAssociateCustomDomain(
	_ context.Context,
	in *associateCustomDomainInput,
) (*associateCustomDomainOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	if in.DomainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", errInvalidRequest)
	}

	enableWWW := true
	if in.EnableWWWSubdomain != nil {
		enableWWW = *in.EnableWWWSubdomain
	}

	cd, err := h.Backend.AssociateCustomDomain(in.ServiceArn, in.DomainName, enableWWW)
	if err != nil {
		return nil, err
	}

	svc, err := h.Backend.DescribeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &associateCustomDomainOutput{
		CustomDomain:  toCustomDomainOutput(cd),
		DNSTarget:     svc.ServiceURL,
		ServiceArn:    in.ServiceArn,
		VpcDNSTargets: []any{},
	}, nil
}

type disassociateCustomDomainInput struct {
	ServiceArn string `json:"ServiceArn"`
	DomainName string `json:"DomainName"`
}

type disassociateCustomDomainOutput struct {
	DNSTarget     string             `json:"DNSTarget"`
	ServiceArn    string             `json:"ServiceArn"`
	VpcDNSTargets []any              `json:"VpcDNSTargets"`
	CustomDomain  customDomainOutput `json:"CustomDomain"`
}

func (h *Handler) handleDisassociateCustomDomain(
	_ context.Context,
	in *disassociateCustomDomainInput,
) (*disassociateCustomDomainOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	if in.DomainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", errInvalidRequest)
	}

	cd, err := h.Backend.DisassociateCustomDomain(in.ServiceArn, in.DomainName)
	if err != nil {
		return nil, err
	}

	svc, err := h.Backend.DescribeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &disassociateCustomDomainOutput{
		CustomDomain:  toCustomDomainOutput(cd),
		DNSTarget:     svc.ServiceURL,
		ServiceArn:    in.ServiceArn,
		VpcDNSTargets: []any{},
	}, nil
}

type describeCustomDomainsInput struct {
	ServiceArn string `json:"ServiceArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type describeCustomDomainsOutput struct {
	DNSTarget     string               `json:"DNSTarget"`
	ServiceArn    string               `json:"ServiceArn"`
	NextToken     string               `json:"NextToken,omitempty"`
	CustomDomains []customDomainOutput `json:"CustomDomains"`
	VpcDNSTargets []any                `json:"VpcDNSTargets"`
}

func (h *Handler) handleDescribeCustomDomains(
	_ context.Context,
	in *describeCustomDomainsInput,
) (*describeCustomDomainsOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	domains, nextToken, dnsTarget, err := h.Backend.DescribeCustomDomains(
		in.ServiceArn, in.MaxResults, in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]customDomainOutput, 0, len(domains))
	for _, d := range domains {
		out = append(out, toCustomDomainOutput(d))
	}

	return &describeCustomDomainsOutput{
		CustomDomains: out,
		DNSTarget:     dnsTarget,
		ServiceArn:    in.ServiceArn,
		VpcDNSTargets: []any{},
		NextToken:     nextToken,
	}, nil
}
