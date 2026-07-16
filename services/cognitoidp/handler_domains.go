package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleCreateUserPoolDomainFull(
	_ context.Context,
	in *createUserPoolDomainFullInput,
) (*createUserPoolDomainFullOutput, error) {
	certArn := ""
	if in.CustomDomainConfig != nil {
		certArn = in.CustomDomainConfig.CertificateArn
	}

	d, err := h.Backend.CreateUserPoolDomainFull(in.UserPoolID, in.Domain, certArn)
	if err != nil {
		return nil, err
	}

	// AWS only returns CloudFrontDomain for custom domains (with a certificate).
	// Managed Cognito domains return an empty response.
	cfDomain := ""
	if certArn != "" {
		cfDomain = d.CloudFrontDistribution
	}

	return &createUserPoolDomainFullOutput{CloudFrontDomain: cfDomain}, nil
}

func (h *Handler) handleUpdateUserPoolDomainFull(
	_ context.Context,
	in *updateUserPoolDomainFullInput,
) (*updateUserPoolDomainFullOutput, error) {
	certArn := ""
	if in.CustomDomainConfig != nil {
		certArn = in.CustomDomainConfig.CertificateArn
	}

	cfDomain, err := h.Backend.UpdateUserPoolDomainFull(in.UserPoolID, in.Domain, certArn)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolDomainFullOutput{CloudFrontDomain: cfDomain}, nil
}

func (h *Handler) handleCreateUserPoolDomain(
	_ context.Context,
	in *createUserPoolDomainInput,
) (*createUserPoolDomainOutput, error) {
	d, err := h.Backend.CreateUserPoolDomain(in.UserPoolID, in.Domain)
	if err != nil {
		return nil, err
	}

	return &createUserPoolDomainOutput{CloudFrontDomain: d.CloudFrontDistribution}, nil
}

func (h *Handler) handleDeleteUserPoolDomain(
	_ context.Context,
	in *deleteUserPoolDomainInput,
) (*deleteUserPoolDomainOutput, error) {
	if err := h.Backend.DeleteUserPoolDomain(in.UserPoolID, in.Domain); err != nil {
		return nil, err
	}

	return &deleteUserPoolDomainOutput{}, nil
}

func (h *Handler) handleDescribeUserPoolDomain(
	_ context.Context,
	in *describeUserPoolDomainInput,
) (*describeUserPoolDomainOutput, error) {
	// FindUserPoolDomain returns nil for unknown domains (AWS returns empty description, not an error).
	d := h.Backend.FindUserPoolDomain(in.Domain)
	if d == nil {
		return &describeUserPoolDomainOutput{DomainDescription: &userPoolDomainDescription{}}, nil
	}

	return &describeUserPoolDomainOutput{
		DomainDescription: &userPoolDomainDescription{
			Domain:                 d.Domain,
			UserPoolID:             d.UserPoolID,
			Status:                 d.Status,
			CloudFrontDistribution: d.CloudFrontDistribution,
		},
	}, nil
}

func (h *Handler) handleUpdateUserPoolDomain(
	_ context.Context,
	in *updateUserPoolDomainInput,
) (*updateUserPoolDomainOutput, error) {
	cfDomain, err := h.Backend.UpdateUserPoolDomain(in.UserPoolID, in.Domain)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolDomainOutput{CloudFrontDomain: cfDomain}, nil
}

func (h *Handler) domainsOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateUserPoolDomain":   service.WrapOp(h.handleCreateUserPoolDomain),
		"DeleteUserPoolDomain":   service.WrapOp(h.handleDeleteUserPoolDomain),
		"DescribeUserPoolDomain": service.WrapOp(h.handleDescribeUserPoolDomain),
		"UpdateUserPoolDomain":   service.WrapOp(h.handleUpdateUserPoolDomain),
	}
}

func (h *Handler) domainsOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateUserPoolDomain: wrapAccuracy(h.handleCreateUserPoolDomainFull),
		opUpdateUserPoolDomain: wrapAccuracy(h.handleUpdateUserPoolDomainFull),
	}
}
