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

	var mlv int32
	if in.ManagedLoginVersion != nil {
		mlv = *in.ManagedLoginVersion
	}

	d, err := h.Backend.CreateUserPoolDomainFull(in.UserPoolID, in.Domain, certArn, mlv)
	if err != nil {
		return nil, err
	}

	// AWS only returns CloudFrontDomain for custom domains (with a certificate).
	// Managed Cognito domains return an empty response.
	cfDomain := ""
	if certArn != "" {
		cfDomain = d.CloudFrontDistribution
	}

	return &createUserPoolDomainFullOutput{
		CloudFrontDomain:    cfDomain,
		ManagedLoginVersion: &d.ManagedLoginVersion,
	}, nil
}

func (h *Handler) handleUpdateUserPoolDomainFull(
	_ context.Context,
	in *updateUserPoolDomainFullInput,
) (*updateUserPoolDomainFullOutput, error) {
	certArn := ""
	if in.CustomDomainConfig != nil {
		certArn = in.CustomDomainConfig.CertificateArn
	}

	var mlv int32
	if in.ManagedLoginVersion != nil {
		mlv = *in.ManagedLoginVersion
	}

	d, err := h.Backend.UpdateUserPoolDomainFull(in.UserPoolID, in.Domain, certArn, mlv)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolDomainFullOutput{
		CloudFrontDomain:    d.CloudFrontDistribution,
		ManagedLoginVersion: &d.ManagedLoginVersion,
	}, nil
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

	desc := &userPoolDomainDescription{
		AWSAccountID:           d.AWSAccountID,
		Domain:                 d.Domain,
		UserPoolID:             d.UserPoolID,
		Status:                 d.Status,
		CloudFrontDistribution: d.CloudFrontDistribution,
		S3Bucket:               d.S3Bucket,
		ManagedLoginVersion:    &d.ManagedLoginVersion,
	}

	// AWS only echoes CustomDomainConfig back for a custom domain (one with an ACM
	// certificate); a managed Cognito-prefix domain has none.
	if d.CertificateArn != "" {
		desc.CustomDomainConfig = &customDomainConfigJSON{CertificateArn: d.CertificateArn}
	}

	return &describeUserPoolDomainOutput{DomainDescription: desc}, nil
}

func (h *Handler) domainsOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DeleteUserPoolDomain":   service.WrapOp(h.handleDeleteUserPoolDomain),
		"DescribeUserPoolDomain": service.WrapOp(h.handleDescribeUserPoolDomain),
	}
}

func (h *Handler) domainsOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateUserPoolDomain: wrapAccuracy(h.handleCreateUserPoolDomainFull),
		opUpdateUserPoolDomain: wrapAccuracy(h.handleUpdateUserPoolDomainFull),
	}
}
