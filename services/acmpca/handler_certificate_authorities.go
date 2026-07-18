package acmpca

import (
	"context"
	"encoding/json"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type caConfigSubjectInput struct {
	CommonName         string `json:"CommonName"`
	Country            string `json:"Country"`
	Organization       string `json:"Organization"`
	OrganizationalUnit string `json:"OrganizationalUnit"`
	State              string `json:"State"`
	Locality           string `json:"Locality"`
}

type caConfigInput struct {
	Subject          caConfigSubjectInput `json:"Subject"`
	KeyAlgorithm     string               `json:"KeyAlgorithm"`
	SigningAlgorithm string               `json:"SigningAlgorithm"`
}

type createCertificateAuthorityInput struct {
	CertificateAuthorityConfiguration caConfigInput `json:"CertificateAuthorityConfiguration"`
	CertificateAuthorityType          string        `json:"CertificateAuthorityType"`
	Tags                              []svcTags.KV  `json:"Tags"`
}

type createCertificateAuthorityOutput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type describeCertificateAuthorityInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type caConfigSubjectOutput struct {
	CommonName         string `json:"CommonName,omitempty"`
	Country            string `json:"Country,omitempty"`
	Organization       string `json:"Organization,omitempty"`
	OrganizationalUnit string `json:"OrganizationalUnit,omitempty"`
	State              string `json:"State,omitempty"`
	Locality           string `json:"Locality,omitempty"`
}

type caConfigOutput struct {
	Subject          caConfigSubjectOutput `json:"Subject"`
	KeyAlgorithm     string                `json:"KeyAlgorithm"`
	SigningAlgorithm string                `json:"SigningAlgorithm"`
}

type revocationConfigOutput struct{}

type certAuthorityOutput struct {
	CertificateAuthorityConfiguration caConfigOutput         `json:"CertificateAuthorityConfiguration"`
	RevocationConfiguration           revocationConfigOutput `json:"RevocationConfiguration"`
	Arn                               string                 `json:"Arn"`
	OwnerAccount                      string                 `json:"OwnerAccount,omitempty"`
	Type                              string                 `json:"Type"`
	Status                            string                 `json:"Status"`
	Serial                            string                 `json:"Serial,omitempty"`
	CreatedAt                         int64                  `json:"CreatedAt"`
	NotBefore                         int64                  `json:"NotBefore,omitempty"`
	NotAfter                          int64                  `json:"NotAfter,omitempty"`
	RestorableUntil                   int64                  `json:"RestorableUntil,omitempty"`
}

type describeCertificateAuthorityOutput struct {
	CertificateAuthority certAuthorityOutput `json:"CertificateAuthority"`
}

type listCertificateAuthoritiesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listCertificateAuthoritiesOutput struct {
	NextToken              string                `json:"NextToken,omitempty"`
	CertificateAuthorities []certAuthorityOutput `json:"CertificateAuthorities"`
}

type deleteCertificateAuthorityInput struct {
	CertificateAuthorityArn     string `json:"CertificateAuthorityArn"`
	PermanentDeletionTimeInDays int32  `json:"PermanentDeletionTimeInDays"`
}

type deleteCertificateAuthorityOutput struct{}

type updateCertificateAuthorityInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	Status                  string `json:"Status"`
}

type updateCertificateAuthorityOutput struct{}

type getCertificateAuthorityCsrInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type getCertificateAuthorityCsrOutput struct {
	Csr string `json:"Csr"`
}

type importCertificateAuthorityCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	Certificate             string `json:"Certificate"`
	CertificateChain        string `json:"CertificateChain"`
}

type importCertificateAuthorityCertificateOutput struct{}

type getCertificateAuthorityCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type getCertificateAuthorityCertificateOutput struct {
	Certificate      string `json:"Certificate"`
	CertificateChain string `json:"CertificateChain,omitempty"`
}

type restoreCertificateAuthorityInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type restoreCertificateAuthorityOutput struct{}

func (h *Handler) jsonCreateCA(ctx context.Context, body []byte) (any, error) {
	var input createCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	cfg := CertificateAuthorityConfiguration{
		Subject: CertificateAuthoritySubject{
			CommonName:         input.CertificateAuthorityConfiguration.Subject.CommonName,
			Country:            input.CertificateAuthorityConfiguration.Subject.Country,
			Organization:       input.CertificateAuthorityConfiguration.Subject.Organization,
			OrganizationalUnit: input.CertificateAuthorityConfiguration.Subject.OrganizationalUnit,
			State:              input.CertificateAuthorityConfiguration.Subject.State,
			Locality:           input.CertificateAuthorityConfiguration.Subject.Locality,
		},
		KeyAlgorithm:     input.CertificateAuthorityConfiguration.KeyAlgorithm,
		SigningAlgorithm: input.CertificateAuthorityConfiguration.SigningAlgorithm,
	}

	ca, err := h.Backend.CreateCertificateAuthority(ctx, input.CertificateAuthorityType, cfg)
	if err != nil {
		return nil, err
	}

	if len(input.Tags) > 0 {
		kv := make(map[string]string, len(input.Tags))
		for _, t := range input.Tags {
			kv[t.Key] = t.Value
		}

		h.setTags(ca.ARN, kv)
	}

	return &createCertificateAuthorityOutput{CertificateAuthorityArn: ca.ARN}, nil
}

func (h *Handler) jsonDescribeCA(ctx context.Context, body []byte) (any, error) {
	var input describeCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	ca, err := h.Backend.DescribeCertificateAuthority(ctx, input.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}

	return &describeCertificateAuthorityOutput{CertificateAuthority: toCAOutput(ca)}, nil
}

func (h *Handler) jsonListCAs(ctx context.Context, body []byte) (any, error) {
	var input listCertificateAuthoritiesInput
	_ = json.Unmarshal(body, &input)

	p := h.Backend.ListCertificateAuthorities(ctx, input.NextToken, input.MaxResults)
	cas := make([]certAuthorityOutput, 0, len(p.Data))

	for _, ca := range p.Data {
		cas = append(cas, toCAOutput(&ca))
	}

	return &listCertificateAuthoritiesOutput{
		CertificateAuthorities: cas,
		NextToken:              p.Next,
	}, nil
}

func (h *Handler) jsonDeleteCA(ctx context.Context, body []byte) (any, error) {
	var input deleteCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.DeleteCertificateAuthority(
		ctx,
		input.CertificateAuthorityArn,
		input.PermanentDeletionTimeInDays,
	); err != nil {
		return nil, err
	}

	h.cleanupTags(input.CertificateAuthorityArn)

	return &deleteCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonUpdateCA(ctx context.Context, body []byte) (any, error) {
	var input updateCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.UpdateCertificateAuthority(ctx, input.CertificateAuthorityArn, input.Status); err != nil {
		return nil, err
	}

	return &updateCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonGetCsr(ctx context.Context, body []byte) (any, error) {
	var input getCertificateAuthorityCsrInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	csr, err := h.Backend.GetCertificateAuthorityCsr(ctx, input.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}

	return &getCertificateAuthorityCsrOutput{Csr: csr}, nil
}

func (h *Handler) jsonImportCACert(ctx context.Context, body []byte) (any, error) {
	var input importCertificateAuthorityCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	certPEM, err := decodeBase64Field(input.Certificate, "Certificate")
	if err != nil {
		return nil, err
	}

	chainPEM, err := decodeBase64Field(input.CertificateChain, "CertificateChain")
	if err != nil {
		return nil, err
	}

	if importErr := h.Backend.ImportCertificateAuthorityCertificate(
		ctx,
		input.CertificateAuthorityArn,
		certPEM,
		chainPEM,
	); importErr != nil {
		return nil, importErr
	}

	return &importCertificateAuthorityCertificateOutput{}, nil
}

func (h *Handler) jsonGetCACert(ctx context.Context, body []byte) (any, error) {
	var input getCertificateAuthorityCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	certPEM, chainPEM, err := h.Backend.GetCertificateAuthorityCertificate(ctx, input.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}

	return &getCertificateAuthorityCertificateOutput{Certificate: certPEM, CertificateChain: chainPEM}, nil
}

func (h *Handler) jsonRestoreCA(ctx context.Context, body []byte) (any, error) {
	var input restoreCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.RestoreCertificateAuthority(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	return &restoreCertificateAuthorityOutput{}, nil
}

func toCAOutput(ca *CertificateAuthority) certAuthorityOutput {
	out := certAuthorityOutput{
		Arn:          ca.ARN,
		OwnerAccount: ca.OwnerAccount,
		Type:         ca.Type,
		Status:       ca.Status,
		Serial:       ca.Serial,
		CreatedAt:    ca.CreatedAt.Unix(),
		CertificateAuthorityConfiguration: caConfigOutput{
			Subject: caConfigSubjectOutput{
				CommonName:         ca.CertificateAuthorityConfiguration.Subject.CommonName,
				Country:            ca.CertificateAuthorityConfiguration.Subject.Country,
				Organization:       ca.CertificateAuthorityConfiguration.Subject.Organization,
				OrganizationalUnit: ca.CertificateAuthorityConfiguration.Subject.OrganizationalUnit,
				State:              ca.CertificateAuthorityConfiguration.Subject.State,
				Locality:           ca.CertificateAuthorityConfiguration.Subject.Locality,
			},
			KeyAlgorithm:     ca.CertificateAuthorityConfiguration.KeyAlgorithm,
			SigningAlgorithm: ca.CertificateAuthorityConfiguration.SigningAlgorithm,
		},
	}

	if !ca.NotBefore.IsZero() {
		out.NotBefore = ca.NotBefore.Unix()
	}

	if !ca.NotAfter.IsZero() {
		out.NotAfter = ca.NotAfter.Unix()
	}

	if !ca.RestorableUntil.IsZero() {
		out.RestorableUntil = ca.RestorableUntil.Unix()
	}

	return out
}
