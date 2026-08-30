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

// crlDistributionPointExtConfigWire mirrors types.CrlDistributionPointExtensionConfiguration.
type crlDistributionPointExtConfigWire struct {
	OmitExtension bool `json:"OmitExtension"`
}

// crlConfigWire mirrors types.CrlConfiguration.
type crlConfigWire struct {
	CrlDistributionPointExtensionConfiguration *crlDistributionPointExtConfigWire `json:"CrlDistributionPointExtensionConfiguration,omitempty"` //nolint:lll // SDK field name
	CustomCname                                string                             `json:"CustomCname,omitempty"`
	CustomPath                                 string                             `json:"CustomPath,omitempty"`
	S3BucketName                               string                             `json:"S3BucketName,omitempty"`
	S3ObjectACL                                string                             `json:"S3ObjectAcl,omitempty"`
	CrlType                                    string                             `json:"CrlType,omitempty"`
	ExpirationInDays                           int32                              `json:"ExpirationInDays,omitempty"`
	Enabled                                    bool                               `json:"Enabled"`
}

// ocspConfigWire mirrors types.OcspConfiguration.
type ocspConfigWire struct {
	OcspCustomCname string `json:"OcspCustomCname,omitempty"`
	Enabled         bool   `json:"Enabled"`
}

// revocationConfigWire mirrors types.RevocationConfiguration on both the
// CreateCertificateAuthority/UpdateCertificateAuthority input and the
// DescribeCertificateAuthority/ListCertificateAuthorities output.
type revocationConfigWire struct {
	CrlConfiguration  *crlConfigWire  `json:"CrlConfiguration,omitempty"`
	OcspConfiguration *ocspConfigWire `json:"OcspConfiguration,omitempty"`
}

func (w *revocationConfigWire) toModel() *RevocationConfiguration {
	if w == nil {
		return nil
	}

	rc := &RevocationConfiguration{}

	if w.CrlConfiguration != nil {
		crl := &CrlConfiguration{
			Enabled:          w.CrlConfiguration.Enabled,
			ExpirationInDays: w.CrlConfiguration.ExpirationInDays,
			CustomCname:      w.CrlConfiguration.CustomCname,
			CustomPath:       w.CrlConfiguration.CustomPath,
			S3BucketName:     w.CrlConfiguration.S3BucketName,
			S3ObjectACL:      w.CrlConfiguration.S3ObjectACL,
			CrlType:          w.CrlConfiguration.CrlType,
		}
		if ext := w.CrlConfiguration.CrlDistributionPointExtensionConfiguration; ext != nil {
			crl.OmitExtension = ext.OmitExtension
		}

		rc.CrlConfiguration = crl
	}

	if w.OcspConfiguration != nil {
		rc.OcspConfiguration = &OcspConfiguration{
			Enabled:         w.OcspConfiguration.Enabled,
			OcspCustomCname: w.OcspConfiguration.OcspCustomCname,
		}
	}

	return rc
}

func revocationConfigFromModel(rc *RevocationConfiguration) *revocationConfigWire {
	if rc == nil {
		return nil
	}

	w := &revocationConfigWire{}

	if rc.CrlConfiguration != nil {
		crl := rc.CrlConfiguration
		w.CrlConfiguration = &crlConfigWire{
			Enabled:          crl.Enabled,
			ExpirationInDays: crl.ExpirationInDays,
			CustomCname:      crl.CustomCname,
			CustomPath:       crl.CustomPath,
			S3BucketName:     crl.S3BucketName,
			S3ObjectACL:      crl.S3ObjectACL,
			CrlType:          crl.CrlType,
		}
		if crl.OmitExtension {
			w.CrlConfiguration.CrlDistributionPointExtensionConfiguration = &crlDistributionPointExtConfigWire{
				OmitExtension: true,
			}
		}
	}

	if rc.OcspConfiguration != nil {
		w.OcspConfiguration = &ocspConfigWire{
			Enabled:         rc.OcspConfiguration.Enabled,
			OcspCustomCname: rc.OcspConfiguration.OcspCustomCname,
		}
	}

	return w
}

type createCertificateAuthorityInput struct {
	CertificateAuthorityConfiguration caConfigInput         `json:"CertificateAuthorityConfiguration"`
	CertificateAuthorityType          string                `json:"CertificateAuthorityType"`
	IdempotencyToken                  string                `json:"IdempotencyToken,omitempty"`
	KeyStorageSecurityStandard        string                `json:"KeyStorageSecurityStandard,omitempty"`
	UsageMode                         string                `json:"UsageMode,omitempty"`
	RevocationConfiguration           *revocationConfigWire `json:"RevocationConfiguration,omitempty"`
	Tags                              []svcTags.KV          `json:"Tags"`
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

type certAuthorityOutput struct {
	CertificateAuthorityConfiguration caConfigOutput        `json:"CertificateAuthorityConfiguration"`
	RevocationConfiguration           *revocationConfigWire `json:"RevocationConfiguration,omitempty"`
	Arn                               string                `json:"Arn"`
	OwnerAccount                      string                `json:"OwnerAccount,omitempty"`
	Type                              string                `json:"Type"`
	Status                            string                `json:"Status"`
	Serial                            string                `json:"Serial,omitempty"`
	KeyStorageSecurityStandard        string                `json:"KeyStorageSecurityStandard,omitempty"`
	UsageMode                         string                `json:"UsageMode,omitempty"`
	CreatedAt                         int64                 `json:"CreatedAt"`
	NotBefore                         int64                 `json:"NotBefore,omitempty"`
	NotAfter                          int64                 `json:"NotAfter,omitempty"`
	RestorableUntil                   int64                 `json:"RestorableUntil,omitempty"`
	LastStateChangeAt                 int64                 `json:"LastStateChangeAt,omitempty"`
}

type describeCertificateAuthorityOutput struct {
	CertificateAuthority certAuthorityOutput `json:"CertificateAuthority"`
}

type listCertificateAuthoritiesInput struct {
	NextToken     string `json:"NextToken"`
	ResourceOwner string `json:"ResourceOwner"`
	MaxResults    int    `json:"MaxResults"`
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
	RevocationConfiguration    *revocationConfigWire `json:"RevocationConfiguration,omitempty"`
	CertificateAuthorityArn    string                `json:"CertificateAuthorityArn"`
	Status                     string                `json:"Status"`
	revocationConfigurationSet bool
}

// UnmarshalJSON records whether the RevocationConfiguration key was present in
// the request body at all (see revocationConfigurationSet), matching the real
// API's "if you don't supply this parameter, existing capabilities remain
// unchanged" semantics for UpdateCertificateAuthorityInput.
func (in *updateCertificateAuthorityInput) UnmarshalJSON(data []byte) error {
	type alias updateCertificateAuthorityInput

	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}

	*in = updateCertificateAuthorityInput(a)

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	_, in.revocationConfigurationSet = raw["RevocationConfiguration"]

	return nil
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
		return nil, ErrInvalidArgs
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

	ca, err := h.Backend.CreateCertificateAuthority(ctx, input.CertificateAuthorityType, cfg,
		WithCreateCAIdempotencyToken(input.IdempotencyToken),
		WithCreateCAKeyStorageSecurityStandard(input.KeyStorageSecurityStandard),
		WithCreateCAUsageMode(input.UsageMode),
		WithCreateCARevocationConfiguration(input.RevocationConfiguration.toModel()),
	)
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
		return nil, ErrInvalidArn
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

	p, err := h.Backend.ListCertificateAuthorities(ctx, input.NextToken, input.MaxResults, input.ResourceOwner)
	if err != nil {
		return nil, err
	}

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
		return nil, ErrInvalidArn
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
		return nil, ErrInvalidArn
	}

	var opts []UpdateCAOption
	if input.revocationConfigurationSet {
		opts = append(opts, WithUpdateCARevocationConfiguration(input.RevocationConfiguration.toModel()))
	}

	if err := h.Backend.UpdateCertificateAuthority(
		ctx, input.CertificateAuthorityArn, input.Status, opts...,
	); err != nil {
		return nil, err
	}

	return &updateCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonGetCsr(ctx context.Context, body []byte) (any, error) {
	var input getCertificateAuthorityCsrInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidArn
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
		return nil, ErrInvalidArn
	}

	certPEM, err := decodeBase64Field(input.Certificate, "Certificate", ErrMalformedCertificate)
	if err != nil {
		return nil, err
	}

	chainPEM, err := decodeBase64Field(input.CertificateChain, "CertificateChain", ErrMalformedCertificate)
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
		return nil, ErrInvalidArn
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
		return nil, ErrInvalidArn
	}

	if err := h.Backend.RestoreCertificateAuthority(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	return &restoreCertificateAuthorityOutput{}, nil
}

func toCAOutput(ca *CertificateAuthority) certAuthorityOutput {
	out := certAuthorityOutput{
		Arn:                        ca.ARN,
		OwnerAccount:               ca.OwnerAccount,
		Type:                       ca.Type,
		Status:                     ca.Status,
		Serial:                     ca.Serial,
		KeyStorageSecurityStandard: ca.KeyStorageSecurityStandard,
		UsageMode:                  ca.UsageMode,
		CreatedAt:                  ca.CreatedAt.Unix(),
		RevocationConfiguration:    revocationConfigFromModel(ca.RevocationConfiguration),
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

	if !ca.LastStateChangeAt.IsZero() {
		out.LastStateChangeAt = ca.LastStateChangeAt.Unix()
	}

	return out
}
