package acmpca

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type validityInput struct {
	Type  string `json:"Type"`
	Value int64  `json:"Value"`
}

// keyUsageWire mirrors types.KeyUsage.
type keyUsageWire struct {
	DigitalSignature bool `json:"DigitalSignature"`
	NonRepudiation   bool `json:"NonRepudiation"`
	KeyEncipherment  bool `json:"KeyEncipherment"`
	DataEncipherment bool `json:"DataEncipherment"`
	KeyAgreement     bool `json:"KeyAgreement"`
	KeyCertSign      bool `json:"KeyCertSign"`
	CRLSign          bool `json:"CRLSign"`
	EncipherOnly     bool `json:"EncipherOnly"`
	DecipherOnly     bool `json:"DecipherOnly"`
}

// extendedKeyUsageWire mirrors types.ExtendedKeyUsage.
type extendedKeyUsageWire struct {
	ExtendedKeyUsageType             string `json:"ExtendedKeyUsageType,omitempty"`
	ExtendedKeyUsageObjectIdentifier string `json:"ExtendedKeyUsageObjectIdentifier,omitempty"`
}

// customExtensionWire mirrors types.CustomExtension.
type customExtensionWire struct {
	ObjectIdentifier string `json:"ObjectIdentifier"`
	Value            string `json:"Value"`
	Critical         bool   `json:"Critical"`
}

// policyInformationWire mirrors types.PolicyInformation. gopherstack does not
// implement CertificatePolicies (see decodeExtensions); this type exists only
// so a request that sets it can be detected and rejected explicitly instead of
// silently dropped. The Go field name follows Go initialism convention (ID, not
// Id); the JSON tag keeps the SDK's exact wire key.
type policyInformationWire struct {
	CertPolicyID string `json:"CertPolicyId"`
}

// generalNameWire mirrors types.GeneralName. Only DnsName/IpAddress/Rfc822Name
// (the three SubjectAlternativeNames variants Terraform's aws_acmpca_certificate
// resource exposes) are implemented -- see decodeGeneralName, which rejects the
// other variants explicitly rather than silently dropping them. Go field names
// follow Go initialism convention (DNS, IP, ID); JSON tags keep the SDK's exact
// wire keys.
type generalNameWire struct {
	DNSName                   string `json:"DnsName,omitempty"`
	IPAddress                 string `json:"IpAddress,omitempty"`
	Rfc822Name                string `json:"Rfc822Name,omitempty"`
	OtherName                 any    `json:"OtherName,omitempty"`
	DirectoryName             any    `json:"DirectoryName,omitempty"`
	EdiPartyName              any    `json:"EdiPartyName,omitempty"`
	UniformResourceIdentifier string `json:"UniformResourceIdentifier,omitempty"`
	RegisteredID              string `json:"RegisteredId,omitempty"`
}

// extensionsWire mirrors types.Extensions.
type extensionsWire struct {
	KeyUsage                *keyUsageWire           `json:"KeyUsage,omitempty"`
	CertificatePolicies     []policyInformationWire `json:"CertificatePolicies,omitempty"`
	CustomExtensions        []customExtensionWire   `json:"CustomExtensions,omitempty"`
	ExtendedKeyUsage        []extendedKeyUsageWire  `json:"ExtendedKeyUsage,omitempty"`
	SubjectAlternativeNames []generalNameWire       `json:"SubjectAlternativeNames,omitempty"`
}

// asn1SubjectWire mirrors types.ASN1Subject. Only the fields also present on
// CertificateAuthoritySubject plus SerialNumber are implemented -- see
// decodeASN1Subject, which rejects the remaining exotic RDN types explicitly.
type asn1SubjectWire struct {
	CommonName                 string `json:"CommonName,omitempty"`
	Country                    string `json:"Country,omitempty"`
	Organization               string `json:"Organization,omitempty"`
	OrganizationalUnit         string `json:"OrganizationalUnit,omitempty"`
	State                      string `json:"State,omitempty"`
	Locality                   string `json:"Locality,omitempty"`
	SerialNumber               string `json:"SerialNumber,omitempty"`
	DistinguishedNameQualifier string `json:"DistinguishedNameQualifier,omitempty"`
	GenerationQualifier        string `json:"GenerationQualifier,omitempty"`
	Initials                   string `json:"Initials,omitempty"`
	Pseudonym                  string `json:"Pseudonym,omitempty"`
	Surname                    string `json:"Surname,omitempty"`
	Title                      string `json:"Title,omitempty"`
	CustomAttributes           []any  `json:"CustomAttributes,omitempty"`
}

// apiPassthroughWire mirrors types.APIPassthrough.
type apiPassthroughWire struct {
	Extensions *extensionsWire  `json:"Extensions,omitempty"`
	Subject    *asn1SubjectWire `json:"Subject,omitempty"`
}

type issueCertificateInput struct {
	ValidityNotBefore       *validityInput      `json:"ValidityNotBefore,omitempty"`
	APIPassthrough          *apiPassthroughWire `json:"ApiPassthrough,omitempty"`
	CertificateAuthorityArn string              `json:"CertificateAuthorityArn"`
	Csr                     string              `json:"Csr"`
	SigningAlgorithm        string              `json:"SigningAlgorithm"`
	TemplateArn             string              `json:"TemplateArn,omitempty"`
	IdempotencyToken        string              `json:"IdempotencyToken,omitempty"`
	Validity                validityInput       `json:"Validity"`
}

type issueCertificateOutput struct {
	CertificateArn string `json:"CertificateArn"`
}

type getCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	CertificateArn          string `json:"CertificateArn"`
}

type getCertificateOutput struct {
	Certificate      string `json:"Certificate"`
	CertificateChain string `json:"CertificateChain,omitempty"`
}

type revokeCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	CertificateSerial       string `json:"CertificateSerial"`
	RevocationReason        string `json:"RevocationReason"`
}

type revokeCertificateOutput struct{}

func (h *Handler) jsonIssueCert(ctx context.Context, body []byte) (any, error) {
	var input issueCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	csrPEM, err := decodeBase64Field(input.Csr, "Csr")
	if err != nil {
		return nil, err
	}

	days, err := resolveValidityDays(input.Validity)
	if err != nil {
		return nil, err
	}

	opts := []IssueCertOption{
		WithIssueCertIdempotencyToken(input.IdempotencyToken),
		WithIssueCertTemplateArn(input.TemplateArn),
	}

	if input.ValidityNotBefore != nil {
		notBefore, notBeforeErr := resolveValidityAbsoluteTime(*input.ValidityNotBefore)
		if notBeforeErr != nil {
			return nil, notBeforeErr
		}

		opts = append(opts, WithIssueCertValidityNotBefore(notBefore))
	}

	if input.APIPassthrough != nil {
		ap, apErr := decodeAPIPassthrough(input.APIPassthrough)
		if apErr != nil {
			return nil, apErr
		}

		opts = append(opts, WithIssueCertAPIPassthrough(ap))
	}

	cert, err := h.Backend.IssueCertificate(ctx, input.CertificateAuthorityArn, csrPEM, days, opts...)
	if err != nil {
		return nil, err
	}

	return &issueCertificateOutput{CertificateArn: cert.ARN}, nil
}

// resolveValidityDays converts a Validity input into a day count, matching
// IssueCertificateInput.Validity's documented Type semantics (DAYS/MONTHS/YEARS
// are relative; ABSOLUTE/END_DATE are treated as a Unix-epoch-seconds "Not
// After" -- see the END_DATE/UTCTime caveat noted in PARITY.md).
func resolveValidityDays(v validityInput) (int, error) {
	switch v.Type {
	case "YEARS":
		return int(v.Value) * daysPerYear, nil
	case "MONTHS":
		return int(v.Value) * daysPerMonth, nil
	case "DAYS", "":
		return int(v.Value), nil
	case "END_DATE", "ABSOLUTE":
		endDate := time.Unix(v.Value, 0)
		days := int(time.Until(endDate).Hours() / hoursPerDay)

		if days <= 0 {
			days = 1
		}

		return days, nil
	default:
		return 0, fmt.Errorf("%w: unsupported Validity.Type %q (must be DAYS, MONTHS, YEARS, or END_DATE)",
			ErrInvalidParameter, v.Type)
	}
}

// resolveValidityAbsoluteTime converts a ValidityNotBefore input to an
// absolute time.Time. Per the real API's doc comment, ValidityNotBefore is
// always expressed using the ABSOLUTE Validity type (Unix epoch seconds).
func resolveValidityAbsoluteTime(v validityInput) (time.Time, error) {
	if v.Type != "ABSOLUTE" && v.Type != "" {
		return time.Time{}, fmt.Errorf("%w: ValidityNotBefore.Type must be ABSOLUTE", ErrInvalidParameter)
	}

	return time.Unix(v.Value, 0).UTC(), nil
}

// decodeAPIPassthrough converts the wire APIPassthrough into the backend's
// APIPassthrough model, rejecting the sub-fields that are not implemented
// (see the wire struct doc comments above) with a clear InvalidParameterException
// instead of silently dropping them.
func decodeAPIPassthrough(w *apiPassthroughWire) (*APIPassthrough, error) {
	ap := &APIPassthrough{}

	if w.Subject != nil {
		subject, err := decodeASN1Subject(w.Subject)
		if err != nil {
			return nil, err
		}

		ap.Subject = subject
	}

	if w.Extensions != nil {
		extensions, err := decodeExtensions(w.Extensions)
		if err != nil {
			return nil, err
		}

		ap.Extensions = extensions
	}

	return ap, nil
}

func decodeASN1Subject(w *asn1SubjectWire) (*APIPassthroughSubject, error) {
	if w.DistinguishedNameQualifier != "" || w.GenerationQualifier != "" || w.Initials != "" ||
		w.Pseudonym != "" || w.Surname != "" || w.Title != "" || len(w.CustomAttributes) > 0 {
		return nil, fmt.Errorf(
			"%w: APIPassthrough.Subject.{DistinguishedNameQualifier,GenerationQualifier,Initials,"+
				"Pseudonym,Surname,Title,CustomAttributes} are not supported", ErrInvalidParameter,
		)
	}

	return &APIPassthroughSubject{
		CommonName:         w.CommonName,
		Country:            w.Country,
		Organization:       w.Organization,
		OrganizationalUnit: w.OrganizationalUnit,
		State:              w.State,
		Locality:           w.Locality,
		SerialNumber:       w.SerialNumber,
	}, nil
}

func decodeExtensions(w *extensionsWire) (*APIPassthroughExtensions, error) {
	if len(w.CertificatePolicies) > 0 {
		return nil, fmt.Errorf(
			"%w: APIPassthrough.Extensions.CertificatePolicies is not supported", ErrInvalidParameter,
		)
	}

	ext := &APIPassthroughExtensions{}

	if w.KeyUsage != nil {
		ku := w.KeyUsage
		ext.KeyUsage = &APIPassthroughKeyUsage{
			DigitalSignature: ku.DigitalSignature,
			NonRepudiation:   ku.NonRepudiation,
			KeyEncipherment:  ku.KeyEncipherment,
			DataEncipherment: ku.DataEncipherment,
			KeyAgreement:     ku.KeyAgreement,
			KeyCertSign:      ku.KeyCertSign,
			CRLSign:          ku.CRLSign,
			EncipherOnly:     ku.EncipherOnly,
			DecipherOnly:     ku.DecipherOnly,
		}
	}

	for _, eku := range w.ExtendedKeyUsage {
		ext.ExtendedKeyUsage = append(ext.ExtendedKeyUsage, APIPassthroughExtendedKeyUsage{
			Type:             eku.ExtendedKeyUsageType,
			ObjectIdentifier: eku.ExtendedKeyUsageObjectIdentifier,
		})
	}

	for _, ce := range w.CustomExtensions {
		ext.CustomExtensions = append(ext.CustomExtensions, APIPassthroughCustomExtension{
			ObjectIdentifier: ce.ObjectIdentifier,
			ValueBase64:      ce.Value,
			Critical:         ce.Critical,
		})
	}

	sans, err := decodeGeneralNames(w.SubjectAlternativeNames)
	if err != nil {
		return nil, err
	}

	ext.SubjectAlternativeNames = sans

	return ext, nil
}

func decodeGeneralNames(wire []generalNameWire) ([]APIPassthroughSAN, error) {
	sans := make([]APIPassthroughSAN, 0, len(wire))

	for _, gn := range wire {
		san, err := decodeGeneralName(gn)
		if err != nil {
			return nil, err
		}

		sans = append(sans, san)
	}

	return sans, nil
}

func decodeGeneralName(gn generalNameWire) (APIPassthroughSAN, error) {
	if gn.OtherName != nil || gn.DirectoryName != nil || gn.EdiPartyName != nil ||
		gn.UniformResourceIdentifier != "" || gn.RegisteredID != "" {
		return APIPassthroughSAN{}, fmt.Errorf(
			"%w: SubjectAlternativeNames.{OtherName,DirectoryName,EdiPartyName,"+
				"UniformResourceIdentifier,RegisteredId} are not supported", ErrInvalidParameter,
		)
	}

	return APIPassthroughSAN{
		DNSName:      gn.DNSName,
		IPAddress:    gn.IPAddress,
		EmailAddress: gn.Rfc822Name,
	}, nil
}

func (h *Handler) jsonGetCert(ctx context.Context, body []byte) (any, error) {
	var input getCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	cert, err := h.Backend.GetCertificate(ctx, input.CertificateAuthorityArn, input.CertificateArn)
	if err != nil {
		return nil, err
	}

	caChain := ""
	if certPEM, chainPEM, chainErr := h.Backend.GetCertificateAuthorityCertificate(
		ctx,
		input.CertificateAuthorityArn,
	); chainErr == nil && certPEM != "" {
		caChain = certPEM
		if chainPEM != "" {
			caChain = certPEM + chainPEM
		}
	}

	return &getCertificateOutput{Certificate: cert.CertBody, CertificateChain: caChain}, nil
}

func (h *Handler) jsonRevokeCert(ctx context.Context, body []byte) (any, error) {
	var input revokeCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.RevokeCertificate(
		ctx,
		input.CertificateAuthorityArn,
		input.CertificateSerial,
		input.RevocationReason,
	); err != nil {
		return nil, err
	}

	return &revokeCertificateOutput{}, nil
}
