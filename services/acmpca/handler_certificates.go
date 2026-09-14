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

// qualifierWire mirrors types.Qualifier. Amazon Web Services Private CA
// supports only the CPS qualifier (types.PolicyQualifierId's sole enum value
// -- see enums.go), so CpsUri is the only field.
type qualifierWire struct {
	CpsURI string `json:"CpsUri"`
}

// policyQualifierInfoWire mirrors types.PolicyQualifierInfo.
type policyQualifierInfoWire struct {
	Qualifier         *qualifierWire `json:"Qualifier,omitempty"`
	PolicyQualifierID string         `json:"PolicyQualifierId"`
}

// policyInformationWire mirrors types.PolicyInformation. The Go field name
// follows Go initialism convention (ID, not Id); the JSON tag keeps the
// SDK's exact wire key.
type policyInformationWire struct {
	CertPolicyID     string                    `json:"CertPolicyId"`
	PolicyQualifiers []policyQualifierInfoWire `json:"PolicyQualifiers,omitempty"`
}

// otherNameWire mirrors types.OtherName.
type otherNameWire struct {
	TypeID string `json:"TypeId"`
	Value  string `json:"Value"`
}

// ediPartyNameWire mirrors types.EdiPartyName.
type ediPartyNameWire struct {
	PartyName    string `json:"PartyName"`
	NameAssigner string `json:"NameAssigner,omitempty"`
}

// generalNameWire mirrors types.GeneralName. Go field names follow Go
// initialism convention (DNS, IP, ID); JSON tags keep the SDK's exact wire
// keys. Exactly one field must be set per decodeGeneralName, matching the
// SDK doc comment's rule that providing more than one option results in an
// InvalidArgsException.
type generalNameWire struct {
	DNSName                   string            `json:"DnsName,omitempty"`
	IPAddress                 string            `json:"IpAddress,omitempty"`
	Rfc822Name                string            `json:"Rfc822Name,omitempty"`
	OtherName                 *otherNameWire    `json:"OtherName,omitempty"`
	DirectoryName             *asn1SubjectWire  `json:"DirectoryName,omitempty"`
	EdiPartyName              *ediPartyNameWire `json:"EdiPartyName,omitempty"`
	UniformResourceIdentifier string            `json:"UniformResourceIdentifier,omitempty"`
	RegisteredID              string            `json:"RegisteredId,omitempty"`
}

// extensionsWire mirrors types.Extensions.
type extensionsWire struct {
	KeyUsage                *keyUsageWire           `json:"KeyUsage,omitempty"`
	CertificatePolicies     []policyInformationWire `json:"CertificatePolicies,omitempty"`
	CustomExtensions        []customExtensionWire   `json:"CustomExtensions,omitempty"`
	ExtendedKeyUsage        []extendedKeyUsageWire  `json:"ExtendedKeyUsage,omitempty"`
	SubjectAlternativeNames []generalNameWire       `json:"SubjectAlternativeNames,omitempty"`
}

// customAttributeWire mirrors types.CustomAttribute.
type customAttributeWire struct {
	ObjectIdentifier string `json:"ObjectIdentifier"`
	Value            string `json:"Value"`
}

// asn1SubjectWire mirrors types.ASN1Subject in full.
type asn1SubjectWire struct {
	CommonName                 string                `json:"CommonName,omitempty"`
	Country                    string                `json:"Country,omitempty"`
	Organization               string                `json:"Organization,omitempty"`
	OrganizationalUnit         string                `json:"OrganizationalUnit,omitempty"`
	State                      string                `json:"State,omitempty"`
	Locality                   string                `json:"Locality,omitempty"`
	SerialNumber               string                `json:"SerialNumber,omitempty"`
	DistinguishedNameQualifier string                `json:"DistinguishedNameQualifier,omitempty"`
	GenerationQualifier        string                `json:"GenerationQualifier,omitempty"`
	GivenName                  string                `json:"GivenName,omitempty"`
	Initials                   string                `json:"Initials,omitempty"`
	Pseudonym                  string                `json:"Pseudonym,omitempty"`
	Surname                    string                `json:"Surname,omitempty"`
	Title                      string                `json:"Title,omitempty"`
	CustomAttributes           []customAttributeWire `json:"CustomAttributes,omitempty"`
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
		return nil, ErrInvalidArgs
	}

	csrPEM, err := decodeBase64Field(input.Csr, "Csr", ErrMalformedCSR)
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
			ErrInvalidArgs, v.Type)
	}
}

// resolveValidityAbsoluteTime converts a ValidityNotBefore input to an
// absolute time.Time. Per the real API's doc comment, ValidityNotBefore is
// always expressed using the ABSOLUTE Validity type (Unix epoch seconds).
func resolveValidityAbsoluteTime(v validityInput) (time.Time, error) {
	if v.Type != "ABSOLUTE" && v.Type != "" {
		return time.Time{}, fmt.Errorf("%w: ValidityNotBefore.Type must be ABSOLUTE", ErrInvalidArgs)
	}

	return time.Unix(v.Value, 0).UTC(), nil
}

// decodeAPIPassthrough converts the wire APIPassthrough into the backend's
// APIPassthrough model.
func decodeAPIPassthrough(w *apiPassthroughWire) (*APIPassthrough, error) {
	ap := &APIPassthrough{}

	if w.Subject != nil {
		ap.Subject = decodeASN1Subject(w.Subject)
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

func decodeASN1Subject(w *asn1SubjectWire) *APIPassthroughSubject {
	attrs := make([]APIPassthroughCustomAttribute, 0, len(w.CustomAttributes))
	for _, a := range w.CustomAttributes {
		attrs = append(attrs, APIPassthroughCustomAttribute(a))
	}

	return &APIPassthroughSubject{
		CommonName:                 w.CommonName,
		Country:                    w.Country,
		Organization:               w.Organization,
		OrganizationalUnit:         w.OrganizationalUnit,
		State:                      w.State,
		Locality:                   w.Locality,
		SerialNumber:               w.SerialNumber,
		DistinguishedNameQualifier: w.DistinguishedNameQualifier,
		GenerationQualifier:        w.GenerationQualifier,
		GivenName:                  w.GivenName,
		Initials:                   w.Initials,
		Pseudonym:                  w.Pseudonym,
		Surname:                    w.Surname,
		Title:                      w.Title,
		CustomAttributes:           attrs,
	}
}

const policyQualifierIDCPS = "CPS" // types.PolicyQualifierIdCps -- the SDK's only PolicyQualifierId enum value.

func decodeCertificatePolicies(wire []policyInformationWire) ([]APIPassthroughPolicyInformation, error) {
	policies := make([]APIPassthroughPolicyInformation, 0, len(wire))

	for _, p := range wire {
		qualifiers := make([]APIPassthroughPolicyQualifier, 0, len(p.PolicyQualifiers))

		for _, q := range p.PolicyQualifiers {
			if q.PolicyQualifierID != policyQualifierIDCPS {
				return nil, fmt.Errorf(
					"%w: PolicyQualifierInfo.PolicyQualifierId must be %q", ErrInvalidArgs, policyQualifierIDCPS,
				)
			}

			if q.Qualifier == nil || q.Qualifier.CpsURI == "" {
				return nil, fmt.Errorf(
					"%w: PolicyQualifierInfo.Qualifier.CpsUri is required", ErrInvalidArgs,
				)
			}

			qualifiers = append(qualifiers, APIPassthroughPolicyQualifier{CPSURI: q.Qualifier.CpsURI})
		}

		policies = append(policies, APIPassthroughPolicyInformation{
			CertPolicyID: p.CertPolicyID,
			Qualifiers:   qualifiers,
		})
	}

	return policies, nil
}

func decodeExtensions(w *extensionsWire) (*APIPassthroughExtensions, error) {
	policies, err := decodeCertificatePolicies(w.CertificatePolicies)
	if err != nil {
		return nil, err
	}

	ext := &APIPassthroughExtensions{CertificatePolicies: policies}

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

// generalNameVariantCount reports how many of GeneralName's 8 CHOICE
// variants are set on gn.
func generalNameVariantCount(gn generalNameWire) int {
	n := 0
	for _, set := range []bool{
		gn.DNSName != "", gn.IPAddress != "", gn.Rfc822Name != "", gn.UniformResourceIdentifier != "",
		gn.RegisteredID != "", gn.OtherName != nil, gn.DirectoryName != nil, gn.EdiPartyName != nil,
	} {
		if set {
			n++
		}
	}

	return n
}

// decodeGeneralName converts one wire GeneralName. Per types.GeneralName's
// doc comment ("Only one of the following naming options should be
// provided. Providing more than one option results in an
// InvalidArgsException error"), exactly one of the 8 CHOICE variants must
// be set.
func decodeGeneralName(gn generalNameWire) (APIPassthroughSAN, error) {
	if n := generalNameVariantCount(gn); n != 1 {
		return APIPassthroughSAN{}, fmt.Errorf(
			"%w: exactly one GeneralName variant must be set per SubjectAlternativeNames entry (got %d)",
			ErrInvalidArgs, n,
		)
	}

	san := APIPassthroughSAN{
		DNSName:                   gn.DNSName,
		IPAddress:                 gn.IPAddress,
		EmailAddress:              gn.Rfc822Name,
		UniformResourceIdentifier: gn.UniformResourceIdentifier,
		RegisteredID:              gn.RegisteredID,
	}

	if gn.OtherName != nil {
		san.OtherName = &APIPassthroughOtherName{TypeID: gn.OtherName.TypeID, Value: gn.OtherName.Value}
	}

	if gn.DirectoryName != nil {
		san.DirectoryName = decodeASN1Subject(gn.DirectoryName)
	}

	if gn.EdiPartyName != nil {
		san.EdiPartyName = &APIPassthroughEdiPartyName{
			PartyName:    gn.EdiPartyName.PartyName,
			NameAssigner: gn.EdiPartyName.NameAssigner,
		}
	}

	return san, nil
}

func (h *Handler) jsonGetCert(ctx context.Context, body []byte) (any, error) {
	var input getCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidArn
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
		return nil, ErrInvalidArn
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
