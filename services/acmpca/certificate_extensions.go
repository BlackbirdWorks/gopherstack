package acmpca

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"net"
)

// X.509/X.500 OIDs used to hand-build extensions crypto/x509 cannot emit
// itself (RFC 5280 §4.1/§4.2).
//
//nolint:gochecknoglobals // read-only OID constants; asn1.ObjectIdentifier is a slice, so these can't be consts
var (
	oidCertificatePolicies = asn1.ObjectIdentifier{2, 5, 29, 32}
	oidSubjectAltName      = asn1.ObjectIdentifier{2, 5, 29, 17}
	oidExtKeyUsage         = asn1.ObjectIdentifier{2, 5, 29, 37}
	oidBasicConstraints    = asn1.ObjectIdentifier{2, 5, 29, 19}

	// oidPolicyQualifierCPS is id-qt-cps (RFC 5280 §4.2.1.4) -- the only
	// PolicyQualifierId the SDK supports (verified: enums.go's
	// PolicyQualifierId.Values() returns only "CPS").
	oidPolicyQualifierCPS = asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 2, 1}

	// Subject RDN OIDs beyond the ones crypto/x509/pkix.Name has direct
	// fields for (RFC 5280 Appendix A.1 / RFC 4519).
	oidDistinguishedNameQualifier = asn1.ObjectIdentifier{2, 5, 4, 46}
	oidGenerationQualifier        = asn1.ObjectIdentifier{2, 5, 4, 44}
	oidGivenName                  = asn1.ObjectIdentifier{2, 5, 4, 42}
	oidInitials                   = asn1.ObjectIdentifier{2, 5, 4, 43}
	oidPseudonym                  = asn1.ObjectIdentifier{2, 5, 4, 65}
	oidSurname                    = asn1.ObjectIdentifier{2, 5, 4, 4}
	oidTitle                      = asn1.ObjectIdentifier{2, 5, 4, 12}
)

// GeneralName's context-specific CHOICE tag numbers (RFC 5280 §4.2.1.6).
// otherName, directoryName, and ediPartyName are constructed (their
// underlying types are a SEQUENCE, or -- for directoryName -- a CHOICE,
// which X.680 forbids tagging IMPLICITly even under this module's default
// IMPLICIT tagging environment, forcing EXPLICIT tagging on the inner
// value); the rest are primitive IMPLICIT tags, matching crypto/x509's own
// unexported nameType* constants and marshalSANs (x509.go).
const (
	gnTagOtherName     = 0
	gnTagRFC822Name    = 1
	gnTagDNSName       = 2
	gnTagDirectoryName = 4
	gnTagEdiPartyName  = 5
	gnTagURI           = 6
	gnTagIPAddress     = 7
	gnTagRegisteredID  = 8
)

// reTagImplicit re-parses fullDER (the complete DER encoding of some
// universal-class ASN.1 value) and re-tags it as an IMPLICIT value under the
// given context-specific tag, preserving the original constructed/primitive
// bit. This is how a struct built with ordinary (universal-tagged) asn1
// field types is turned into one CHOICE alternative of a hand-built
// GeneralName.
func reTagImplicit(fullDER []byte, tag int) (asn1.RawValue, error) {
	var raw asn1.RawValue
	if _, err := asn1.Unmarshal(fullDER, &raw); err != nil {
		return asn1.RawValue{}, fmt.Errorf("re-tag ASN.1 value: %w", err)
	}

	return asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: tag, IsCompound: raw.IsCompound, Bytes: raw.Bytes}, nil
}

// apiPassthroughSubjectToPKIX builds the pkix.Name for s, mapping every
// APIPassthroughSubject field to its RFC 5280/RFC 4519 OID. The fields
// pkix.Name has no direct support for are carried via ExtraNames, which
// pkix.Name.ToRDNSequence appends as individual RDNs alongside the standard
// fields (see crypto/x509/pkix's Name.ToRDNSequence/appendRDNs).
func apiPassthroughSubjectToPKIX(s *APIPassthroughSubject) (pkix.Name, error) {
	name := pkix.Name{
		CommonName:         s.CommonName,
		SerialNumber:       s.SerialNumber,
		Country:            nonEmptySlice(s.Country),
		Organization:       nonEmptySlice(s.Organization),
		OrganizationalUnit: nonEmptySlice(s.OrganizationalUnit),
		Province:           nonEmptySlice(s.State),
		Locality:           nonEmptySlice(s.Locality),
	}

	addExtraName := func(oid asn1.ObjectIdentifier, value string) {
		if value == "" {
			return
		}

		name.ExtraNames = append(name.ExtraNames, pkix.AttributeTypeAndValue{Type: oid, Value: value})
	}

	addExtraName(oidDistinguishedNameQualifier, s.DistinguishedNameQualifier)
	addExtraName(oidGenerationQualifier, s.GenerationQualifier)
	addExtraName(oidGivenName, s.GivenName)
	addExtraName(oidInitials, s.Initials)
	addExtraName(oidPseudonym, s.Pseudonym)
	addExtraName(oidSurname, s.Surname)
	addExtraName(oidTitle, s.Title)

	for _, attr := range s.CustomAttributes {
		oid, err := parseOID(attr.ObjectIdentifier)
		if err != nil {
			return pkix.Name{}, fmt.Errorf(
				"%w: Subject.CustomAttributes ObjectIdentifier %q: %w", ErrInvalidArgs, attr.ObjectIdentifier, err,
			)
		}

		name.ExtraNames = append(name.ExtraNames, pkix.AttributeTypeAndValue{Type: oid, Value: attr.Value})
	}

	return name, nil
}

// policyQualifierInfoASN1 mirrors RFC 5280 §4.2.1.4's PolicyQualifierInfo,
// specialized to the CPS qualifier (the SDK's only supported one): Qualifier
// ::= CHOICE { cPSuri CPSuri }, CPSuri ::= IA5String -- since the CHOICE has
// a single alternative, its DER encoding is simply that alternative's own
// encoding (no extra wrapper), hence the plain "ia5" tag below rather than
// an "explicit" one.
//
//nolint:govet // ASN.1 SEQUENCE field order is wire-significant (RFC 5280 §4.2.1.4); fieldalignment must not reorder it
type policyQualifierInfoASN1 struct {
	PolicyQualifierID asn1.ObjectIdentifier
	Qualifier         string `asn1:"ia5"`
}

// policyInformationASN1 mirrors RFC 5280 §4.2.1.4's PolicyInformation.
type policyInformationASN1 struct {
	PolicyIdentifier asn1.ObjectIdentifier
	PolicyQualifiers []policyQualifierInfoASN1 `asn1:"optional,omitempty"`
}

// applyCertificatePolicies hand-builds the certificatePolicies extension
// (RFC 5280 §4.2.1.4, OID 2.5.29.32) and appends it to tmpl.ExtraExtensions.
// crypto/x509's own Policies/PolicyIdentifiers fields cannot express
// PolicyQualifiers, so this is built directly rather than through those
// fields (which are left unset, so x509.CreateCertificate does not also try
// to emit its own copy -- see x509.go's buildCertExtensions, which checks
// ExtraExtensions before auto-generating any extension by OID).
func applyCertificatePolicies(tmpl *x509.Certificate, policies []APIPassthroughPolicyInformation) error {
	if len(policies) == 0 {
		return nil
	}

	asn1Policies := make([]policyInformationASN1, len(policies))

	for i, p := range policies {
		oid, err := parseOID(p.CertPolicyID)
		if err != nil {
			return fmt.Errorf("%w: CertificatePolicies CertPolicyId %q: %w", ErrInvalidArgs, p.CertPolicyID, err)
		}

		qualifiers := make([]policyQualifierInfoASN1, len(p.Qualifiers))
		for j, q := range p.Qualifiers {
			qualifiers[j] = policyQualifierInfoASN1{PolicyQualifierID: oidPolicyQualifierCPS, Qualifier: q.CPSURI}
		}

		asn1Policies[i] = policyInformationASN1{PolicyIdentifier: oid, PolicyQualifiers: qualifiers}
	}

	value, err := asn1.Marshal(asn1Policies)
	if err != nil {
		return fmt.Errorf("marshal CertificatePolicies: %w", err)
	}

	tmpl.ExtraExtensions = append(tmpl.ExtraExtensions, pkix.Extension{Id: oidCertificatePolicies, Value: value})

	return nil
}

// otherNameASN1 mirrors RFC 5280 §4.2.1.6's OtherName: SEQUENCE { type-id
// OBJECT IDENTIFIER, value [0] EXPLICIT ANY DEFINED BY type-id }.
//
//nolint:govet // ASN.1 SEQUENCE field order is wire-significant (RFC 5280 §4.2.1.6); fieldalignment must not reorder it
type otherNameASN1 struct {
	TypeID asn1.ObjectIdentifier
	Value  string `asn1:"utf8,explicit,tag:0"`
}

func buildOtherNameRawValue(o *APIPassthroughOtherName) (asn1.RawValue, error) {
	oid, err := parseOID(o.TypeID)
	if err != nil {
		return asn1.RawValue{}, fmt.Errorf(
			"%w: SubjectAlternativeNames.OtherName.TypeId %q: %w", ErrInvalidArgs, o.TypeID, err,
		)
	}

	full, err := asn1.Marshal(otherNameASN1{TypeID: oid, Value: o.Value})
	if err != nil {
		return asn1.RawValue{}, fmt.Errorf("marshal OtherName: %w", err)
	}

	return reTagImplicit(full, gnTagOtherName)
}

// ediPartyNameASN1 mirrors RFC 5280 §4.2.1.6's EDIPartyName. Both fields are
// DirectoryString (itself a CHOICE), so their [0]/[1] tags are EXPLICIT --
// see APIPassthroughEdiPartyName's doc comment.
type ediPartyNameASN1 struct {
	NameAssigner string `asn1:"utf8,optional,explicit,tag:0"`
	PartyName    string `asn1:"utf8,explicit,tag:1"`
}

func buildEdiPartyNameRawValue(e *APIPassthroughEdiPartyName) (asn1.RawValue, error) {
	full, err := asn1.Marshal(ediPartyNameASN1{NameAssigner: e.NameAssigner, PartyName: e.PartyName})
	if err != nil {
		return asn1.RawValue{}, fmt.Errorf("marshal EdiPartyName: %w", err)
	}

	return reTagImplicit(full, gnTagEdiPartyName)
}

func buildDirectoryNameRawValue(s *APIPassthroughSubject) (asn1.RawValue, error) {
	name, err := apiPassthroughSubjectToPKIX(s)
	if err != nil {
		return asn1.RawValue{}, err
	}

	full, err := asn1.Marshal(name.ToRDNSequence())
	if err != nil {
		return asn1.RawValue{}, fmt.Errorf("marshal DirectoryName: %w", err)
	}

	return reTagImplicit(full, gnTagDirectoryName)
}

func buildRegisteredIDRawValue(dotted string) (asn1.RawValue, error) {
	oid, err := parseOID(dotted)
	if err != nil {
		return asn1.RawValue{}, fmt.Errorf(
			"%w: SubjectAlternativeNames RegisteredId %q: %w", ErrInvalidArgs, dotted, err,
		)
	}

	full, err := asn1.Marshal(oid)
	if err != nil {
		return asn1.RawValue{}, fmt.Errorf("marshal RegisteredId: %w", err)
	}

	return reTagImplicit(full, gnTagRegisteredID)
}

func buildGeneralNameRawValue(san APIPassthroughSAN) (asn1.RawValue, error) {
	switch {
	case san.DNSName != "":
		if err := isIA5String(san.DNSName); err != nil {
			return asn1.RawValue{}, fmt.Errorf("%w: %w", ErrInvalidArgs, err)
		}

		return asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: gnTagDNSName, Bytes: []byte(san.DNSName)}, nil
	case san.EmailAddress != "":
		if err := isIA5String(san.EmailAddress); err != nil {
			return asn1.RawValue{}, fmt.Errorf("%w: %w", ErrInvalidArgs, err)
		}

		return asn1.RawValue{
			Class: asn1.ClassContextSpecific, Tag: gnTagRFC822Name, Bytes: []byte(san.EmailAddress),
		}, nil
	case san.IPAddress != "":
		ip := net.ParseIP(san.IPAddress)
		if ip == nil {
			return asn1.RawValue{}, fmt.Errorf(
				"%w: invalid SubjectAlternativeNames IpAddress %q",
				ErrInvalidArgs,
				san.IPAddress,
			)
		}

		if v4 := ip.To4(); v4 != nil {
			ip = v4
		}

		return asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: gnTagIPAddress, Bytes: ip}, nil
	case san.UniformResourceIdentifier != "":
		if err := isIA5String(san.UniformResourceIdentifier); err != nil {
			return asn1.RawValue{}, fmt.Errorf("%w: %w", ErrInvalidArgs, err)
		}

		return asn1.RawValue{
			Class: asn1.ClassContextSpecific, Tag: gnTagURI, Bytes: []byte(san.UniformResourceIdentifier),
		}, nil
	case san.RegisteredID != "":
		return buildRegisteredIDRawValue(san.RegisteredID)
	case san.OtherName != nil:
		return buildOtherNameRawValue(san.OtherName)
	case san.DirectoryName != nil:
		return buildDirectoryNameRawValue(san.DirectoryName)
	case san.EdiPartyName != nil:
		return buildEdiPartyNameRawValue(san.EdiPartyName)
	default:
		return asn1.RawValue{}, fmt.Errorf(
			"%w: SubjectAlternativeNames entry has no GeneralName variant set",
			ErrInvalidArgs,
		)
	}
}

// isIA5String matches crypto/x509's own unexported check (x509.go): RFC 5280
// requires IA5String content to be limited to ASCII.
func isIA5String(s string) error {
	for _, r := range s {
		if r > 127 { //nolint:mnd // 127 is the maximum ASCII code point, the IA5String limit (RFC 5280)
			return fmt.Errorf("%w: %q", errNotIA5String, s)
		}
	}

	return nil
}

// applySubjectAlternativeNames hand-builds the entire subjectAltName
// extension (RFC 5280 §4.2.1.6, OID 2.5.29.17) from sans, covering both the
// simple variants crypto/x509 can otherwise emit itself (DnsName, IpAddress,
// Rfc822Name, UniformResourceIdentifier) and the exotic ones it cannot
// (OtherName, DirectoryName, EdiPartyName, RegisteredId). The standard SAN
// fields are cleared so x509.CreateCertificate does not also try to emit its
// own subjectAltName extension (it already skips auto-generation once
// ExtraExtensions carries 2.5.29.17 -- see x509.go's buildCertExtensions --
// but clearing them keeps tmpl's fields consistent with what was issued).
func applySubjectAlternativeNames(tmpl *x509.Certificate, sans []APIPassthroughSAN) error {
	if len(sans) == 0 {
		return nil
	}

	rawValues := make([]asn1.RawValue, len(sans))

	for i, san := range sans {
		raw, err := buildGeneralNameRawValue(san)
		if err != nil {
			return err
		}

		rawValues[i] = raw
	}

	value, err := asn1.Marshal(rawValues)
	if err != nil {
		return fmt.Errorf("marshal SubjectAlternativeNames: %w", err)
	}

	tmpl.DNSNames = nil
	tmpl.IPAddresses = nil
	tmpl.EmailAddresses = nil
	tmpl.URIs = nil

	tmpl.ExtraExtensions = append(tmpl.ExtraExtensions, pkix.Extension{Id: oidSubjectAltName, Value: value})

	return nil
}

// criticalExtKeyUsageExtension hand-builds a Critical extendedKeyUsage
// extension (RFC 5280 §4.2.1.12, OID 2.5.29.37): crypto/x509's own
// ExtKeyUsage/UnknownExtKeyUsage fields always marshal as non-critical (see
// x509.go's marshalExtKeyUsage, which sets no Critical field), but
// CodeSigningCertificate/OCSPSigningCertificate template families require a
// Critical EKU (template-definitions.md).
func criticalExtKeyUsageExtension(ekus []x509.ExtKeyUsage) (pkix.Extension, error) {
	oids := make([]asn1.ObjectIdentifier, len(ekus))

	for i, u := range ekus {
		oid, err := parseOID(u.OID().String())
		if err != nil {
			return pkix.Extension{}, fmt.Errorf("resolve ExtKeyUsage OID: %w", err)
		}

		oids[i] = oid
	}

	value, err := asn1.Marshal(oids)
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("marshal ExtendedKeyUsage: %w", err)
	}

	return pkix.Extension{Id: oidExtKeyUsage, Critical: true, Value: value}, nil
}

// basicConstraintsASN1 mirrors RFC 5280 §4.2.1.9's BasicConstraints, the same
// shape as crypto/x509's own unexported basicConstraints struct (x509.go).
type basicConstraintsASN1 struct {
	IsCA       bool `asn1:"optional"`
	MaxPathLen int  `asn1:"optional,default:-1"`
}

// basicConstraintsExtension hand-builds a basicConstraints extension with an
// explicit (non-default) Critical flag: crypto/x509's own BasicConstraintsValid
// path always marshals Critical:true (x509.go's marshalBasicConstraints),
// but the plain (non-CriticalBasicConstraints) Blank end-entity template
// family documents a non-critical basicConstraints extension
// (template-definitions.md).
func basicConstraintsExtension(isCA bool, pathLen int, critical bool) (pkix.Extension, error) {
	if pathLen < 0 {
		pathLen = -1
	}

	value, err := asn1.Marshal(basicConstraintsASN1{IsCA: isCA, MaxPathLen: pathLen})
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("marshal BasicConstraints: %w", err)
	}

	return pkix.Extension{Id: oidBasicConstraints, Critical: critical, Value: value}, nil
}
