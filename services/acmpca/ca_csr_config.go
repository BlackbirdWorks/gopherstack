package acmpca

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
)

//nolint:gochecknoglobals // read-only OID constants; asn1.ObjectIdentifier is a slice
var (
	oidKeyUsage                 = asn1.ObjectIdentifier{2, 5, 29, 15}
	oidSubjectInformationAccess = asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 11}
)

const (
	keyUsageBitCount = 9
	bitsPerByte      = 8
	topBit           = 0x80
)

const (
	accessMethodCARepository = "CA_REPOSITORY"
	accessMethodRPKIManifest = "RESOURCE_PKI_MANIFEST"
	accessMethodRPKINotify   = "RESOURCE_PKI_NOTIFY"
)

func accessMethodOID(d AccessDescription) (asn1.ObjectIdentifier, error) {
	if (d.AccessMethodType == "") == (d.CustomObjectIdentifier == "") {
		return nil, fmt.Errorf(
			"%w: AccessMethod needs exactly one of AccessMethodType and CustomObjectIdentifier", ErrInvalidArgs,
		)
	}

	switch d.AccessMethodType {
	case accessMethodCARepository:
		return asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 48, 5}, nil
	case accessMethodRPKIManifest:
		return asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 48, 10}, nil
	case accessMethodRPKINotify:
		return asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 48, 13}, nil
	case "":
		return parseOID(d.CustomObjectIdentifier)
	default:
		return nil, fmt.Errorf("%w: unsupported AccessMethodType %q", ErrInvalidArgs, d.AccessMethodType)
	}
}

type accessDescriptionASN1 struct {
	AccessMethod   asn1.ObjectIdentifier
	AccessLocation asn1.RawValue
}

// csrExtensionList builds the KeyUsage and SubjectInformationAccess
// extensions CsrExtensions asks for; it also validates them.
func csrExtensionList(c *CsrExtensions) ([]pkix.Extension, error) {
	if c == nil {
		return nil, nil
	}

	var exts []pkix.Extension

	if c.KeyUsage != nil {
		ext, err := keyUsageExtension(apiPassthroughKeyUsageBits(c.KeyUsage))
		if err != nil {
			return nil, err
		}

		exts = append(exts, ext)
	}

	if len(c.SubjectInformationAccess) > 0 {
		entries := make([]accessDescriptionASN1, 0, len(c.SubjectInformationAccess))

		for _, d := range c.SubjectInformationAccess {
			method, err := accessMethodOID(d)
			if err != nil {
				return nil, err
			}

			loc, err := buildGeneralNameRawValue(d.AccessLocation)
			if err != nil {
				return nil, err
			}

			entries = append(entries, accessDescriptionASN1{AccessMethod: method, AccessLocation: loc})
		}

		value, err := asn1.Marshal(entries)
		if err != nil {
			return nil, fmt.Errorf("marshal SubjectInformationAccess: %w", err)
		}

		exts = append(exts, pkix.Extension{Id: oidSubjectInformationAccess, Value: value})
	}

	return exts, nil
}

// keyUsageExtension encodes ku as a critical keyUsage BIT STRING (RFC 5280 §4.2.1.3).
func keyUsageExtension(ku x509.KeyUsage) (pkix.Extension, error) {
	var b [2]byte

	for i := range keyUsageBitCount {
		if ku&(1<<i) != 0 {
			b[i/bitsPerByte] |= topBit >> (i % bitsPerByte)
		}
	}

	length, nbits := 1, bitsPerByte
	if b[1] != 0 {
		length, nbits = len(b), len(b)*bitsPerByte
	}

	for nbits > 0 && b[(nbits-1)/bitsPerByte]&(topBit>>((nbits-1)%bitsPerByte)) == 0 {
		nbits--
	}

	value, err := asn1.Marshal(asn1.BitString{Bytes: b[:length], BitLength: nbits})
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("marshal KeyUsage: %w", err)
	}

	return pkix.Extension{Id: oidKeyUsage, Critical: true, Value: value}, nil
}

// passthrough maps s onto the shared subject model so the RDN encoding is common with IssueCertificate.
func (s CertificateAuthoritySubject) passthrough() *APIPassthroughSubject {
	return &APIPassthroughSubject{
		CommonName:                 s.CommonName,
		Country:                    s.Country,
		Organization:               s.Organization,
		OrganizationalUnit:         s.OrganizationalUnit,
		State:                      s.State,
		Locality:                   s.Locality,
		SerialNumber:               s.SerialNumber,
		DistinguishedNameQualifier: s.DistinguishedNameQualifier,
		GenerationQualifier:        s.GenerationQualifier,
		GivenName:                  s.GivenName,
		Initials:                   s.Initials,
		Pseudonym:                  s.Pseudonym,
		Surname:                    s.Surname,
		Title:                      s.Title,
		CustomAttributes:           s.CustomAttributes,
	}
}

// caSubjectName returns the pkix.Name for a CA subject, defaulting an empty CommonName.
func caSubjectName(s CertificateAuthoritySubject) (pkix.Name, error) {
	if s.CommonName == "" {
		s.CommonName = "Gopherstack Root CA"
	}

	return apiPassthroughSubjectToPKIX(s.passthrough())
}

func (w *csrExtensionsWire) toModel() (*CsrExtensions, error) {
	if w == nil {
		return nil, nil //nolint:nilnil // absent CsrExtensions is not an error
	}

	c := &CsrExtensions{}

	if ku := w.KeyUsage; ku != nil {
		c.KeyUsage = &APIPassthroughKeyUsage{
			DigitalSignature: ku.DigitalSignature, NonRepudiation: ku.NonRepudiation,
			KeyEncipherment: ku.KeyEncipherment, DataEncipherment: ku.DataEncipherment,
			KeyAgreement: ku.KeyAgreement, KeyCertSign: ku.KeyCertSign, CRLSign: ku.CRLSign,
			EncipherOnly: ku.EncipherOnly, DecipherOnly: ku.DecipherOnly,
		}
	}

	for _, d := range w.SubjectInformationAccess {
		if d.AccessMethod == nil || d.AccessLocation == nil {
			return nil, fmt.Errorf(
				"%w: SubjectInformationAccess entries need AccessMethod and AccessLocation", ErrInvalidArgs,
			)
		}

		loc, err := decodeGeneralName(*d.AccessLocation)
		if err != nil {
			return nil, err
		}

		c.SubjectInformationAccess = append(c.SubjectInformationAccess, AccessDescription{
			AccessMethodType:       d.AccessMethod.AccessMethodType,
			CustomObjectIdentifier: d.AccessMethod.CustomObjectIdentifier,
			AccessLocation:         loc,
		})
	}

	if _, err := csrExtensionList(c); err != nil {
		return nil, err
	}

	return c, nil
}

func csrExtensionsToWire(c *CsrExtensions) *csrExtensionsWire {
	if c == nil {
		return nil
	}

	w := &csrExtensionsWire{}

	if ku := c.KeyUsage; ku != nil {
		w.KeyUsage = &keyUsageWire{
			DigitalSignature: ku.DigitalSignature, NonRepudiation: ku.NonRepudiation,
			KeyEncipherment: ku.KeyEncipherment, DataEncipherment: ku.DataEncipherment,
			KeyAgreement: ku.KeyAgreement, KeyCertSign: ku.KeyCertSign, CRLSign: ku.CRLSign,
			EncipherOnly: ku.EncipherOnly, DecipherOnly: ku.DecipherOnly,
		}
	}

	for _, d := range c.SubjectInformationAccess {
		loc := generalNameToWire(d.AccessLocation)
		w.SubjectInformationAccess = append(w.SubjectInformationAccess, accessDescriptionWire{
			AccessMethod: &accessMethodWire{
				AccessMethodType:       d.AccessMethodType,
				CustomObjectIdentifier: d.CustomObjectIdentifier,
			},
			AccessLocation: &loc,
		})
	}

	return w
}

func generalNameToWire(san APIPassthroughSAN) generalNameWire {
	gn := generalNameWire{
		DNSName:                   san.DNSName,
		IPAddress:                 san.IPAddress,
		Rfc822Name:                san.EmailAddress,
		UniformResourceIdentifier: san.UniformResourceIdentifier,
		RegisteredID:              san.RegisteredID,
	}

	if san.OtherName != nil {
		gn.OtherName = &otherNameWire{TypeID: san.OtherName.TypeID, Value: san.OtherName.Value}
	}

	if san.DirectoryName != nil {
		gn.DirectoryName = subjectToWire(san.DirectoryName)
	}

	if san.EdiPartyName != nil {
		gn.EdiPartyName = &ediPartyNameWire{
			PartyName: san.EdiPartyName.PartyName, NameAssigner: san.EdiPartyName.NameAssigner,
		}
	}

	return gn
}

func subjectToWire(s *APIPassthroughSubject) *asn1SubjectWire {
	w := &asn1SubjectWire{
		CommonName: s.CommonName, Country: s.Country, Organization: s.Organization,
		OrganizationalUnit: s.OrganizationalUnit, State: s.State, Locality: s.Locality,
		SerialNumber: s.SerialNumber, DistinguishedNameQualifier: s.DistinguishedNameQualifier,
		GenerationQualifier: s.GenerationQualifier, GivenName: s.GivenName, Initials: s.Initials,
		Pseudonym: s.Pseudonym, Surname: s.Surname, Title: s.Title,
	}

	for _, a := range s.CustomAttributes {
		w.CustomAttributes = append(w.CustomAttributes, customAttributeWire(a))
	}

	return w
}

func decodeCASubject(w *asn1SubjectWire) CertificateAuthoritySubject {
	attrs := make([]APIPassthroughCustomAttribute, 0, len(w.CustomAttributes))
	for _, a := range w.CustomAttributes {
		attrs = append(attrs, APIPassthroughCustomAttribute(a))
	}

	s := CertificateAuthoritySubject{
		CommonName: w.CommonName, Country: w.Country, Organization: w.Organization,
		OrganizationalUnit: w.OrganizationalUnit, State: w.State, Locality: w.Locality,
		SerialNumber: w.SerialNumber, DistinguishedNameQualifier: w.DistinguishedNameQualifier,
		GenerationQualifier: w.GenerationQualifier, GivenName: w.GivenName, Initials: w.Initials,
		Pseudonym: w.Pseudonym, Surname: w.Surname, Title: w.Title,
	}

	if len(attrs) > 0 {
		s.CustomAttributes = attrs
	}

	return s
}
