package acmpca

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"slices"
)

//nolint:gochecknoglobals // read-only OID constants; asn1.ObjectIdentifier is a slice, so these can't be consts
var (
	oidCRLDistribution = asn1.ObjectIdentifier{2, 5, 29, 31}
)

const maxKnownExtKeyUsage = 20

// applyCSRPassthrough copies the CSR's SAN, KeyUsage and ExtendedKeyUsage onto
// tmpl for *CSRPassthrough templates; KeyUsage/EKU only when the family does
// not fix them. API passthrough and the template profile run afterwards and win.
func applyCSRPassthrough(tmpl *x509.Certificate, csr *x509.CertificateRequest, rt resolvedTemplate) {
	if !rt.allowCSRPassthrough {
		return
	}

	tmpl.DNSNames = csr.DNSNames
	tmpl.EmailAddresses = csr.EmailAddresses
	tmpl.IPAddresses = csr.IPAddresses
	tmpl.URIs = csr.URIs

	for _, ext := range csr.Extensions {
		switch {
		case ext.Id.Equal(oidKeyUsage) && !rt.profile.keyUsageFixed:
			if ku, ok := parseKeyUsage(ext.Value); ok {
				tmpl.KeyUsage = ku
			}
		case ext.Id.Equal(oidExtKeyUsage) && !rt.profile.ekuFixed:
			applyCSRExtKeyUsage(tmpl, ext.Value)
		}
	}
}

func parseKeyUsage(der []byte) (x509.KeyUsage, bool) {
	var bits asn1.BitString
	if rest, err := asn1.Unmarshal(der, &bits); err != nil || len(rest) != 0 {
		return 0, false
	}

	var ku x509.KeyUsage

	for i := range bits.BitLength {
		if bits.At(i) != 0 {
			ku |= 1 << uint(i)
		}
	}

	return ku, true
}

func applyCSRExtKeyUsage(tmpl *x509.Certificate, der []byte) {
	var oids []asn1.ObjectIdentifier
	if rest, err := asn1.Unmarshal(der, &oids); err != nil || len(rest) != 0 {
		return
	}

	tmpl.ExtKeyUsage = nil
	tmpl.UnknownExtKeyUsage = nil

	for _, oid := range oids {
		if u, ok := extKeyUsageForOID(oid); ok {
			tmpl.ExtKeyUsage = append(tmpl.ExtKeyUsage, u)

			continue
		}

		tmpl.UnknownExtKeyUsage = append(tmpl.UnknownExtKeyUsage, oid)
	}
}

func extKeyUsageForOID(oid asn1.ObjectIdentifier) (x509.ExtKeyUsage, bool) {
	for i := range maxKnownExtKeyUsage {
		u := x509.ExtKeyUsage(i)
		if u.OID().String() == oid.String() {
			return u, true
		}
	}

	return 0, false
}

// csrCRLDistributionPoints returns the fullName URIs of the CSR's
// cRLDistributionPoints extension (RFC 5280 §4.2.1.13), if present.
func csrCRLDistributionPoints(csr *x509.CertificateRequest) []string {
	idx := slices.IndexFunc(csr.Extensions, func(e pkix.Extension) bool { return e.Id.Equal(oidCRLDistribution) })
	if idx < 0 {
		return nil
	}

	var points []asn1.RawValue
	if rest, err := asn1.Unmarshal(csr.Extensions[idx].Value, &points); err != nil || len(rest) != 0 {
		return nil
	}

	var urls []string

	for _, dp := range points {
		for _, distPoint := range rawChildren(dp) {
			for _, fullName := range rawChildren(distPoint) {
				for _, name := range rawChildren(fullName) {
					if name.Class == asn1.ClassContextSpecific && name.Tag == gnTagURI {
						urls = append(urls, string(name.Bytes))
					}
				}
			}
		}
	}

	return urls
}

func rawChildren(v asn1.RawValue) []asn1.RawValue {
	var out []asn1.RawValue

	rest := v.Bytes
	for len(rest) > 0 {
		var child asn1.RawValue

		var err error
		if rest, err = asn1.Unmarshal(rest, &child); err != nil {
			return nil
		}

		out = append(out, child)
	}

	return out
}
