package azurekeyvault

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
)

const (
	sanTagOther = 0
	sanTagEmail = 1
	sanTagDNS   = 2
	sanTagURI   = 6
	sanTagIP    = 7
)

func oidSubjectAltName() asn1.ObjectIdentifier { return asn1.ObjectIdentifier{2, 5, 29, 17} }

func oidUPN() asn1.ObjectIdentifier { return asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 311, 20, 2, 3} }

func sanValue(tag int, b []byte) asn1.RawValue {
	return asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: tag, Bytes: b}
}

// upnOtherName encodes a Microsoft UPN as an otherName GeneralName
// (RFC 5280 4.2.1.6: [0] { OID, [0] EXPLICIT UTF8String }).
func upnOtherName(upn string) (asn1.RawValue, error) {
	val, err := asn1.MarshalWithParams(upn, "utf8")
	if err != nil {
		return asn1.RawValue{}, err
	}

	explicit, err := asn1.Marshal(asn1.RawValue{
		Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: val,
	})
	if err != nil {
		return asn1.RawValue{}, err
	}

	oid, err := asn1.Marshal(oidUPN())
	if err != nil {
		return asn1.RawValue{}, err
	}

	other := sanValue(sanTagOther, append(oid, explicit...))
	other.IsCompound = true

	return other, nil
}

// applyUPNs replaces the template's SAN extension with one that also carries
// the UPNs: Go has no field for otherName, so the whole extension is built by
// hand and the template's own name fields are re-encoded into it.
func applyUPNs(t *x509.Certificate, upns []string) error {
	if len(upns) == 0 {
		return nil
	}

	var names []asn1.RawValue

	for _, d := range t.DNSNames {
		names = append(names, sanValue(sanTagDNS, []byte(d)))
	}

	for _, e := range t.EmailAddresses {
		names = append(names, sanValue(sanTagEmail, []byte(e)))
	}

	for _, u := range t.URIs {
		names = append(names, sanValue(sanTagURI, []byte(u.String())))
	}

	for _, ip := range t.IPAddresses {
		raw := ip.To4()
		if raw == nil {
			raw = ip.To16()
		}

		names = append(names, sanValue(sanTagIP, raw))
	}

	for _, u := range upns {
		if u == "" {
			return errBadParameter("A UPN subject alternative name must not be empty.")
		}

		other, err := upnOtherName(u)
		if err != nil {
			return err
		}

		names = append(names, other)
	}

	der, err := asn1.Marshal(names)
	if err != nil {
		return err
	}

	t.ExtraExtensions = append(t.ExtraExtensions, pkix.Extension{Id: oidSubjectAltName(), Value: der})

	return nil
}
