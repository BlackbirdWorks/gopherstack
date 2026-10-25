package iot

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"net"
	"strconv"
	"strings"
)

const (
	certVarPrefix    = "iot:certificate."
	certVarMaxItems  = 5
	certVarMaxLength = 1024
	certVarWildcards = "*$+?#"
)

// X.509 attribute OIDs the certificate policy variables expose.
var certAttrOIDs = map[string]string{
	"2.5.4.46": "DistinguishedNameQualifier",
	"2.5.4.6":  "Country",
	"2.5.4.10": "Organization",
	"2.5.4.11": "OrganizationalUnit",
	"2.5.4.8":  "State",
	"2.5.4.3":  "CommonName",
	"2.5.4.5":  "SerialNumber",
	"2.5.4.12": "Title",
	"2.5.4.4":  "Surname",
	"2.5.4.42": "GivenName",
	"2.5.4.43": "Initials",
	"2.5.4.65": "Pseudonym",
	"2.5.4.44": "GenerationQualifier",
}

// Issuer alternative name GeneralName tags (RFC 5280 4.2.1.6) the variables expose.
const (
	gnRFC822Name = 1
	gnDNSName    = 2
	gnURI        = 6
	gnIPAddress  = 7
	oidIssuerAlt = "2.5.29.18"
)

// certPolicyVariables maps lower-cased iot:Certificate.* variable names to their values.
// Multi-valued attributes also get .N (zero-based beyond the first) and .List (comma-joined)
// variants; values holding a wildcard character or over 1024 bytes are omitted so the
// variable stays unsubstituted, as the IoT docs state.
func certPolicyVariables(cert *x509.Certificate) map[string]string {
	vars := map[string]string{}
	if cert == nil {
		return vars
	}

	addCertValues(vars, "Subject", pkixAttrValues(cert.Subject))
	addCertValues(vars, "Issuer", pkixAttrValues(cert.Issuer))

	if cert.SerialNumber != nil {
		addCertValues(vars, "SerialNumber", map[string][]string{"": {cert.SerialNumber.String()}})
	}

	subjectAlt := map[string][]string{
		"DNSName": cert.DNSNames, "RFC822Name": cert.EmailAddresses,
	}

	for _, u := range cert.URIs {
		subjectAlt["UniformResourceIdentifier"] = append(subjectAlt["UniformResourceIdentifier"], u.String())
	}

	for _, ip := range cert.IPAddresses {
		subjectAlt["IPAddress"] = append(subjectAlt["IPAddress"], ip.String())
	}

	addCertValues(vars, "Subject.AlternativeName", subjectAlt)
	addCertValues(vars, "Issuer.AlternativeName", issuerAltNames(cert))

	return vars
}

func pkixAttrValues(n pkix.Name) map[string][]string {
	out := map[string][]string{}

	for _, atv := range n.Names {
		name, ok := certAttrOIDs[atv.Type.String()]
		if !ok {
			continue
		}

		if s, isStr := atv.Value.(string); isStr {
			out[name] = append(out[name], s)
		}
	}

	return out
}

// addCertValues registers base.<attr>, base.<attr>.<n> and base.<attr>.List for every attribute.
// An empty attr key (single-level variable such as SerialNumber) uses base alone.
func addCertValues(vars map[string]string, base string, attrs map[string][]string) {
	for attr, values := range attrs {
		name := certVarPrefix + strings.ToLower(base)
		if attr != "" {
			name += "." + strings.ToLower(attr)
		}

		kept := make([]string, 0, len(values))

		for i, v := range values {
			if i >= certVarMaxItems {
				break
			}

			if len(v) > certVarMaxLength || strings.ContainsAny(v, certVarWildcards) {
				kept = append(kept, "")

				continue
			}

			kept = append(kept, v)
		}

		for i, v := range kept {
			if v == "" {
				continue
			}

			if i == 0 {
				vars[name] = v
			} else {
				vars[name+"."+strconv.Itoa(i)] = v
			}
		}

		if joined := strings.Join(nonEmpty(kept), ","); joined != "" {
			vars[name+".list"] = joined
		}
	}
}

func nonEmpty(in []string) []string {
	out := in[:0:0]

	for _, s := range in {
		if s != "" {
			out = append(out, s)
		}
	}

	return out
}

// issuerAltNames reads the issuer alternative name extension, which crypto/x509 does not parse.
func issuerAltNames(cert *x509.Certificate) map[string][]string {
	out := map[string][]string{}

	for _, ext := range cert.Extensions {
		if ext.Id.String() != oidIssuerAlt {
			continue
		}

		var seq asn1.RawValue
		if _, err := asn1.Unmarshal(ext.Value, &seq); err != nil {
			return out
		}

		rest := seq.Bytes

		for len(rest) > 0 {
			var gn asn1.RawValue

			var err error
			if rest, err = asn1.Unmarshal(rest, &gn); err != nil {
				return out
			}

			switch gn.Tag {
			case gnRFC822Name:
				out["RFC822Name"] = append(out["RFC822Name"], string(gn.Bytes))
			case gnDNSName:
				out["DNSName"] = append(out["DNSName"], string(gn.Bytes))
			case gnURI:
				out["UniformResourceIdentifier"] = append(out["UniformResourceIdentifier"], string(gn.Bytes))
			case gnIPAddress:
				out["IPAddress"] = append(out["IPAddress"], net.IP(gn.Bytes).String())
			}
		}
	}

	return out
}
