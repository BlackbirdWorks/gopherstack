package azurekeyvault

import (
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // x5t is a SHA-1 thumbprint by definition
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	pemContentType    = "application/x-pem-file"
	pkcs12ContentType = "application/x-pkcs12"
	issuerSelf        = "Self"
	defaultValidity   = 12
	certSerialBits    = 127
	statusCompleted   = "completed"
	maxValidityMonth  = 1200
	radixDecimal      = 10
	minOIDArcs        = 2
)

// KeyProps are the certificate policy key properties.
type KeyProps struct {
	Exportable *bool  `json:"exportable,omitempty"`
	ReuseKey   *bool  `json:"reuse_key,omitempty"`
	Kty        string `json:"kty,omitempty"`
	Crv        string `json:"crv,omitempty"`
	KeySize    int    `json:"key_size,omitempty"`
}

// SecretProps are the certificate policy secret properties.
type SecretProps struct {
	ContentType string `json:"contentType,omitempty"`
}

// SANs are subject alternative names.
type SANs struct {
	Emails      []string `json:"emails,omitempty"`
	DNSNames    []string `json:"dns_names,omitempty"`
	UPNs        []string `json:"upns,omitempty"`
	URIs        []string `json:"uris,omitempty"`
	IPAddresses []string `json:"ipAddresses,omitempty"`
}

// X509Props are the certificate policy X.509 properties.
type X509Props struct {
	SANs           *SANs    `json:"sans,omitempty"`
	Subject        string   `json:"subject,omitempty"`
	EKUs           []string `json:"ekus,omitempty"`
	KeyUsage       []string `json:"key_usage,omitempty"`
	ValidityMonths int      `json:"validity_months,omitempty"`
}

// LifetimeTrigger is a certificate lifetime-action trigger.
type LifetimeTrigger struct {
	LifetimePercentage int `json:"lifetime_percentage,omitempty"`
	DaysBeforeExpiry   int `json:"days_before_expiry,omitempty"`
}

// LifetimeActionType names a lifetime action.
type LifetimeActionType struct {
	ActionType string `json:"action_type"`
}

// CertLifetimeAction pairs a trigger with an action.
type CertLifetimeAction struct {
	Action  LifetimeActionType `json:"action"`
	Trigger LifetimeTrigger    `json:"trigger"`
}

// IssuerRef references a certificate issuer.
type IssuerRef struct {
	CertTransparency *bool  `json:"cert_transparency,omitempty"`
	Name             string `json:"name,omitempty"`
	CertType         string `json:"cty,omitempty"`
}

// CertPolicy is a certificate policy.
type CertPolicy struct {
	KeyProps        *KeyProps            `json:"key_props,omitempty"`
	SecretProps     *SecretProps         `json:"secret_props,omitempty"`
	X509Props       *X509Props           `json:"x509_props,omitempty"`
	Issuer          *IssuerRef           `json:"issuer,omitempty"`
	Attributes      *Attributes          `json:"attributes,omitempty"`
	ID              string               `json:"id,omitempty"`
	LifetimeActions []CertLifetimeAction `json:"lifetime_actions,omitempty"`
}

// normalized returns a copy of the policy with defaults filled in.
func (p *CertPolicy) normalized(now int64) *CertPolicy {
	out := &CertPolicy{LifetimeActions: p.LifetimeActions, KeyProps: p.keyPropsOrDefault()}
	out.SecretProps = &SecretProps{ContentType: pemContentType}

	if p.SecretProps != nil && p.SecretProps.ContentType != "" {
		out.SecretProps.ContentType = p.SecretProps.ContentType
	}

	if p.X509Props != nil {
		x := *p.X509Props
		if x.ValidityMonths == 0 {
			x.ValidityMonths = defaultValidity
		}

		out.X509Props = &x
	}

	out.Issuer = &IssuerRef{Name: issuerSelf}
	if p.Issuer != nil && p.Issuer.Name != "" {
		iss := *p.Issuer
		out.Issuer = &iss
	}

	enabled := true
	out.Attributes = &Attributes{Enabled: &enabled, Created: &now, Updated: &now}

	if p.Attributes != nil && p.Attributes.Enabled != nil {
		out.Attributes.Enabled = p.Attributes.Enabled
	}

	return out
}

func (p *CertPolicy) keyPropsOrDefault() *KeyProps {
	kp := KeyProps{}
	if p.KeyProps != nil {
		kp = *p.KeyProps
	}

	if kp.Kty == "" {
		kp.Kty = ktyRSA
	}

	if kp.Exportable == nil {
		kp.Exportable = new(true)
	}

	if kp.ReuseKey == nil {
		kp.ReuseKey = new(false)
	}

	switch ktyFamily(kp.Kty) {
	case ktyRSA:
		if kp.KeySize == 0 {
			kp.KeySize = defaultRSABits
		}
	case ktyEC:
		if kp.Crv == "" {
			kp.Crv = curveP256
		}
	}

	return &kp
}

func parseSubject(s string) (pkix.Name, error) {
	var name pkix.Name

	for part := range strings.SplitSeq(s, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}

		val = strings.TrimSpace(val)

		switch strings.ToUpper(strings.TrimSpace(k)) {
		case "CN":
			name.CommonName = val
		case "O":
			name.Organization = append(name.Organization, val)
		case "OU":
			name.OrganizationalUnit = append(name.OrganizationalUnit, val)
		case "C":
			name.Country = append(name.Country, val)
		case "L":
			name.Locality = append(name.Locality, val)
		case "ST", "S":
			name.Province = append(name.Province, val)
		}
	}

	if name.CommonName == "" && len(name.Organization) == 0 {
		return name, errBadParameter("The certificate subject %q is not valid; expected e.g. CN=example.com.", s)
	}

	return name, nil
}

func keyUsageBits(names []string) (x509.KeyUsage, error) {
	if len(names) == 0 {
		return x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, nil
	}

	table := map[string]x509.KeyUsage{
		"digitalsignature": x509.KeyUsageDigitalSignature,
		"nonrepudiation":   x509.KeyUsageContentCommitment,
		"keyencipherment":  x509.KeyUsageKeyEncipherment,
		"dataencipherment": x509.KeyUsageDataEncipherment,
		"keyagreement":     x509.KeyUsageKeyAgreement,
		"keycertsign":      x509.KeyUsageCertSign,
		"crlsign":          x509.KeyUsageCRLSign,
		"encipheronly":     x509.KeyUsageEncipherOnly,
		"decipheronly":     x509.KeyUsageDecipherOnly,
	}

	var out x509.KeyUsage

	for _, n := range names {
		bit, ok := table[strings.ToLower(n)]
		if !ok {
			return 0, errBadParameter("Unsupported key usage %q.", n)
		}

		out |= bit
	}

	return out, nil
}

func extKeyUsages(oids []string) ([]x509.ExtKeyUsage, []asn1.ObjectIdentifier, error) {
	known := map[string]x509.ExtKeyUsage{
		"1.3.6.1.5.5.7.3.1": x509.ExtKeyUsageServerAuth,
		"1.3.6.1.5.5.7.3.2": x509.ExtKeyUsageClientAuth,
		"1.3.6.1.5.5.7.3.3": x509.ExtKeyUsageCodeSigning,
		"1.3.6.1.5.5.7.3.4": x509.ExtKeyUsageEmailProtection,
		"1.3.6.1.5.5.7.3.8": x509.ExtKeyUsageTimeStamping,
		"1.3.6.1.5.5.7.3.9": x509.ExtKeyUsageOCSPSigning,
	}

	var (
		eku     []x509.ExtKeyUsage
		unknown []asn1.ObjectIdentifier
	)

	for _, o := range oids {
		if u, ok := known[o]; ok {
			eku = append(eku, u)

			continue
		}

		oid, err := parseOID(o)
		if err != nil {
			return nil, nil, errBadParameter("Invalid extended key usage %q.", o)
		}

		unknown = append(unknown, oid)
	}

	return eku, unknown, nil
}

func parseOID(s string) (asn1.ObjectIdentifier, error) {
	var oid asn1.ObjectIdentifier

	for p := range strings.SplitSeq(s, ".") {
		n, ok := new(big.Int).SetString(p, radixDecimal)
		if !ok || !n.IsInt64() {
			return nil, errBadParameter("invalid OID")
		}

		oid = append(oid, int(n.Int64()))
	}

	if len(oid) < minOIDArcs {
		return nil, errBadParameter("invalid OID")
	}

	return oid, nil
}

func applySANs(t *x509.Certificate, s *SANs) error {
	if s == nil {
		return nil
	}

	t.EmailAddresses = s.Emails
	t.DNSNames = s.DNSNames

	for _, u := range s.URIs {
		pu, err := url.Parse(u)
		if err != nil {
			return errBadParameter("Invalid URI SAN %q.", u)
		}

		t.URIs = append(t.URIs, pu)
	}

	for _, ip := range s.IPAddresses {
		parsed := net.ParseIP(ip)
		if parsed == nil {
			return errBadParameter("Invalid IP SAN %q.", ip)
		}

		t.IPAddresses = append(t.IPAddresses, parsed)
	}

	return applyUPNs(t, s.UPNs)
}

// selfSign generates key material and a self-signed certificate for pol.
func selfSign(pol *CertPolicy, now time.Time) ([]byte, []byte, error) {
	return selfSignWithKey(pol, nil, now)
}

// selfSignWithKey is selfSign reusing keyDER (PKCS#8) when non-nil.
func selfSignWithKey(pol *CertPolicy, keyDER []byte, now time.Time) ([]byte, []byte, error) {
	if pol.X509Props == nil || pol.X509Props.Subject == "" {
		return nil, nil, errBadParameter("The policy must contain x509_props.subject.")
	}

	subject, err := parseSubject(pol.X509Props.Subject)
	if err != nil {
		return nil, nil, err
	}

	months := pol.X509Props.ValidityMonths
	if months < 1 || months > maxValidityMonth {
		return nil, nil, errBadParameter("validity_months must be between 1 and %d.", maxValidityMonth)
	}

	if keyDER == nil {
		keyDER, _, _, err = generateKeyMaterial(pol.KeyProps.Kty, pol.KeyProps.KeySize, pol.KeyProps.Crv)
		if err != nil {
			return nil, nil, err
		}
	}

	priv, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return nil, nil, err
	}

	tmpl, err := certTemplate(pol, subject, now, months)
	if err != nil {
		return nil, nil, err
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, publicOf(priv), priv)

	return certDER, keyDER, err
}

func certTemplate(pol *CertPolicy, subject pkix.Name, now time.Time, months int) (*x509.Certificate, error) {
	ku, err := keyUsageBits(pol.X509Props.KeyUsage)
	if err != nil {
		return nil, err
	}

	eku, unknown, err := extKeyUsages(pol.X509Props.EKUs)
	if err != nil {
		return nil, err
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), certSerialBits))
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               subject,
		NotBefore:             now.UTC(),
		NotAfter:              now.UTC().AddDate(0, months, 0),
		KeyUsage:              ku,
		ExtKeyUsage:           eku,
		UnknownExtKeyUsage:    unknown,
		BasicConstraintsValid: true,
		IsCA:                  ku&x509.KeyUsageCertSign != 0,
	}

	if err = applySANs(tmpl, pol.X509Props.SANs); err != nil {
		return nil, err
	}

	return tmpl, nil
}

func thumbprint(der []byte) string {
	sum := sha1.Sum(der) //nolint:gosec // thumbprint, not a security hash

	return b64u(sum[:])
}
