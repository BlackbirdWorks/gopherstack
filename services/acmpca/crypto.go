package acmpca

import (
	"crypto/ecdsa"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// generateCSR generates a PEM-encoded CSR from the given private key and subject.
func generateCSR(privKey *ecdsa.PrivateKey, subject CertificateAuthoritySubject) (string, error) {
	cn := subject.CommonName
	if cn == "" {
		cn = "Gopherstack Root CA"
	}

	tmpl := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:         cn,
			Country:            nonEmptySlice(subject.Country),
			Organization:       nonEmptySlice(subject.Organization),
			OrganizationalUnit: nonEmptySlice(subject.OrganizationalUnit),
			Province:           nonEmptySlice(subject.State),
			Locality:           nonEmptySlice(subject.Locality),
		},
	}

	csrDER, err := x509.CreateCertificateRequest(cryptorand.Reader, tmpl, privKey)
	if err != nil {
		return "", fmt.Errorf("create CSR: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})), nil
}

// selfSignCA generates a self-signed certificate for the given CA and returns the PEM and serial hex.
func selfSignCA(ca *CertificateAuthority, now time.Time) (string, string, error) {
	if ca.privKey == nil {
		return "", "", errCAPrivKeyNil
	}

	serial, err := cryptorand.Int(
		cryptorand.Reader,
		new(big.Int).Lsh(big.NewInt(1), serialBitLen),
	)
	if err != nil {
		return "", "", fmt.Errorf("generate serial: %w", err)
	}

	cn := ca.CertificateAuthorityConfiguration.Subject.CommonName
	if cn == "" {
		cn = "Gopherstack Root CA"
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         cn,
			Organization:       nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.Organization),
			Country:            nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.Country),
			OrganizationalUnit: nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.OrganizationalUnit),
			Province:           nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.State),
			Locality:           nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.Locality),
		},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &ca.privKey.PublicKey, ca.privKey)
	if err != nil {
		return "", "", fmt.Errorf("create certificate: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})),
		hex.EncodeToString(serial.Bytes()),
		nil
}

// signCSR signs a CSR using the CA's private key and returns the PEM certificate and serial.
// notBefore, when non-zero, overrides the default "now" NotBefore (ValidityNotBefore on
// IssueCertificateInput); ap, when non-nil, applies APIPassthrough/APICSRPassthrough template
// overrides (subject + X.509 extensions) exactly as aws-sdk-go-v2's APIPassthrough input does.
func signCSR(
	ca *CertificateAuthority, csrPEM string, validityDays int, notBefore time.Time, ap *APIPassthrough,
) (string, string, error) {
	if ca.privKey == nil {
		return "", "", errCAPrivKeyNil
	}

	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil {
		return "", "", errDecodeCSRPEM
	}

	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CSR: %w", err)
	}

	// Parse CA certificate to get the issuer details.
	caBlock, _ := pem.Decode([]byte(ca.CertificateBody))
	if caBlock == nil {
		return "", "", errDecodeCACertPEM
	}

	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CA certificate: %w", err)
	}

	serial, err := cryptorand.Int(
		cryptorand.Reader,
		new(big.Int).Lsh(big.NewInt(1), serialBitLen),
	)
	if err != nil {
		return "", "", fmt.Errorf("generate serial: %w", err)
	}

	now := time.Now().UTC()
	if notBefore.IsZero() {
		notBefore = now
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      csr.Subject,
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(time.Duration(validityDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     csr.DNSNames,
	}

	if applyErr := applyAPIPassthrough(tmpl, ap); applyErr != nil {
		return "", "", applyErr
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, caCert, csr.PublicKey, ca.privKey)
	if err != nil {
		return "", "", fmt.Errorf("create certificate: %w", err)
	}

	serialHex := hex.EncodeToString(serial.Bytes())

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})), serialHex, nil
}

// applyAPIPassthrough overlays ap's subject/extension overrides onto tmpl, mirroring
// how Amazon Web Services Private CA applies the APIPassthrough input of IssueCertificate
// (only honored when the request's TemplateArn selects an APIPassthrough/APICSRPassthrough
// template variant -- see decodeAPIPassthrough in handler_certificates.go, which enforces
// that gating before this is ever called with a non-nil ap).
func applyAPIPassthrough(tmpl *x509.Certificate, ap *APIPassthrough) error {
	if ap == nil {
		return nil
	}

	if ap.Subject != nil {
		tmpl.Subject = apiPassthroughSubjectToPKIX(ap.Subject)
	}

	if ap.Extensions == nil {
		return nil
	}

	if ap.Extensions.KeyUsage != nil {
		tmpl.KeyUsage = apiPassthroughKeyUsageBits(ap.Extensions.KeyUsage)
	}

	if err := applyExtendedKeyUsage(tmpl, ap.Extensions.ExtendedKeyUsage); err != nil {
		return err
	}

	if err := applySubjectAlternativeNames(tmpl, ap.Extensions.SubjectAlternativeNames); err != nil {
		return err
	}

	return applyCustomExtensions(tmpl, ap.Extensions.CustomExtensions)
}

func apiPassthroughSubjectToPKIX(s *APIPassthroughSubject) pkix.Name {
	return pkix.Name{
		CommonName:         s.CommonName,
		SerialNumber:       s.SerialNumber,
		Country:            nonEmptySlice(s.Country),
		Organization:       nonEmptySlice(s.Organization),
		OrganizationalUnit: nonEmptySlice(s.OrganizationalUnit),
		Province:           nonEmptySlice(s.State),
		Locality:           nonEmptySlice(s.Locality),
	}
}

func apiPassthroughKeyUsageBits(ku *APIPassthroughKeyUsage) x509.KeyUsage {
	var bits x509.KeyUsage

	add := func(cond bool, bit x509.KeyUsage) {
		if cond {
			bits |= bit
		}
	}

	add(ku.DigitalSignature, x509.KeyUsageDigitalSignature)
	add(ku.NonRepudiation, x509.KeyUsageContentCommitment)
	add(ku.KeyEncipherment, x509.KeyUsageKeyEncipherment)
	add(ku.DataEncipherment, x509.KeyUsageDataEncipherment)
	add(ku.KeyAgreement, x509.KeyUsageKeyAgreement)
	add(ku.KeyCertSign, x509.KeyUsageCertSign)
	add(ku.CRLSign, x509.KeyUsageCRLSign)
	add(ku.EncipherOnly, x509.KeyUsageEncipherOnly)
	add(ku.DecipherOnly, x509.KeyUsageDecipherOnly)

	return bits
}

// onceStandardExtKeyUsage lazily builds the ExtendedKeyUsageType enum values
// that have a crypto/x509.ExtKeyUsage equivalent (see types.ExtendedKeyUsageType
// in aws-sdk-go-v2). The remaining three enum values (SMART_CARD_LOGIN,
// DOCUMENT_SIGNING, CERTIFICATE_TRANSPARENCY) have no crypto/x509 constant --
// see onceStandardExtKeyUsageOID below, which encodes them by raw OID instead.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var onceStandardExtKeyUsage = sync.OnceValue(func() map[string]x509.ExtKeyUsage {
	return map[string]x509.ExtKeyUsage{
		"SERVER_AUTH":      x509.ExtKeyUsageServerAuth,
		"CLIENT_AUTH":      x509.ExtKeyUsageClientAuth,
		"CODE_SIGNING":     x509.ExtKeyUsageCodeSigning,
		"EMAIL_PROTECTION": x509.ExtKeyUsageEmailProtection,
		"TIME_STAMPING":    x509.ExtKeyUsageTimeStamping,
		"OCSP_SIGNING":     x509.ExtKeyUsageOCSPSigning,
	}
})

// onceStandardExtKeyUsageOID lazily builds the ExtendedKeyUsageType enum values
// crypto/x509 has no named ExtKeyUsage constant for; each is applied via
// x509.Certificate.UnknownExtKeyUsage using its well-known OID (Microsoft's
// Smart Card Logon and Document Signing OIDs, and the Certificate Transparency
// EKU OID from RFC 6962-bis drafts / CA/Browser Forum usage).
//
//nolint:gochecknoglobals // read-only package-level lookup table
var onceStandardExtKeyUsageOID = sync.OnceValue(func() map[string]asn1.ObjectIdentifier {
	return map[string]asn1.ObjectIdentifier{
		"SMART_CARD_LOGIN":         {1, 3, 6, 1, 4, 1, 311, 20, 2, 2},
		"DOCUMENT_SIGNING":         {1, 3, 6, 1, 4, 1, 311, 10, 3, 12},
		"CERTIFICATE_TRANSPARENCY": {1, 3, 6, 1, 4, 1, 11129, 2, 4, 4},
	}
})

func applyExtendedKeyUsage(tmpl *x509.Certificate, ekus []APIPassthroughExtendedKeyUsage) error {
	if len(ekus) == 0 {
		return nil
	}

	tmpl.ExtKeyUsage = nil
	tmpl.UnknownExtKeyUsage = nil

	for _, eku := range ekus {
		if err := applyOneExtendedKeyUsage(tmpl, eku); err != nil {
			return err
		}
	}

	return nil
}

func applyOneExtendedKeyUsage(tmpl *x509.Certificate, eku APIPassthroughExtendedKeyUsage) error {
	if eku.ObjectIdentifier != "" {
		oid, err := parseOID(eku.ObjectIdentifier)
		if err != nil {
			return fmt.Errorf("%w: ExtendedKeyUsageObjectIdentifier %q: %w",
				ErrInvalidParameter, eku.ObjectIdentifier, err)
		}

		tmpl.UnknownExtKeyUsage = append(tmpl.UnknownExtKeyUsage, oid)

		return nil
	}

	if xku, ok := onceStandardExtKeyUsage()[eku.Type]; ok {
		tmpl.ExtKeyUsage = append(tmpl.ExtKeyUsage, xku)

		return nil
	}

	if oid, ok := onceStandardExtKeyUsageOID()[eku.Type]; ok {
		tmpl.UnknownExtKeyUsage = append(tmpl.UnknownExtKeyUsage, oid)

		return nil
	}

	return fmt.Errorf("%w: unsupported ExtendedKeyUsageType %q", ErrInvalidParameter, eku.Type)
}

func applySubjectAlternativeNames(tmpl *x509.Certificate, sans []APIPassthroughSAN) error {
	if len(sans) == 0 {
		return nil
	}

	tmpl.DNSNames = nil

	var ips []net.IP

	var emails []string

	var dns []string

	for _, san := range sans {
		switch {
		case san.DNSName != "":
			dns = append(dns, san.DNSName)
		case san.IPAddress != "":
			ip := net.ParseIP(san.IPAddress)
			if ip == nil {
				return fmt.Errorf(
					"%w: invalid SubjectAlternativeNames IpAddress %q", ErrInvalidParameter, san.IPAddress,
				)
			}

			ips = append(ips, ip)
		case san.EmailAddress != "":
			emails = append(emails, san.EmailAddress)
		}
	}

	tmpl.DNSNames = dns
	tmpl.IPAddresses = ips
	tmpl.EmailAddresses = emails

	return nil
}

func applyCustomExtensions(tmpl *x509.Certificate, exts []APIPassthroughCustomExtension) error {
	for _, ext := range exts {
		oid, err := parseOID(ext.ObjectIdentifier)
		if err != nil {
			return fmt.Errorf(
				"%w: CustomExtensions ObjectIdentifier %q: %w", ErrInvalidParameter, ext.ObjectIdentifier, err,
			)
		}

		value, err := base64.StdEncoding.DecodeString(ext.ValueBase64)
		if err != nil {
			return fmt.Errorf("%w: CustomExtensions Value must be base64-encoded: %w", ErrInvalidParameter, err)
		}

		tmpl.ExtraExtensions = append(tmpl.ExtraExtensions, pkix.Extension{
			Id:       oid,
			Critical: ext.Critical,
			Value:    value,
		})
	}

	return nil
}

// parseOID parses a dotted-decimal OID string (e.g. "2.5.29.32.0") into an
// asn1.ObjectIdentifier.
func parseOID(dotted string) (asn1.ObjectIdentifier, error) {
	parts := strings.Split(dotted, ".")
	if len(parts) < 2 { //nolint:mnd // an OID needs at least two arcs
		return nil, fmt.Errorf("%w: OID must have at least two components", ErrInvalidParameter)
	}

	oid := make(asn1.ObjectIdentifier, len(parts))

	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%w: OID component %q is not numeric", ErrInvalidParameter, p)
		}

		oid[i] = n
	}

	return oid, nil
}

// nonEmptySlice returns a slice containing the string if it is non-empty, or nil otherwise.
func nonEmptySlice(s string) []string {
	if s == "" {
		return nil
	}

	return []string{s}
}

// extractCAID extracts the CA ID from a CA ARN.
// e.g. arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/abc123 → abc123.
func extractCAID(caARN string) string {
	parts := splitARN(caARN)
	if len(parts) == 0 {
		return caARN
	}

	last := parts[len(parts)-1]
	// strip "certificate-authority/" prefix
	const prefix = "certificate-authority/"
	if len(last) > len(prefix) {
		return last[len(prefix):]
	}

	return last
}

// newRandomID generates a 16-byte cryptographically-random ID formatted as a
// dashed UUID (8-4-4-4-12 hex digits), matching the shape of real ACM PCA
// resource IDs (e.g. "arn:aws:acm-pca:region:account:certificate-authority/
// 12345678-1234-1234-1234-123456789012" -- see aws-sdk-go-v2's
// CreateCertificateAuthorityOutput doc comment). gopherstack previously used a
// flat 32-char hex string with no dashes here, which is opaque to SDK clients
// (harmless) but would fail a client-side regex validating ARN shape against
// AWS's literal UUID pattern (see PARITY.md gaps, fixed this pass).
func newRandomID() (string, error) {
	const uuidBytes = 16

	buf := make([]byte, uuidBytes)
	if _, err := io.ReadFull(cryptorand.Reader, buf); err != nil {
		return "", fmt.Errorf("generate random ID: %w", err)
	}

	hexStr := hex.EncodeToString(buf)

	return strings.Join([]string{
		hexStr[0:8], hexStr[8:12], hexStr[12:16], hexStr[16:20], hexStr[20:32],
	}, "-"), nil
}

func splitARN(a string) []string {
	// ARN format: arn:partition:service:region:account:resource
	// We want the resource part after the 5th colon.
	count := 0
	for i, c := range a {
		if c == ':' {
			count++
			if count == 5 { //nolint:mnd // 5th colon separates account from resource
				return []string{a[i+1:]}
			}
		}
	}

	return nil
}
