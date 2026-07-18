package acmpca

import (
	"crypto/ecdsa"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
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
func signCSR(ca *CertificateAuthority, csrPEM string, validityDays int) (string, string, error) {
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
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      csr.Subject,
		NotBefore:    now,
		NotAfter:     now.Add(time.Duration(validityDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     csr.DNSNames,
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, caCert, csr.PublicKey, ca.privKey)
	if err != nil {
		return "", "", fmt.Errorf("create certificate: %w", err)
	}

	serialHex := hex.EncodeToString(serial.Bytes())

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})), serialHex, nil
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

// newRandomID generates a 16-byte cryptographically-random hex ID for ARN resource segments.
func newRandomID() (string, error) {
	buf := make([]byte, 16) //nolint:mnd // 16-byte random ID
	if _, err := io.ReadFull(cryptorand.Reader, buf); err != nil {
		return "", fmt.Errorf("generate random ID: %w", err)
	}

	return hex.EncodeToString(buf), nil
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
