package iot

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"sync"
	"time"
)

const (
	deviceKeyBits  = 2048
	iotIssuerOrgU  = "Amazon Web Services O=Amazon.com Inc. L=Seattle ST=Washington C=US"
	serialBitCount = 128
)

// iotCertNotAfter is the fixed expiry AWS IoT stamps on certificates it issues.
func iotCertNotAfter() time.Time {
	return time.Date(2049, time.December, 31, 23, 59, 59, 0, time.UTC)
}

type issuer struct {
	cert *x509.Certificate
	key  crypto.Signer
}

type certIssuer struct {
	err  error
	iss  *issuer
	once sync.Once
}

func (c *certIssuer) get() (*issuer, error) {
	c.once.Do(func() { c.iss, c.err = newIssuer() })

	return c.iss, c.err
}

func newIssuer() (*issuer, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate issuer key: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{OrganizationalUnit: []string{iotIssuerOrgU}},
		NotBefore:             time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              iotCertNotAfter(),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, fmt.Errorf("create issuer certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse issuer certificate: %w", err)
	}

	return &issuer{cert: cert, key: key}, nil
}

// issueDeviceCertificate signs pub with the AWS IoT issuer and returns the certificate PEM.
func (b *InMemoryBackend) issueDeviceCertificate(pub crypto.PublicKey) (string, error) {
	iss, err := b.issuer.get()
	if err != nil {
		return "", err
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBitCount))
	if err != nil {
		return "", fmt.Errorf("serial: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "AWS IoT Certificate"},
		NotBefore:    time.Now().UTC().Add(-time.Minute),
		NotAfter:     iotCertNotAfter(),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, iss.cert, pub, iss.key)
	if err != nil {
		return "", fmt.Errorf("sign certificate: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), nil
}

// generateKeyAndCertificate returns a fresh RSA key pair and its issued certificate, all PEM-encoded.
func (b *InMemoryBackend) generateKeyAndCertificate() (string, string, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, deviceKeyBits)
	if err != nil {
		return "", "", "", fmt.Errorf("generate device key: %w", err)
	}

	certPEM, err := b.issueDeviceCertificate(&key.PublicKey)
	if err != nil {
		return "", "", "", err
	}

	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", "", "", fmt.Errorf("marshal public key: %w", err)
	}

	return certPEM,
		string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})),
		string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
		nil
}

// certificateFromCSR issues a certificate for the public key in a PEM CSR; ok is false for an unusable CSR.
func (b *InMemoryBackend) certificateFromCSR(csrPEM string) (string, bool) {
	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil {
		return "", false
	}

	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || csr.CheckSignature() != nil {
		return "", false
	}

	certPEM, err := b.issueDeviceCertificate(csr.PublicKey)
	if err != nil {
		return "", false
	}

	return certPEM, true
}

// certificateIDFor returns the AWS IoT certificate ID (hex SHA-256 of the DER) of a parseable certificate PEM.
func certificateIDFor(certPEM string) (string, *x509.Certificate, bool) {
	cert := parseCertPEM(certPEM)
	if cert == nil {
		return "", nil, false
	}

	sum := sha256.Sum256(cert.Raw)

	return hex.EncodeToString(sum[:]), cert, true
}
