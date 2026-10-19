package devtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// ErrInvalidCAPEM is returned when a persisted CA cannot be decoded.
var ErrInvalidCAPEM = errors.New("devtls: invalid CA PEM")

// CA is an in-memory certificate authority that signs short-lived leaf certificates.
type CA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

// NewCA creates a self-signed CA certificate valid for [DefaultValidity].
func NewCA() (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("devtls: generate ca key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "gopherstack local CA", Organization: []string{orgName}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(DefaultValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("devtls: create ca certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("devtls: parse ca certificate: %w", err)
	}

	return &CA{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// CertPEM returns the CA certificate in PEM form, for clients to trust.
func (c *CA) CertPEM() []byte { return append([]byte(nil), c.certPEM...) }

// Pool returns a cert pool containing only this CA.
func (c *CA) Pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(c.cert)

	return p
}

// Issue signs a server leaf certificate covering the given DNS names and IPs
// (each entry is tried as an IP first).
func (c *CA) Issue(hosts ...string) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("devtls: generate leaf key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	dnsNames, ips := hostsToNamesAndIPs(hosts)

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: orgName, Organization: []string{orgName}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(DefaultValidity),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		return nil, fmt.Errorf("devtls: sign leaf certificate: %w", err)
	}

	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

func randomSerial() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBits))
	if err != nil {
		return nil, fmt.Errorf("devtls: generate serial: %w", err)
	}

	return serial, nil
}

const (
	orgName    = "gopherstack"
	caCertFile = "ca.pem"
	caKeyFile  = "ca-key.pem"
	caDirMode  = 0o700
	caFileMode = 0o600
)

// LoadOrCreateCA returns the CA persisted in dir, creating and saving one when absent,
// so clients can trust it once across restarts.
func LoadOrCreateCA(dir string) (*CA, error) {
	certPath, keyPath := filepath.Join(dir, caCertFile), filepath.Join(dir, caKeyFile)

	certPEM, certErr := os.ReadFile(certPath)
	keyPEM, keyErr := os.ReadFile(keyPath)

	if certErr == nil && keyErr == nil {
		return parseCA(certPEM, keyPEM)
	}

	ca, err := NewCA()
	if err != nil {
		return nil, err
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(ca.key)
	if err != nil {
		return nil, fmt.Errorf("devtls: marshal ca key: %w", err)
	}

	if mkErr := os.MkdirAll(dir, caDirMode); mkErr != nil {
		return nil, fmt.Errorf("devtls: create ca dir: %w", mkErr)
	}

	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if wErr := os.WriteFile(keyPath, keyPEM, caFileMode); wErr != nil {
		return nil, fmt.Errorf("devtls: write ca key: %w", wErr)
	}

	if wErr := os.WriteFile(certPath, ca.certPEM, caFileMode); wErr != nil {
		return nil, fmt.Errorf("devtls: write ca cert: %w", wErr)
	}

	return ca, nil
}

func parseCA(certPEM, keyPEM []byte) (*CA, error) {
	cb, _ := pem.Decode(certPEM)
	kb, _ := pem.Decode(keyPEM)

	if cb == nil || kb == nil {
		return nil, ErrInvalidCAPEM
	}

	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, fmt.Errorf("devtls: parse ca certificate: %w", err)
	}

	parsed, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return nil, fmt.Errorf("devtls: parse ca key: %w", err)
	}

	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, ErrInvalidCAPEM
	}

	return &CA{cert: cert, key: key, certPEM: certPEM}, nil
}
