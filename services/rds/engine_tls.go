package rds

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
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	engineCAFile     = "rds-ca.pem"
	engineCAKeyFile  = "rds-ca-key.pem"
	engineCAValidity = 10 * 365 * 24 * time.Hour
	engineLeafLife   = 365 * 24 * time.Hour
	engineSecretMode = 0o600
	engineDirMode    = 0o700
	engineSerialBits = 62
)

var (
	errInvalidCAPEM = errors.New("invalid ca pem")
	errCAExpired    = errors.New("ca expired")
)

// engineCA is the local certificate authority that signs the proxy's server certificate.
type engineCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newSerial() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), engineSerialBits))
	if err != nil {
		return nil, fmt.Errorf("serial: %w", err)
	}

	return n, nil
}

func generateEngineCA() (*engineCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ca key: %w", err)
	}

	serial, err := newSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Gopherstack RDS Local CA", Organization: []string{"Gopherstack"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(engineCAValidity),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("ca cert: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse ca: %w", err)
	}

	return &engineCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// loadOrCreateEngineCA reuses the CA persisted in dir; an empty dir keeps the CA in memory only.
func loadOrCreateEngineCA(dir string) (*engineCA, error) {
	if dir == "" {
		return generateEngineCA()
	}

	if ca, err := loadEngineCA(dir); err == nil {
		return ca, nil
	}

	ca, err := generateEngineCA()
	if err != nil {
		return nil, err
	}

	if err = ca.save(dir); err != nil {
		return nil, err
	}

	return ca, nil
}

func loadEngineCA(dir string) (*engineCA, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, engineCAFile))
	if err != nil {
		return nil, fmt.Errorf("read ca: %w", err)
	}

	keyPEM, err := os.ReadFile(filepath.Join(dir, engineCAKeyFile))
	if err != nil {
		return nil, fmt.Errorf("read ca key: %w", err)
	}

	certBlock, _ := pem.Decode(certPEM)
	keyBlock, _ := pem.Decode(keyPEM)

	if certBlock == nil || keyBlock == nil {
		return nil, errInvalidCAPEM
	}

	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse ca: %w", err)
	}

	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse ca key: %w", err)
	}

	if time.Now().After(cert.NotAfter) {
		return nil, errCAExpired
	}

	return &engineCA{cert: cert, key: key, pem: certPEM}, nil
}

func (ca *engineCA) save(dir string) error {
	if err := os.MkdirAll(dir, engineDirMode); err != nil {
		return fmt.Errorf("ca dir: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(ca.key)
	if err != nil {
		return fmt.Errorf("marshal ca key: %w", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if err = os.WriteFile(filepath.Join(dir, engineCAKeyFile), keyPEM, engineSecretMode); err != nil {
		return fmt.Errorf("write ca key: %w", err)
	}

	if err = os.WriteFile(filepath.Join(dir, engineCAFile), ca.pem, engineSecretMode); err != nil {
		return fmt.Errorf("write ca: %w", err)
	}

	return nil
}

// serverCert issues a leaf whose SANs cover host and the loopback names.
func (ca *engineCA) serverCert(host string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("leaf key: %w", err)
	}

	serial, err := newSerial()
	if err != nil {
		return tls.Certificate{}, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "gopherstack-rds"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(engineLeafLife),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{hostLocalhost},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
	} else if host != "" && host != hostLocalhost {
		tmpl.DNSNames = append(tmpl.DNSNames, host)
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("leaf cert: %w", err)
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
