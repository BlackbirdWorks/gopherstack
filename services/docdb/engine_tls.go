package docdb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	tlsMountDir   = "/gopherstack-tls"
	tlsServerFile = "server.pem"
	tlsCAFile     = "ca.pem"
	tlsCertTTL    = 10 * 365 * 24 * time.Hour
	tlsSerialBits = 62
	tlsDirPerm    = 0o755
	tlsFilePerm   = 0o644
	tlsServerName = "localhost"
)

// tlsAuthority is the per-backend CA that signs each cluster's mongod server certificate.
type tlsAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
	pem  []byte
}

func randSerial() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), tlsSerialBits))
	if err != nil {
		return nil, fmt.Errorf("serial: %w", err)
	}

	return n, nil
}

func newTLSAuthority() (*tlsAuthority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}

	serial, err := randSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "Gopherstack DocumentDB CA",
			Organization: []string{"Gopherstack"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(tlsCertTTL),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("sign CA: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse CA: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	return &tlsAuthority{
		cert: cert, key: key, pool: pool,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}, nil
}

// serverPEM issues a certificate for localhost, loopback and host, concatenated with its key as mongod expects.
func (a *tlsAuthority) serverPEM(host string) ([]byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate server key: %w", err)
	}

	serial, err := randSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: tlsServerName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(tlsCertTTL),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{tlsServerName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}

	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
	} else if host != "" {
		tmpl.DNSNames = append(tmpl.DNSNames, host)
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, fmt.Errorf("sign server cert: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal server key: %w", err)
	}

	out := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	return append(out, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})...), nil
}

// writeDir writes the server cert+key and CA into a fresh directory that is bind-mounted into the container.
func (a *tlsAuthority) writeDir(host string) (string, error) {
	srv, err := a.serverPEM(host)
	if err != nil {
		return "", err
	}

	dir, err := os.MkdirTemp("", "gopherstack-docdb-tls-")
	if err != nil {
		return "", fmt.Errorf("tls dir: %w", err)
	}

	files := map[string][]byte{tlsServerFile: srv, tlsCAFile: a.pem}
	for name, data := range files {
		if err = os.WriteFile(filepath.Join(dir, name), data, tlsFilePerm); err != nil {
			_ = os.RemoveAll(dir)

			return "", fmt.Errorf("write %s: %w", name, err)
		}
	}

	if err = os.Chmod(dir, tlsDirPerm); err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("chmod tls dir: %w", err)
	}

	return dir, nil
}

func (a *tlsAuthority) clientConfig() *tls.Config {
	return &tls.Config{RootCAs: a.pool, ServerName: tlsServerName, MinVersion: tls.VersionTLS12}
}

// tlsArgs are the mongod flags that require TLS using the mounted server certificate.
func tlsArgs() []string {
	return []string{
		"mongod", "--tlsMode", "requireTLS", "--tlsCertificateKeyFile", tlsMountDir + "/" + tlsServerFile,
		"--tlsCAFile", tlsMountDir + "/" + tlsCAFile, "--tlsAllowConnectionsWithoutCertificates", "--bind_ip_all",
	}
}

// TLSCAPEM returns the CA certificate (PEM) that signs the docker engine's mongod certificates, or nil when
// TLS is off; clients pass it as tlsCAFile.
func (b *InMemoryBackend) TLSCAPEM() []byte {
	b.mu.RLock("TLSCAPEM")
	defer b.mu.RUnlock()

	if b.engine == nil || b.engine.tls == nil {
		return nil
	}

	return append([]byte(nil), b.engine.tls.pem...)
}

func probeMongoTLS(cfg *tls.Config) func(context.Context, string) error {
	return func(ctx context.Context, addr string) error {
		return probeMongoConn(ctx, addr, cfg)
	}
}
