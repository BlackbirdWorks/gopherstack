package smtptest

import (
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
)

// TLSConfigs returns a server config with a fresh self-signed 127.0.0.1 cert and a client config trusting it.
func TLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()

	certPEM, keyPEM, err := devtls.GenerateSelfSignedCertPEM("127.0.0.1")
	if err != nil {
		t.Fatalf("smtptest cert: %v", err)
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("smtptest keypair: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)

	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}
