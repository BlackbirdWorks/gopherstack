package neptune

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
)

const envTLS = "NEPTUNE_TLS"

// tlsListener serves the cluster port over TLS 1.2+ (HTTPS, WSS and Bolt over TLS) with a self-signed certificate.
func tlsListener(ln net.Listener, host string) (net.Listener, []byte, error) {
	certPEM, keyPEM, err := devtls.GenerateSelfSignedCertPEM(host)
	if err != nil {
		return nil, nil, fmt.Errorf("neptune TLS certificate: %w", err)
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("neptune TLS key pair: %w", err)
	}

	return tls.NewListener(
		ln,
		&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	), certPEM, nil
}
