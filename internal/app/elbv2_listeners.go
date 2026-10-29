package app

import (
	"context"

	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
)

// ELBv2Settings selects how ELBv2 listeners carry traffic.
type ELBv2Settings struct {
	Listeners    string `name:"listeners"     env:"ELBV2_LISTENERS"     default:"routed"    enum:"routed,real" help:"ELBv2 data plane: routed (path/host routing on the emulator port) or real (a local socket per listener, TLS termination for HTTPS/TLS)."`                        //nolint:lll // config struct tags are intentionally verbose
	ListenerHost string `name:"listener-host" env:"ELBV2_LISTENER_HOST" default:"127.0.0.1"                    help:"Bind address for real ELBv2 listeners. Requested ports below 1024 or already taken map to a port from the shared pool; see GET /_gopherstack/elbv2/_listeners."` //nolint:lll // config struct tags are intentionally verbose
}

// GetELBv2Listeners returns the ELBv2 data-plane mode (elbv2.ListenerModeConfig).
func (c *CLI) GetELBv2Listeners() string { return c.ELBv2.Listeners }

// GetELBv2ListenerHost returns the real-listener bind address (elbv2.ListenerModeConfig).
func (c *CLI) GetELBv2ListenerHost() string { return c.ELBv2.ListenerHost }

// CertificateKeyPair returns the ACM certificate body with its chain and private key.
// IAM server certificates carry no private key in the emulator, so they cannot terminate TLS.
func (a *elbv2CertificateResolverAdapter) CertificateKeyPair(certARN string) ([]byte, []byte, bool) {
	if a.acmBackend == nil {
		return nil, nil, false
	}

	return acmKeyPair(a.acmBackend, certARN)
}

func acmKeyPair(b *acmbackend.InMemoryBackend, certARN string) ([]byte, []byte, bool) {
	cert, err := b.DescribeCertificate(context.Background(), certARN)
	if err != nil || cert.PrivateKey == "" || cert.CertificateBody == "" {
		return nil, nil, false
	}

	pemBytes := cert.CertificateBody
	if cert.CertificateChain != "" {
		pemBytes += "\n" + cert.CertificateChain
	}

	return []byte(pemBytes), []byte(cert.PrivateKey), true
}
