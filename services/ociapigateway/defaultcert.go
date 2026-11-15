package ociapigateway

import (
	"crypto/tls"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// defaultCerts holds the self-signed certificates served for gateways created without a certificate.
type defaultCerts struct {
	mu    *lockmetrics.RWMutex
	certs map[string]tls.Certificate
}

func newDefaultCerts() *defaultCerts {
	return &defaultCerts{mu: lockmetrics.New("ociapigateway.defaultcerts"), certs: map[string]tls.Certificate{}}
}

func (d *defaultCerts) forGateway(gwID, hostname string) (tls.Certificate, error) {
	d.mu.Lock("forGateway")
	defer d.mu.Unlock()

	if c, ok := d.certs[gwID]; ok {
		return c, nil
	}

	c, err := devtls.GenerateSelfSignedCert(hostname)
	if err != nil {
		return tls.Certificate{}, err
	}

	d.certs[gwID] = c

	return c, nil
}

func (d *defaultCerts) forget(gwID string) {
	d.mu.Lock("forget")
	delete(d.certs, gwID)
	d.mu.Unlock()
}

func (d *defaultCerts) reset() {
	d.mu.Lock("reset")
	clear(d.certs)
	d.mu.Unlock()
}
