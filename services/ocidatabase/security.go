package ocidatabase

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
)

type retiredCA struct {
	until time.Time
	cert  *x509.Certificate
}

type material struct {
	serverCA *devtls.CA
	clientCA *devtls.CA
	server   *tls.Certificate
	retired  []retiredCA
}

// security is the TLS material of one database: a stable server CA and certificate (trusted by the wallet) and a
// client CA that wallet rotation replaces.
type security struct {
	cur atomic.Pointer[material]
	now func() time.Time
}

func newSecurity(now func() time.Time) (*security, error) {
	serverCA, err := devtls.NewCA()
	if err != nil {
		return nil, fmt.Errorf("server ca: %w", err)
	}

	clientCA, err := devtls.NewCA()
	if err != nil {
		return nil, fmt.Errorf("client ca: %w", err)
	}

	server, err := serverCA.Issue()
	if err != nil {
		return nil, fmt.Errorf("server certificate: %w", err)
	}

	s := &security{now: now}
	s.cur.Store(&material{serverCA: serverCA, clientCA: clientCA, server: server})

	return s, nil
}

// rotate replaces the client CA; the previous one stays trusted for grace.
func (s *security) rotate(grace time.Duration) error {
	next, err := devtls.NewCA()
	if err != nil {
		return fmt.Errorf("client ca: %w", err)
	}

	old := s.cur.Load()
	m := &material{serverCA: old.serverCA, clientCA: next, server: old.server}

	t := s.now()
	for _, r := range old.retired {
		if r.until.After(t) {
			m.retired = append(m.retired, r)
		}
	}

	if grace > 0 {
		m.retired = append(m.retired, retiredCA{cert: old.clientCA.Certificate(), until: t.Add(grace)})
	}

	s.cur.Store(m)

	return nil
}

func (s *security) clientPool() *x509.CertPool {
	m := s.cur.Load()
	pool := m.clientCA.Pool()

	t := s.now()
	for _, r := range m.retired {
		if r.until.After(t) {
			pool.AddCert(r.cert)
		}
	}

	return pool
}

// serverConfig returns the listener TLS configuration; with requireClient it verifies client certificates.
func (s *security) serverConfig(requireClient bool) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*s.cur.Load().server}}
			if requireClient {
				cfg.ClientAuth = tls.RequireAndVerifyClientCert
				cfg.ClientCAs = s.clientPool()
			}

			return cfg, nil
		},
	}
}

func (s *security) issueClient(cn string) (*tls.Certificate, error) {
	cert, err := s.cur.Load().clientCA.IssueClient(cn)
	if err != nil {
		return nil, fmt.Errorf("client certificate: %w", err)
	}

	return cert, nil
}

func (s *security) trustCerts() []*x509.Certificate {
	m := s.cur.Load()

	return []*x509.Certificate{m.serverCA.Certificate(), m.clientCA.Certificate()}
}

func parseLeaf(c *tls.Certificate) (*x509.Certificate, error) {
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse client certificate: %w", err)
	}

	return leaf, nil
}
