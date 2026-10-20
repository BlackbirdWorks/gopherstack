// Package transparent serves gopherstack over HTTPS for unmodified AWS SDKs
// that target real *.amazonaws.com endpoints (DNS points them at gopherstack).
package transparent

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
)

const (
	// DefaultAddr is the HTTPS listen address real AWS endpoints expect.
	DefaultAddr = ":443"
	// CAPath serves the CA certificate clients must trust.
	CAPath = "/_gopherstack/ca.pem"

	maxCachedCerts    = 512
	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 5 * time.Second
)

// IsAWSHost reports whether host (port optional) is a real AWS endpoint name.
func IsAWSHost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	host = strings.Trim(strings.ToLower(host), ".")

	for _, z := range []string{"amazonaws.com", "amazonaws.com.cn", "api.aws", "on.aws"} {
		if strings.HasSuffix(host, "."+z) {
			return true
		}
	}

	return false
}

// CertSource issues leaf certificates on demand from SNI, signed by a CA.
type CertSource struct {
	ca    *devtls.CA
	cache *safemap.Map[string, *tls.Certificate]
}

// NewCertSource creates a CertSource backed by ca.
func NewCertSource(ca *devtls.CA) *CertSource {
	return &CertSource{ca: ca, cache: safemap.New[string, *tls.Certificate]("transparent.certs")}
}

// Close releases the cache.
func (c *CertSource) Close() { c.cache.Close() }

// GetCertificate implements [tls.Config.GetCertificate].
// A name's certificate also covers its siblings via a one-label wildcard on the parent zone.
func (c *CertSource) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := strings.Trim(strings.ToLower(hello.ServerName), ".")
	if name == "" || net.ParseIP(name) != nil {
		return c.get("default", "localhost", "*.amazonaws.com", "*.amazonaws.com.cn", "*.api.aws", "*.on.aws")
	}

	_, parent, ok := strings.Cut(name, ".")
	if !ok || !strings.Contains(parent, ".") {
		return c.get(name, name)
	}

	return c.get(parent, name, "*."+parent)
}

func (c *CertSource) get(key string, hosts ...string) (*tls.Certificate, error) {
	if cert, ok := c.cache.Get(key); ok {
		return cert, nil
	}

	cert, err := c.ca.Issue(hosts...)
	if err != nil {
		return nil, fmt.Errorf("transparent: issue %q: %w", key, err)
	}

	if c.cache.Len() >= maxCachedCerts {
		c.cache.Clear()
	}

	c.cache.Set(key, cert)

	return cert, nil
}

// CAHandler serves the CA certificate PEM.
func CAHandler(ca *devtls.CA) http.Handler {
	pem := ca.CertPEM()

	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-pem-file")
		_, _ = w.Write(pem)
	})
}

// Server is a running transparent HTTPS listener.
type Server struct {
	srv   *http.Server
	certs *CertSource
	ln    net.Listener
	done  chan struct{}
	once  sync.Once
}

// Start listens on addr and serves h over TLS with on-demand AWS certificates until ctx is done.
func Start(ctx context.Context, addr string, h http.Handler, ca *devtls.CA) (*Server, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transparent: listen %s: %w", addr, err)
	}

	certs := NewCertSource(ca)
	s := &Server{
		certs: certs,
		ln:    ln,
		done:  make(chan struct{}),
		srv: &http.Server{
			Handler:           h,
			ReadHeaderTimeout: readHeaderTimeout,
			TLSConfig: &tls.Config{
				MinVersion:     tls.VersionTLS12,
				GetCertificate: certs.GetCertificate,
				NextProtos:     []string{"h2", "http/1.1"},
			},
		},
	}

	go func() {
		defer close(s.done)

		if serr := s.srv.ServeTLS(ln, "", ""); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			_ = ln.Close()
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-s.done:
		}
	}()

	return s, nil
}

// Addr returns the bound listen address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Close gracefully shuts the listener down and waits for it to exit.
func (s *Server) Close() error {
	var err error

	s.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		err = s.srv.Shutdown(ctx)
		<-s.done
		s.certs.Close()
	})

	return err
}
