// Package dns provides an embedded DNS server for Gopherstack that resolves
// synthetic AWS-style hostnames (e.g. my-cluster.abc.us-east-1.cache.amazonaws.com)
// back to a configured IP address (typically 127.0.0.1).
//
// Usage:
//
//	srv, err := dns.New(dns.Config{ListenAddr: ":10053", ResolveIP: "127.0.0.1"})
//	srv.Register("my-cluster.abc.us-east-1.cache.amazonaws.com")
//	if err := srv.Start(ctx); err != nil { ... }
//	defer srv.Stop()
package dns

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// DefaultListenAddr is the default UDP/TCP address the DNS server binds to.
const DefaultListenAddr = ":10053"

// DefaultResolveIP is the IP address returned for every registered hostname.
const DefaultResolveIP = "127.0.0.1"

// DefaultReadTimeout is the read timeout for the underlying DNS server.
const DefaultReadTimeout = 5 * time.Second

// DefaultWriteTimeout is the write timeout for the underlying DNS server.
const DefaultWriteTimeout = 5 * time.Second

// defaultTTL is the DNS time-to-live (in seconds) for synthetic A records.
const defaultTTL = 60

// ErrInvalidResolveIP is returned when the configured resolve IP cannot be parsed.
var ErrInvalidResolveIP = errors.New("invalid resolve IP")

// ErrIPv4Required is returned when the configured resolve IP is not an IPv4 address.
var ErrIPv4Required = errors.New("resolve IP must be an IPv4 address")

// Config holds the configuration for the embedded DNS server.
type Config struct {
	// Logger is an optional structured logger.
	Logger *slog.Logger
	// ListenAddr is the host:port to bind (default ":10053").
	ListenAddr string
	// ResolveIP is the IP address returned for every registered name (default "127.0.0.1").
	ResolveIP string
}

// Server is an embedded DNS server that answers A queries for registered
// synthetic hostnames with a fixed IP address.
type Server struct {
	names     map[string]struct{}
	udpServer *dns.Server
	tcpServer *dns.Server

	mu     *lockmetrics.RWMutex
	stopCh chan struct{}

	cfg        Config
	listenAddr string
	resolveIP  net.IP

	stopOnce sync.Once
}

// New creates a new Server with the given config.
// Zero-value Config fields are filled with defaults.
func New(cfg Config) (*Server, error) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = DefaultListenAddr
	}

	if cfg.ResolveIP == "" {
		cfg.ResolveIP = DefaultResolveIP
	}

	ip := net.ParseIP(cfg.ResolveIP)
	if ip == nil {
		return nil, fmt.Errorf("%w: %q", ErrInvalidResolveIP, cfg.ResolveIP)
	}

	ip = ip.To4()
	if ip == nil {
		return nil, fmt.Errorf("%w: got %q", ErrIPv4Required, cfg.ResolveIP)
	}

	return &Server{
		names:      make(map[string]struct{}),
		cfg:        cfg,
		resolveIP:  ip,
		listenAddr: cfg.ListenAddr,
		mu:         lockmetrics.New("dns"),
	}, nil
}

// Register adds a hostname to the set of names the server will resolve.
// The trailing dot required by DNS is added automatically.
// Hostnames are stored in lower-case so lookups are case-insensitive.
// Calls are safe for concurrent use.
func (s *Server) Register(hostname string) {
	fqdn := strings.ToLower(dns.Fqdn(hostname))

	s.mu.Lock("Register")
	s.names[fqdn] = struct{}{}
	s.mu.Unlock()

	if s.cfg.Logger != nil {
		s.cfg.Logger.Debug("dns: registered", "hostname", fqdn)
	}
}

// Deregister removes a hostname from the set the server will resolve.
// Calls are safe for concurrent use.
func (s *Server) Deregister(hostname string) {
	fqdn := strings.ToLower(dns.Fqdn(hostname))

	s.mu.Lock("Deregister")
	delete(s.names, fqdn)
	s.mu.Unlock()
}

// IsRegistered reports whether the hostname is registered.
func (s *Server) IsRegistered(hostname string) bool {
	fqdn := strings.ToLower(dns.Fqdn(hostname))

	s.mu.RLock("IsRegistered")
	_, ok := s.names[fqdn]
	s.mu.RUnlock()

	return ok
}

// Start launches the DNS server in the background.
// It returns once both the UDP and TCP servers are ready to accept queries.
// Call Stop or cancel ctx to shut down.
func (s *Server) Start(ctx context.Context) error {
	mux := dns.NewServeMux()
	mux.HandleFunc(".", s.handleQuery)

	udpReady := make(chan struct{})
	tcpReady := make(chan struct{})
	udpErrCh := make(chan error, 1)
	tcpErrCh := make(chan error, 1)

	s.udpServer = &dns.Server{
		Addr:              s.listenAddr,
		Net:               "udp",
		Handler:           mux,
		ReadTimeout:       DefaultReadTimeout,
		WriteTimeout:      DefaultWriteTimeout,
		NotifyStartedFunc: func() { close(udpReady) },
	}

	s.tcpServer = &dns.Server{
		Addr:              s.listenAddr,
		Net:               "tcp",
		Handler:           mux,
		ReadTimeout:       DefaultReadTimeout,
		WriteTimeout:      DefaultWriteTimeout,
		NotifyStartedFunc: func() { close(tcpReady) },
	}

	go serveOrSend(s.udpServer, udpErrCh)
	go serveOrSend(s.tcpServer, tcpErrCh)

	if err := waitForReady(ctx, udpReady, udpErrCh, tcpErrCh); err != nil {
		return err
	}

	if err := waitForReady(ctx, tcpReady, udpErrCh, tcpErrCh); err != nil {
		return err
	}

	// Watch for context cancellation to trigger shutdown.
	s.stopCh = make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = s.Stop()
		case <-s.stopCh:
			// Stop() was called directly; goroutine exits cleanly.
		}
	}()

	if s.cfg.Logger != nil {
		s.cfg.Logger.InfoContext(ctx, "dns: server started", "addr", s.listenAddr)
	}

	return nil
}

// serveOrSend starts a DNS server and sends any unexpected error to errCh.
func serveOrSend(srv *dns.Server, errCh chan<- error) {
	if err := srv.ListenAndServe(); err != nil {
		if !errors.Is(err, net.ErrClosed) && !isServerClosed(err) {
			errCh <- err
		}
	}
}

// waitForReady blocks until the readyCh is closed, an error arrives, or ctx is cancelled.
func waitForReady(ctx context.Context, readyCh <-chan struct{}, udpErrCh, tcpErrCh <-chan error) error {
	select {
	case err := <-udpErrCh:
		return fmt.Errorf("dns udp server: %w", err)
	case err := <-tcpErrCh:
		return fmt.Errorf("dns tcp server: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	case <-readyCh:
		return nil
	}
}

// Stop shuts down both the UDP and TCP servers.
func (s *Server) Stop() error {
	var udpErr, tcpErr error

	s.stopOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}

		if s.udpServer != nil {
			udpErr = s.udpServer.Shutdown()
		}

		if s.tcpServer != nil {
			tcpErr = s.tcpServer.Shutdown()
		}
	})

	return errors.Join(udpErr, tcpErr)
}

// handleQuery is the DNS handler that answers A queries for registered names.
func (s *Server) handleQuery(w dns.ResponseWriter, r *dns.Msg) {
	msg := new(dns.Msg)
	msg.SetReply(r)
	msg.Authoritative = true
	msg.RecursionAvailable = false

	// Common resolver behavior: only process the first question to avoid
	// inconsistent Rcode/answer combinations for multi-question messages.
	if len(r.Question) > 0 {
		q := r.Question[0]

		switch q.Qtype {
		case dns.TypeA:
			name := strings.ToLower(q.Name)

			s.mu.RLock("handleQuery")
			_, registered := s.names[name]
			s.mu.RUnlock()

			if registered {
				rr := &dns.A{
					Hdr: dns.RR_Header{
						Name:   q.Name,
						Rrtype: dns.TypeA,
						Class:  dns.ClassINET,
						Ttl:    defaultTTL,
					},
					A: s.resolveIP,
				}
				msg.Answer = append(msg.Answer, rr)
			} else {
				msg.Rcode = dns.RcodeNameError
			}
		default:
			// For non-A queries, return NOERROR with empty answer (NODATA).
		}
	}

	if werr := w.WriteMsg(msg); werr != nil {
		if s.cfg.Logger != nil {
			s.cfg.Logger.Warn("dns: write response failed", "error", werr)
		}
	}
}

// SyntheticHostname generates an AWS-style synthetic hostname for a resource.
// serviceType is one of: "cache", "rds", "redshift", "es".
func SyntheticHostname(resourceID, randomSuffix, region, serviceType string) string {
	switch serviceType {
	case "cache":
		return fmt.Sprintf("%s.%s.%s.cache.amazonaws.com", resourceID, randomSuffix, region)
	case "rds":
		return fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", resourceID, randomSuffix, region)
	case "redshift":
		return fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", resourceID, randomSuffix, region)
	case "es":
		return fmt.Sprintf("search-%s.%s.es.amazonaws.com", resourceID, region)
	default:
		return fmt.Sprintf("%s.%s.%s.%s.amazonaws.com", resourceID, randomSuffix, region, serviceType)
	}
}

// isServerClosed reports whether err indicates a clean server shutdown.
// miekg/dns returns its own "server closed" error on Shutdown() which is
// distinct from [net.ErrClosed], so we check both.
func isServerClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "server closed")
}
