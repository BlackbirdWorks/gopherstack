// Package dns provides an embedded DNS server for Gopherstack that resolves
// synthetic AWS-style hostnames (e.g. my-cluster.abc.us-east-1.cache.amazonaws.com)
// back to a configured IP address (typically 127.0.0.1).
// It also supports per-record values for A, CNAME, and AAAA records via RegisterRecord.
//
// Usage:
//
//	srv, err := dns.New(dns.Config{ListenAddr: ":10053", ResolveIP: "127.0.0.1"})
//	srv.Register("my-cluster.abc.us-east-1.cache.amazonaws.com")
//	srv.RegisterRecord("www.example.com", "A", []string{"1.2.3.4"})
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
	// Transparent enables real-AWS-endpoint interception and upstream forwarding.
	Transparent TransparentConfig
}

// dnsEntry stores the record type and values for a per-record DNS registration.
type dnsEntry struct {
	recordType string
	values     []string
}

// Server is an embedded DNS server that answers A queries for registered
// synthetic hostnames with a fixed IP address.
type Server struct {
	interceptor Interceptor
	transparent *transparent
	names       map[string]struct{}
	records     map[string][]*dnsEntry // per-record typed values (RegisterRecord)

	udpServer *dns.Server
	tcpServer *dns.Server
	runs      []*serveRun

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

	var tr *transparent

	if len(cfg.Transparent.Suffixes) > 0 {
		var err error

		if tr, err = newTransparent(cfg.Transparent, cfg.ListenAddr); err != nil {
			return nil, err
		}
	}

	return &Server{
		transparent: tr,
		names:       make(map[string]struct{}),
		records:     make(map[string][]*dnsEntry),
		cfg:         cfg,
		resolveIP:   ip,
		listenAddr:  cfg.ListenAddr,
		mu:          lockmetrics.New("dns"),
		stopCh:      make(chan struct{}),
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

// RegisterRecord stores per-record typed values for a hostname.
// recordType must be one of "A", "CNAME", "AAAA", or "ALIAS".
// Multiple calls with the same hostname and recordType append values.
// Calls are safe for concurrent use.
func (s *Server) RegisterRecord(hostname, recordType string, values []string) {
	if len(values) == 0 {
		return
	}

	fqdn := strings.ToLower(dns.Fqdn(hostname))

	cp := make([]string, len(values))
	copy(cp, values)

	s.mu.Lock("RegisterRecord")
	for _, e := range s.records[fqdn] {
		if e.recordType == recordType {
			e.values = append(e.values, cp...)
			s.mu.Unlock()

			return
		}
	}

	s.records[fqdn] = append(s.records[fqdn], &dnsEntry{recordType: recordType, values: cp})
	s.mu.Unlock()
}

// Deregister removes a hostname from the set the server will resolve.
// It clears both simple registrations (Register) and typed records (RegisterRecord).
// Calls are safe for concurrent use.
func (s *Server) Deregister(hostname string) {
	fqdn := strings.ToLower(dns.Fqdn(hostname))

	s.mu.Lock("Deregister")
	delete(s.names, fqdn)
	delete(s.records, fqdn)
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

	udpRun := &serveRun{srv: s.udpServer, ready: udpReady, done: make(chan struct{})}
	tcpRun := &serveRun{srv: s.tcpServer, ready: tcpReady, done: make(chan struct{})}
	s.runs = []*serveRun{udpRun, tcpRun}

	go udpRun.serve(udpErrCh)
	go tcpRun.serve(tcpErrCh)

	err := waitForReady(ctx, udpReady, udpErrCh, tcpErrCh)
	if err == nil {
		err = waitForReady(ctx, tcpReady, udpErrCh, tcpErrCh)
	}

	if err != nil {
		_ = s.Stop()

		return err
	}

	// Watch for context cancellation to trigger shutdown.
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

// serveRun tracks one serve goroutine so Stop can wait for it to exit.
type serveRun struct {
	srv   *dns.Server
	ready chan struct{}
	done  chan struct{}
}

// serve runs the server and sends any unexpected error to errCh.
func (r *serveRun) serve(errCh chan<- error) {
	defer close(r.done)

	if err := r.srv.ListenAndServe(); err != nil {
		if !errors.Is(err, net.ErrClosed) && !isServerClosed(err) {
			errCh <- err
		}
	}
}

// stop shuts the server down and waits for its serve goroutine to return.
// Shutdown before the listener is up would fail and leave the goroutine
// running, so wait until it is either ready or already finished.
func (r *serveRun) stop() error {
	select {
	case <-r.ready:
	case <-r.done:
	}

	var err error

	select {
	case <-r.done:
	default:
		err = r.srv.Shutdown()
	}

	<-r.done

	return err
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

// Stop shuts down both the UDP and TCP servers and waits for their serve
// goroutines to exit. It is safe to call more than once, and after a failed Start.
func (s *Server) Stop() error {
	var errs []error

	s.stopOnce.Do(func() {
		close(s.stopCh)

		for _, r := range s.runs {
			errs = append(errs, r.stop())
		}
	})

	return errors.Join(errs...)
}

// handleQuery is the DNS handler that answers A/CNAME/AAAA queries for registered names.
func (s *Server) handleQuery(w dns.ResponseWriter, r *dns.Msg) {
	msg := s.respond(w, r)

	if werr := w.WriteMsg(msg); werr != nil {
		if s.cfg.Logger != nil {
			s.cfg.Logger.Warn("dns: write response failed", "error", werr)
		}
	}
}

func (s *Server) respond(w dns.ResponseWriter, r *dns.Msg) *dns.Msg {
	s.mu.RLock("respond")
	ic := s.interceptor
	s.mu.RUnlock()

	if ic == nil {
		return s.localAnswer(r)
	}

	resp := ic.Intercept(w.RemoteAddr(), r, func() *dns.Msg { return s.localAnswer(r) })
	if resp == nil {
		return s.localAnswer(r)
	}

	resp.Id = r.Id
	resp.Question = r.Question
	resp.Response = true

	return resp
}

func (s *Server) localAnswer(r *dns.Msg) *dns.Msg {
	msg := new(dns.Msg)
	msg.SetReply(r)
	msg.Authoritative = true
	msg.RecursionAvailable = false

	// Only the first question is processed so multi-question messages cannot
	// produce inconsistent Rcode/answer combinations.
	if len(r.Question) > 0 {
		q := r.Question[0]
		name := strings.ToLower(q.Name)

		s.mu.RLock("handleQuery")
		entries := s.records[name]
		_, inNames := s.names[name]
		s.mu.RUnlock()

		if s.transparent != nil && entries == nil && !inNames {
			return s.transparent.answer(s, r, q)
		}

		switch q.Qtype {
		case dns.TypeA:
			s.handleAQuery(msg, q, entries, inNames)
		case dns.TypeCNAME:
			s.handleCNAMEQuery(msg, q, entries)
		case dns.TypeAAAA:
			s.handleAAAAQuery(msg, q, entries)
		default:
			// Other query types: NOERROR with an empty answer (NODATA).
		}
	}

	return msg
}

func (s *Server) handleAQuery(msg *dns.Msg, q dns.Question, entries []*dnsEntry, inNames bool) {
	answered := false

	for _, e := range entries {
		if e.recordType != "A" {
			continue
		}

		for _, v := range e.values {
			ip := net.ParseIP(v)
			if ip4 := ip.To4(); ip4 != nil {
				msg.Answer = append(msg.Answer, &dns.A{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: defaultTTL},
					A:   ip4,
				})
				answered = true
			}
		}
	}

	if !answered {
		if inNames {
			msg.Answer = append(msg.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: defaultTTL},
				A:   s.resolveIP,
			})
		} else {
			msg.Rcode = dns.RcodeNameError
		}
	}
}

func (s *Server) handleCNAMEQuery(msg *dns.Msg, q dns.Question, entries []*dnsEntry) {
	answered := false

	for _, e := range entries {
		if e.recordType != "CNAME" && e.recordType != "ALIAS" {
			continue
		}

		for _, v := range e.values {
			msg.Answer = append(msg.Answer, &dns.CNAME{
				Hdr: dns.RR_Header{
					Name:   q.Name,
					Rrtype: dns.TypeCNAME,
					Class:  dns.ClassINET,
					Ttl:    defaultTTL,
				},
				Target: dns.Fqdn(strings.TrimSuffix(v, ".")),
			})
			answered = true
		}
	}

	if !answered {
		msg.Rcode = dns.RcodeNameError
	}
}

func (s *Server) handleAAAAQuery(msg *dns.Msg, q dns.Question, entries []*dnsEntry) {
	answered := false

	for _, e := range entries {
		if e.recordType != "AAAA" {
			continue
		}

		for _, v := range e.values {
			ip := net.ParseIP(v)
			if ip != nil && ip.To4() == nil {
				msg.Answer = append(msg.Answer, &dns.AAAA{
					Hdr: dns.RR_Header{
						Name:   q.Name,
						Rrtype: dns.TypeAAAA,
						Class:  dns.ClassINET,
						Ttl:    defaultTTL,
					},
					AAAA: ip.To16(),
				})
				answered = true
			}
		}
	}

	if !answered {
		msg.Rcode = dns.RcodeNameError
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
