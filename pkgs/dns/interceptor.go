package dns

import (
	"net"
	"strings"

	"github.com/miekg/dns"
)

// Interceptor sees every query before the static records do. local computes the
// static answer; returning nil falls back to it.
type Interceptor interface {
	Intercept(src net.Addr, req *dns.Msg, local func() *dns.Msg) *dns.Msg
}

// SetInterceptor installs (or, with nil, removes) the query interceptor.
func (s *Server) SetInterceptor(i Interceptor) {
	s.mu.Lock("SetInterceptor")
	s.interceptor = i
	s.mu.Unlock()
}

// Owns reports whether hostname has a static registration of any kind.
func (s *Server) Owns(hostname string) bool {
	fqdn := strings.ToLower(dns.Fqdn(hostname))

	s.mu.RLock("Owns")
	defer s.mu.RUnlock()

	if _, ok := s.names[fqdn]; ok {
		return true
	}

	_, ok := s.records[fqdn]

	return ok
}
