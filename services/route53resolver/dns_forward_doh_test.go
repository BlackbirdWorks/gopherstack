package route53resolver_test

import (
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func startDoH(t *testing.T, answerIP string, status int) (*httptest.Server, *x509.CertPool) {
	t.Helper()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/dns-query" ||
			r.Header.Get("Content-Type") != "application/dns-message" {
			http.Error(w, "bad request", http.StatusBadRequest)

			return
		}

		body, _ := io.ReadAll(r.Body)
		q := new(dns.Msg)

		if q.Unpack(body) != nil {
			http.Error(w, "bad message", http.StatusBadRequest)

			return
		}

		m := new(dns.Msg)
		m.SetReply(q)
		m.Answer = append(m.Answer, &dns.A{
			Hdr: dns.RR_Header{Name: q.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 5},
			A:   net.ParseIP(answerIP),
		})

		out, _ := m.Pack()
		w.Header().Set("Content-Type", "application/dns-message")
		w.WriteHeader(status)
		_, _ = w.Write(out)
	}))
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())

	return srv, pool
}

func TestDNSForwardDoH(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		protocol string
		sni      string
		wantIPs  []string
		status   int
		trust    bool
	}{
		{name: "doh", protocol: "DoH", status: http.StatusOK, trust: true, wantIPs: []string{"192.0.2.44"}},
		{
			name:     "doh_fips_sni",
			protocol: "DoH-FIPS",
			sni:      "example.com",
			status:   http.StatusOK,
			trust:    true,
			wantIPs:  []string{"192.0.2.44"},
		},
		{name: "sni_mismatch", protocol: "DoH", sni: "wrong.invalid", status: http.StatusOK, trust: true},
		{name: "untrusted_certificate", protocol: "DoH", status: http.StatusOK},
		{name: "server_error", protocol: "DoH", status: http.StatusInternalServerError, trust: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, pool := startDoH(t, "192.0.2.44", tt.status)
			_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
			require.NoError(t, err)

			port, err := strconv.Atoi(portStr)
			require.NoError(t, err)

			e := newDNSEnv(t)
			if tt.trust {
				route53resolver.SetDoHRootCAs(e.res, pool)
			}

			e.associate(t, e.rule(t, "example.com", "FORWARD", route53resolver.TargetIP{
				IP: "127.0.0.1", Port: int32(port), Protocol: tt.protocol, ServerNameIndication: tt.sni,
			}))

			resp := e.ask(vpc1Client, "www.example.com", dns.TypeA)
			assert.Equal(t, tt.wantIPs, answerIPs(resp))

			if tt.wantIPs == nil {
				assert.Equal(t, dns.RcodeServerFailure, resp.Rcode)
			}
		})
	}
}
