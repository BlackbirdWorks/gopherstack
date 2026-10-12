package dns_test

import (
	"net"
	"sync"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeInterceptor struct {
	gotSrc   net.Addr
	override *dns.Msg
	mu       sync.Mutex
	useLocal bool
}

func (f *fakeInterceptor) Intercept(src net.Addr, _ *dns.Msg, local func() *dns.Msg) *dns.Msg {
	f.mu.Lock()
	f.gotSrc = src
	f.mu.Unlock()

	if f.useLocal {
		return local()
	}

	return f.override
}

func TestServer_Interceptor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ic       *fakeInterceptor
		wantIP   string
		wantCode int
	}{
		{
			name: "nil_response_falls_back_to_static",
			ic:   &fakeInterceptor{}, wantIP: "127.0.0.1", wantCode: dns.RcodeSuccess,
		},
		{
			name: "local_callback_returns_static",
			ic:   &fakeInterceptor{useLocal: true}, wantIP: "127.0.0.1", wantCode: dns.RcodeSuccess,
		},
		{
			name: "override_replaces_static",
			ic: &fakeInterceptor{override: &dns.Msg{
				Rcode: dns.RcodeServerFailure,
			}},
			wantCode: dns.RcodeServerFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, addr, _ := startTestServer(t)
			srv.Register("known.example.com")
			srv.SetInterceptor(tt.ic)

			ip, rcode := queryA(t, addr, "known.example.com")
			assert.Equal(t, tt.wantCode, rcode)
			assert.Equal(t, tt.wantIP, ip)

			tt.ic.mu.Lock()
			defer tt.ic.mu.Unlock()

			require.NotNil(t, tt.ic.gotSrc)
			assert.Equal(t, "127.0.0.1", tt.ic.gotSrc.(*net.UDPAddr).IP.String())
		})
	}
}

func TestServer_Owns(t *testing.T) {
	t.Parallel()

	srv, _, _ := startTestServer(t)
	srv.Register("simple.example.com")
	srv.RegisterRecord("typed.example.com", "A", []string{"10.0.0.1"})

	tests := []struct {
		host string
		want bool
	}{
		{"simple.example.com", true},
		{"SIMPLE.example.com.", true},
		{"typed.example.com", true},
		{"other.example.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, srv.Owns(tt.host))
		})
	}
}
