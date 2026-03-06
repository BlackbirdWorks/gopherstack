package dns_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
)

func TestNew_InvalidIP(t *testing.T) {
	t.Parallel()

	_, err := gopherDNS.New(gopherDNS.Config{ResolveIP: "not-an-ip"})
	assert.ErrorIs(t, err, gopherDNS.ErrInvalidResolveIP)
}

func TestNew_IPv6NotSupported(t *testing.T) {
	t.Parallel()

	_, err := gopherDNS.New(gopherDNS.Config{ResolveIP: "::1"})
	assert.ErrorIs(t, err, gopherDNS.ErrIPv4Required)
}

func TestNew_Defaults(t *testing.T) {
	t.Parallel()

	s, err := gopherDNS.New(gopherDNS.Config{})
	require.NoError(t, err)
	assert.NotNil(t, s)
}

func TestRegisterDeregister(t *testing.T) {
	t.Parallel()

	s, err := gopherDNS.New(gopherDNS.Config{})
	require.NoError(t, err)

	hostname := "my-cluster.abc.us-east-1.cache.amazonaws.com"
	assert.False(t, s.IsRegistered(hostname))

	s.Register(hostname)
	assert.True(t, s.IsRegistered(hostname))

	s.Deregister(hostname)
	assert.False(t, s.IsRegistered(hostname))
}

func TestSyntheticHostname(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceID  string
		serviceType string
		want        string
	}{
		{
			name:        "cache",
			resourceID:  "my-cluster",
			serviceType: "cache",
			want:        "my-cluster.abc123.us-east-1.cache.amazonaws.com",
		},
		{
			name:        "rds",
			resourceID:  "my-db",
			serviceType: "rds",
			want:        "my-db.abc123.us-east-1.rds.amazonaws.com",
		},
		{
			name:        "redshift",
			resourceID:  "my-rs",
			serviceType: "redshift",
			want:        "my-rs.abc123.us-east-1.redshift.amazonaws.com",
		},
		{
			name:        "es",
			resourceID:  "my-domain",
			serviceType: "es",
			want:        "search-my-domain.us-east-1.es.amazonaws.com",
		},
		{
			name:        "custom",
			resourceID:  "my-resource",
			serviceType: "foo",
			want:        "my-resource.abc123.us-east-1.foo.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := gopherDNS.SyntheticHostname(tt.resourceID, "abc123", "us-east-1", tt.serviceType)
			assert.Equal(t, tt.want, got)
		})
	}
}

// startTestServer starts a DNS server on a free port and returns the server and its address.
// It retries up to 5 times with a fresh random port to avoid TOCTOU port-conflict races in CI.
func startTestServer(t *testing.T) (*gopherDNS.Server, string) {
	t.Helper()

	const maxAttempts = 5

	for range maxAttempts {
		// Find a free UDP port.
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)

		port := pc.LocalAddr().(*net.UDPAddr).Port
		_ = pc.Close()

		addr := fmt.Sprintf("127.0.0.1:%d", port)

		srv, err := gopherDNS.New(gopherDNS.Config{
			ListenAddr: addr,
			ResolveIP:  "127.0.0.1",
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())

		if err = srv.Start(ctx); err != nil {
			cancel()
			_ = srv.Stop()

			continue
		}

		t.Cleanup(func() {
			cancel()
			_ = srv.Stop()
		})

		return srv, addr
	}

	t.Fatal("failed to start DNS test server after retries")

	return nil, ""
}

// queryA performs a DNS A query and returns the first IP answer.
func queryA(t *testing.T, addr, hostname string) (string, int) {
	t.Helper()

	c := new(dns.Client)
	c.Net = "udp"
	c.Timeout = 2 * time.Second

	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(hostname), dns.TypeA)
	m.RecursionDesired = false

	r, _, err := c.Exchange(m, addr)
	require.NoError(t, err)

	if len(r.Answer) == 0 {
		return "", r.Rcode
	}

	a, ok := r.Answer[0].(*dns.A)
	if !ok {
		return "", r.Rcode
	}

	return a.A.String(), r.Rcode
}

func TestServer_RegisteredHostnameResolvesToIP(t *testing.T) {
	t.Parallel()

	srv, addr := startTestServer(t)

	hostname := "my-cluster.abc.us-east-1.cache.amazonaws.com"
	srv.Register(hostname)

	ip, rcode := queryA(t, addr, hostname)
	assert.Equal(t, dns.RcodeSuccess, rcode)
	assert.Equal(t, "127.0.0.1", ip)
}

func TestServer_UnregisteredHostnameReturnsNXDOMAIN(t *testing.T) {
	t.Parallel()

	_, addr := startTestServer(t)

	_, rcode := queryA(t, addr, "unknown.example.com")
	assert.Equal(t, dns.RcodeNameError, rcode)
}

func TestServer_DeregisteredHostnameReturnsNXDOMAIN(t *testing.T) {
	t.Parallel()

	srv, addr := startTestServer(t)

	hostname := "temp.us-east-1.rds.amazonaws.com"
	srv.Register(hostname)

	ip, rcode := queryA(t, addr, hostname)
	assert.Equal(t, dns.RcodeSuccess, rcode)
	assert.Equal(t, "127.0.0.1", ip)

	srv.Deregister(hostname)

	_, rcode = queryA(t, addr, hostname)
	assert.Equal(t, dns.RcodeNameError, rcode)
}

func TestServer_NonAQueryReturnsNoData(t *testing.T) {
	t.Parallel()

	srv, addr := startTestServer(t)

	hostname := "my-cluster.abc.us-east-1.cache.amazonaws.com"
	srv.Register(hostname)

	c := new(dns.Client)
	c.Net = "udp"
	c.Timeout = 2 * time.Second

	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(hostname), dns.TypeMX)
	m.RecursionDesired = false

	r, _, err := c.Exchange(m, addr)
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeSuccess, r.Rcode)
	assert.Empty(t, r.Answer)
}

func TestServer_Stop(t *testing.T) {
	t.Parallel()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := gopherDNS.New(gopherDNS.Config{
		ListenAddr: addr,
		ResolveIP:  "127.0.0.1",
	})
	require.NoError(t, err)

	ctx := t.Context()

	err = srv.Start(ctx)
	require.NoError(t, err)

	err = srv.Stop()
	assert.NoError(t, err)
}

func TestServer_TCPQuery(t *testing.T) {
	t.Parallel()

	srv, addr := startTestServer(t)

	hostname := "my-db.abc.us-east-1.rds.amazonaws.com"
	srv.Register(hostname)

	c := new(dns.Client)
	c.Net = "tcp"
	c.Timeout = 2 * time.Second

	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(hostname), dns.TypeA)

	r, _, err := c.Exchange(m, addr)
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeSuccess, r.Rcode)
	require.NotEmpty(t, r.Answer)
	assert.Equal(t, "127.0.0.1", r.Answer[0].(*dns.A).A.String())
}

func TestRegister_WithLogger(t *testing.T) {
	t.Parallel()

	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s, err := gopherDNS.New(gopherDNS.Config{Logger: log})
	require.NoError(t, err)

	// Should trigger the logger path in Register.
	s.Register("test.us-east-1.cache.amazonaws.com")
	assert.True(t, s.IsRegistered("test.us-east-1.cache.amazonaws.com"))
}

func TestServer_ContextCancellation(t *testing.T) {
	t.Parallel()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, startErr := gopherDNS.New(gopherDNS.Config{
		ListenAddr: addr,
		ResolveIP:  "127.0.0.1",
	})
	require.NoError(t, startErr)

	ctx, cancel := context.WithCancel(t.Context())

	err = srv.Start(ctx)
	require.NoError(t, err)

	// Cancel the context — the server should shut down via the background goroutine.
	cancel()
	time.Sleep(100 * time.Millisecond)
}
