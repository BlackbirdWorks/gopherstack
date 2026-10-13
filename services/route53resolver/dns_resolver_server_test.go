package route53resolver_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
)

func TestDNSResolverThroughEmbeddedServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		query  string
		wantIP string
		static bool
	}{
		{name: "forwarded", query: "www.example.com", wantIP: "192.0.2.10"},
		{name: "owned_name_stays_local", query: "owned.example.com", static: true, wantIP: "127.0.0.1"},
		{name: "unmatched_domain_static", query: "other.test", static: true, wantIP: "127.0.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pc, err := net.ListenPacket("udp", "127.0.0.1:0")
			require.NoError(t, err)

			addr := fmt.Sprintf("127.0.0.1:%d", pc.LocalAddr().(*net.UDPAddr).Port)
			require.NoError(t, pc.Close())

			srv, err := gopherDNS.New(gopherDNS.Config{ListenAddr: addr, ResolveIP: "127.0.0.1"})
			require.NoError(t, err)
			require.NoError(t, srv.Start(t.Context()))
			t.Cleanup(func() { _ = srv.Stop() })

			e := newDNSEnv(t)
			e.b.SetVPCLocator(func(net.IP) (string, string, bool) { return testRegion, vpc1, true })
			srv.SetInterceptor(e.b.NewDNSResolver(srv.Owns))
			srv.Register("owned.example.com")
			srv.Register("other.test")

			up := startUpstream(t, "192.0.2.10")
			e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))

			req := new(dns.Msg)
			req.SetQuestion(dns.Fqdn(tt.query), dns.TypeA)

			resp, _, err := (&dns.Client{Net: "udp", Timeout: 3 * time.Second}).Exchange(req, addr)
			require.NoError(t, err)
			assert.Equal(t, []string{tt.wantIP}, answerIPs(resp))
		})
	}
}
