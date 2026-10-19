package route53resolver_test

import (
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/dns/dnstest"
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

			srv, addr, _ := dnstest.Start(t, gopherDNS.Config{ResolveIP: "127.0.0.1"})

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
