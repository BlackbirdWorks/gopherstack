package dns_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/dns/dnstest"
)

func startWithConfig(t *testing.T, cfg gopherDNS.Config) (*gopherDNS.Server, string) {
	t.Helper()

	srv, addr, _ := dnstest.Start(t, cfg)

	return srv, addr
}

func TestTransparentMode(t *testing.T) {
	t.Parallel()

	upstream, upAddr := startWithConfig(t, gopherDNS.Config{ResolveIP: "127.0.0.1"})
	for _, n := range []string{"example.org", "sts.amazonaws.com"} {
		upstream.RegisterRecord(n, "A", []string{"9.9.9.9"})
	}

	srv, addr := startWithConfig(t, gopherDNS.Config{
		ResolveIP: "10.1.2.3",
		Transparent: gopherDNS.TransparentConfig{
			Suffixes:  gopherDNS.DefaultTransparentSuffixes(),
			Skip:      []string{"sts.amazonaws.com"},
			Upstreams: []string{upAddr},
		},
	})
	srv.Register("db.abc.us-east-1.rds.amazonaws.com")

	tests := []struct {
		name    string
		qname   string
		wantIP  string
		wantAns int
		qtype   uint16
	}{
		{"aws regional", "sqs.us-east-1.amazonaws.com.", "10.1.2.3", 1, dns.TypeA},
		{"aws vhost s3", "bkt.s3.us-west-2.amazonaws.com.", "10.1.2.3", 1, dns.TypeA},
		{"aws cn", "ec2.cn-north-1.amazonaws.com.cn.", "10.1.2.3", 1, dns.TypeA},
		{"api aws", "s3.us-east-1.api.aws.", "10.1.2.3", 1, dns.TypeA},
		{"aws aaaa nodata", "sqs.us-east-1.amazonaws.com.", "", 0, dns.TypeAAAA},
		{"skip upstream", "sts.amazonaws.com.", "9.9.9.9", 1, dns.TypeA},
		{"other upstream", "example.org.", "9.9.9.9", 1, dns.TypeA},
		{"registered stays local", "db.abc.us-east-1.rds.amazonaws.com.", "10.1.2.3", 1, dns.TypeA},
		{"unknown upstream nxdomain", "nope.example.net.", "", 0, dns.TypeA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := new(dns.Msg)
			m.SetQuestion(tt.qname, tt.qtype)

			resp, _, err := (&dns.Client{}).Exchange(m, addr)
			require.NoError(t, err)
			require.Len(t, resp.Answer, tt.wantAns)

			if tt.wantIP != "" {
				a, ok := resp.Answer[0].(*dns.A)
				require.True(t, ok)
				assert.Equal(t, tt.wantIP, a.A.String())
			}
		})
	}
}

func TestResolvConfUpstreams(t *testing.T) {
	t.Parallel()

	p := filepath.Join(t.TempDir(), "resolv.conf")
	require.NoError(t, os.WriteFile(p, []byte("nameserver 1.2.3.4\nnameserver 5.6.7.8\n"), 0o600))

	assert.Equal(t, []string{"1.2.3.4:53", "5.6.7.8:53"}, gopherDNS.ResolvConfUpstreams(p))
	assert.Empty(t, gopherDNS.ResolvConfUpstreams(filepath.Join(t.TempDir(), "missing")))
}
