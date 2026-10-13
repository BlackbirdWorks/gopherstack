package route53resolver_test

import (
	"context"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDNSResolverRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, e *dnsEnv)
		name     string
		src      string
		query    string
		wantIP   string
		wantCode int
	}{
		{
			name: "forward_match",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))
			},
			src: vpc1Client, query: "www.example.com", wantIP: "192.0.2.10",
		},
		{
			name: "forward_apex",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com.", "FORWARD", up.target()))
			},
			src: vpc1Client, query: "example.com", wantIP: "192.0.2.10",
		},
		{
			name: "no_association",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.rule(t, "example.com", "FORWARD", up.target())
			},
			src: vpc1Client, query: "www.example.com", wantIP: localMarker,
		},
		{
			name: "other_vpc",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))
			},
			src: vpc2Client, query: "www.example.com", wantIP: localMarker,
		},
		{
			name: "unmapped_source",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))
			},
			src: "203.0.113.9", query: "www.example.com", wantIP: localMarker,
		},
		{
			name: "unrelated_domain",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))
			},
			src: vpc1Client, query: "notexample.com", wantIP: localMarker,
		},
		{
			name: "most_specific_wins",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				broad := startUpstream(t, "192.0.2.10")
				narrow := startUpstream(t, "192.0.2.20")
				e.associate(t, e.rule(t, "example.com", "FORWARD", broad.target()))
				e.associate(t, e.rule(t, "corp.example.com", "FORWARD", narrow.target()))
			},
			src: vpc1Client, query: "a.corp.example.com", wantIP: "192.0.2.20",
		},
		{
			name: "broad_still_applies",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				broad := startUpstream(t, "192.0.2.10")
				narrow := startUpstream(t, "192.0.2.20")
				e.associate(t, e.rule(t, "example.com", "FORWARD", broad.target()))
				e.associate(t, e.rule(t, "corp.example.com", "FORWARD", narrow.target()))
			},
			src: vpc1Client, query: "www.example.com", wantIP: "192.0.2.10",
		},
		{
			name: "system_exception",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))
				e.associate(t, e.rule(t, "internal.example.com", "SYSTEM"))
			},
			src: vpc1Client, query: "db.internal.example.com", wantIP: localMarker,
		},
		{
			name: "recursive_resolves_normally",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))
				e.associate(t, e.rule(t, "pub.example.com", "RECURSIVE"))
			},
			src: vpc1Client, query: "x.pub.example.com", wantIP: localMarker,
		},
		{
			name: "emulator_owned_not_forwarded",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))
				e.owned["svc.example.com"] = true
			},
			src: vpc1Client, query: "svc.example.com", wantIP: localMarker,
		},
		{
			name: "failover_to_live_target",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.associate(t, e.rule(t, "example.com", "FORWARD", deadTarget(t), up.target()))
			},
			src: vpc1Client, query: "www.example.com", wantIP: "192.0.2.10",
		},
		{
			name: "all_targets_down_servfail",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.associate(t, e.rule(t, "example.com", "FORWARD", deadTarget(t), deadTarget(t)))
			},
			src: vpc1Client, query: "www.example.com", wantCode: dns.RcodeServerFailure,
		},
		{
			name: "association_removed",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				id := e.rule(t, "example.com", "FORWARD", up.target())
				e.associate(t, id)
				_, err := e.b.DisassociateResolverRule(context.Background(), id, vpc1)
				require.NoError(t, err)
			},
			src: vpc1Client, query: "www.example.com", wantIP: localMarker,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDNSEnv(t)
			tt.setup(t, e)

			resp := e.ask(tt.src, tt.query, dns.TypeA)
			assert.Equal(t, tt.wantCode, resp.Rcode)

			if tt.wantIP != "" {
				assert.Equal(t, []string{tt.wantIP}, answerIPs(resp))
			}
		})
	}
}

func TestDNSResolverForwardedPort(t *testing.T) {
	t.Parallel()

	e := newDNSEnv(t)
	up := startUpstream(t, "192.0.2.10")
	e.associate(t, e.rule(t, "example.com", "FORWARD", up.target()))

	e.ask(vpc1Client, "a.example.com", dns.TypeA)
	e.ask(vpc1Client, "b.example.com", dns.TypeA)

	assert.EqualValues(t, 2, up.hits.Load())
}
