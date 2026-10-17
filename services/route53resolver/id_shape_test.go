package route53resolver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestGeneratedIDShapes(t *testing.T) {
	t.Parallel()

	ips := []route53resolver.IPAddress{{SubnetID: "subnet-1"}, {SubnetID: "subnet-2"}}

	endpoint := func(
		t *testing.T, b *route53resolver.InMemoryBackend, direction string,
	) *route53resolver.ResolverEndpoint {
		t.Helper()

		ep, err := b.CreateResolverEndpoint(
			context.Background(), "ep", direction, "", ips, nil, "", nil, "", "", "", false, false, false, false,
		)
		require.NoError(t, err)

		return ep
	}

	tests := []struct {
		mint    func(t *testing.T, b *route53resolver.InMemoryBackend) string
		name    string
		pattern string
	}{
		{
			name:    "inbound endpoint",
			pattern: `^rslvr-in-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				return endpoint(t, b, "INBOUND").ID
			},
		},
		{
			name:    "outbound endpoint",
			pattern: `^rslvr-out-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				return endpoint(t, b, "OUTBOUND").ID
			},
		},
		{
			name:    "endpoint ip",
			pattern: `^rni-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				return endpoint(t, b, "INBOUND").IPAddresses[0].IPID
			},
		},
		{
			name:    "resolver rule",
			pattern: `^rslvr-rr-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				r, err := b.CreateResolverRule(context.Background(), "r", "example.com", "SYSTEM", "", "", "", nil)
				require.NoError(t, err)

				return r.ID
			},
		},
		{
			name:    "rule association",
			pattern: `^rslvr-rrassoc-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				r, err := b.CreateResolverRule(context.Background(), "r", "example.com", "SYSTEM", "", "", "", nil)
				require.NoError(t, err)

				a, err := b.AssociateResolverRule(context.Background(), r.ID, "vpc-1", "a")
				require.NoError(t, err)

				return a.ID
			},
		},
		{
			name:    "firewall domain list",
			pattern: `^rslvr-fdl-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				l, err := b.CreateFirewallDomainList(context.Background(), "l", "")
				require.NoError(t, err)

				return l.ID
			},
		},
		{
			name:    "firewall rule group",
			pattern: `^rslvr-frg-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				g, err := b.CreateFirewallRuleGroup(context.Background(), "g", "")
				require.NoError(t, err)

				return g.ID
			},
		},
		{
			name:    "firewall rule group association",
			pattern: `^rslvr-frgassoc-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				g, err := b.CreateFirewallRuleGroup(context.Background(), "g", "")
				require.NoError(t, err)

				a, err := b.AssociateFirewallRuleGroup(context.Background(), g.ID, "vpc-1", "a", "", "DISABLED", 101)
				require.NoError(t, err)

				return a.ID
			},
		},
		{
			name:    "query log config",
			pattern: `^rslvr-rqlc-[0-9a-f]{17}$`,
			mint: func(t *testing.T, b *route53resolver.InMemoryBackend) string {
				t.Helper()

				c, err := b.CreateResolverQueryLogConfig(context.Background(), "q", "", "arn:aws:s3:::bucket")
				require.NoError(t, err)

				return c.ID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			assert.Regexp(t, tt.pattern, tt.mint(t, b))
		})
	}
}
