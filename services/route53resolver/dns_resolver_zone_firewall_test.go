package route53resolver_test

import (
	"context"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func (e *dnsEnv) privateZone(t *testing.T, zone string, recs ...route53backend.ResourceRecordSet) {
	t.Helper()

	hz, err := e.r53.CreateHostedZone(zone, "ref-"+zone, "", true, "", vpc1, testRegion)
	require.NoError(t, err)

	changes := make([]route53backend.Change, 0, len(recs))
	for _, r := range recs {
		changes = append(
			changes,
			route53backend.Change{Action: route53backend.ChangeActionCreate, ResourceRecordSet: r},
		)
	}

	_, err = e.r53.ChangeResourceRecordSets(hz.ID, changes)
	require.NoError(t, err)
}

func aRecord(name string) route53backend.ResourceRecordSet {
	return route53backend.ResourceRecordSet{
		Name: name, Type: "A", TTL: 30, Records: []route53backend.ResourceRecord{{Value: "10.9.9.9"}},
	}
}

func TestDNSResolverPrivateZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, e *dnsEnv)
		name     string
		src      string
		query    string
		wantIP   string
		wantCode int
		qtype    uint16
		wantNone bool
	}{
		{
			name: "associated_zone_answers",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.privateZone(t, "priv.example.", aRecord("db.priv.example."))
			},
			src: vpc1Client, query: "db.priv.example", qtype: dns.TypeA, wantIP: "10.9.9.9",
		},
		{
			name: "unassociated_vpc_skips_zone",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.privateZone(t, "priv.example.", aRecord("db.priv.example."))
			},
			src: vpc2Client, query: "db.priv.example", qtype: dns.TypeA, wantIP: localMarker,
		},
		{
			name: "missing_name_is_nxdomain",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.privateZone(t, "priv.example.", aRecord("db.priv.example."))
			},
			src: vpc1Client, query: "nope.priv.example", qtype: dns.TypeA, wantCode: dns.RcodeNameError,
			wantNone: true,
		},
		{
			name: "other_type_is_nodata",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.privateZone(t, "priv.example.", aRecord("db.priv.example."))
			},
			src: vpc1Client, query: "db.priv.example", qtype: dns.TypeAAAA, wantNone: true,
		},
		{
			name: "same_domain_rule_overrides_zone",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.privateZone(t, "priv.example.", aRecord("db.priv.example."))
				e.associate(t, e.rule(t, "priv.example", "FORWARD", up.target()))
			},
			src: vpc1Client, query: "db.priv.example", qtype: dns.TypeA, wantIP: "192.0.2.10",
		},
		{
			name: "more_specific_zone_beats_rule",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.privateZone(t, "sub.priv.example.", aRecord("db.sub.priv.example."))
				e.associate(t, e.rule(t, "priv.example", "FORWARD", up.target()))
			},
			src: vpc1Client, query: "db.sub.priv.example", qtype: dns.TypeA, wantIP: "10.9.9.9",
		},
		{
			name: "system_rule_defers_to_zone",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				up := startUpstream(t, "192.0.2.10")
				e.privateZone(t, "priv.example.", aRecord("db.priv.example."))
				e.associate(t, e.rule(t, "example", "FORWARD", up.target()))
				e.associate(t, e.rule(t, "priv.example", "SYSTEM"))
			},
			src: vpc1Client, query: "db.priv.example", qtype: dns.TypeA, wantIP: "10.9.9.9",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDNSEnv(t)
			tt.setup(t, e)

			resp := e.ask(tt.src, tt.query, tt.qtype)
			assert.Equal(t, tt.wantCode, resp.Rcode)

			if tt.wantNone {
				assert.Empty(t, resp.Answer)
			} else {
				assert.Equal(t, []string{tt.wantIP}, answerIPs(resp))
			}
		})
	}
}

type fwSpec struct {
	action, blockResponse, overrideDomain, qtype string
	domains                                      []string
	priority, groupPriority                      int32
}

func (e *dnsEnv) firewall(t *testing.T, specs ...fwSpec) []string {
	t.Helper()

	ctx := context.Background()
	assocs := make([]string, 0, len(specs))

	for i, s := range specs {
		grp, err := e.b.CreateFirewallRuleGroup(ctx, "grp", "")
		require.NoError(t, err)

		dl, err := e.b.CreateFirewallDomainList(ctx, "dl", "")
		require.NoError(t, err)

		_, err = e.b.UpdateFirewallDomains(ctx, dl.ID, "ADD", s.domains)
		require.NoError(t, err)

		p := route53resolver.CreateFirewallRuleParams{
			FirewallRuleGroupID: grp.ID, Name: "r", Action: s.action, FirewallDomainListID: dl.ID,
			BlockResponse: s.blockResponse, Qtype: s.qtype, Priority: s.priority,
		}
		if s.blockResponse == "OVERRIDE" {
			p.BlockOverrideDomain, p.BlockOverrideDNSType, p.BlockOverrideTTL = s.overrideDomain, "CNAME", 20
		}

		_, err = e.b.CreateFirewallRule(ctx, p)
		require.NoError(t, err)

		a, err := e.b.AssociateFirewallRuleGroup(ctx, grp.ID, vpc1, "a", "", "", s.groupPriority+int32(i))
		require.NoError(t, err)

		assocs = append(assocs, a.ID)
	}

	return assocs
}

func TestDNSResolverFirewall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, e *dnsEnv)
		name         string
		src          string
		query        string
		wantCNAME    string
		wantIP       string
		wantCode     int
		wantNoAnswer bool
	}{
		{
			name: "block_nodata",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NODATA", domains: []string{"bad.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, query: "bad.example", wantNoAnswer: true,
		},
		{
			name: "block_nxdomain",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"bad.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, query: "bad.example", wantCode: dns.RcodeNameError, wantNoAnswer: true,
		},
		{
			name: "block_override",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "OVERRIDE", overrideDomain: "sinkhole.test",
					domains: []string{"bad.example"}, priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, query: "bad.example", wantCNAME: "sinkhole.test.",
		},
		{
			name: "wildcard_domain",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"*.bad.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, query: "x.bad.example", wantCode: dns.RcodeNameError, wantNoAnswer: true,
		},
		{
			name: "unlisted_domain_passes",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"bad.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, query: "good.example", wantIP: localMarker,
		},
		{
			name: "other_vpc_unaffected",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"bad.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc2Client, query: "bad.example", wantIP: localMarker,
		},
		{
			name: "qtype_filter",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", qtype: "AAAA", domains: []string{"bad.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, query: "bad.example", wantIP: localMarker,
		},
		{
			name: "allow_priority_beats_later_block",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t,
					fwSpec{action: "ALLOW", domains: []string{"bad.example"}, priority: 100, groupPriority: 100},
					fwSpec{
						action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"bad.example"},
						priority: 100, groupPriority: 200,
					},
				)
			},
			src: vpc1Client, query: "bad.example", wantIP: localMarker,
		},
		{
			name: "block_priority_beats_later_allow",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(t,
					fwSpec{
						action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"bad.example"},
						priority: 100, groupPriority: 100,
					},
					fwSpec{action: "ALLOW", domains: []string{"bad.example"}, priority: 100, groupPriority: 200},
				)
			},
			src: vpc1Client, query: "bad.example", wantCode: dns.RcodeNameError, wantNoAnswer: true,
		},
		{
			name: "alert_does_not_block",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.firewall(
					t,
					fwSpec{action: "ALERT", domains: []string{"bad.example"}, priority: 100, groupPriority: 100},
				)
			},
			src: vpc1Client, query: "bad.example", wantIP: localMarker,
		},
		{
			name: "disassociated_group_stops_blocking",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				ids := e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"bad.example"},
					priority: 100, groupPriority: 100,
				})
				_, err := e.b.DisassociateFirewallRuleGroup(context.Background(), ids[0])
				require.NoError(t, err)
			},
			src: vpc1Client, query: "bad.example", wantIP: localMarker,
		},
		{
			name: "blocks_private_zone_names",
			setup: func(t *testing.T, e *dnsEnv) {
				t.Helper()

				e.privateZone(t, "priv.example.", aRecord("db.priv.example."))
				e.firewall(t, fwSpec{
					action: "BLOCK", blockResponse: "NXDOMAIN", domains: []string{"db.priv.example"},
					priority: 100, groupPriority: 100,
				})
			},
			src: vpc1Client, query: "db.priv.example", wantCode: dns.RcodeNameError, wantNoAnswer: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDNSEnv(t)
			tt.setup(t, e)

			resp := e.ask(tt.src, tt.query, dns.TypeA)
			assert.Equal(t, tt.wantCode, resp.Rcode)

			switch {
			case tt.wantCNAME != "":
				require.Len(t, resp.Answer, 1)
				cn, ok := resp.Answer[0].(*dns.CNAME)
				require.True(t, ok)
				assert.Equal(t, tt.wantCNAME, cn.Target)
			case tt.wantNoAnswer:
				assert.Empty(t, resp.Answer)
			default:
				assert.Equal(t, []string{tt.wantIP}, answerIPs(resp))
			}
		})
	}
}
