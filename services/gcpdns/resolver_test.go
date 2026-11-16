package gcpdns_test

import (
	"context"
	"sync"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

type captureSink struct {
	entries []*loggingpb.LogEntry
	mu      sync.Mutex
}

func (c *captureSink) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = append(c.entries, req.GetEntries()...)

	return nil
}

func (c *captureSink) all() []*loggingpb.LogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]*loggingpb.LogEntry(nil), c.entries...)
}

func (l *liveEnv) privateZone(t *testing.T, name, dnsName string, networks ...string) {
	t.Helper()

	cfg := &dnsapi.ManagedZonePrivateVisibilityConfig{}
	for _, n := range networks {
		cfg.Networks = append(cfg.Networks, &dnsapi.ManagedZonePrivateVisibilityConfigNetwork{NetworkUrl: n})
	}

	_, err := l.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: name, DnsName: dnsName, Description: "d", Visibility: "private", PrivateVisibilityConfig: cfg,
	}).Context(t.Context()).Do()
	require.NoError(t, err)
}

func TestPrivateZoneServing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		networks []string
		wantAns  []string
		wantRC   int
	}{
		{"attached", []string{netURL}, []string{"db.corp.internal. 60 IN A 10.0.0.5"}, dns.RcodeSuccess},
		{"unattached", nil, nil, dns.RcodeRefused},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newLiveEnv(t)
			l.privateZone(t, "corp", "corp.internal.", tt.networks...)
			l.addRRSet(t, "corp", rrset("db.corp.internal.", "A", 60, "10.0.0.5"))

			r := l.query(t, "udp", "db.corp.internal.", dns.TypeA, 0)
			assert.Equal(t, tt.wantRC, r.Rcode)
			assert.Equal(t, tt.wantAns, rrStrings(r.Answer))
		})
	}
}

func TestPrivateZoneShadowsPublic(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	l.zone(t, "pub", "example.com.")
	l.addRRSet(t, "pub", rrset("www.example.com.", "A", 60, "192.0.2.1"))
	l.privateZone(t, "priv", "example.com.", netURL)
	l.addRRSet(t, "priv", rrset("www.example.com.", "A", 60, "10.1.1.1"))

	r := l.query(t, "udp", "www.example.com.", dns.TypeA, 0)
	assert.Equal(t, []string{"www.example.com. 60 IN A 10.1.1.1"}, rrStrings(r.Answer))
}

func TestForwardingZoneNotServed(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	_, err := l.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "fwd", DnsName: "fwd.internal.", Description: "d", Visibility: "private",
		PrivateVisibilityConfig: &dnsapi.ManagedZonePrivateVisibilityConfig{
			Networks: []*dnsapi.ManagedZonePrivateVisibilityConfigNetwork{{NetworkUrl: netURL}},
		},
		ForwardingConfig: &dnsapi.ManagedZoneForwardingConfig{
			TargetNameServers: []*dnsapi.ManagedZoneForwardingConfigNameServerTarget{{Ipv4Address: "10.9.9.9"}},
		},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	assert.Equal(t, dns.RcodeRefused, l.query(t, "udp", "x.fwd.internal.", dns.TypeA, 0).Rcode)
}

func (l *liveEnv) responsePolicy(t *testing.T, attached bool, rules ...*dnsapi.ResponsePolicyRule) {
	t.Helper()

	rp := &dnsapi.ResponsePolicy{ResponsePolicyName: "rp"}
	if attached {
		rp.Networks = []*dnsapi.ResponsePolicyNetwork{{NetworkUrl: netURL}}
	}

	_, err := l.svc.ResponsePolicies.Create(project, rp).Context(t.Context()).Do()
	require.NoError(t, err)

	for _, r := range rules {
		_, err = l.svc.ResponsePolicyRules.Create(project, "rp", r).Context(t.Context()).Do()
		require.NoError(t, err)
	}
}

func localRule(name, dnsName string, rr ...*dnsapi.ResourceRecordSet) *dnsapi.ResponsePolicyRule {
	return &dnsapi.ResponsePolicyRule{
		RuleName: name, DnsName: dnsName, LocalData: &dnsapi.ResponsePolicyRuleLocalData{LocalDatas: rr},
	}
}

func TestResponsePolicyServing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		qname    string
		rules    []*dnsapi.ResponsePolicyRule
		wantAns  []string
		qtype    uint16
		attached bool
	}{
		{
			attached: true,
			qtype:    dns.TypeA,
			name:     "exact override",
			rules: []*dnsapi.ResponsePolicyRule{
				localRule("r", "www.example.com.", rrset("www.example.com.", "A", 30, "10.2.2.2")),
			},
			qname:   "www.example.com.",
			wantAns: []string{"www.example.com. 30 IN A 10.2.2.2"},
		},
		{
			attached: true,
			qtype:    dns.TypeA,
			name:     "wildcard",
			rules: []*dnsapi.ResponsePolicyRule{
				localRule("r", "*.example.com.", rrset("*.example.com.", "A", 30, "10.3.3.3")),
			},
			qname:   "a.example.com.",
			wantAns: []string{"a.example.com. 30 IN A 10.3.3.3"},
		},
		{
			attached: true,
			qtype:    dns.TypeAAAA,
			name:     "type absent is nodata",
			rules: []*dnsapi.ResponsePolicyRule{
				localRule("r", "www.example.com.", rrset("www.example.com.", "A", 30, "10.2.2.2")),
			},
			qname:   "www.example.com.",
			wantAns: nil,
		},
		{
			attached: true,
			qtype:    dns.TypeA,
			name:     "bypass keeps zone",
			rules: []*dnsapi.ResponsePolicyRule{
				localRule("w", "*.example.com.", rrset("*.example.com.", "A", 30, "10.3.3.3")),
				{RuleName: "b", DnsName: "www.example.com.", Behavior: "bypassResponsePolicy"},
			},
			qname:   "www.example.com.",
			wantAns: []string{"www.example.com. 60 IN A 192.0.2.1"},
		},
		{
			attached: false,
			qtype:    dns.TypeA,
			name:     "unattached policy ignored",
			rules: []*dnsapi.ResponsePolicyRule{
				localRule("r", "www.example.com.", rrset("www.example.com.", "A", 30, "10.2.2.2")),
			},
			qname:   "www.example.com.",
			wantAns: []string{"www.example.com. 60 IN A 192.0.2.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newLiveEnv(t)
			l.zone(t, "pub", "example.com.")
			l.addRRSet(t, "pub", rrset("www.example.com.", "A", 60, "192.0.2.1"))
			l.responsePolicy(t, tt.attached, tt.rules...)

			r := l.query(t, "udp", tt.qname, tt.qtype, 0)
			assert.Equal(t, dns.RcodeSuccess, r.Rcode)
			assert.Equal(t, tt.wantAns, rrStrings(r.Answer))
		})
	}
}

func TestQueryLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyLogs bool
		attach     bool
		wantLogs   int
	}{
		{"policy logging on", true, true, 1},
		{"policy logging off", false, true, 0},
		{"policy on other network", true, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newLiveEnv(t)
			sink := &captureSink{}
			l.backend.SetLogSink(sink)

			l.privateZone(t, "corp", "corp.internal.", netURL)
			l.addRRSet(t, "corp", rrset("db.corp.internal.", "A", 60, "10.0.0.5"))

			pol := &dnsapi.Policy{Name: "pol", EnableLogging: tt.policyLogs}
			if tt.attach {
				pol.Networks = []*dnsapi.PolicyNetwork{{NetworkUrl: netURL}}
			}

			_, err := l.svc.Policies.Create(project, pol).Context(t.Context()).Do()
			require.NoError(t, err)

			l.query(t, "udp", "db.corp.internal.", dns.TypeA, 0)

			got := sink.all()
			require.Len(t, got, tt.wantLogs)

			if tt.wantLogs == 0 {
				return
			}

			e := got[0]
			assert.Equal(t, "projects/test-project/logs/dns.googleapis.com%2Fdns_queries", e.GetLogName())
			assert.Equal(t, "dns_query", e.GetResource().GetType())

			f := e.GetJsonPayload().AsMap()
			assert.Equal(t, "db.corp.internal.", f["queryName"])
			assert.Equal(t, "A", f["queryType"])
			assert.Equal(t, "NOERROR", f["responseCode"])
			assert.Equal(t, "UDP", f["protocol"])
			assert.Equal(t, "private-zone", f["target_type"])
			assert.Equal(t, "corp", f["target_name"])
			assert.Equal(t, netURL, f["sourceNetwork"])
			assert.Contains(t, f["rdata"], "10.0.0.5")
		})
	}
}

func TestPublicZoneCloudLogging(t *testing.T) {
	t.Parallel()

	for _, on := range []bool{true, false} {
		name := "off"
		if on {
			name = "on"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			l := newLiveEnv(t)
			sink := &captureSink{}
			l.backend.SetLogSink(sink)

			_, err := l.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
				Name: "pub", DnsName: "example.com.", Description: "d",
				CloudLoggingConfig: &dnsapi.ManagedZoneCloudLoggingConfig{EnableLogging: on},
			}).Context(t.Context()).Do()
			require.NoError(t, err)

			l.query(t, "tcp", "example.com.", dns.TypeSOA, 0)

			if !on {
				assert.Empty(t, sink.all())

				return
			}

			require.Len(t, sink.all(), 1)
			assert.Equal(t, "TCP", sink.all()[0].GetJsonPayload().AsMap()["protocol"])
		})
	}
}
