package gcpdns_test

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
	"google.golang.org/api/option"

	"github.com/blackbirdworks/gopherstack/services/gcpdns"
)

type liveEnv struct {
	*env
	dnsAddr string
}

func newLiveEnv(t *testing.T) *liveEnv {
	t.Helper()

	backend := gcpdns.NewInMemoryBackend()
	h := gcpdns.NewHandler(backend)
	h.Port, h.DNSPort = freePort(t), freePort(t)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	url := "http://127.0.0.1:" + strconv.Itoa(h.Port)

	svc, err := dnsapi.NewService(t.Context(), option.WithEndpoint(url), option.WithoutAuthentication())
	require.NoError(t, err)

	return &liveEnv{
		env:     &env{svc: svc, backend: backend, url: url},
		dnsAddr: net.JoinHostPort("127.0.0.1", strconv.Itoa(h.DNSPort)),
	}
}

func (l *liveEnv) query(t *testing.T, network, name string, qtype uint16, edns uint16) *dns.Msg {
	t.Helper()

	m := new(dns.Msg)
	m.SetQuestion(name, qtype)

	if edns > 0 {
		m.SetEdns0(edns, false)
	}

	c := &dns.Client{Net: network}

	r, _, err := c.ExchangeContext(t.Context(), m, l.dnsAddr)
	require.NoError(t, err)

	return r
}

func rrStrings(rrs []dns.RR) []string {
	if len(rrs) == 0 {
		return nil
	}

	out := make([]string, len(rrs))
	for i, rr := range rrs {
		out[i] = strings.Join(strings.Fields(rr.String()), " ")
	}

	return out
}

func seedDataPlane(t *testing.T, l *liveEnv) {
	t.Helper()

	l.zone(t, "ex", "example.com.")

	long := make([]string, 30)
	for i := range long {
		long[i] = `"` + strings.Repeat(string(rune('a'+i%26)), 100) + `"`
	}

	adds := []*dnsapi.ResourceRecordSet{
		rrset("www.example.com.", "A", 300, "1.2.3.4"),
		rrset("www.example.com.", "AAAA", 300, "2001:db8::1"),
		rrset("alias.example.com.", "CNAME", 300, "www.example.com."),
		rrset("c1.example.com.", "CNAME", 300, "c2.example.com."),
		rrset("c2.example.com.", "CNAME", 300, "www.example.com."),
		rrset("ext.example.com.", "CNAME", 300, "other.org."),
		rrset("example.com.", "MX", 300, "10 mail.example.com."),
		rrset("mail.example.com.", "A", 300, "1.2.3.5"),
		rrset("example.com.", "TXT", 300, `"v=spf1 -all"`),
		rrset("_sip._tcp.example.com.", "SRV", 300, "1 2 5060 www.example.com."),
		rrset("example.com.", "CAA", 300, `0 issue "letsencrypt.org"`),
		rrset("4.3.2.1.in-addr.arpa.example.com.", "PTR", 300, "www.example.com."),
		rrset("*.wild.example.com.", "A", 300, "9.9.9.9"),
		rrset("sub.wild.example.com.", "A", 300, "8.8.8.8"),
		rrset("deleg.example.com.", "NS", 300, "ns.deleg.example.com."),
		rrset("ns.deleg.example.com.", "A", 300, "7.7.7.7"),
		rrset("a.b.example.com.", "A", 300, "1.1.1.1"),
		rrset("big.example.com.", "TXT", 300, long...),
	}

	_, err := l.svc.Changes.Create(project, "ex", &dnsapi.Change{Additions: adds}).Context(t.Context()).Do()
	require.NoError(t, err)
}

func TestDataPlaneAnswers(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	seedDataPlane(t, l)

	tests := []struct {
		name      string
		qname     string
		answer    []string
		authority []string
		qtype     uint16
		rcode     uint16
		aa        bool
	}{
		{
			"a",
			"www.example.com.",
			[]string{"www.example.com. 300 IN A 1.2.3.4"},
			nil,
			dns.TypeA,
			dns.RcodeSuccess,
			true,
		},
		{
			"case insensitive",
			"WWW.Example.COM.",
			[]string{"www.example.com. 300 IN A 1.2.3.4"},
			nil,
			dns.TypeA,
			0,
			true,
		},
		{"aaaa", "www.example.com.", []string{"www.example.com. 300 IN AAAA 2001:db8::1"}, nil, dns.TypeAAAA, 0, true},
		{"cname chased", "alias.example.com.", []string{
			"alias.example.com. 300 IN CNAME www.example.com.", "www.example.com. 300 IN A 1.2.3.4",
		}, nil, dns.TypeA, 0, true},
		{"cname chain", "c1.example.com.", []string{
			"c1.example.com. 300 IN CNAME c2.example.com.", "c2.example.com. 300 IN CNAME www.example.com.",
			"www.example.com. 300 IN A 1.2.3.4",
		}, nil, dns.TypeA, 0, true},
		{
			"cname out of zone",
			"ext.example.com.",
			[]string{"ext.example.com. 300 IN CNAME other.org."},
			nil,
			dns.TypeA,
			0,
			true,
		},
		{
			"cname query",
			"alias.example.com.",
			[]string{"alias.example.com. 300 IN CNAME www.example.com."},
			nil,
			dns.TypeCNAME,
			0,
			true,
		},
		{"mx", "example.com.", []string{"example.com. 300 IN MX 10 mail.example.com."}, nil, dns.TypeMX, 0, true},
		{"txt", "example.com.", []string{`example.com. 300 IN TXT "v=spf1 -all"`}, nil, dns.TypeTXT, 0, true},
		{
			"srv",
			"_sip._tcp.example.com.",
			[]string{"_sip._tcp.example.com. 300 IN SRV 1 2 5060 www.example.com."},
			nil,
			dns.TypeSRV,
			0,
			true,
		},
		{
			"caa",
			"example.com.",
			[]string{`example.com. 300 IN CAA 0 issue "letsencrypt.org"`},
			nil,
			dns.TypeCAA,
			0,
			true,
		},
		{
			"ptr",
			"4.3.2.1.in-addr.arpa.example.com.",
			[]string{"4.3.2.1.in-addr.arpa.example.com. 300 IN PTR www.example.com."},
			nil,
			dns.TypePTR,
			0,
			true,
		},
		{
			"wildcard",
			"anything.wild.example.com.",
			[]string{"anything.wild.example.com. 300 IN A 9.9.9.9"},
			nil,
			dns.TypeA,
			0,
			true,
		},
		{
			"wildcard deep",
			"a.b.c.wild.example.com.",
			[]string{"a.b.c.wild.example.com. 300 IN A 9.9.9.9"},
			nil,
			dns.TypeA,
			0,
			true,
		},
		{
			"explicit beats wildcard",
			"sub.wild.example.com.",
			[]string{"sub.wild.example.com. 300 IN A 8.8.8.8"},
			nil,
			dns.TypeA,
			0,
			true,
		},
		{"wildcard nodata", "x.wild.example.com.", nil, []string{"SOA"}, dns.TypeTXT, 0, true},
		{"nodata", "www.example.com.", nil, []string{"SOA"}, dns.TypeTXT, 0, true},
		{"empty non terminal", "b.example.com.", nil, []string{"SOA"}, dns.TypeA, 0, true},
		{"nxdomain", "nothing.example.com.", nil, []string{"SOA"}, dns.TypeA, dns.RcodeNameError, true},
		{"nxdomain below leaf", "x.www.example.com.", nil, []string{"SOA"}, dns.TypeA, dns.RcodeNameError, true},
		{"refused outside zones", "other.org.", nil, nil, dns.TypeA, dns.RcodeRefused, false},
		{
			"delegation",
			"host.deleg.example.com.",
			nil,
			[]string{"deleg.example.com. 300 IN NS ns.deleg.example.com."},
			dns.TypeA,
			0,
			false,
		},
	}

	for _, network := range []string{"udp", "tcp"} {
		for _, tt := range tests {
			t.Run(network+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				r := l.query(t, network, tt.qname, tt.qtype, 0)
				assert.Equal(t, int(tt.rcode), r.Rcode)
				assert.Equal(t, tt.aa, r.Authoritative)
				assert.False(t, r.RecursionAvailable)
				assert.Equal(t, tt.answer, rrStrings(r.Answer))

				if len(tt.authority) == 1 && tt.authority[0] == "SOA" {
					require.Len(t, r.Ns, 1)
					assert.Equal(t, dns.TypeSOA, r.Ns[0].Header().Rrtype)
					assert.Equal(t, "example.com.", r.Ns[0].Header().Name)
					assert.LessOrEqual(t, r.Ns[0].Header().Ttl, uint32(300))

					return
				}

				assert.Equal(t, tt.authority, rrStrings(r.Ns))
			})
		}
	}
}

func TestDataPlaneApexAndGlue(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	seedDataPlane(t, l)

	z, err := l.svc.ManagedZones.Get(project, "ex").Context(t.Context()).Do()
	require.NoError(t, err)

	for _, network := range []string{"udp", "tcp"} {
		t.Run(network, func(t *testing.T) {
			t.Parallel()

			soa := l.query(t, network, "example.com.", dns.TypeSOA, 0)
			require.Len(t, soa.Answer, 1)
			assert.Equal(t, z.NameServers[0], soa.Answer[0].(*dns.SOA).Ns)
			assert.Equal(t, uint32(2), soa.Answer[0].(*dns.SOA).Serial)

			ns := l.query(t, network, "example.com.", dns.TypeNS, 0)
			assert.ElementsMatch(t, z.NameServers, func() []string {
				out := make([]string, 0, len(ns.Answer))
				for _, rr := range ns.Answer {
					out = append(out, rr.(*dns.NS).Ns)
				}

				return out
			}())

			mx := l.query(t, network, "example.com.", dns.TypeMX, 0)
			require.Len(t, mx.Extra, 1, "in-zone MX target comes with its address")
			assert.Equal(t, "mail.example.com.", mx.Extra[0].Header().Name)

			ref := l.query(t, network, "host.deleg.example.com.", dns.TypeA, 0)
			require.Len(t, ref.Extra, 1)
			assert.Equal(t, "7.7.7.7", ref.Extra[0].(*dns.A).A.String())

			anyQ := l.query(t, network, "www.example.com.", dns.TypeANY, 0)
			assert.Len(t, anyQ.Answer, 2)
		})
	}
}

func TestDataPlaneTruncation(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	seedDataPlane(t, l)

	small := l.query(t, "udp", "big.example.com.", dns.TypeTXT, 0)
	assert.True(t, small.Truncated, "UDP answer over 512 bytes sets TC")
	assert.Less(t, len(small.Answer), 30)

	edns := l.query(t, "udp", "big.example.com.", dns.TypeTXT, 4096)
	assert.False(t, edns.Truncated)
	assert.Len(t, edns.Answer, 30)
	require.NotNil(t, edns.IsEdns0())

	tcp := l.query(t, "tcp", "big.example.com.", dns.TypeTXT, 0)
	assert.False(t, tcp.Truncated)
	assert.Len(t, tcp.Answer, 30)
}

func TestDataPlaneOpcodesAndTransfers(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	seedDataPlane(t, l)

	axfr := l.query(t, "tcp", "example.com.", dns.TypeAXFR, 0)
	assert.Equal(t, dns.RcodeRefused, axfr.Rcode)

	m := new(dns.Msg)
	m.SetQuestion("www.example.com.", dns.TypeA)
	m.Opcode = dns.OpcodeNotify

	r, _, err := (&dns.Client{Net: "udp"}).ExchangeContext(t.Context(), m, l.dnsAddr)
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeNotImplemented, r.Rcode)

	chaos := new(dns.Msg)
	chaos.SetQuestion("www.example.com.", dns.TypeA)
	chaos.Question[0].Qclass = dns.ClassCHAOS

	r, _, err = (&dns.Client{Net: "udp"}).ExchangeContext(t.Context(), chaos, l.dnsAddr)
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeRefused, r.Rcode)
}

func TestDataPlaneFollowsChanges(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	l.zone(t, "ex", "example.com.")

	r := l.query(t, "udp", "new.example.com.", dns.TypeA, 0)
	assert.Equal(t, dns.RcodeNameError, r.Rcode)

	l.addRRSet(t, "ex", rrset("new.example.com.", "A", 60, "4.4.4.4"))

	r = l.query(t, "udp", "new.example.com.", dns.TypeA, 0)
	require.Len(t, r.Answer, 1)
	assert.Equal(t, uint32(60), r.Answer[0].Header().Ttl)

	_, err := l.svc.ResourceRecordSets.Patch(project, "ex", "new.example.com.", "A", &dnsapi.ResourceRecordSet{
		Rrdatas: []string{"5.5.5.5"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	r = l.query(t, "udp", "new.example.com.", dns.TypeA, 0)
	require.Len(t, r.Answer, 1)
	assert.Equal(t, "5.5.5.5", r.Answer[0].(*dns.A).A.String())

	_, err = l.svc.ResourceRecordSets.Delete(project, "ex", "new.example.com.", "A").Context(t.Context()).Do()
	require.NoError(t, err)

	r = l.query(t, "udp", "new.example.com.", dns.TypeA, 0)
	assert.Equal(t, dns.RcodeNameError, r.Rcode)

	require.NoError(t, l.svc.ManagedZones.Delete(project, "ex").Context(t.Context()).Do())
	r = l.query(t, "udp", "example.com.", dns.TypeSOA, 0)
	assert.Equal(t, dns.RcodeRefused, r.Rcode)
}

func TestDataPlaneZoneSelection(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	l.zone(t, "parent", "example.com.")
	l.zone(t, "child", "eu.example.com.")
	l.addRRSet(t, "parent", rrset("www.eu.example.com.", "A", 60, "1.1.1.1"))
	l.addRRSet(t, "child", rrset("www.eu.example.com.", "A", 60, "2.2.2.2"))

	r := l.query(t, "udp", "www.eu.example.com.", dns.TypeA, 0)
	require.Len(t, r.Answer, 1)
	assert.Equal(t, "2.2.2.2", r.Answer[0].(*dns.A).A.String(), "the longest matching zone answers")

	_, err := l.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "priv", DnsName: "internal.", Description: "d", Visibility: "private",
		PrivateVisibilityConfig: &dnsapi.ManagedZonePrivateVisibilityConfig{},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	r = l.query(t, "udp", "internal.", dns.TypeSOA, 0)
	assert.Equal(t, dns.RcodeRefused, r.Rcode, "private zones are not served on the public data plane")
}

func TestDataPlaneDNSKEY(t *testing.T) {
	t.Parallel()

	l := newLiveEnv(t)
	_, err := l.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "sec", DnsName: "sec.com.", Description: "d",
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "on"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	keys, err := l.svc.DnsKeys.List(project, "sec").Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, keys.DnsKeys, 2)

	r := l.query(t, "tcp", "sec.com.", dns.TypeDNSKEY, 0)
	require.Len(t, r.Answer, 2)

	tags := map[int64]bool{}
	for _, rr := range r.Answer {
		tags[int64(rr.(*dns.DNSKEY).KeyTag())] = true
	}

	for _, k := range keys.DnsKeys {
		assert.True(t, tags[k.KeyTag], "key tag %d served", k.KeyTag)
	}
}
