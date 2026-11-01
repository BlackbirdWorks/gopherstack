package ocidns_test

import (
	"net"
	"strconv"
	"testing"

	"github.com/miekg/dns"
	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) query(t *testing.T, network, name string, qtype uint16) *dns.Msg {
	t.Helper()

	m := new(dns.Msg)
	m.SetQuestion(name, qtype)

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(e.handler.DNSPort))

	r, _, err := (&dns.Client{Net: network}).ExchangeContext(t.Context(), m, addr)
	require.NoError(t, err)

	return r
}

func answers(r *dns.Msg) []string {
	out := make([]string, 0, len(r.Answer))
	for _, a := range r.Answer {
		out = append(out, a.String())
	}

	return out
}

func TestDataPlaneAnswers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createZone(t, "dp.example", nil)
	e.mustPatch(t, "dp.example",
		add("www.dp.example", "A", "192.0.2.1", 300), add("www.dp.example", "A", "192.0.2.2", 300),
		add("www.dp.example", "AAAA", "2001:db8::1", 300),
		add("alias.dp.example", "CNAME", "www.dp.example.", 60),
		add("dp.example", "MX", "10 mail.dp.example.", 600),
		add("mail.dp.example", "A", "192.0.2.25", 600),
		add("dp.example", "TXT", `"v=spf1 -all"`, 120),
		add("*.wild.dp.example", "A", "192.0.2.99", 30),
		add("sub.dp.example", "NS", "ns.sub.dp.example.", 300),
		add("ns.sub.dp.example", "A", "192.0.2.53", 300),
		add("alias2.dp.example", "ALIAS", "www.dp.example.", 60),
	)

	for _, network := range []string{"udp", "tcp"} {
		t.Run(network, func(t *testing.T) {
			t.Parallel()

			r := e.query(t, network, "www.dp.example.", dns.TypeA)
			assert.Equal(t, dns.RcodeSuccess, r.Rcode)
			assert.True(t, r.Authoritative)
			assert.ElementsMatch(
				t,
				[]string{"www.dp.example.\t300\tIN\tA\t192.0.2.1", "www.dp.example.\t300\tIN\tA\t192.0.2.2"},
				answers(r),
			)

			assert.Equal(
				t,
				[]string{"www.dp.example.\t300\tIN\tAAAA\t2001:db8::1"},
				answers(e.query(t, network, "WWW.dp.example.", dns.TypeAAAA)),
			)

			cn := e.query(t, network, "alias.dp.example.", dns.TypeA)
			require.Len(t, cn.Answer, 3, "the CNAME and the A set it points to")
			assert.Equal(t, dns.TypeCNAME, cn.Answer[0].Header().Rrtype)

			assert.Equal(
				t,
				[]string{"alias.dp.example.\t60\tIN\tCNAME\twww.dp.example."},
				answers(e.query(t, network, "alias.dp.example.", dns.TypeCNAME)),
			)

			mx := e.query(t, network, "dp.example.", dns.TypeMX)
			assert.Equal(t, []string{"dp.example.\t600\tIN\tMX\t10 mail.dp.example."}, answers(mx))
			require.Len(t, mx.Extra, 1, "glue for the in-zone exchange")
			assert.Equal(t, "mail.dp.example.", mx.Extra[0].Header().Name)

			assert.Equal(
				t,
				[]string{`dp.example.	120	IN	TXT	"v=spf1 -all"`},
				answers(e.query(t, network, "dp.example.", dns.TypeTXT)),
			)

			soa := e.query(t, network, "dp.example.", dns.TypeSOA)
			require.Len(t, soa.Answer, 1)
			assert.Equal(t, uint32(1800), soa.Answer[0].(*dns.SOA).Minttl)

			ns := e.query(t, network, "dp.example.", dns.TypeNS)
			assert.Len(t, ns.Answer, 4)

			wild := e.query(t, network, "anything.wild.dp.example.", dns.TypeA)
			require.Len(t, wild.Answer, 1)
			assert.Equal(t, "anything.wild.dp.example.", wild.Answer[0].Header().Name)

			nx := e.query(t, network, "missing.dp.example.", dns.TypeA)
			assert.Equal(t, dns.RcodeNameError, nx.Rcode)
			require.Len(t, nx.Ns, 1, "negative answers carry the SOA")
			assert.Equal(t, dns.TypeSOA, nx.Ns[0].Header().Rrtype)

			nodata := e.query(t, network, "www.dp.example.", dns.TypeMX)
			assert.Equal(t, dns.RcodeSuccess, nodata.Rcode)
			assert.Empty(t, nodata.Answer)
			assert.Len(t, nodata.Ns, 1)

			deleg := e.query(t, network, "host.sub.dp.example.", dns.TypeA)
			assert.False(t, deleg.Authoritative)
			assert.Len(t, deleg.Ns, 1)
			assert.Len(t, deleg.Extra, 1, "in-zone glue")

			assert.Equal(t, dns.RcodeRefused, e.query(t, network, "www.elsewhere.test.", dns.TypeA).Rcode)

			// ALIAS has no wire form, so a query for it has nothing to return.
			assert.Empty(t, e.query(t, network, "alias2.dp.example.", dns.TypeA).Answer)
		})
	}
}

func TestDataPlaneFollowsUpdates(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createZone(t, "live.example", nil)

	assert.Empty(t, e.query(t, "udp", "a.live.example.", dns.TypeA).Answer)
	assert.Equal(t, dns.RcodeNameError, e.query(t, "udp", "a.live.example.", dns.TypeA).Rcode)

	e.mustPatch(t, "live.example", add("a.live.example", "A", "192.0.2.1", 60))
	assert.Equal(
		t,
		[]string{"a.live.example.\t60\tIN\tA\t192.0.2.1"},
		answers(e.query(t, "udp", "a.live.example.", dns.TypeA)),
	)

	e.mustPatch(t, "live.example", add("a.live.example", "A", "192.0.2.2", 30))
	assert.Len(t, e.query(t, "tcp", "a.live.example.", dns.TypeA).Answer, 2)
	assert.Equal(t, uint32(30), e.query(t, "tcp", "a.live.example.", dns.TypeA).Answer[0].Header().Ttl)

	e.mustPatch(t, "live.example", remove("a.live.example", "A", "192.0.2.1"))
	assert.Equal(
		t,
		[]string{"a.live.example.\t30\tIN\tA\t192.0.2.2"},
		answers(e.query(t, "udp", "a.live.example.", dns.TypeA)),
	)

	soa := e.query(t, "udp", "live.example.", dns.TypeSOA).Answer[0].(*dns.SOA)
	assert.Equal(t, uint32(4), soa.Serial, "the SOA serial follows the zone version")

	_, err := e.client.UpdateZoneRecords(t.Context(), ocidnssdk.UpdateZoneRecordsRequest{
		ZoneNameOrId: new("live.example"),
		Items: []ocidnssdk.RecordDetails{
			{Domain: new("b.live.example"), Rtype: new("TXT"), Rdata: new(`"replaced"`), Ttl: new(10)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeNameError, e.query(t, "udp", "a.live.example.", dns.TypeA).Rcode)
	assert.Len(t, e.query(t, "udp", "b.live.example.", dns.TypeTXT).Answer, 1)

	_, err = e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{ZoneNameOrId: new("live.example")})
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeRefused, e.query(t, "udp", "b.live.example.", dns.TypeTXT).Rcode)
}

func TestDataPlaneZoneSelection(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.createView(t, "v")
	e.createZone(t, "sel.example", nil)
	e.createZone(t, "child.sel.example", nil)
	e.createZone(
		t,
		"sel.example",
		func(d *ocidnssdk.CreateZoneDetails) { d.Scope, d.ViewId = ocidnssdk.ScopePrivate, v.Id },
	)
	e.createZone(
		t,
		"secret.internal",
		func(d *ocidnssdk.CreateZoneDetails) { d.Scope, d.ViewId = ocidnssdk.ScopePrivate, v.Id },
	)

	e.mustPatch(t, "sel.example", add("x.sel.example", "A", "192.0.2.1", 60))
	e.mustPatch(t, "child.sel.example", add("y.child.sel.example", "A", "192.0.2.2", 60))

	assert.Len(t, e.query(t, "udp", "x.sel.example.", dns.TypeA).Answer, 1)
	assert.Len(t, e.query(t, "udp", "y.child.sel.example.", dns.TypeA).Answer, 1, "the longest enclosing zone answers")
	assert.Equal(t, dns.RcodeNameError, e.query(t, "udp", "z.child.sel.example.", dns.TypeA).Rcode)
	assert.Equal(
		t,
		dns.RcodeRefused,
		e.query(t, "udp", "secret.internal.", dns.TypeSOA).Rcode,
		"private zones are not served",
	)

	m := new(dns.Msg)
	m.SetQuestion("x.sel.example.", dns.TypeA)
	m.SetEdns0(4096, false)

	r, _, err := (&dns.Client{Net: "udp"}).ExchangeContext(
		t.Context(),
		m,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(e.handler.DNSPort)),
	)
	require.NoError(t, err)
	assert.NotNil(t, r.IsEdns0())

	m = new(dns.Msg)
	m.SetQuestion("x.sel.example.", dns.TypeAXFR)

	r, _, err = (&dns.Client{Net: "udp"}).ExchangeContext(
		t.Context(),
		m,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(e.handler.DNSPort)),
	)
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeRefused, r.Rcode)
}

func TestDataPlaneUDPTruncation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createZone(t, "big.example", nil)

	ops := make([]ocidnssdk.RecordOperation, 0, 60)
	for i := range 60 {
		ops = append(
			ops,
			add("many.big.example", "TXT", `"record-`+strconv.Itoa(i)+`-padding-padding-padding-padding"`, 60),
		)
	}

	e.mustPatch(t, "big.example", ops...)

	udp := e.query(t, "udp", "many.big.example.", dns.TypeTXT)
	assert.True(t, udp.Truncated, "a large answer is truncated over classic UDP")

	tcp := e.query(t, "tcp", "many.big.example.", dns.TypeTXT)
	assert.False(t, tcp.Truncated)
	assert.Len(t, tcp.Answer, 60)
}
