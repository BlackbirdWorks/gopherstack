package app

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
)

func TestSESInboundDNSLookups(t *testing.T) {
	t.Parallel()

	bk := route53backend.NewInMemoryBackend()
	zone, err := bk.CreateHostedZone("sender.test.", "ref", "", false, "", "", "")
	require.NoError(t, err)

	rev, err := bk.CreateHostedZone("127.in-addr.arpa.", "ref2", "", false, "", "", "")
	require.NoError(t, err)

	set := func(zoneID, name, typ string, vals ...string) {
		recs := make([]route53backend.ResourceRecord, len(vals))
		for i, v := range vals {
			recs[i] = route53backend.ResourceRecord{Value: v}
		}

		_, cerr := bk.ChangeResourceRecordSets(zoneID, []route53backend.Change{{
			Action:            route53backend.ChangeActionCreate,
			ResourceRecordSet: route53backend.ResourceRecordSet{Name: name, Type: typ, TTL: 60, Records: recs},
		}})
		require.NoError(t, cerr)
	}

	set(zone.ID, "sender.test.", "A", "127.0.0.1")
	set(zone.ID, "sender.test.", "AAAA", "2001:db8::1")
	set(zone.ID, "sender.test.", "MX", "10 mail.sender.test.", "20 mx2.sender.test.")
	set(zone.ID, "sender.test.", "TXT", `"v=spf1 " "a -all"`)
	set(rev.ID, "1.0.0.127.in-addr.arpa.", "PTR", "host.sender.test.")

	dns := sesInboundDNS{bk: bk}

	ips, err := dns.LookupIP(t.Context(), "sender.test")
	require.NoError(t, err)
	assert.ElementsMatch(t, []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("2001:db8::1")}, ips)

	mx, err := dns.LookupMX(t.Context(), "sender.test")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"mail.sender.test.", "mx2.sender.test."}, mx)

	ptr, err := dns.LookupAddr(t.Context(), netip.MustParseAddr("127.0.0.1"))
	require.NoError(t, err)
	assert.Equal(t, []string{"host.sender.test."}, ptr)

	txt, err := dns.LookupTXT(t.Context(), "sender.test")
	require.NoError(t, err)
	assert.Equal(t, []string{"v=spf1 a -all"}, txt)

	_, err = dns.LookupIP(t.Context(), "missing.sender.test")
	require.ErrorIs(t, err, sesbackend.ErrDNSNotFound)

	_, err = dns.LookupAddr(t.Context(), netip.MustParseAddr("127.0.0.9"))
	require.ErrorIs(t, err, sesbackend.ErrDNSNotFound)
}

func TestReverseName(t *testing.T) {
	t.Parallel()

	tests := []struct{ ip, want string }{
		{"192.0.2.10", "10.2.0.192.in-addr.arpa"},
		{"::ffff:192.0.2.10", "10.2.0.192.in-addr.arpa"},
		{"2001:db8::1", "1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.ip6.arpa"},
	}

	for _, tt := range tests {
		t.Run(tt.ip, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, reverseName(netip.MustParseAddr(tt.ip)))
		})
	}
}
