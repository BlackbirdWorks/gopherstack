package gcpdns_test

import (
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/gcpdns"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

func ecSpecs() []*dnsapi.DnsKeySpec {
	return []*dnsapi.DnsKeySpec{
		{Algorithm: "ecdsap256sha256", KeyType: "keySigning"},
		{Algorithm: "ecdsap256sha256", KeyType: "zoneSigning"},
	}
}

func signedEnv(t *testing.T, nonExistence string) (*env, map[string]*dns.DNSKEY) {
	t.Helper()

	e := newEnv(t)
	_, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name:        "sec",
		DnsName:     "sec.com.",
		Description: "d",
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{
			State:           "on",
			NonExistence:    nonExistence,
			DefaultKeySpecs: ecSpecs(),
		},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	e.addRRSet(t, "sec", rrset("www.sec.com.", "A", 60, "192.0.2.1"))
	e.addRRSet(t, "sec", rrset("*.wild.sec.com.", "A", 60, "192.0.2.9"))
	e.addRRSet(t, "sec", rrset("a.b.sec.com.", "A", 60, "192.0.2.3"))

	list, err := e.svc.DnsKeys.List(project, "sec").Context(t.Context()).Do()
	require.NoError(t, err)

	keys := map[string]*dns.DNSKEY{}

	for _, k := range list.DnsKeys {
		flags := uint16(256)
		if k.Type == "keySigning" {
			flags = 257
		}

		keys[k.Type] = &dns.DNSKEY{
			Hdr:   dns.RR_Header{Name: "sec.com.", Rrtype: dns.TypeDNSKEY, Class: dns.ClassINET},
			Flags: flags, Protocol: 3, Algorithm: dns.ECDSAP256SHA256, PublicKey: k.PublicKey,
		}
	}

	return e, keys
}

func ask(e *env, name string, qtype uint16, do bool) *dns.Msg {
	m := new(dns.Msg)
	m.SetQuestion(name, qtype)
	m.SetEdns0(4096, do)

	return e.backend.Answer(m, "tcp")
}

func sigsFor(rrs []dns.RR, typ uint16) ([]dns.RR, []*dns.RRSIG) {
	var (
		set  []dns.RR
		sigs []*dns.RRSIG
	)

	for _, rr := range rrs {
		if s, ok := rr.(*dns.RRSIG); ok {
			if s.TypeCovered == typ {
				sigs = append(sigs, s)
			}

			continue
		}

		if rr.Header().Rrtype == typ {
			set = append(set, rr)
		}
	}

	return set, sigs
}

func requireVerified(t *testing.T, rrs []dns.RR, typ uint16, key *dns.DNSKEY) {
	t.Helper()

	set, sigs := sigsFor(rrs, typ)
	require.NotEmpty(t, set, "no %s records", dns.TypeToString[typ])
	require.NotEmpty(t, sigs, "no RRSIG covering %s", dns.TypeToString[typ])

	for _, s := range sigs {
		assert.Equal(t, key.KeyTag(), s.KeyTag)
		assert.Equal(t, "sec.com.", s.SignerName)

		var owned []dns.RR

		for _, rr := range set {
			if rr.Header().Name == s.Hdr.Name {
				owned = append(owned, rr)
			}
		}

		require.NoError(t, s.Verify(key, owned))
	}
}

func TestDNSSECSigning(t *testing.T) {
	t.Parallel()

	for _, ne := range []string{"nsec", "nsec3"} {
		t.Run(ne, func(t *testing.T) {
			t.Parallel()

			e, keys := signedEnv(t, ne)
			zsk, ksk := keys["zoneSigning"], keys["keySigning"]

			t.Run("positive", func(t *testing.T) {
				t.Parallel()

				r := ask(e, "www.sec.com.", dns.TypeA, true)
				requireVerified(t, r.Answer, dns.TypeA, zsk)
			})

			t.Run("dnskey signed by ksk", func(t *testing.T) {
				t.Parallel()

				r := ask(e, "sec.com.", dns.TypeDNSKEY, true)
				requireVerified(t, r.Answer, dns.TypeDNSKEY, ksk)
			})

			t.Run("no do bit", func(t *testing.T) {
				t.Parallel()

				r := ask(e, "www.sec.com.", dns.TypeA, false)
				_, sigs := sigsFor(r.Answer, dns.TypeA)
				assert.Empty(t, sigs)
			})

			t.Run("nodata", func(t *testing.T) {
				t.Parallel()

				r := ask(e, "www.sec.com.", dns.TypeAAAA, true)
				assert.Equal(t, dns.RcodeSuccess, r.Rcode)
				requireVerified(t, r.Ns, dns.TypeSOA, zsk)

				if ne == "nsec" {
					requireVerified(t, r.Ns, dns.TypeNSEC, zsk)
					nsec := firstOf[*dns.NSEC](r.Ns)
					assert.Equal(t, "www.sec.com.", nsec.Hdr.Name)
					assert.Contains(t, nsec.TypeBitMap, dns.TypeA)
					assert.NotContains(t, nsec.TypeBitMap, dns.TypeAAAA)

					return
				}

				requireVerified(t, r.Ns, dns.TypeNSEC3, zsk)
				n3 := firstOf[*dns.NSEC3](r.Ns)
				assert.True(
					t,
					strings.HasPrefix(n3.Hdr.Name, strings.ToLower(dns.HashName("www.sec.com.", dns.SHA1, 0, ""))),
				)
				assert.Contains(t, n3.TypeBitMap, dns.TypeA)
			})

			t.Run("nxdomain", func(t *testing.T) {
				t.Parallel()

				r := ask(e, "nope.sec.com.", dns.TypeA, true)
				assert.Equal(t, dns.RcodeNameError, r.Rcode)
				requireVerified(t, r.Ns, dns.TypeSOA, zsk)

				typ := dns.TypeNSEC
				if ne == "nsec3" {
					typ = dns.TypeNSEC3
				}

				requireVerified(t, r.Ns, typ, zsk)
			})

			t.Run("wildcard", func(t *testing.T) {
				t.Parallel()

				r := ask(e, "x.wild.sec.com.", dns.TypeA, true)
				require.NotEmpty(t, r.Answer)
				assert.Equal(t, "x.wild.sec.com.", r.Answer[0].Header().Name)

				requireVerified(t, r.Answer, dns.TypeA, zsk)

				_, sigs := sigsFor(r.Answer, dns.TypeA)
				assert.Equal(t, uint8(3), sigs[0].Labels)
				assert.NotEmpty(t, filterNS(r.Ns), "wildcard answers carry a non-existence proof")
			})
		})
	}
}

func firstOf[T dns.RR](rrs []dns.RR) T {
	var zero T

	for _, rr := range rrs {
		if v, ok := rr.(T); ok {
			return v
		}
	}

	return zero
}

func filterNS(rrs []dns.RR) []dns.RR {
	var out []dns.RR

	for _, rr := range rrs {
		switch rr.(type) {
		case *dns.NSEC, *dns.NSEC3:
			out = append(out, rr)
		}
	}

	return out
}

func TestDNSSECSigningSurvivesRestore(t *testing.T) {
	t.Parallel()

	e, keys := signedEnv(t, "nsec3")
	dst := gcpdns.NewInMemoryBackend()
	require.NoError(t, dst.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	m := new(dns.Msg)
	m.SetQuestion("www.sec.com.", dns.TypeA)
	m.SetEdns0(4096, true)

	requireVerified(t, dst.Answer(m, "tcp").Answer, dns.TypeA, keys["zoneSigning"])
}

func TestDNSSECNSEC3Param(t *testing.T) {
	t.Parallel()

	e, keys := signedEnv(t, "nsec3")
	r := ask(e, "sec.com.", dns.TypeNSEC3PARAM, true)
	requireVerified(t, r.Answer, dns.TypeNSEC3PARAM, keys["zoneSigning"])

	p := firstOf[*dns.NSEC3PARAM](r.Answer)
	assert.Equal(t, dns.SHA1, p.Hash)
	assert.Zero(t, p.Iterations)

	e2, _ := signedEnv(t, "nsec")
	assert.Empty(t, ask(e2, "sec.com.", dns.TypeNSEC3PARAM, true).Answer)
}

func TestDNSSECWildcardNoData(t *testing.T) {
	t.Parallel()

	hashOf := func(name string) string { return strings.ToLower(dns.HashName(name, dns.SHA1, 0, "")) }

	tests := []struct {
		name string
		ne   string
		want []string
	}{
		{"nsec", "nsec", []string{"*.wild.sec.com."}},
		{"nsec3", "nsec3", []string{hashOf("wild.sec.com."), hashOf("*.wild.sec.com.")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, zsk := signedEnv(t, tt.ne)

			r := ask(e, "x.wild.sec.com.", dns.TypeAAAA, true)
			assert.Equal(t, dns.RcodeSuccess, r.Rcode)
			assert.Empty(t, r.Answer)

			var owners []string

			for _, rr := range filterNS(r.Ns) {
				owners = append(owners, rr.Header().Name)
			}

			for _, w := range tt.want {
				found := false

				for _, o := range owners {
					found = found || strings.HasPrefix(o, w)
				}

				assert.True(t, found, "no proof record owned by %s in %v", w, owners)
			}

			typ := dns.TypeNSEC
			if tt.ne == "nsec3" {
				typ = dns.TypeNSEC3
			}

			requireVerified(t, r.Ns, typ, zsk["zoneSigning"])
		})
	}
}

func TestDNSSECSignaturesAreCached(t *testing.T) {
	t.Parallel()

	e, _ := signedEnv(t, "nsec3")

	_, first := sigsFor(ask(e, "www.sec.com.", dns.TypeA, true).Answer, dns.TypeA)
	_, again := sigsFor(ask(e, "www.sec.com.", dns.TypeA, true).Answer, dns.TypeA)

	require.Len(t, first, 1)
	require.Len(t, again, 1)
	assert.Equal(t, first[0].Signature, again[0].Signature, "ECDSA is randomised, so equal bytes prove a cache hit")
}
