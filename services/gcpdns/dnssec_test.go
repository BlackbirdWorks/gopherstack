package gcpdns_test

import (
	"encoding/base64"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

func TestDNSSECKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantAlg string
		specs   []*dnsapi.DnsKeySpec
		wantLen int64
	}{
		{"defaults", "rsasha256", nil, 2048},
		{"ecdsa p256", "ecdsap256sha256", []*dnsapi.DnsKeySpec{
			{Algorithm: "ecdsap256sha256", KeyType: "keySigning"},
			{Algorithm: "ecdsap256sha256", KeyType: "zoneSigning"},
		}, 256},
		{"ecdsa p384", "ecdsap384sha384", []*dnsapi.DnsKeySpec{
			{Algorithm: "ecdsap384sha384", KeyType: "keySigning"},
			{Algorithm: "ecdsap384sha384", KeyType: "zoneSigning"},
		}, 384},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			z, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
				Name: "sec", DnsName: "sec.com.", Description: "d",
				DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "on", DefaultKeySpecs: tt.specs},
			}).Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, "on", z.DnssecConfig.State)
			assert.Equal(t, "nsec3", z.DnssecConfig.NonExistence)
			assert.Len(t, z.DnssecConfig.DefaultKeySpecs, 2)

			keys, err := e.svc.DnsKeys.List(project, "sec").Context(t.Context()).Do()
			require.NoError(t, err)
			require.Len(t, keys.DnsKeys, 2)

			ksk, zsk := keys.DnsKeys[0], keys.DnsKeys[1]
			assert.Equal(t, "keySigning", ksk.Type)
			assert.Equal(t, "zoneSigning", zsk.Type)
			assert.Equal(t, "0", ksk.Id)
			assert.Equal(t, "dns#dnsKey", ksk.Kind)
			assert.Equal(t, tt.wantAlg, ksk.Algorithm)
			assert.Equal(t, tt.wantLen, ksk.KeyLength)
			assert.True(t, ksk.IsActive)
			assert.NotEqual(t, ksk.PublicKey, zsk.PublicKey)

			raw, err := base64.StdEncoding.DecodeString(ksk.PublicKey)
			require.NoError(t, err)
			assert.NotEmpty(t, raw)

			require.Len(t, ksk.Digests, 1, "KSKs carry the sha256 digest by default")
			assert.Equal(t, "sha256", ksk.Digests[0].Type)
			assert.Len(t, ksk.Digests[0].Digest, 64)
			assert.Empty(t, zsk.Digests)

			dk := &dns.DNSKEY{
				Hdr:   dns.RR_Header{Name: "sec.com.", Rrtype: dns.TypeDNSKEY, Class: dns.ClassINET},
				Flags: 257, Protocol: 3, Algorithm: map[string]uint8{
					"rsasha256": dns.RSASHA256, "ecdsap256sha256": dns.ECDSAP256SHA256, "ecdsap384sha384": dns.ECDSAP384SHA384,
				}[ksk.Algorithm], PublicKey: ksk.PublicKey,
			}
			assert.Equal(t, int64(dk.KeyTag()), ksk.KeyTag, "key tag is derived from the DNSKEY record")
			assert.Equal(t, dk.ToDS(dns.SHA256).Digest, ksk.Digests[0].Digest)
		})
	}
}

func TestDNSSECKeyGetDigests(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "sec", DnsName: "sec.com.", Description: "d",
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "on"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	k, err := e.svc.DnsKeys.Get(project, "sec", "0").DigestType("sha1,sha384").Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, k.Digests, 2)
	assert.Len(t, k.Digests[0].Digest, 40)
	assert.Len(t, k.Digests[1].Digest, 96)

	_, err = e.svc.DnsKeys.Get(project, "sec", "0").DigestType("md5").Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	_, err = e.svc.DnsKeys.Get(project, "sec", "9").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	_, err = e.svc.DnsKeys.List("p", "nozone").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")
}

func TestDNSSECStateTransitions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "sec", "sec.com.")

	keys, err := e.svc.DnsKeys.List(project, "sec").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, keys.DnsKeys, "no keys while DNSSEC is off")

	_, err = e.svc.ManagedZones.Patch(project, "sec", &dnsapi.ManagedZone{
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "on"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	keys, err = e.svc.DnsKeys.List(project, "sec").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Len(t, keys.DnsKeys, 2)

	_, err = e.svc.ManagedZones.Patch(project, "sec", &dnsapi.ManagedZone{
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{NonExistence: "nsec"},
	}).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	_, err = e.svc.ManagedZones.Patch(project, "sec", &dnsapi.ManagedZone{
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "off"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	keys, err = e.svc.DnsKeys.List(project, "sec").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, keys.DnsKeys)

	_, err = e.svc.ManagedZones.Patch(project, "sec", &dnsapi.ManagedZone{
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "bogus"},
	}).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")
}
