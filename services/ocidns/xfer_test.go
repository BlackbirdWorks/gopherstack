package ocidns_test

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/miekg/dns"
	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	xferSecret      = "c2VjcmV0LWJ5dGVzLTEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
	xferOtherSecret = "b3RoZXItc2VjcmV0LWJ5dGVzLTEyMzQ1Njc4OTAxMjM0NTY="
	xferKeyName     = "transfer-key.example"
)

func (e *env) tsigKey(t *testing.T, name, algo, secret string) *string {
	t.Helper()

	resp, err := e.client.CreateTsigKey(t.Context(), ocidnssdk.CreateTsigKeyRequest{
		Name: new(name), Algorithm: new(algo), Secret: new(secret), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)

	return resp.Id
}

func dnsPortOf(e *env) *int { return new(e.handler.DNSPort) }

// pair is a primary and a secondary emulator wired to each other over loopback.
type pair struct {
	primary   *env
	secondary *env
	zoneID    string
}

type pairOpts struct {
	algo          string
	primaryKey    string
	secondaryKey  string
	downstream    string
	primaryWants  bool
	secondaryUses bool
	notifyPort    bool
}

func newPair(t *testing.T, o pairOpts) *pair {
	t.Helper()

	p := &pair{primary: newEnv(t, envOpts{}), secondary: newEnv(t, envOpts{})}

	var downKey, masterKey *string

	if o.primaryWants {
		downKey = p.primary.tsigKey(t, xferKeyName, o.algo, o.primaryKey)
	}

	if o.secondaryUses {
		masterKey = p.secondary.tsigKey(t, xferKeyName, o.algo, o.secondaryKey)
	}

	downstream := o.downstream
	if downstream == "" {
		downstream = "127.0.0.1"
	}

	p.primary.createZone(t, "xfer.example", func(d *ocidnssdk.CreateZoneDetails) {
		ds := ocidnssdk.ExternalDownstream{Address: new(downstream), TsigKeyId: downKey}
		if o.notifyPort {
			ds.Port = dnsPortOf(p.secondary)
		}

		d.ExternalDownstreams = []ocidnssdk.ExternalDownstream{ds}
	})
	p.primary.mustPatch(t, "xfer.example",
		add("www.xfer.example", "A", "192.0.2.1", 300), add("www.xfer.example", "A", "192.0.2.2", 300),
		add("xfer.example", "MX", "10 mail.xfer.example.", 600), add("mail.xfer.example", "A", "192.0.2.25", 600),
		add("xfer.example", "TXT", `"v=spf1 -all"`, 120), add("alias.xfer.example", "CNAME", "www.xfer.example.", 60),
		add("*.wild.xfer.example", "A", "192.0.2.99", 30),
	)

	zone := p.secondary.createZone(t, "xfer.example", func(d *ocidnssdk.CreateZoneDetails) {
		d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
		d.ExternalMasters = []ocidnssdk.ExternalMaster{
			{Address: new("127.0.0.1"), Port: dnsPortOf(p.primary), TsigKeyId: masterKey},
		}
	})
	p.zoneID = *zone.Id

	return p
}

func TestSecondaryZoneTransfer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		opts    pairOpts
	}{
		{name: "sha256", opts: pairOpts{
			algo:          "hmac-sha256",
			primaryKey:    xferSecret,
			secondaryKey:  xferSecret,
			primaryWants:  true,
			secondaryUses: true,
		}},
		{name: "sha224", opts: pairOpts{
			algo:          "hmac-sha224",
			primaryKey:    xferSecret,
			secondaryKey:  xferSecret,
			primaryWants:  true,
			secondaryUses: true,
		}},
		{name: "sha384", opts: pairOpts{
			algo:          "hmac-sha384",
			primaryKey:    xferSecret,
			secondaryKey:  xferSecret,
			primaryWants:  true,
			secondaryUses: true,
		}},
		{name: "sha512", opts: pairOpts{
			algo:          "hmac-sha512",
			primaryKey:    xferSecret,
			secondaryKey:  xferSecret,
			primaryWants:  true,
			secondaryUses: true,
		}},
		{name: "unsigned", opts: pairOpts{}},
		{
			name: "wrongsecret",
			opts: pairOpts{
				algo: "hmac-sha256", primaryKey: xferSecret, secondaryKey: xferOtherSecret,
				primaryWants: true, secondaryUses: true,
			},
			wantErr: "no signature found",
		},
		{
			name:    "unsignedrequired",
			opts:    pairOpts{algo: "hmac-sha256", primaryKey: xferSecret, primaryWants: true},
			wantErr: "bad xfr rcode: 9",
		},
		{
			name:    "notdownstream",
			opts:    pairOpts{downstream: "192.0.2.10"},
			wantErr: "bad xfr rcode: 5",
		},
		{
			name: "weakalgorithm",
			opts: pairOpts{
				algo:          "hmac-sha1",
				primaryKey:    xferSecret,
				secondaryKey:  xferSecret,
				primaryWants:  true,
				secondaryUses: true,
			},
			wantErr: "TSIG algorithm is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newPair(t, tt.opts)
			err := p.secondary.backend.Pull(t.Context(), p.zoneID)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Empty(t, userRecords(p.secondary.records(t, "xfer.example")))

				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, userRecords(p.primary.records(t, "xfer.example")),
				userRecords(p.secondary.records(t, "xfer.example")))

			pz, sz := p.primary.getZone(t, "xfer.example"), p.secondary.getZone(t, "xfer.example")
			assert.Equal(t, *pz.Serial, *sz.Serial)

			r := p.secondary.query(t, "udp", "www.xfer.example.", dns.TypeA)
			assert.True(t, r.Authoritative)
			assert.ElementsMatch(t,
				[]string{"www.xfer.example.\t300\tIN\tA\t192.0.2.1", "www.xfer.example.\t300\tIN\tA\t192.0.2.2"},
				answers(r))

			soa := p.secondary.query(t, "tcp", "xfer.example.", dns.TypeSOA)
			require.Len(t, soa.Answer, 1)
			assert.Equal(t, uint32(*pz.Serial), soa.Answer[0].(*dns.SOA).Serial)
		})
	}
}

func TestSecondaryZoneFollowsPrimary(t *testing.T) {
	t.Parallel()

	p := newPair(t, pairOpts{
		algo: "hmac-sha256", primaryKey: xferSecret, secondaryKey: xferSecret,
		primaryWants: true, secondaryUses: true, notifyPort: true,
	})

	require.NoError(t, p.secondary.backend.Pull(t.Context(), p.zoneID))

	p.primary.mustPatch(t, "xfer.example", add("new.xfer.example", "A", "192.0.2.77", 60),
		remove("www.xfer.example", "A", "192.0.2.2"))

	require.Eventually(t, func() bool {
		return len(answers(p.secondary.query(t, "udp", "new.xfer.example.", dns.TypeA))) == 1
	}, 10*time.Second, 20*time.Millisecond, "NOTIFY makes the secondary transfer")

	assert.Len(t, answers(p.secondary.query(t, "udp", "www.xfer.example.", dns.TypeA)), 1)
	assert.Equal(t, *p.primary.getZone(t, "xfer.example").Serial, *p.secondary.getZone(t, "xfer.example").Serial)
}

func TestSecondaryZoneTransfersOnCreate(t *testing.T) {
	t.Parallel()

	p := newPair(t, pairOpts{})

	require.Eventually(t, func() bool {
		return len(answers(p.secondary.query(t, "tcp", "www.xfer.example.", dns.TypeA))) == 2
	}, 10*time.Second, 20*time.Millisecond, "creating a SECONDARY zone starts a transfer")
}

func TestNotifyAcceptance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		masterAddr string
		signKey    string
		secret     string
		keyOnZone  bool
		wantRcode  int
	}{
		{name: "master", masterAddr: "127.0.0.1", wantRcode: dns.RcodeSuccess},
		{name: "stranger", masterAddr: "192.0.2.10", wantRcode: dns.RcodeNotAuth},
		{
			name: "signed", masterAddr: "127.0.0.1", signKey: xferKeyName, secret: xferSecret, keyOnZone: true,
			wantRcode: dns.RcodeSuccess,
		},
		{
			name:       "badsignature",
			masterAddr: "127.0.0.1",
			signKey:    xferKeyName,
			secret:     xferOtherSecret,
			keyOnZone:  true,
			wantRcode:  dns.RcodeNotAuth,
		},
		{name: "unsignedrequired", masterAddr: "127.0.0.1", keyOnZone: true, wantRcode: dns.RcodeNotAuth},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})

			var keyID *string
			if tt.keyOnZone {
				keyID = e.tsigKey(t, xferKeyName, "hmac-sha256", xferSecret)
			}

			e.createZone(t, "n.example", func(d *ocidnssdk.CreateZoneDetails) {
				d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
				d.ExternalMasters = []ocidnssdk.ExternalMaster{
					{Address: new(tt.masterAddr), Port: new(1), TsigKeyId: keyID},
				}
			})

			m := new(dns.Msg)
			m.SetNotify("n.example.")

			c := &dns.Client{Net: "udp"}

			if tt.signKey != "" {
				c.TsigSecret = map[string]string{tt.signKey + ".": tt.secret}
				m.SetTsig(tt.signKey+".", dns.HmacSHA256, 300, time.Now().Unix())
			}

			addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(e.handler.DNSPort))
			r, _, err := c.ExchangeContext(t.Context(), m, addr)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRcode, r.Rcode)
			assert.Equal(t, dns.OpcodeNotify, r.Opcode)
		})
	}
}

func TestZoneTransferWire(t *testing.T) {
	t.Parallel()

	p := newPair(t, pairOpts{})
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(p.primary.handler.DNSPort))
	serial := uint32(*p.primary.getZone(t, "xfer.example").Serial)

	axfr := func(t *testing.T, qtype uint16, have uint32) []dns.RR {
		t.Helper()

		m := new(dns.Msg)
		m.SetQuestion("xfer.example.", qtype)

		if qtype == dns.TypeIXFR {
			m.Ns = []dns.RR{&dns.SOA{
				Hdr: dns.RR_Header{Name: "xfer.example.", Rrtype: dns.TypeSOA, Class: dns.ClassINET},
				Ns:  ".", Mbox: ".", Serial: have,
			}}
		}

		envs, err := (&dns.Transfer{}).In(m, addr)
		require.NoError(t, err)

		var out []dns.RR

		for env := range envs {
			require.NoError(t, env.Error)

			out = append(out, env.RR...)
		}

		return out
	}

	tests := []struct {
		name     string
		qtype    uint16
		have     uint32
		wantSOAs int
		wantMin  int
	}{
		{name: "axfr", qtype: dns.TypeAXFR, wantSOAs: 2, wantMin: 10},
		{name: "ixfrcurrent", qtype: dns.TypeIXFR, have: serial, wantSOAs: 1, wantMin: 1},
		{name: "ixfrbehind", qtype: dns.TypeIXFR, have: serial - 1, wantSOAs: 2, wantMin: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rrs := axfr(t, tt.qtype, tt.have)
			assert.GreaterOrEqual(t, len(rrs), tt.wantMin)

			soas := 0

			for _, rr := range rrs {
				if rr.Header().Rrtype == dns.TypeSOA {
					soas++
				}
			}

			assert.Equal(t, tt.wantSOAs, soas)
		})
	}
}

func TestSecondaryZoneSurvivesRestore(t *testing.T) {
	t.Parallel()

	p := newPair(t, pairOpts{})
	require.NoError(t, p.secondary.backend.Pull(t.Context(), p.zoneID))

	snap := p.secondary.handler.Snapshot(t.Context())
	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	assert.Equal(t, *p.primary.getZone(t, "xfer.example").Serial, *dst.getZone(t, "xfer.example").Serial)
	assert.ElementsMatch(
		t,
		userRecords(p.primary.records(t, "xfer.example")),
		userRecords(dst.records(t, "xfer.example")),
	)
	assert.Len(t, dst.query(t, "udp", "www.xfer.example.", dns.TypeA).Answer, 2)

	soa := dst.query(t, "udp", "xfer.example.", dns.TypeSOA)
	require.Len(t, soa.Answer, 1)
	assert.Equal(t, uint32(*p.primary.getZone(t, "xfer.example").Serial), soa.Answer[0].(*dns.SOA).Serial)
}
