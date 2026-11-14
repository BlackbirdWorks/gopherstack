package dns_test

import (
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
)

func TestTSIGProviderRoundTrip(t *testing.T) {
	t.Parallel()

	const secret = "c2VjcmV0LWJ5dGVzLTEyMzQ1Njc4OTA="

	tests := []struct {
		wantSign  error
		wantVerif error
		name      string
		algorithm string
		verifyAs  string
		verifySec string
	}{
		{name: "sha224", algorithm: "hmac-sha224", verifyAs: "hmac-sha224", verifySec: secret},
		{name: "sha256", algorithm: "hmac-sha256", verifyAs: "hmac-sha256", verifySec: secret},
		{name: "sha384", algorithm: "hmac-sha384", verifyAs: "hmac-sha384", verifySec: secret},
		{name: "sha512", algorithm: "hmac-sha512", verifyAs: "hmac-sha512", verifySec: secret},
		{
			name:      "wrongsecret",
			algorithm: "hmac-sha256",
			verifyAs:  "hmac-sha256",
			verifySec: "b3RoZXI=",
			wantVerif: dns.ErrSig,
		},
		{name: "sha1", algorithm: "hmac-sha1", wantSign: gopherDNS.ErrWeakTSIGAlgorithm},
		{name: "unknown", algorithm: "hmac-nope", wantSign: gopherDNS.ErrUnknownTSIGAlgorithm},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			signer := gopherDNS.SingleKeyProvider(
				gopherDNS.TSIGKey{Name: "k.example", Algorithm: tt.algorithm, Secret: secret},
			)

			m := new(dns.Msg)
			m.SetQuestion("example.", dns.TypeSOA)
			m.SetTsig("k.example.", gopherDNS.CanonicalTSIGAlgorithm(tt.algorithm), 300, time.Now().Unix())

			wire, _, err := dns.TsigGenerateWithProvider(m, signer, "", false)
			if tt.wantSign != nil {
				require.ErrorIs(t, err, tt.wantSign)
				require.ErrorIs(t, gopherDNS.CheckTSIGAlgorithm(tt.algorithm), tt.wantSign)

				return
			}

			require.NoError(t, err)

			verifier := gopherDNS.SingleKeyProvider(
				gopherDNS.TSIGKey{Name: "k.example", Algorithm: tt.verifyAs, Secret: tt.verifySec},
			)
			err = dns.TsigVerifyWithProvider(wire, verifier, "", false)

			if tt.wantVerif != nil {
				require.ErrorIs(t, err, tt.wantVerif)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestTSIGProviderUnknownKey(t *testing.T) {
	t.Parallel()

	p := gopherDNS.NewTSIGProvider(func(string) (gopherDNS.TSIGKey, bool) { return gopherDNS.TSIGKey{}, false })

	m := new(dns.Msg)
	m.SetQuestion("example.", dns.TypeSOA)
	m.SetTsig("nokey.", dns.HmacSHA256, 300, time.Now().Unix())

	_, _, err := dns.TsigGenerateWithProvider(m, p, "", false)
	require.ErrorIs(t, err, dns.ErrSecret)
}
