package azurearm

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

func testPFX(t *testing.T, key any, pub any, password string) map[string]any {
	t.Helper()

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "app"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	raw, err := pkcs12.Modern2023.Encode(key, cert, nil, password)
	require.NoError(t, err)

	return map[string]any{"pfx": base64.StdEncoding.EncodeToString(raw), "password": password}
}

func TestLogicClientAssertionClaims(t *testing.T) {
	t.Parallel()

	rk, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pfx, err := loadLogicPFX(testPFX(t, rk, &rk.PublicKey, "pw"))
	require.NoError(t, err)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := map[string]bool{}

	for range 3 {
		raw, aerr := pfx.clientAssertion("cid", "https://login.example/t/oauth2/token", now)
		require.NoError(t, aerr)

		claims := jwt.MapClaims{}
		_, perr := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}), jwt.WithoutClaimsValidation()).
			ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return &rk.PublicKey, nil })
		require.NoError(t, perr)

		jti, _ := claims["jti"].(string)
		assert.NotEmpty(t, jti)
		assert.False(t, seen[jti], "jti is unique per assertion")

		seen[jti] = true

		assert.Equal(t, "cid", claims["iss"])
		assert.Equal(t, "cid", claims["sub"])
		assert.Equal(t, "https://login.example/t/oauth2/token", claims["aud"])
		assert.InDelta(t, float64(now.Unix()), claims["nbf"], 0)
		assert.InDelta(t, float64(now.Add(logicAssertionTTL).Unix()), claims["exp"], 0)
	}
}

func TestLogicLoadPFX(t *testing.T) {
	t.Parallel()

	ek, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	good := testPFX(t, ek, &ek.PublicKey, "pw")

	tests := []struct {
		auth        map[string]any
		name        string
		wantLoadErr bool
	}{
		{name: "ecdsa key loads for tls", auth: good},
		{name: "wrong password", auth: map[string]any{"pfx": good["pfx"], "password": "nope"}, wantLoadErr: true},
		{name: "not base64", auth: map[string]any{"pfx": "!!!", "password": "pw"}, wantLoadErr: true},
		{name: "empty", auth: map[string]any{}, wantLoadErr: true},
		{name: "not a pfx", auth: map[string]any{"pfx": "AAAA", "password": "pw"}, wantLoadErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pfx, lerr := loadLogicPFX(tc.auth)
			if tc.wantLoadErr {
				require.ErrorIs(t, lerr, errLogicClientCert)
				assert.NotContains(t, lerr.Error(), "pw")

				return
			}

			require.NoError(t, lerr)

			_, aerr := pfx.clientAssertion("cid", "aud", time.Now())
			require.ErrorIs(t, aerr, errLogicClientCert, "an ECDSA key cannot sign the RS256 assertion")
		})
	}
}
