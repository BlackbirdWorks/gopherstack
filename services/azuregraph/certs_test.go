package azuregraph_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"maps"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

type testCert struct {
	key   *rsa.PrivateKey
	b64   string
	thumb string
}

func newTestCert(t *testing.T, notBefore, notAfter time.Time) testCert {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "gopherstack-test"},
		NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	sum := sha1.Sum(der)

	return testCert{
		key:   key,
		b64:   base64.StdEncoding.EncodeToString(der),
		thumb: base64.RawURLEncoding.EncodeToString(sum[:]),
	}
}

// clockCert is valid around the test clock (2026-01-02).
func clockCert(t *testing.T) testCert {
	t.Helper()

	return newTestCert(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
}

func (c testCert) sign(t *testing.T, claims jwt.MapClaims, header map[string]any) string {
	t.Helper()

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	maps.Copy(tok.Header, header)

	s, err := tok.SignedString(c.key)
	require.NoError(t, err)

	return s
}

func (c testCert) keyBody() map[string]any {
	return map[string]any{"type": "AsymmetricX509Cert", "usage": "Verify", "key": c.b64, "displayName": "cert"}
}

func proofClaims(now time.Time, iss string) jwt.MapClaims {
	return jwt.MapClaims{
		"aud": "00000002-0000-0000-c000-000000000000", "iss": iss, "nbf": now.Unix(),
		"exp": now.Add(10 * time.Minute).Unix(),
	}
}

func TestKeyCredentialsRetainCertificate(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Keys")
	base := "/v1.0/applications/" + str(app, "id")
	cert := clockCert(t)

	add := e.req(t, http.MethodPost, base+"/addKey", map[string]any{"keyCredential": cert.keyBody()})
	require.Equal(t, http.StatusOK, add.status, add.raw)
	assert.Nil(t, add.body["key"], "key material is not returned")
	assert.Equal(t, "2026-01-01T00:00:00Z", str(add.body, "startDateTime"))
	assert.Equal(t, "2027-01-01T00:00:00Z", str(add.body, "endDateTime"))
	assert.NotEmpty(t, str(add.body, "customKeyIdentifier"))

	for name, body := range map[string]any{
		"bad type": map[string]any{"keyCredential": map[string]any{"type": "x", "usage": "Verify", "key": cert.b64}},
		"bad usage": map[string]any{"keyCredential": map[string]any{
			"type": "AsymmetricX509Cert", "usage": "x", "key": cert.b64,
		}},
		"bad base64": map[string]any{"keyCredential": map[string]any{
			"type": "AsymmetricX509Cert", "usage": "Verify", "key": "%%",
		}},
		"not a certificate": map[string]any{"keyCredential": map[string]any{
			"type": "AsymmetricX509Cert", "usage": "Verify", "key": "AAEC",
		}},
		"no key": map[string]any{"keyCredential": map[string]any{"type": "AsymmetricX509Cert", "usage": "Verify"}},
	} {
		assertErr(t, e.req(t, http.MethodPost, base+"/addKey", body), http.StatusBadRequest, "Request_BadRequest")
		_ = name
	}

	require.Equal(t, http.StatusNoContent,
		e.req(t, http.MethodPost, base+"/removeKey", map[string]any{"keyId": str(add.body, "keyId")}).status)
	assertErr(t, e.req(t, http.MethodPost, base+"/removeKey", map[string]any{"keyId": str(add.body, "keyId")}),
		http.StatusBadRequest, "Request_BadRequest")
}

func TestAddKeyProofOfPossession(t *testing.T) {
	t.Parallel()

	first, second, stranger := clockCert(t), clockCert(t), clockCert(t)

	tests := []struct {
		proof func(now time.Time, appID string) string
		name  string
		ok    bool
	}{
		{name: "valid proof", proof: func(now time.Time, app string) string {
			return first.sign(t, proofClaims(now, app), nil)
		}, ok: true},
		{name: "missing proof", proof: func(time.Time, string) string { return "" }},
		{name: "signed by an unknown key", proof: func(now time.Time, app string) string {
			return stranger.sign(t, proofClaims(now, app), nil)
		}},
		{name: "wrong audience", proof: func(now time.Time, app string) string {
			c := proofClaims(now, app)
			c["aud"] = "https://graph.microsoft.com"

			return first.sign(t, c, nil)
		}},
		{name: "wrong issuer", proof: func(now time.Time, _ string) string {
			return first.sign(t, proofClaims(now, newID0()), nil)
		}},
		{name: "expired", proof: func(now time.Time, app string) string {
			return first.sign(t, proofClaims(now.Add(-time.Hour), app), nil)
		}},
		{name: "lifetime over ten minutes", proof: func(now time.Time, app string) string {
			c := proofClaims(now, app)
			c["exp"] = now.Add(time.Hour).Unix()

			return first.sign(t, c, nil)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			app := newApp(t, e, "Proof")
			base := "/v1.0/applications/" + str(app, "id")
			require.Equal(t, http.StatusOK,
				e.req(t, http.MethodPost, base+"/addKey", map[string]any{"keyCredential": first.keyBody()}).status)

			r := e.req(t, http.MethodPost, base+"/addKey", map[string]any{
				"keyCredential": second.keyBody(), "proof": tt.proof(e.clock.now(), str(app, "appId")),
			})

			if tt.ok {
				require.Equal(t, http.StatusOK, r.status, r.raw)

				return
			}

			assertErr(t, r, http.StatusBadRequest, "Request_BadRequest")
		})
	}
}

func assertionEnv(t *testing.T) (*env, map[string]any, map[string]any) {
	t.Helper()

	e := newEnv(t)
	app := newApp(t, e, "Assert")
	sp := e.mustCreate(t, "/v1.0/servicePrincipals", map[string]any{"appId": str(app, "appId")})

	return e, app, sp
}

func certAssertionClaims(now time.Time, appID string) jwt.MapClaims {
	return jwt.MapClaims{
		"aud": "https://login.test/t/oauth2/v2.0/token", "iss": appID, "sub": appID, "jti": newID0(),
		"nbf": now.Unix(), "exp": now.Add(10 * time.Minute).Unix(),
	}
}

func TestAuthenticateAssertionCertificate(t *testing.T) {
	t.Parallel()

	good, other := clockCert(t), clockCert(t)
	audiences := []string{"https://login.test/t/oauth2/v2.0/token"}

	tests := []struct {
		assertion func(now time.Time, appID string) string
		name      string
		advance   time.Duration
		code      int
	}{
		{name: "valid", assertion: func(now time.Time, app string) string {
			return good.sign(t, certAssertionClaims(now, app), map[string]any{"x5t": good.thumb})
		}},
		{name: "valid without thumbprint header", assertion: func(now time.Time, app string) string {
			return good.sign(t, certAssertionClaims(now, app), nil)
		}},
		{name: "unregistered certificate", assertion: func(now time.Time, app string) string {
			return other.sign(t, certAssertionClaims(now, app), nil)
		}, code: 700027},
		{name: "thumbprint of another certificate", assertion: func(now time.Time, app string) string {
			return good.sign(t, certAssertionClaims(now, app), map[string]any{"x5t": other.thumb})
		}, code: 700027},
		{name: "wrong audience", assertion: func(now time.Time, app string) string {
			c := certAssertionClaims(now, app)
			c["aud"] = "https://elsewhere.test/"

			return good.sign(t, c, nil)
		}, code: 700023},
		{name: "expired assertion", assertion: func(now time.Time, app string) string {
			return good.sign(t, certAssertionClaims(now, app), nil)
		}, advance: time.Hour, code: 700024},
		{name: "malformed", assertion: func(time.Time, string) string { return "not.a.jwt" }, code: 50027},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, app, sp := assertionEnv(t)
			require.Equal(t, http.StatusOK, e.req(t, http.MethodPost,
				"/v1.0/applications/"+str(app, "id")+"/addKey", map[string]any{"keyCredential": good.keyBody()}).status)

			assertion := tt.assertion(e.clock.now(), str(app, "appId"))
			e.clock.advance(tt.advance)

			oid, known, err := e.h.Backend.AuthenticateAssertion(str(app, "appId"), assertion, audiences)
			assert.True(t, known)

			if tt.code == 0 {
				require.NoError(t, err)
				assert.Equal(t, str(sp, "id"), oid)

				return
			}

			var ce *azuregraph.ClientAuthError

			require.ErrorAs(t, err, &ce)
			assert.Equal(t, tt.code, ce.Code)
		})
	}

	t.Run("unknown client", func(t *testing.T) {
		t.Parallel()

		e, _, _ := assertionEnv(t)
		_, known, err := e.h.Backend.AuthenticateAssertion(newID0(), "x.y.z", audiences)
		require.NoError(t, err)
		assert.False(t, known)
	})

	t.Run("sign-only credential is not used for client authentication", func(t *testing.T) {
		t.Parallel()

		e, app, _ := assertionEnv(t)
		body := good.keyBody()
		body["usage"] = "Sign"
		require.Equal(t, http.StatusOK, e.req(t, http.MethodPost,
			"/v1.0/applications/"+str(app, "id")+"/addKey", map[string]any{"keyCredential": body}).status)

		_, _, err := e.h.Backend.AuthenticateAssertion(str(app, "appId"),
			good.sign(t, certAssertionClaims(e.clock.now(), str(app, "appId")), nil), audiences)

		var ce *azuregraph.ClientAuthError

		require.ErrorAs(t, err, &ce)
		assert.Equal(t, 700027, ce.Code)
	})

	t.Run("removed key stops authenticating", func(t *testing.T) {
		t.Parallel()

		e, app, _ := assertionEnv(t)
		base := "/v1.0/applications/" + str(app, "id")
		add := e.req(t, http.MethodPost, base+"/addKey", map[string]any{"keyCredential": good.keyBody()})
		require.Equal(t, http.StatusOK, add.status)
		require.Equal(t, http.StatusNoContent,
			e.req(t, http.MethodPost, base+"/removeKey", map[string]any{"keyId": str(add.body, "keyId")}).status)

		_, _, err := e.h.Backend.AuthenticateAssertion(str(app, "appId"),
			good.sign(t, certAssertionClaims(e.clock.now(), str(app, "appId")), nil), audiences)

		var ce *azuregraph.ClientAuthError

		require.ErrorAs(t, err, &ce)
	})
}

func TestKeyMaterialPersists(t *testing.T) {
	t.Parallel()

	e, app, sp := assertionEnv(t)
	cert := clockCert(t)
	require.Equal(t, http.StatusOK, e.req(t, http.MethodPost,
		"/v1.0/applications/"+str(app, "id")+"/addKey", map[string]any{"keyCredential": cert.keyBody()}).status)

	fresh := newEnv(t)
	require.NoError(t, fresh.h.Restore(t.Context(), e.h.Snapshot(t.Context())))

	oid, _, err := fresh.h.Backend.AuthenticateAssertion(str(app, "appId"),
		cert.sign(t, certAssertionClaims(fresh.clock.now(), str(app, "appId")), nil),
		[]string{"https://login.test/t/oauth2/v2.0/token"})
	require.NoError(t, err)
	assert.Equal(t, str(sp, "id"), oid)
}
