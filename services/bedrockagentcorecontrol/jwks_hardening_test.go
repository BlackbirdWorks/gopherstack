package bedrockagentcorecontrol_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/v2"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type switchIDP struct {
	*httptest.Server
	key       *rsa.PrivateKey
	down      atomic.Bool
	noIssuer  bool
	discovery atomic.Int32 // requests to the discovery document
}

func newSwitchIDP(t *testing.T, noIssuer, useTLS bool) *switchIDP {
	t.Helper()

	return newSwitchIDPKey(t, noIssuer, useTLS, 2048)
}

func newSwitchIDPKey(t *testing.T, noIssuer, useTLS bool, bits int) *switchIDP {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, bits)
	require.NoError(t, err)

	idp := &switchIDP{key: key, noIssuer: noIssuer}
	idp.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			idp.discovery.Add(1)
		}

		if idp.down.Load() {
			http.Error(w, "down", http.StatusInternalServerError)

			return
		}

		var body any

		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			doc := map[string]string{"jwks_uri": idp.URL + "/jwks"}
			if !idp.noIssuer {
				doc["issuer"] = idp.URL
			}

			body = doc
		case "/jwks":
			body = map[string]any{"keys": []map[string]string{{
				"kty": "RSA", "kid": testKID,
				"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			}}}
		default:
			http.NotFound(w, r)

			return
		}

		raw, _ := json.Marshal(body, jsonx.V1())
		_, _ = w.Write(raw)
	}))

	if useTLS {
		idp.StartTLS()
	} else {
		idp.Start()
	}

	t.Cleanup(idp.Close)

	return idp
}

func (i *switchIDP) sign(t *testing.T, kid string, claims jwt.MapClaims) string {
	t.Helper()

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid

	s, err := tok.SignedString(i.key)
	require.NoError(t, err)

	return s
}

func securedRuntimeWorkload(t *testing.T, m *mcpFixture, discovery string) string {
	t.Helper()

	return securedNamedRuntime(t, m, "secured", discovery, "aud")
}

func securedNamedRuntime(t *testing.T, m *mcpFixture, name, discovery, audience string) string {
	t.Helper()

	m.backend.SetReferences(fakeInvoker{repos: map[string]bool{"agent": true}, calls: m.calls})

	out, err := m.client.CreateAgentRuntime(t.Context(), &sdk.CreateAgentRuntimeInput{
		AgentRuntimeName:     aws.String(name),
		AgentRuntimeArtifact: containerArtifact(testImage),
		RoleArn:              aws.String(testRole),
		NetworkConfiguration: &types.NetworkConfiguration{NetworkMode: types.NetworkModePublic},
		AuthorizerConfiguration: &types.AuthorizerConfigurationMemberCustomJWTAuthorizer{
			Value: types.CustomJWTAuthorizerConfiguration{
				DiscoveryUrl:    aws.String(discovery),
				AllowedAudience: []string{audience},
			},
		},
	})
	require.NoError(t, err)

	arn := aws.ToString(out.WorkloadIdentityDetails.WorkloadIdentityArn)

	return arn[strings.LastIndex(arn, "/")+1:]
}

func TestJWKSCancelledCallerDoesNotPinFailure(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	idp := newSwitchIDP(t, false, false)
	token := idp.sign(t, testKID, jwt.MapClaims{"iss": idp.URL, "sub": "a", "exp": time.Now().Add(time.Hour).Unix()})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := f.backend.DataPlane().VerifyUserJWT(ctx, token)
	require.Error(t, err)

	sub, err := f.backend.DataPlane().VerifyUserJWT(t.Context(), token)
	require.NoError(t, err, "a caller that gave up must not negative-cache a healthy issuer")
	assert.Equal(t, "a", sub)
}

func TestJWKSKidMissAfterOutageIsNotRefetchedPerRequest(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	idp := newSwitchIDP(t, false, false)
	good := idp.sign(t, testKID, jwt.MapClaims{"iss": idp.URL, "sub": "a", "exp": time.Now().Add(time.Hour).Unix()})
	bad := idp.sign(t, "unknown", jwt.MapClaims{"iss": idp.URL, "sub": "a", "exp": time.Now().Add(time.Hour).Unix()})

	_, err := f.backend.DataPlane().VerifyUserJWT(t.Context(), good)
	require.NoError(t, err)

	idp.down.Store(true)
	f.clock.step(time.Minute)

	_, err = f.backend.DataPlane().VerifyUserJWT(t.Context(), bad)
	require.Error(t, err)

	before := idp.discovery.Load()

	for range 4 {
		_, err = f.backend.DataPlane().VerifyUserJWT(t.Context(), bad)
		require.Error(t, err)
	}

	assert.Equal(t, int32(0), idp.discovery.Load()-before, "a down issuer is not hit once the failure is cached")

	_, err = f.backend.DataPlane().VerifyUserJWT(t.Context(), good)
	require.NoError(t, err, "the cached key set keeps serving known keys while the refresh fails")
}

func TestVerifyWorkloadJWTRejectsMissingIssuer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		claims   jwt.MapClaims
		name     string
		noIssuer bool
		wantErr  bool
	}{
		{name: "issuer matches", claims: jwt.MapClaims{"sub": "alice"}},
		{name: "document lacks issuer", noIssuer: true, claims: jwt.MapClaims{"sub": "alice"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			idp := newSwitchIDP(t, tt.noIssuer, true)
			pool := x509.NewCertPool()
			pool.AddCert(idp.Certificate())
			m.backend.SetTrustedRoots(pool)
			workload := securedRuntimeWorkload(t, m, idp.URL+"/.well-known/openid-configuration")

			claims := jwt.MapClaims{"aud": "aud", "exp": time.Now().Add(time.Hour).Unix()}
			if !tt.noIssuer {
				claims["iss"] = idp.URL
			}

			maps.Copy(claims, tt.claims)

			token := idp.sign(t, testKID, claims)

			_, err := m.backend.DataPlane().VerifyWorkloadJWT(t.Context(), dpScope(), workload, token)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDataPlaneOutboundClientIsLoopbackGated(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://192.0.2.1:9/token", http.NoBody)
	require.NoError(t, err)

	resp, err := f.backend.DataPlane().OutboundHTTP().Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}

	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside loopback")
}

func TestVerifyWorkloadJWTIsBoundToTheWorkload(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	idp := newSwitchIDP(t, false, true)
	pool := x509.NewCertPool()
	pool.AddCert(idp.Certificate())
	m.backend.SetTrustedRoots(pool)

	disc := idp.URL + "/.well-known/openid-configuration"
	a := securedNamedRuntime(t, m, "rta", disc, "aud-a")
	b := securedNamedRuntime(t, m, "rtb", disc, "aud-b")

	tokenA := idp.sign(t, testKID, jwt.MapClaims{
		"iss": idp.URL, "sub": "alice", "aud": "aud-a", "exp": time.Now().Add(time.Hour).Unix(),
	})

	sub, err := m.backend.DataPlane().VerifyWorkloadJWT(t.Context(), dpScope(), a, tokenA)
	require.NoError(t, err)
	assert.Equal(t, "alice", sub)

	_, err = m.backend.DataPlane().VerifyWorkloadJWT(t.Context(), dpScope(), b, tokenA)
	require.Error(t, err, "a token the first workload accepts does not mint a token for the second")
}

func TestJWKSKidMissIsRateLimited(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	idp := newSwitchIDP(t, false, false)
	exp := time.Now().Add(time.Hour).Unix()
	good := idp.sign(t, testKID, jwt.MapClaims{"iss": idp.URL, "sub": "a", "exp": exp})
	bad := idp.sign(t, "unknown", jwt.MapClaims{"iss": idp.URL, "sub": "a", "exp": exp})

	_, err := f.backend.DataPlane().VerifyUserJWT(t.Context(), good)
	require.NoError(t, err)

	for range 3 {
		_, err = f.backend.DataPlane().VerifyUserJWT(t.Context(), bad)
		require.Error(t, err)
	}

	assert.Equal(t, int32(1), idp.discovery.Load(), "unknown kids do not refetch inside the refetch gap")

	f.clock.step(time.Minute)

	_, err = f.backend.DataPlane().VerifyUserJWT(t.Context(), bad)
	require.Error(t, err)
	assert.Equal(t, int32(2), idp.discovery.Load(), "one refetch once the gap has passed")
}

func TestJWKSNegativeCacheIsBounded(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	exp := time.Now().Add(time.Hour).Unix()
	key := newSwitchIDP(t, false, false).key

	for range 40 {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "down", http.StatusInternalServerError)
		}))
		t.Cleanup(srv.Close)

		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": srv.URL, "sub": "a", "exp": exp})
		tok.Header["kid"] = testKID

		signed, err := tok.SignedString(key)
		require.NoError(t, err)

		_, err = f.backend.DataPlane().VerifyUserJWT(t.Context(), signed)
		require.Error(t, err)
	}

	assert.Equal(t, 32, f.backend.JWKSNegativeSize())
}

func TestVerifyWorkloadJWTRejectsBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		claim func(idp *switchIDP) jwt.MapClaims
		name  string
		bits  int
		ok    bool
	}{
		{name: "valid", bits: 2048, ok: true, claim: func(i *switchIDP) jwt.MapClaims {
			return jwt.MapClaims{"iss": i.URL, "sub": "a", "aud": "aud", "exp": time.Now().Add(time.Hour).Unix()}
		}},
		{name: "no expiry", bits: 2048, claim: func(i *switchIDP) jwt.MapClaims {
			return jwt.MapClaims{"iss": i.URL, "sub": "a", "aud": "aud"}
		}},
		{name: "expired", bits: 2048, claim: func(i *switchIDP) jwt.MapClaims {
			return jwt.MapClaims{"iss": i.URL, "sub": "a", "aud": "aud", "exp": time.Now().Add(-time.Hour).Unix()}
		}},
		{name: "not yet valid", bits: 2048, claim: func(i *switchIDP) jwt.MapClaims {
			return jwt.MapClaims{
				"iss": i.URL, "sub": "a", "aud": "aud",
				"exp": time.Now().Add(2 * time.Hour).Unix(), "nbf": time.Now().Add(time.Hour).Unix(),
			}
		}},
		{name: "weak key", bits: 1024, claim: func(i *switchIDP) jwt.MapClaims {
			return jwt.MapClaims{"iss": i.URL, "sub": "a", "aud": "aud", "exp": time.Now().Add(time.Hour).Unix()}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			idp := newSwitchIDPKey(t, false, true, tt.bits)
			pool := x509.NewCertPool()
			pool.AddCert(idp.Certificate())
			m.backend.SetTrustedRoots(pool)

			workload := securedRuntimeWorkload(t, m, idp.URL+"/.well-known/openid-configuration")
			token := idp.sign(t, testKID, tt.claim(idp))

			_, err := m.backend.DataPlane().VerifyWorkloadJWT(t.Context(), dpScope(), workload, token)
			if tt.ok {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
		})
	}
}
