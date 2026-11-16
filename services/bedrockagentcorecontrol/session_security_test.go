package bedrockagentcorecontrol_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

type tickClock struct{ ns atomic.Int64 }

func (c *tickClock) now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *tickClock) add(d time.Duration) { c.ns.Add(int64(d)) }

func (m *mcpFixture) pendingTarget(t *testing.T, gid, name, providerARN, callback string) string {
	t.Helper()

	created, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: aws.String(gid), Name: aws.String(name),
		TargetConfiguration:              openAPISpecTarget("http://127.0.0.1:1"),
		CredentialProviderConfigurations: codeCredential(providerARN),
	})
	require.NoError(t, err)

	tid := aws.ToString(created.TargetId)

	got, err := m.client.GetGatewayTarget(t.Context(), &sdk.GetGatewayTargetInput{
		GatewayIdentifier: aws.String(gid), TargetId: aws.String(tid),
	})
	require.NoError(t, err)

	authz, ok := got.AuthorizationData.(*types.AuthorizationDataMemberOauth2)
	require.True(t, ok)

	authURL, err := url.Parse(aws.ToString(authz.Value.AuthorizationUrl))
	require.NoError(t, err)

	return m.url + callbackPath(callback) + "?code=%s&state=" + authURL.Query().Get("state")
}

func (m *mcpFixture) hit(t *testing.T, target string) int {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, http.NoBody)
	require.NoError(t, err)

	resp, err := noRedirect(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	return resp.StatusCode
}

func TestOAuthCallbackSingleUse(t *testing.T) {
	t.Parallel()

	idp := newCodeIDP(t)
	m := newMCPFixture(t)
	providerARN, callback := m.codeProvider(t, idp)
	gid := aws.ToString(createGateway(t, m.client, "Replay-Gw").GatewayId)
	tpl := m.pendingTarget(t, gid, "code", providerARN, callback)

	assert.Equal(t, http.StatusFound, m.hit(t, fmt.Sprintf(tpl, "first")))
	assert.Equal(t, http.StatusBadRequest, m.hit(t, fmt.Sprintf(tpl, "second")), "the state is spent")

	idp.mu.Lock()
	defer idp.mu.Unlock()

	assert.Equal(t, []string{"authorization_code:first"}, idp.grants)
}

func TestOAuthSessionLifetime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, m *mcpFixture, clock *tickClock, idp *codeIDP, providerARN, callback, gid string)
		name string
	}{
		{
			name: "expired callback rejected",
			run: func(t *testing.T, m *mcpFixture, clock *tickClock, idp *codeIDP, providerARN, callback, gid string) {
				t.Helper()

				tpl := m.pendingTarget(t, gid, "code", providerARN, callback)
				clock.add(11 * time.Minute)

				assert.Equal(t, http.StatusBadRequest, m.hit(t, fmt.Sprintf(tpl, "late")))
				assert.Empty(t, idp.grants)
			},
		},
		{
			name: "stale sessions pruned",
			run: func(t *testing.T, m *mcpFixture, clock *tickClock, _ *codeIDP, providerARN, callback, gid string) {
				t.Helper()

				m.pendingTarget(t, gid, "one", providerARN, callback)
				m.pendingTarget(t, gid, "two", providerARN, callback)
				require.Equal(t, 2, m.backend.AuthSessionCount())

				clock.add(11 * time.Minute)
				m.pendingTarget(t, gid, "three", providerARN, callback)

				assert.Equal(t, 1, m.backend.AuthSessionCount())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newCodeIDP(t)
			m := newMCPFixture(t)
			clock := &tickClock{}
			clock.add(time.Hour)
			m.backend.SetClock(clock.now)

			providerARN, callback := m.codeProvider(t, idp)
			gid := aws.ToString(createGateway(t, m.client, "Life-Gw").GatewayId)

			tt.run(t, m, clock, idp, providerARN, callback, gid)
		})
	}
}

func TestFederationReturnURLAllowList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		allowed []string
		wantErr bool
	}{
		{name: "listed", allowed: []string{"https://app.example/cb"}},
		{name: "not listed", allowed: []string{"https://other.example/cb"}, wantErr: true},
		{name: "empty list", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newCodeIDP(t)
			m := newMCPFixture(t)
			m.codeProvider(t, idp)

			_, err := m.client.CreateWorkloadIdentity(t.Context(), &sdk.CreateWorkloadIdentityInput{
				Name: aws.String("wl"), AllowedResourceOauth2ReturnUrls: tt.allowed,
			})
			require.NoError(t, err)

			ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Account: testAccount, Region: "us-east-1"})
			_, err = m.backend.DataPlane().FederatedToken(ctx,
				dataplane.Scope{Account: testAccount, Region: "us-east-1"}, dataplane.FederationRequest{
					Provider: "idp", Workload: "wl", UserID: "alice", ReturnURL: "https://app.example/cb",
				})

			if tt.wantErr {
				require.ErrorIs(t, err, dataplane.ErrInvalidFederation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestVerifyUserJWTKeyPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		docIssuer string
		bits      int
	}{
		{name: "weak key", bits: 1024},
		{name: "issuer mismatch", bits: 2048, docIssuer: "https://other.example"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newJWKSIDPWith(t, tt.bits, tt.docIssuer)
			f := newFixture(t)
			token := idp.sign(t, idp.key, jwt.MapClaims{
				"iss": idp.URL, "sub": "alice", "exp": time.Now().Add(time.Hour).Unix(),
			})

			_, err := f.backend.DataPlane().VerifyUserJWT(t.Context(), token)
			require.Error(t, err)
		})
	}
}

func TestJWKSCacheBounded(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		prefix, tail, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")

		var body any

		switch tail {
		case ".well-known/openid-configuration":
			body = map[string]string{"issuer": base + "/" + prefix, "jwks_uri": base + "/" + prefix + "/jwks"}
		case "jwks":
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
	t.Cleanup(srv.Close)

	f := newFixture(t)

	for i := range 100 {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": fmt.Sprintf("%s/i%d", srv.URL, i), "sub": "alice", "exp": time.Now().Add(time.Hour).Unix(),
		})
		tok.Header["kid"] = testKID

		signed, serr := tok.SignedString(key)
		require.NoError(t, serr)

		_, verr := f.backend.DataPlane().VerifyUserJWT(t.Context(), signed)
		require.NoError(t, verr)
	}

	assert.LessOrEqual(t, f.backend.JWKSCacheSize(), 32)
}
