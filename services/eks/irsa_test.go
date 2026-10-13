package eks_test

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

var issuerPattern = regexp.MustCompile(`^https://oidc\.eks\.us-east-1\.amazonaws\.com/id/([0-9a-f]{32})$`)

func jwtParts(t *testing.T, token string) (map[string]any, map[string]any, []byte) {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)

	var hdr, claims map[string]any

	for i, dst := range []*map[string]any{&hdr, &claims} {
		raw, err := base64.RawURLEncoding.DecodeString(parts[i])
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, dst))
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)

	return hdr, claims, sig
}

func verifyWithJWKS(t *testing.T, token string, set *eks.JWKS) {
	t.Helper()

	hdr, _, sig := jwtParts(t, token)
	require.Equal(t, "RS256", hdr["alg"])

	var jwk *eks.JWK

	for i := range set.Keys {
		if set.Keys[i].Kid == hdr["kid"] {
			jwk = &set.Keys[i]
		}
	}

	require.NotNil(t, jwk, "kid must appear in JWKS")

	n, err := base64.RawURLEncoding.DecodeString(jwk.N)
	require.NoError(t, err)

	e, err := base64.RawURLEncoding.DecodeString(jwk.E)
	require.NoError(t, err)

	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	signing := strings.Join(strings.Split(token, ".")[:2], ".")
	digest := sha256.Sum256([]byte(signing))
	require.NoError(t, rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig))
}

func doRaw(t *testing.T, h *eks.Handler, method, path, host, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if host != "" {
		req.Host = host
	}

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func TestMintServiceAccountToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		cluster string
		wantSub string
		wantAud []any
		req     eks.ServiceAccountTokenRequest
		wantTTL time.Duration
		wantPod bool
	}{
		{
			name:    "defaults",
			cluster: "c1",
			req:     eks.ServiceAccountTokenRequest{Namespace: "default", Name: "app"},
			wantAud: []any{"sts.amazonaws.com"},
			wantTTL: time.Hour,
			wantSub: "system:serviceaccount:default:app",
		},
		{
			name:    "custom",
			cluster: "c1",
			req: eks.ServiceAccountTokenRequest{
				Namespace: "kube-system", Name: "sa", PodName: "p-1",
				Audiences: []string{"a", "b"}, TTL: 10 * time.Minute,
			},
			wantAud: []any{"a", "b"},
			wantTTL: 10 * time.Minute,
			wantPod: true,
			wantSub: "system:serviceaccount:kube-system:sa",
		},
		{
			name:    "no namespace",
			cluster: "c1",
			req:     eks.ServiceAccountTokenRequest{Name: "app"},
			wantErr: eks.ErrValidation,
		},
		{
			name:    "no name",
			cluster: "c1",
			req:     eks.ServiceAccountTokenRequest{Namespace: "d"},
			wantErr: eks.ErrValidation,
		},
		{
			name: "negative ttl", cluster: "c1",
			req:     eks.ServiceAccountTokenRequest{Namespace: "d", Name: "a", TTL: -time.Second},
			wantErr: eks.ErrValidation,
		},
		{
			name:    "unknown cluster",
			cluster: "nope",
			req:     eks.ServiceAccountTokenRequest{Namespace: "d", Name: "a"},
			wantErr: eks.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			c, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
			require.NoError(t, err)

			m := issuerPattern.FindStringSubmatch(c.OIDCIssuer)
			require.NotNil(t, m, "issuer %q", c.OIDCIssuer)

			tok, err := b.MintServiceAccountToken(tt.cluster, tt.req)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, c.OIDCIssuer, tok.Issuer)

			set, err := b.OIDCJWKS(m[1])
			require.NoError(t, err)
			verifyWithJWKS(t, tok.Token, set)

			_, claims, _ := jwtParts(t, tok.Token)
			assert.Equal(t, c.OIDCIssuer, claims["iss"])
			assert.Equal(t, tt.wantSub, claims["sub"])
			assert.Equal(t, tt.wantAud, claims["aud"])
			assert.InDelta(t, tt.wantTTL.Seconds(), claims["exp"].(float64)-claims["iat"].(float64), 1)
			assert.NotEmpty(t, claims["jti"])

			k8s, ok := claims["kubernetes.io"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.req.Namespace, k8s["namespace"])
			assert.Contains(t, k8s["serviceaccount"], "uid")
			assert.Equal(t, tt.wantPod, k8s["pod"] != nil)
		})
	}
}

func TestOIDCIssuerKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	c1, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	c2, err := b.CreateCluster("c2", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	k1, ok := b.OIDCIssuerKeys(c1.OIDCIssuer)
	require.True(t, ok)

	k1again, ok := b.OIDCIssuerKeys(c1.OIDCIssuer + "/")
	require.True(t, ok)
	assert.Zero(t, k1[0].N.Cmp(k1again[0].N))

	k2, ok := b.OIDCIssuerKeys(c2.OIDCIssuer)
	require.True(t, ok)
	assert.NotZero(t, k1[0].N.Cmp(k2[0].N), "each cluster has its own key")

	_, ok = b.OIDCIssuerKeys("https://oidc.eks.us-east-1.amazonaws.com/id/00000000000000000000000000000000")
	assert.False(t, ok)

	_, err = b.DeleteCluster("c1")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, found := b.OIDCIssuerKeys(c1.OIDCIssuer)

		return !found
	}, 5*time.Second, 20*time.Millisecond)
}

func TestOIDCKeysSurviveRestore(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	c, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	before, ok := b.OIDCIssuerKeys(c.OIDCIssuer)
	require.True(t, ok)

	tok, err := b.MintServiceAccountToken("c1", eks.ServiceAccountTokenRequest{Namespace: "d", Name: "a"})
	require.NoError(t, err)

	b2 := newBackend(t)
	require.NoError(t, b2.Restore(t.Context(), b.Snapshot(t.Context())))

	after, ok := b2.OIDCIssuerKeys(c.OIDCIssuer)
	require.True(t, ok)
	assert.Zero(t, before[0].N.Cmp(after[0].N))

	m := issuerPattern.FindStringSubmatch(c.OIDCIssuer)
	set, err := b2.OIDCJWKS(m[1])
	require.NoError(t, err)
	verifyWithJWKS(t, tok.Token, set)
}

func TestOIDCEndpoints(t *testing.T) {
	t.Parallel()

	h, b := newHandlerAndBackend(t)
	c, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	id := issuerPattern.FindStringSubmatch(c.OIDCIssuer)[1]

	const (
		emu     = "/_gopherstack/eks/oidc/id/"
		mint    = "/_gopherstack/eks/c1/serviceaccount-token"
		oidcAWS = "oidc.eks.us-east-1.amazonaws.com"
		wellKn  = "/.well-known/openid-configuration"
		zeroID  = "ffffffffffffffffffffffffffffffff"
	)

	tests := []struct {
		name       string
		method     string
		path       string
		host       string
		body       string
		wantKey    string
		wantStatus int
	}{
		{name: "discovery", method: http.MethodGet, path: emu + id + wellKn, wantStatus: 200, wantKey: "jwks_uri"},
		{name: "keys", method: http.MethodGet, path: emu + id + "/keys", wantStatus: 200, wantKey: "keys"},
		{
			name: "host style discovery", method: http.MethodGet, path: "/id/" + id + wellKn,
			host: oidcAWS, wantStatus: 200, wantKey: "issuer",
		},
		{
			name: "host style keys", method: http.MethodGet, path: "/id/" + id + "/keys",
			host: oidcAWS, wantStatus: 200, wantKey: "keys",
		},
		{name: "unknown id", method: http.MethodGet, path: emu + zeroID + "/keys", wantStatus: 404},
		{
			name: "mint", method: http.MethodPost, path: mint, wantStatus: 200, wantKey: "token",
			body: `{"namespace":"d","name":"a","audience":"x","ttlSeconds":60}`,
		},
		{
			name: "mint unknown cluster", method: http.MethodPost, wantStatus: 404,
			path: "/_gopherstack/eks/zz/serviceaccount-token", body: `{"namespace":"d","name":"a"}`,
		},
		{name: "mint invalid", method: http.MethodPost, path: mint, body: `{"name":"a"}`, wantStatus: 400},
		{name: "mint bad json", method: http.MethodPost, path: mint, body: `{`, wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRaw(t, h, tt.method, tt.path, tt.host, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantKey != "" {
				var doc map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
				assert.Contains(t, doc, tt.wantKey)
			}
		})
	}
}

func TestOIDCRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		host string
		want bool
	}{
		{name: "emulator path", path: "/_gopherstack/eks/oidc/id/abc/keys", want: true},
		{name: "oidc host", path: "/id/abc/keys", host: "oidc.eks.us-east-1.amazonaws.com", want: true},
		{name: "id path other host", path: "/id/abc/keys", host: "example.com", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.host != "" {
				req.Host = tt.host
			}

			assert.Equal(t, tt.want, matcher(echo.New().NewContext(req, httptest.NewRecorder())))
		})
	}
}
