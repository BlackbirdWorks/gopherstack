package sts_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	irsaIssuer   = "https://oidc.eks.us-east-1.amazonaws.com/id/0123456789abcdef0123456789abcdef"
	irsaIssuerNo = "https://oidc.eks.us-east-1.amazonaws.com/id/ffffffffffffffffffffffffffffffff"
	irsaRoleArn  = "arn:aws:iam::123456789012:role/irsa"
	irsaHost     = "oidc.eks.us-east-1.amazonaws.com/id/0123456789abcdef0123456789abcdef"
	irsaSub      = "system:serviceaccount:default:app"
)

type irsaResolver struct{ keys map[string]*rsa.PublicKey }

func (r irsaResolver) OIDCIssuerKeys(issuer string) ([]*rsa.PublicKey, bool) {
	k, ok := r.keys[issuer]
	if !ok {
		return nil, false
	}

	return []*rsa.PublicKey{k}, true
}

type irsaOIDCLookup struct{ providers map[string][]string }

func (l irsaOIDCLookup) OIDCProviderExists(issuer string) bool {
	_, ok := l.providers[issuer]

	return ok
}

func (l irsaOIDCLookup) OIDCProviderClientIDs(issuer string) ([]string, bool) {
	ids, ok := l.providers[issuer]

	return ids, ok
}

func signIRSAToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()

	hdr, err := json.Marshal(map[string]string{"alg": "RS256", "kid": "k"})
	require.NoError(t, err)

	body, err := json.Marshal(claims)
	require.NoError(t, err)

	signing := base64.RawURLEncoding.EncodeToString(hdr) + "." + base64.RawURLEncoding.EncodeToString(body)
	digest := sha256.Sum256([]byte(signing))

	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)

	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func irsaTrustPolicy(cond string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":` +
		`"arn:aws:iam::123456789012:oidc-provider/` + irsaHost + `"},` +
		`"Action":"sts:AssumeRoleWithWebIdentity","Condition":` + cond + `}]}`
}

func forgeIRSAPayload(t *testing.T, token string, claims map[string]any) string {
	t.Helper()

	forged, err := json.Marshal(claims)
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	parts[1] = base64.RawURLEncoding.EncodeToString(forged)

	return strings.Join(parts, ".")
}

func unsignedIRSAToken(t *testing.T, claims map[string]any) string {
	t.Helper()

	body, err := json.Marshal(claims)
	require.NoError(t, err)

	return base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." +
		base64.RawURLEncoding.EncodeToString(body) + ".c2ln"
}

func TestAssumeRoleWithWebIdentity_ClusterIssuer(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	cond := `{"StringEquals":{"` + irsaHost + `:sub":"` + irsaSub + `","` + irsaHost + `:aud":"sts.amazonaws.com"}}`
	now := time.Now()

	baseClaims := func(mut func(map[string]any)) map[string]any {
		c := map[string]any{
			"iss": irsaIssuer, "sub": irsaSub, "aud": []string{"sts.amazonaws.com"},
			"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		}
		if mut != nil {
			mut(c)
		}

		return c
	}

	tests := []struct {
		wantErr   error
		mutate    func(map[string]any)
		providers map[string][]string
		name      string
		mode      string
	}{
		{name: "valid"},
		{name: "provider without client ids", providers: map[string][]string{irsaIssuer: nil}},
		{
			name:    "wrong sub",
			mutate:  func(c map[string]any) { c["sub"] = "system:serviceaccount:default:other" },
			wantErr: sts.ErrAccessDenied,
		},
		{
			name:    "aud not in client ids",
			mutate:  func(c map[string]any) { c["aud"] = []string{"other"} },
			wantErr: sts.ErrInvalidIdentityToken,
		},
		{
			name:      "aud fails trust condition",
			mutate:    func(c map[string]any) { c["aud"] = []string{"extra"} },
			providers: map[string][]string{irsaIssuer: {"extra"}},
			wantErr:   sts.ErrAccessDenied,
		},
		{
			name:    "expired",
			mutate:  func(c map[string]any) { c["exp"] = now.Add(-time.Minute).Unix() },
			wantErr: sts.ErrExpiredToken,
		},
		{
			name:      "unregistered provider",
			providers: map[string][]string{},
			wantErr:   sts.ErrInvalidIdentityToken,
		},
		{name: "tampered payload", mode: "tamper", wantErr: sts.ErrInvalidIdentityToken},
		{name: "foreign key", mode: "foreign", wantErr: sts.ErrInvalidIdentityToken},
		{name: "alg none", mode: "none", wantErr: sts.ErrInvalidIdentityToken},
		{
			name:      "unknown cluster issuer",
			mutate:    func(c map[string]any) { c["iss"] = irsaIssuerNo },
			providers: map[string][]string{irsaIssuerNo: {"sts.amazonaws.com"}},
			wantErr:   sts.ErrInvalidIdentityToken,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			claims := baseClaims(tt.mutate)

			var token string

			switch tt.mode {
			case "tamper":
				token = forgeIRSAPayload(t, signIRSAToken(t, key, claims),
					baseClaims(func(c map[string]any) { c["sub"] = "system:serviceaccount:default:x" }))
			case "foreign":
				token = signIRSAToken(t, other, claims)
			case "none":
				token = unsignedIRSAToken(t, claims)
			default:
				token = signIRSAToken(t, key, claims)
			}

			providers := tt.providers
			if providers == nil {
				providers = map[string][]string{irsaIssuer: {"sts.amazonaws.com"}}
			}

			b := sts.NewInMemoryBackend()
			b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: irsaTrustPolicy(cond)}})
			b.SetOIDCLookup(irsaOIDCLookup{providers: providers})
			b.SetIssuerKeyResolver(irsaResolver{keys: map[string]*rsa.PublicKey{irsaIssuer: &key.PublicKey}})

			out, aErr := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          irsaRoleArn,
				RoleSessionName:  "pod",
				WebIdentityToken: token,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, aErr, tt.wantErr)

				return
			}

			require.NoError(t, aErr)
			assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/irsa/pod",
				out.AssumeRoleWithWebIdentityResult.AssumedRoleUser.Arn)
			assert.Equal(t, irsaSub, out.AssumeRoleWithWebIdentityResult.SubjectFromWebIdentityToken)
		})
	}
}
