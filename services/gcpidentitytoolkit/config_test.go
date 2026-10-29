package gcpidentitytoolkit_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/identitytoolkit/v1"
	"google.golang.org/api/option"
)

func TestProjectConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		check  func(*testing.T, result)
		name   string
		method string
		path   string
		status int
	}{
		{
			name: "defaults", method: http.MethodGet, status: http.StatusOK,
			check: func(t *testing.T, r result) {
				t.Helper()

				signIn := r.body["signIn"].(map[string]any)
				assert.Equal(t, true, signIn["email"].(map[string]any)["enabled"])
				assert.Equal(t, true, signIn["anonymous"].(map[string]any)["enabled"])
				assert.Equal(t, "SCRYPT", signIn["hashConfig"].(map[string]any)["algorithm"])
				assert.Equal(t, true, r.body["emailPrivacyConfig"].(map[string]any)["enableImprovedEmailPrivacy"])
			},
		},
		{
			name:   "patch with mask",
			method: http.MethodPatch,
			path:   "?updateMask=authorizedDomains",
			body: map[string]any{
				"authorizedDomains":        []string{"a.example.com"},
				"autodeleteAnonymousUsers": true,
			},
			status: http.StatusOK,
			check: func(t *testing.T, r result) {
				t.Helper()

				assert.Equal(t, []any{"a.example.com"}, r.body["authorizedDomains"])
				assert.NotContains(t, r.body, "autodeleteAnonymousUsers", "fields outside the mask are ignored")
			},
		},
		{
			name:   "patch nested",
			method: http.MethodPatch,
			body: map[string]any{
				"signIn": map[string]any{"anonymous": map[string]any{"enabled": false}},
			},
			status: http.StatusOK,
			check: func(t *testing.T, r result) {
				t.Helper()

				signIn := r.body["signIn"].(map[string]any)
				assert.NotContains(t, signIn["anonymous"], "enabled")
				assert.Equal(t, true, signIn["email"].(map[string]any)["enabled"], "siblings are preserved")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.do(t, tt.method, "/v2/projects/"+e.project+"/config"+tt.path, adminAuth, tt.body)
			require.Equal(t, tt.status, r.status, r.body)
			assert.Equal(t, "projects/"+e.project+"/config", r.str("name"))
			tt.check(t, r)
		})
	}
}

func TestPasswordPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state string
		pw    string
		msg   string
	}{
		{name: "enforced and met", state: "ENFORCE", pw: "Abcdef1!"},
		{
			name:  "enforced missing upper and digit",
			state: "ENFORCE",
			pw:    "abcdefg!",
			msg: "PASSWORD_DOES_NOT_MEET_REQUIREMENTS : Missing password requirements: " +
				"[Password must contain an upper case character, Password must contain a numeric character]",
		},
		{
			name:  "enforced too short",
			state: "ENFORCE",
			pw:    "Ab1!",
			msg:   "WEAK_PASSWORD : Password should be at least 6 characters",
		},
		{name: "off", state: "OFF", pw: "abcdefg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.setConfig(t, map[string]any{"passwordPolicyConfig": map[string]any{
				"passwordPolicyEnforcementState": tt.state,
				"passwordPolicyVersions": []any{map[string]any{"customStrengthOptions": map[string]any{
					"minPasswordLength": 8, "containsUppercaseCharacter": true, "containsNumericCharacter": true,
				}}},
			}})

			r := e.client(t, "signUp", map[string]any{"email": "p@example.com", "password": tt.pw})
			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			assert.Equal(t, http.StatusOK, r.status, r.body)
		})
	}
}

func TestTenantsREST(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		msg  string
	}{
		{name: "ok", body: map[string]any{"displayName": "my-tenant", "allowPasswordSignup": true}},
		{name: "too short", body: map[string]any{"displayName": "ab"}, msg: "Invalid tenant display name"},
		{
			name: "bad chars",
			body: map[string]any{"displayName": "no spaces allowed"},
			msg:  "Invalid tenant display name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			base := "/v2/projects/" + e.project + "/tenants"
			r := e.do(t, http.MethodPost, base, adminAuth, tt.body)

			if tt.msg != "" {
				assert.Equal(t, http.StatusBadRequest, r.status)
				assert.Contains(t, r.errMsg(), tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)

			name := r.str("name")
			id := name[len("projects/"+e.project+"/tenants/"):]
			assert.Contains(t, id, "my-tenant-")

			sign := e.do(t, http.MethodPost, "/v1/projects/"+e.project+"/tenants/"+id+"/accounts:signUp?key=k", "",
				map[string]any{"email": "t@example.com", "password": "secret1"})
			require.Equal(t, http.StatusOK, sign.status, sign.body)

			root := e.client(t, "signInWithPassword", map[string]any{"email": "t@example.com", "password": "secret1"})
			assert.Equal(t, http.StatusBadRequest, root.status, "project level is a separate namespace")

			list := e.do(t, http.MethodGet, base+"?pageSize=1", adminAuth, nil)
			assert.Len(t, list.body["tenants"], 1)

			miss := e.do(t, http.MethodGet, base+"/nope-12345", adminAuth, nil)
			e.assertErr(t, miss, http.StatusNotFound, "TENANT_NOT_FOUND")

			unknown := e.do(
				t,
				http.MethodPost,
				"/v1/projects/"+e.project+"/tenants/nope-12345/accounts:signUp?key=k",
				"",
				map[string]any{},
			)
			e.assertErr(t, unknown, http.StatusNotFound, "TENANT_NOT_FOUND")

			del := e.do(t, http.MethodDelete, base+"/"+id, adminAuth, nil)
			require.Equal(t, http.StatusOK, del.status, del.body)

			gone := e.do(t, http.MethodGet, base+"/"+id, adminAuth, nil)
			assert.Equal(t, http.StatusNotFound, gone.status)
		})
	}
}

func TestTenantProviderFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tenant map[string]any
		body   map[string]any
		msg    string
	}{
		{
			name:   "password signup off by default",
			tenant: map[string]any{"displayName": "no-pass"},
			body:   map[string]any{"email": "a@example.com", "password": "secret1"},
			msg:    "OPERATION_NOT_ALLOWED : Password sign-in is disabled for this project.",
		},
		{
			name:   "anonymous off by default",
			tenant: map[string]any{"displayName": "no-anon"},
			body:   map[string]any{},
			msg:    "ADMIN_ONLY_OPERATION",
		},
		{
			name:   "anonymous on",
			tenant: map[string]any{"displayName": "yes-anon", "enableAnonymousUser": true},
			body:   map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tn := e.do(t, http.MethodPost, "/v2/projects/"+e.project+"/tenants", adminAuth, tt.tenant)
			require.Equal(t, http.StatusOK, tn.status, tn.body)

			id := tn.str("name")[len("projects/"+e.project+"/tenants/"):]
			r := e.do(
				t,
				http.MethodPost,
				"/v1/projects/"+e.project+"/tenants/"+id+"/accounts:signUp?key=k",
				"",
				tt.body,
			)

			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			assert.Equal(t, http.StatusOK, r.status, r.body)
		})
	}
}

func TestSigningKeyEndpoints(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "k@example.com")

	x509Resp := e.do(t, http.MethodGet, "/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com", "", nil)
	require.Equal(t, http.StatusOK, x509Resp.status)
	require.Len(t, x509Resp.body, 1)

	var kid, certPEM string
	for k, v := range x509Resp.body {
		kid, certPEM = k, v.(string)
	}

	block, _ := pem.Decode([]byte(certPEM))
	require.NotNil(t, block)

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	claims := jwt.MapClaims{}
	tok, err := jwt.ParseWithClaims(up.str("idToken"), claims, func(tk *jwt.Token) (any, error) {
		assert.Equal(t, kid, tk.Header["kid"])

		return cert.PublicKey, nil
	}, jwt.WithValidMethods([]string{"RS256"}), jwt.WithTimeFunc(e.clock.Now))
	require.NoError(t, err)
	assert.True(t, tok.Valid)
	assert.Equal(t, "https://securetoken.google.com/"+e.project, claims["iss"])
	assert.Equal(t, e.project, claims["aud"])
	assert.Equal(t, up.str("localId"), claims["user_id"])
	assert.Equal(t, up.str("localId"), claims["sub"])
	assert.Equal(t, "password", claims["firebase"].(map[string]any)["sign_in_provider"])
	assert.InDelta(t, 3600, claims["exp"].(float64)-claims["iat"].(float64), 0)
	assert.NotZero(t, claims["auth_time"])

	jwk := e.do(t, http.MethodGet, "/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com", "", nil)
	keys := jwk.body["keys"].([]any)
	require.Len(t, keys, 1)

	k := keys[0].(map[string]any)
	assert.Equal(t, kid, k["kid"])
	assert.Equal(t, "RS256", k["alg"])

	n, err := base64.RawURLEncoding.DecodeString(k["n"].(string))
	require.NoError(t, err)
	assert.Equal(t, cert.PublicKey.(*rsa.PublicKey).N.Bytes(), n)
}

func TestCustomTokenRS256(t *testing.T) {
	t.Parallel()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	const sa = "svc@example.iam.gserviceaccount.com"

	mint := func(key *rsa.PrivateKey, iss string, now time.Time, uid string) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss":    iss,
			"sub":    iss,
			"aud":    "https://identitytoolkit.googleapis.com/google.identity.identitytoolkit.v1.IdentityToolkit",
			"iat":    now.Unix(),
			"exp":    now.Add(time.Hour).Unix(),
			"uid":    uid,
			"claims": map[string]any{"k": "v"},
		})
		s, signErr := tok.SignedString(key)
		require.NoError(t, signErr)

		return s
	}

	tests := []struct {
		key     *rsa.PrivateKey
		name    string
		iss     string
		uid     string
		advance time.Duration
		wired   bool
		ok      bool
	}{
		{name: "known service account", key: priv, iss: sa, wired: true, uid: "u1", ok: true},
		{name: "wrong key", key: other, iss: sa, wired: true, uid: "u1"},
		{name: "unknown issuer", key: priv, iss: "ghost@example.com", wired: true, uid: "u1"},
		{name: "resolver not wired", key: priv, iss: sa, uid: "u1"},
		{name: "empty uid", key: priv, iss: sa, wired: true},
		{name: "expired", key: priv, iss: sa, wired: true, uid: "u1", advance: 3 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.wired {
				e.backend.SetCustomTokenKeys(func(email string) []*rsa.PublicKey {
					if email == sa {
						return []*rsa.PublicKey{&priv.PublicKey}
					}

					return nil
				})
			}

			ct := mint(tt.key, tt.iss, e.clock.Now(), tt.uid)
			e.clock.Advance(tt.advance)

			r := e.client(t, "signInWithCustomToken", map[string]any{"token": ct, "returnSecureToken": true})
			if !tt.ok {
				assert.Equal(t, http.StatusBadRequest, r.status, r.body)
				assert.Contains(t, r.errMsg(), "INVALID_CUSTOM_TOKEN")

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.NotEmpty(t, r.str("refreshToken"))
		})
	}
}

func TestPersistence(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	up := src.signUp(t, "keep@example.com")
	src.client(t, "sendOobCode", map[string]any{"requestType": "PASSWORD_RESET", "email": "keep@example.com"})
	src.setConfig(t, map[string]any{"authorizedDomains": []string{"keep.example.com"}})

	tn := src.do(
		t,
		http.MethodPost,
		"/v2/projects/"+src.project+"/tenants",
		adminAuth,
		map[string]any{"displayName": "kept-tenant"},
	)
	require.Equal(t, http.StatusOK, tn.status)

	data := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	dst.project = src.project
	require.NoError(t, dst.backend.Restore(t.Context(), data))

	lk := dst.client(t, "lookup", map[string]any{"idToken": up.str("idToken")})
	require.Equal(t, http.StatusOK, lk.status, "tokens signed before the snapshot still verify")

	in := dst.client(t, "signInWithPassword", map[string]any{"email": "keep@example.com", "password": "secret1"})
	assert.Equal(t, http.StatusOK, in.status, in.body)

	rf := dst.do(
		t,
		http.MethodPost,
		"/v1/token?key=k",
		"",
		map[string]any{"grant_type": "refresh_token", "refresh_token": up.str("refreshToken")},
	)
	assert.Equal(t, http.StatusOK, rf.status, rf.body)

	assert.Len(t, dst.oobCodes(t), 1)

	cfg := dst.do(t, http.MethodGet, "/v2/projects/"+dst.project+"/config", adminAuth, nil)
	assert.Equal(t, []any{"keep.example.com"}, cfg.body["authorizedDomains"])

	list := dst.do(t, http.MethodGet, "/v2/projects/"+dst.project+"/tenants", adminAuth, nil)
	assert.Len(t, list.body["tenants"], 1)

	dst.backend.Reset()

	gone := dst.client(t, "lookup", map[string]any{"idToken": up.str("idToken")})
	assert.Equal(t, http.StatusBadRequest, gone.status)

	assert.Error(t, dst.backend.Restore(t.Context(), []byte(`{"projects":{"p":null},"version":1}`)))
}

func TestGeneratedClientWireShapes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	svc, err := identitytoolkit.NewService(
		t.Context(),
		option.WithEndpoint(e.url+"/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(
			&http.Client{Transport: keyAdder{rt: e.hc.Transport}}),
	)
	require.NoError(t, err)

	up, err := svc.Accounts.SignUp(&identitytoolkit.GoogleCloudIdentitytoolkitV1SignUpRequest{
		Email: "gen@example.com", Password: "secret1",
	}).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(3600), up.ExpiresIn)
	assert.NotEmpty(t, up.IdToken)

	in, err := svc.Accounts.SignInWithPassword(&identitytoolkit.GoogleCloudIdentitytoolkitV1SignInWithPasswordRequest{
		Email: "gen@example.com", Password: "secret1", ReturnSecureToken: true,
	}).Do()
	require.NoError(t, err)
	assert.True(t, in.Registered)
	assert.Equal(t, up.LocalId, in.LocalId)

	lookupReq := &identitytoolkit.GoogleCloudIdentitytoolkitV1GetAccountInfoRequest{IdToken: in.IdToken}
	lk, err := svc.Accounts.Lookup(lookupReq).Do()
	require.NoError(t, err)
	require.Len(t, lk.Users, 1)
	assert.Positive(t, lk.Users[0].CreatedAt)
	assert.Equal(t, "UkVEQUNURUQ=", lk.Users[0].PasswordHash)
	assert.Equal(t, "password", lk.Users[0].ProviderUserInfo[0].ProviderId)

	upd, err := svc.Accounts.Update(&identitytoolkit.GoogleCloudIdentitytoolkitV1SetAccountInfoRequest{
		IdToken: in.IdToken, DisplayName: "Gen", ReturnSecureToken: true,
	}).Do()
	require.NoError(t, err)
	assert.Equal(t, "Gen", upd.DisplayName)
}

type keyAdder struct{ rt http.RoundTripper }

func (k keyAdder) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("X-Goog-Api-Key", "k")

	return k.rt.RoundTrip(r)
}
