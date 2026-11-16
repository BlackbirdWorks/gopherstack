package gcpidentitytoolkit_test

import (
	"encoding/base64"
	"encoding/json/v2"
	"maps"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (e *env) idpPath(coll, id string) string {
	p := "/v2/projects/" + e.project + "/" + coll
	if id != "" {
		p += "/" + id
	}

	return p
}

func TestIdpConfigCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		patch   map[string]any
		name    string
		coll    string
		idParam string
		id      string
		field   string
		want    string
		badID   string
	}{
		{
			name:    "default supported",
			coll:    "defaultSupportedIdpConfigs",
			idParam: "defaultSupportedIdpId",
			id:      "google.com",
			body: map[string]any{
				"clientId":     "cid",
				"clientSecret": "s",
				"enabled":      true,
			},
			field: "clientId",
			want:  "cid2",
			patch: map[string]any{"clientId": "cid2"},
			badID: "nonsense.com",
		},
		{
			name: "oauth", coll: "oauthIdpConfigs", idParam: "oauthIdpConfigId", id: "oidc.acme",
			body:  map[string]any{"clientId": "cid", "issuer": "https://issuer.example", "enabled": true},
			field: "displayName", want: "Acme", patch: map[string]any{"displayName": "Acme"}, badID: "acme",
		},
		{
			name: "saml", coll: "inboundSamlConfigs", idParam: "inboundSamlConfigId", id: "saml.acme",
			body: map[string]any{
				"enabled":   true,
				"idpConfig": map[string]any{"idpEntityId": "idp", "ssoUrl": "https://sso.example/login"},
				"spConfig":  map[string]any{"spEntityId": "sp", "callbackUri": "https://app.example/cb"},
			},
			field: "displayName", want: "Acme SAML", patch: map[string]any{"displayName": "Acme SAML"}, badID: "acme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			create := func(id string) result {
				return e.do(t, http.MethodPost, e.idpPath(tt.coll, "")+"?"+tt.idParam+"="+id, adminAuth, tt.body)
			}

			r := create(tt.id)
			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.Equal(t, "projects/"+e.project+"/"+tt.coll+"/"+tt.id, r.str("name"))

			assert.Equal(t, http.StatusConflict, create(tt.id).status)
			assert.Equal(t, http.StatusBadRequest, create(tt.badID).status)

			r = e.do(t, http.MethodPatch, e.idpPath(tt.coll, tt.id)+"?updateMask="+tt.field, adminAuth, tt.patch)
			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.Equal(t, tt.want, r.str(tt.field))

			r = e.do(t, http.MethodGet, e.idpPath(tt.coll, tt.id), adminAuth, nil)
			assert.Equal(t, tt.want, r.str(tt.field))

			r = e.do(t, http.MethodGet, e.idpPath(tt.coll, ""), adminAuth, nil)
			require.Equal(t, http.StatusOK, r.status)
			assert.Len(t, r.body[tt.coll], 1)

			assert.Equal(t, http.StatusOK, e.do(t, http.MethodDelete, e.idpPath(tt.coll, tt.id), adminAuth, nil).status)
			assert.Equal(
				t,
				http.StatusNotFound,
				e.do(t, http.MethodGet, e.idpPath(tt.coll, tt.id), adminAuth, nil).status,
			)
			assert.Equal(t, http.StatusUnauthorized, e.do(t, http.MethodGet, e.idpPath(tt.coll, ""), "", nil).status)
		})
	}
}

func TestIdpConfigTenantScope(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tr := e.do(
		t,
		http.MethodPost,
		"/v2/projects/"+e.project+"/tenants",
		adminAuth,
		map[string]any{"displayName": "tenant-one"},
	)
	require.Equal(t, http.StatusOK, tr.status, tr.body)

	tid := tr.str("name")[len("projects/"+e.project+"/tenants/"):]
	path := "/v2/projects/" + e.project + "/tenants/" + tid + "/oauthIdpConfigs"

	r := e.do(t, http.MethodPost, path+"?oauthIdpConfigId=oidc.t", adminAuth,
		map[string]any{"clientId": "c", "issuer": "https://i.example"})
	require.Equal(t, http.StatusOK, r.status, r.body)
	assert.Equal(t, "projects/"+e.project+"/tenants/"+tid+"/oauthIdpConfigs/oidc.t", r.str("name"))

	assert.Equal(
		t,
		http.StatusNotFound,
		e.do(t, http.MethodGet, e.idpPath("oauthIdpConfigs", "oidc.t"), adminAuth, nil).status,
	)
}

func fakeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()

	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	pl, err := json.Marshal(claims, jsonx.V1())
	require.NoError(t, err)

	return hdr + "." + base64.RawURLEncoding.EncodeToString(pl) + "."
}

func (e *env) enableGoogle(t *testing.T) {
	t.Helper()

	r := e.do(
		t,
		http.MethodPost,
		e.idpPath("defaultSupportedIdpConfigs", "")+"?defaultSupportedIdpId=google.com",
		adminAuth,
		map[string]any{"clientId": "cid", "clientSecret": "s", "enabled": true},
	)
	require.Equal(t, http.StatusOK, r.status, r.body)
}

func (e *env) signInIdp(t *testing.T, body url.Values, extra map[string]any) result {
	t.Helper()

	req := map[string]any{"requestUri": "http://localhost", "postBody": body.Encode(), "returnSecureToken": true}
	maps.Copy(req, extra)

	return e.client(t, "signInWithIdp", req)
}

func TestSignInWithIdpGoogle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.enableGoogle(t)

	tok := fakeJWT(t, map[string]any{"sub": "g-1", "email": "Ann@Example.com", "email_verified": true, "name": "Ann"})
	body := url.Values{"id_token": {tok}, "providerId": {"google.com"}}

	first := e.signInIdp(t, body, map[string]any{"returnIdpCredential": true})
	require.Equal(t, http.StatusOK, first.status, first.body)
	assert.Equal(t, true, first.body["isNewUser"])
	assert.Equal(t, "ann@example.com", first.str("email"))
	assert.Equal(t, "google.com", first.str("providerId"))
	assert.Equal(t, tok, first.str("oauthIdToken"))
	assert.NotEmpty(t, first.str("idToken"))
	assert.NotEmpty(t, first.str("refreshToken"))

	second := e.signInIdp(t, body, nil)
	require.Equal(t, http.StatusOK, second.status, second.body)
	assert.Nil(t, second.body["isNewUser"])
	assert.Equal(t, first.str("localId"), second.str("localId"))

	lk := e.client(t, "lookup", map[string]any{"idToken": second.str("idToken")})
	users, _ := lk.body["users"].([]any)
	require.Len(t, users, 1)

	infos, _ := users[0].(map[string]any)["providerUserInfo"].([]any)
	require.Len(t, infos, 1)
	assert.Equal(t, "google.com", infos[0].(map[string]any)["providerId"])
}

func TestSignInWithIdpErrors(t *testing.T) {
	t.Parallel()

	good := fakeJWT(t, map[string]any{"sub": "x", "email": "x@example.com"})
	tests := []struct {
		name string
		body url.Values
		want string
		cfg  bool
	}{
		{"unconfigured", url.Values{"id_token": {good}, "providerId": {"google.com"}}, "OPERATION_NOT_ALLOWED", false},
		{
			"no provider",
			url.Values{"id_token": {good}},
			"INVALID_IDP_RESPONSE : The post body carries no providerId.",
			true,
		},
		{
			"no token",
			url.Values{"providerId": {"google.com"}},
			"INVALID_IDP_RESPONSE : The post body carries no id_token or access_token.",
			true,
		},
		{
			"no subject",
			url.Values{"id_token": {fakeJWT(t, map[string]any{"email": "a@b.co"})}, "providerId": {"google.com"}},
			"INVALID_IDP_RESPONSE : The credential carries no subject.",
			true,
		},
		{
			"expired",
			url.Values{"id_token": {fakeJWT(t, map[string]any{"sub": "x", "exp": 1})}, "providerId": {"google.com"}},
			"INVALID_IDP_RESPONSE : The credential has expired.",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.cfg {
				e.enableGoogle(t)
			}

			r := e.signInIdp(t, tt.body, nil)
			assert.Equal(t, http.StatusBadRequest, r.status)
			assert.Contains(t, r.errMsg(), tt.want)
		})
	}
}

func TestSignInWithIdpAccountLinking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		verified     bool
		wantConfirm  bool
		wantSameUser bool
	}{
		{"verified email links", true, false, true},
		{"unverified email needs confirmation", false, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.enableGoogle(t)

			existing := e.signUp(t, "link@example.com")

			tok := fakeJWT(t, map[string]any{"sub": "g-9", "email": "link@example.com", "email_verified": tt.verified})
			r := e.signInIdp(t, url.Values{"id_token": {tok}, "providerId": {"google.com"}}, nil)
			require.Equal(t, http.StatusOK, r.status, r.body)

			if tt.wantConfirm {
				assert.Equal(t, true, r.body["needConfirmation"])
				assert.Empty(t, r.str("idToken"))

				return
			}

			assert.Equal(t, existing.str("localId"), r.str("localId"))
			assert.Equal(t, true, r.body["emailVerified"])
		})
	}
}

func TestSignInWithIdpLinkToSignedInUser(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.enableGoogle(t)

	a, b := e.signUp(t, "a@example.com"), e.signUp(t, "b@example.com")
	body := url.Values{"id_token": {fakeJWT(t, map[string]any{"sub": "g-5"})}, "providerId": {"google.com"}}

	r := e.signInIdp(t, body, map[string]any{"idToken": a.str("idToken")})
	require.Equal(t, http.StatusOK, r.status, r.body)
	assert.Equal(t, a.str("localId"), r.str("localId"))

	r = e.signInIdp(t, body, map[string]any{"idToken": b.str("idToken")})
	assert.Equal(t, http.StatusBadRequest, r.status)
	assert.Equal(t, "FEDERATED_USER_ID_ALREADY_IN_USE", r.errMsg())
}

func TestSignInWithIdpOIDC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		claims map[string]any
		name   string
		want   int
	}{
		{
			name: "matching", want: http.StatusOK,
			claims: map[string]any{"iss": "https://issuer.example", "aud": "cid", "sub": "o1"},
		},
		{
			name: "audience list", want: http.StatusOK,
			claims: map[string]any{"iss": "https://issuer.example", "aud": []string{"x", "cid"}, "sub": "o1"},
		},
		{
			name: "wrong issuer", want: http.StatusBadRequest,
			claims: map[string]any{"iss": "https://evil.example", "aud": "cid", "sub": "o1"},
		},
		{
			name: "wrong audience", want: http.StatusBadRequest,
			claims: map[string]any{"iss": "https://issuer.example", "aud": "other", "sub": "o1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.do(t, http.MethodPost, e.idpPath("oauthIdpConfigs", "")+"?oauthIdpConfigId=oidc.acme", adminAuth,
				map[string]any{"clientId": "cid", "issuer": "https://issuer.example", "enabled": true})
			require.Equal(t, http.StatusOK, r.status, r.body)

			out := e.signInIdp(t, url.Values{"id_token": {fakeJWT(t, tt.claims)}, "providerId": {"oidc.acme"}}, nil)
			assert.Equal(t, tt.want, out.status, out.body)
		})
	}
}

func samlBody(issuer, audience string, notAfter time.Time) string {
	const (
		respNS = `<Response xmlns="urn:oasis:names:tc:SAML:2.0:protocol">`
		asrtNS = `<Assertion xmlns="urn:oasis:names:tc:SAML:2.0:assertion">`
		attr   = `<Attribute Name="name"><AttributeValue>Sam Saml</AttributeValue></Attribute>`
	)

	xmlDoc := respNS + asrtNS +
		`<Issuer>` + issuer + `</Issuer><Subject><NameID>sam@example.com</NameID></Subject>` +
		`<Conditions NotOnOrAfter="` + notAfter.UTC().
		Format(time.RFC3339) +
		`"><AudienceRestriction><Audience>` + audience +
		`</Audience></AudienceRestriction></Conditions><AttributeStatement>` +
		attr + `</AttributeStatement></Assertion></Response>`

	return base64.StdEncoding.EncodeToString([]byte(xmlDoc))
}

func TestSignInWithIdpSAML(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour)
	tests := []struct {
		name string
		resp string
		want int
	}{
		{"valid", samlBody("idp", "sp", future), http.StatusOK},
		{"wrong issuer", samlBody("other", "sp", future), http.StatusBadRequest},
		{"wrong audience", samlBody("idp", "other", future), http.StatusBadRequest},
		{"expired", samlBody("idp", "sp", time.Now().Add(-time.Hour)), http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.do(
				t,
				http.MethodPost,
				e.idpPath("inboundSamlConfigs", "")+"?inboundSamlConfigId=saml.acme",
				adminAuth,
				map[string]any{
					"enabled":   true,
					"idpConfig": map[string]any{"idpEntityId": "idp", "ssoUrl": "https://sso.example/login"},
					"spConfig":  map[string]any{"spEntityId": "sp", "callbackUri": "https://app.example/cb"},
				},
			)
			require.Equal(t, http.StatusOK, r.status, r.body)

			out := e.signInIdp(t, url.Values{"SAMLResponse": {tt.resp}, "providerId": {"saml.acme"}}, nil)
			require.Equal(t, tt.want, out.status, out.body)

			if tt.want == http.StatusOK {
				assert.Equal(t, "sam@example.com", out.str("email"))
				assert.Equal(t, "Sam Saml", out.str("displayName"))
			}
		})
	}
}

func TestCreateAuthURIFederated(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	body := map[string]any{"providerId": "google.com", "continueUri": "http://localhost/cb"}

	assert.Equal(t, "OPERATION_NOT_ALLOWED", e.client(t, "createAuthUri", body).errMsg()[:21])

	e.enableGoogle(t)

	r := e.client(t, "createAuthUri", body)
	require.Equal(t, http.StatusOK, r.status, r.body)
	assert.Equal(t, "google.com", r.str("providerId"))
	assert.NotEmpty(t, r.str("sessionId"))

	u, err := url.Parse(r.str("authUri"))
	require.NoError(t, err)
	assert.Equal(t, "accounts.google.com", u.Host)
	assert.Equal(t, "cid", u.Query().Get("client_id"))
	assert.Equal(t, "http://localhost/cb", u.Query().Get("redirect_uri"))
	assert.Equal(t, r.str("sessionId"), u.Query().Get("state"))

	delete(body, "continueUri")
	assert.Equal(t, "MISSING_CONTINUE_URI", e.client(t, "createAuthUri", body).errMsg())
}

func TestIdpConfigSurvivesRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.enableGoogle(t)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))

	dst.project = src.project
	r := dst.do(t, http.MethodGet, dst.idpPath("defaultSupportedIdpConfigs", "google.com"), adminAuth, nil)
	assert.Equal(t, "cid", r.str("clientId"))
}
