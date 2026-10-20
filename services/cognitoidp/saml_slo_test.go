package cognitoidp_test

import (
	"bytes"
	"compress/flate"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func sloEnv(t *testing.T, signing bool, signout string) *samlEnv {
	t.Helper()

	extra := map[string]string{}
	if signout != "" {
		extra["IDPSignout"] = signout
	}

	if signing {
		extra["RequestSigningAlgorithm"] = "rsa-sha256"
	}

	return newSAMLEnvWith(t, false, func(i *samlIDP) { i.slo = true }, extra)
}

// loginSAML signs in through SAML and returns the access token; the session cookie lands in the env jar.
func (e *samlEnv) loginSAML(t *testing.T) string {
	t.Helper()

	reqID, relay, _ := e.start(t)
	doc := e.idp.response(reqID, e.acs(), e.poolID, &samlOpts{})
	code := locationQuery(t, e.post(t, relay, doc)).Get("code")
	require.NotEmpty(t, code)

	resp, body := e.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"authorization_code"}, "code": {code},
		"redirect_uri": {oauthRedirect}, "client_id": {e.clientID},
	}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var tok map[string]any
	require.NoError(t, json.Unmarshal(body, &tok))

	return tok["access_token"].(string)
}

func (e *samlEnv) userInfoStatus(t *testing.T, token string) int {
	t.Helper()

	resp, _ := e.get(t, "/oauth2/userInfo", map[string]string{"Authorization": "Bearer " + token})

	return resp.StatusCode
}

func (e *samlEnv) sloURL() string { return e.srv.URL + "/saml2/logout" }

// idpMessage builds an IdP protocol message root with its Issuer first (so a signature can follow it).
func idpMessage(kind string, attrs map[string]string, after string) (string, string, string, string) {
	var attrStr strings.Builder

	for _, k := range []string{"Destination", "ID", "InResponseTo", "IssueInstant", "NotOnOrAfter", "Version"} {
		if v, ok := attrs[k]; ok {
			attrStr.WriteString(" " + k + `="` + v + `"`)
		}
	}

	open := `<samlp:` + kind + ` xmlns:samlp="` + samlNSProtocol + `"` + attrStr.String() + `>`
	before := `<saml:Issuer xmlns:saml="` + samlNSAssert + `">` + attrs["issuer"] + `</saml:Issuer>`

	return open, before, after, `</samlp:` + kind + `>`
}

type sloOpts struct {
	signKey     *rsa.PrivateKey
	issuer      string
	destination string
	nameID      string
	id          string
	notAfter    time.Duration
	unsigned    bool
}

func (o *sloOpts) fill(i *samlIDP, dest string) {
	if o.signKey == nil {
		o.signKey = i.key
	}

	if o.issuer == "" {
		o.issuer = samlEntityID
	}

	if o.destination == "" {
		o.destination = dest
	}

	if o.nameID == "" {
		o.nameID = samlNameID
	}

	if o.notAfter == 0 {
		o.notAfter = 5 * time.Minute
	}

	if o.id == "" {
		o.id = "_lr" + regexp.MustCompile(`\d+`).FindString(time.Now().Format("150405.000000000"))
	}
}

func (o *sloOpts) attrs() map[string]string {
	now := time.Now().UTC()

	return map[string]string{
		"Destination": o.destination, "ID": o.id, "IssueInstant": now.Format(time.RFC3339), "Version": "2.0",
		"NotOnOrAfter": now.Add(o.notAfter).Format(time.RFC3339), "issuer": o.issuer,
	}
}

// logoutRequestXML returns the IdP's LogoutRequest, enveloped-signed unless o.unsigned.
func (i *samlIDP) logoutRequestXML(dest string, o *sloOpts) string {
	o.fill(i, dest)

	name := `<saml:NameID xmlns:saml="` + samlNSAssert + `">` + o.nameID + `</saml:NameID>`
	open, before, after, closeTag := idpMessage("LogoutRequest", o.attrs(), name)

	if o.unsigned {
		return open + before + after + closeTag
	}

	return i.signed(o.signKey, o.id, open, before, after, closeTag)
}

func (i *samlIDP) logoutResponseXML(dest, inResponseTo string, o *sloOpts) string {
	o.fill(i, dest)

	attrs := o.attrs()
	attrs["InResponseTo"] = inResponseTo
	delete(attrs, "NotOnOrAfter")

	status := `<samlp:Status><samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success">` +
		`</samlp:StatusCode></samlp:Status>`
	open, before, after, closeTag := idpMessage("LogoutResponse", attrs, status)

	if o.unsigned {
		return open + before + after + closeTag
	}

	return i.signed(o.signKey, o.id, open, before, after, closeTag)
}

// redirectQuery deflates xmlMsg and signs the HTTP-Redirect query with key.
func redirectQuery(t *testing.T, key *rsa.PrivateKey, param, xmlMsg, relay string) string {
	t.Helper()

	var buf bytes.Buffer

	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	require.NoError(t, err)
	_, err = w.Write([]byte(xmlMsg))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	signed := param + "=" + url.QueryEscape(base64.StdEncoding.EncodeToString(buf.Bytes())) +
		"&RelayState=" + url.QueryEscape(relay) +
		"&SigAlg=" + url.QueryEscape("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256")
	sum := sha256.Sum256([]byte(signed))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	require.NoError(t, err)

	return signed + "&Signature=" + url.QueryEscape(base64.StdEncoding.EncodeToString(sig))
}

func TestSAMLSPInitiatedLogout(t *testing.T) {
	t.Parallel()

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		resp        *sloOpts
		inResponse  string
		name        string
		signout     string
		wantStatus  int
		wantLogout  bool
		signing     bool
		replayTwice bool
	}{
		{name: "idp logout completes", signout: "true", wantLogout: true, wantStatus: http.StatusFound},
		{name: "signed logout request", signout: "true", signing: true, wantLogout: true, wantStatus: http.StatusFound},
		{name: "signout disabled skips idp", signout: "false", wantStatus: http.StatusFound},
		{
			name: "unsigned logout response", signout: "true", wantLogout: true,
			resp: &sloOpts{unsigned: true}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "logout response signed by wrong key", signout: "true", wantLogout: true,
			resp: &sloOpts{signKey: otherKey}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "logout response for another request", signout: "true", wantLogout: true,
			inResponse: "_other", wantStatus: http.StatusBadRequest,
		},
		{
			name: "logout response wrong issuer", signout: "true", wantLogout: true,
			resp: &sloOpts{issuer: "https://evil.example"}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "logout response wrong destination", signout: "true", wantLogout: true,
			resp: &sloOpts{destination: "https://evil.example/slo"}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "replayed logout response", signout: "true", wantLogout: true,
			wantStatus: http.StatusFound, replayTwice: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := sloEnv(t, tt.signing, tt.signout)
			spKey, _, kerr := cognitoidp.PoolSAMLKeyForTest(env.backend, env.poolID)
			require.NoError(t, kerr)

			env.loginSAML(t)

			resp, _ := env.get(t, "/logout?client_id="+env.clientID+"&logout_uri="+url.QueryEscape(oauthLogout), nil)
			require.Equal(t, http.StatusFound, resp.StatusCode)

			loc, perr := url.Parse(resp.Header.Get("Location"))
			require.NoError(t, perr)

			if !tt.wantLogout {
				assert.Equal(t, oauthLogout, loc.String())

				return
			}

			require.Equal(t, env.idp.srv.URL+"/slo", loc.Scheme+"://"+loc.Host+loc.Path)

			lr := inflate(t, loc.Query().Get("SAMLRequest"))
			assert.Contains(t, lr, "LogoutRequest")
			assert.Contains(t, lr, ">"+samlNameID+"<")
			assert.Contains(t, lr, `Destination="`+env.idp.srv.URL+`/slo"`)

			if tt.signing {
				verifyLogoutSig(t, loc.RawQuery, &spKey.PublicKey)
			}

			id := regexp.MustCompile(`ID="([^"]+)"`).FindStringSubmatch(lr)[1]
			if tt.inResponse != "" {
				id = tt.inResponse
			}

			opts := tt.resp
			if opts == nil {
				opts = &sloOpts{}
			}

			doc := env.idp.logoutResponseXML(env.sloURL(), id, opts)
			post := func() *http.Response {
				r, _ := env.postForm(t, "/saml2/logout", url.Values{
					"SAMLResponse": {base64.StdEncoding.EncodeToString([]byte(doc))},
					"RelayState":   {loc.Query().Get("RelayState")},
				}, nil)

				return r
			}

			got := post()
			require.Equal(t, tt.wantStatus, got.StatusCode)

			if tt.wantStatus == http.StatusFound {
				assert.Equal(t, oauthLogout, got.Header.Get("Location"))
			}

			if tt.replayTwice {
				assert.Equal(t, http.StatusBadRequest, post().StatusCode)
			}
		})
	}
}

func verifyLogoutSig(t *testing.T, rawQuery string, pub *rsa.PublicKey) {
	t.Helper()

	verifyRedirectSig(t, rawQuery, pub)
}

func TestSAMLIdPInitiatedLogout(t *testing.T) {
	t.Parallel()

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		opts       *sloOpts
		name       string
		signout    string
		binding    string
		wantSignOk bool
		replay     bool
	}{
		{name: "post signed", signout: "true", binding: "post", opts: &sloOpts{}, wantSignOk: true},
		{name: "redirect signed", signout: "true", binding: "redirect", opts: &sloOpts{}, wantSignOk: true},
		{name: "replayed request", signout: "true", binding: "post", opts: &sloOpts{}, wantSignOk: true, replay: true},
		{name: "unsigned", signout: "true", binding: "post", opts: &sloOpts{unsigned: true}},
		{name: "wrong key", signout: "true", binding: "post", opts: &sloOpts{signKey: otherKey}},
		{name: "wrong issuer", signout: "true", binding: "post", opts: &sloOpts{issuer: "https://evil.example"}},
		{
			name:    "wrong destination",
			signout: "true",
			binding: "post",
			opts:    &sloOpts{destination: "https://evil.example/"},
		},
		{name: "expired", signout: "true", binding: "post", opts: &sloOpts{notAfter: -time.Hour}},
		{name: "signout disabled", signout: "false", binding: "post", opts: &sloOpts{}},
		{name: "redirect tampered", signout: "true", binding: "redirect-tampered", opts: &sloOpts{}},
		{name: "redirect unsigned", signout: "true", binding: "redirect-unsigned", opts: &sloOpts{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := sloEnv(t, false, tt.signout)
			token := env.loginSAML(t)
			require.Equal(t, http.StatusOK, env.userInfoStatus(t, token))

			send := func() *http.Response { return env.sendLogoutRequest(t, tt.binding, tt.opts) }

			resp := send()

			if !tt.wantSignOk {
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Equal(t, http.StatusOK, env.userInfoStatus(t, token), "session must survive a rejected request")

				return
			}

			assert.Contains(t, []int{http.StatusOK, http.StatusFound}, resp.StatusCode)
			assert.Equal(t, http.StatusUnauthorized, env.userInfoStatus(t, token), "tokens must be revoked")

			if tt.replay {
				assert.Equal(t, http.StatusBadRequest, send().StatusCode)
			}
		})
	}
}

func (e *samlEnv) sendLogoutRequest(t *testing.T, binding string, o *sloOpts) *http.Response {
	t.Helper()

	doc := e.idp.logoutRequestXML(e.sloURL(), o)

	switch binding {
	case "post":
		resp, _ := e.postForm(t, "/saml2/logout", url.Values{
			"SAMLRequest": {base64.StdEncoding.EncodeToString([]byte(doc))}, "RelayState": {"rs1"},
		}, nil)

		return resp
	default:
		key := e.idp.key
		if binding == "redirect-unsigned" {
			q := "SAMLRequest=" + url.QueryEscape(deflateB64(t, doc)) + "&RelayState=rs1"
			resp, _ := e.get(t, "/saml2/logout?"+q, nil)

			return resp
		}

		q := redirectQuery(t, key, "SAMLRequest", doc, "rs1")
		if binding == "redirect-tampered" {
			q = regexp.MustCompile(`RelayState=rs1`).ReplaceAllString(q, "RelayState=evil")
		}

		resp, _ := e.get(t, "/saml2/logout?"+q, nil)

		return resp
	}
}

func deflateB64(t *testing.T, s string) string {
	t.Helper()

	var buf bytes.Buffer

	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	require.NoError(t, err)
	_, err = w.Write([]byte(s))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func TestSAMLStateKindsNotInterchangeable(t *testing.T) {
	t.Parallel()

	t.Run("login state at logout endpoint", func(t *testing.T) {
		t.Parallel()

		env := sloEnv(t, false, "true")
		reqID, relay, _ := env.start(t)
		doc := env.idp.logoutResponseXML(env.sloURL(), reqID, &sloOpts{})

		resp, _ := env.postForm(t, "/saml2/logout", url.Values{
			"SAMLResponse": {base64.StdEncoding.EncodeToString([]byte(doc))}, "RelayState": {relay},
		}, nil)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("logout state at login endpoint", func(t *testing.T) {
		t.Parallel()

		env := sloEnv(t, false, "true")
		env.loginSAML(t)

		resp, _ := env.get(t, "/logout?client_id="+env.clientID+"&logout_uri="+url.QueryEscape(oauthLogout), nil)
		loc, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)

		login := env.idp.response("_x", env.acs(), env.poolID, &samlOpts{})
		post, _ := env.postForm(t, "/saml2/idpresponse", url.Values{
			"SAMLResponse": {login}, "RelayState": {loc.Query().Get("RelayState")},
		}, nil)
		assert.Equal(t, http.StatusBadRequest, post.StatusCode)
	})
}
