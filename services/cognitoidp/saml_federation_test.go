package cognitoidp_test

import (
	"bytes"
	"compress/flate"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"io"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	samlEntityID   = "https://idp.corp.example/entity"
	samlNameID     = "alice@corp.example"
	samlEmailAttr  = "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress"
	samlNSDSig     = "http://www.w3.org/2000/09/xmldsig#"
	samlNSAssert   = "urn:oasis:names:tc:SAML:2.0:assertion"
	samlNSProtocol = "urn:oasis:names:tc:SAML:2.0:protocol"
	samlExcC14N    = "http://www.w3.org/2001/10/xml-exc-c14n#"
)

type samlOpts struct {
	signKey    *rsa.PrivateKey
	mutate     func(string) string
	audience   string
	issuer     string
	inResponse string
	nameID     string
	sign       string
	extraAud   string
	encryptTo  *rsa.PublicKey
	encMode    string
	emailVals  []string
	noDest     bool
	unsolicit  bool
	expires    time.Duration
}

type samlIDP struct {
	srv        *httptest.Server
	key        *rsa.PrivateKey
	cert       []byte
	ssoPost    bool
	slo        bool
	wantSigned bool
}

func newSAMLIDP(t *testing.T) *samlIDP {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "corp idp"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	idp := &samlIDP{key: key, cert: der}
	idp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metadata" {
			_, _ = io.WriteString(w, idp.metadata())

			return
		}

		http.NotFound(w, r)
	}))
	t.Cleanup(idp.srv.Close)

	return idp
}

func (i *samlIDP) metadata() string {
	binding, want := "Redirect", ""
	if i.ssoPost {
		binding = "POST"
	}

	if i.wantSigned {
		want = ` WantAuthnRequestsSigned="true"`
	}

	slo := ""
	if i.slo {
		slo = `<md:SingleLogoutService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="` +
			i.srv.URL + `/slo"/>`
	}

	return `<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="` + samlEntityID + `">` +
		`<md:IDPSSODescriptor` + want + ` protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">` +
		`<md:KeyDescriptor use="signing"><ds:KeyInfo xmlns:ds="` + samlNSDSig + `"><ds:X509Data><ds:X509Certificate>` +
		base64.StdEncoding.EncodeToString(
			i.cert,
		) + `</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor>` +
		slo + `<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-` + binding +
		`" Location="` + i.srv.URL + `/sso"/></md:IDPSSODescriptor></md:EntityDescriptor>`
}

// signed builds open+before+Signature+after+close, signing the element's hand-written exclusive-C14N form.
func (i *samlIDP) signed(key *rsa.PrivateKey, id, open, before, after, closeTag string) string {
	canon := open + before + after + closeTag
	sum := sha256.Sum256([]byte(canon))

	info := `<ds:CanonicalizationMethod Algorithm="` + samlExcC14N + `"></ds:CanonicalizationMethod>` +
		`<ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"></ds:SignatureMethod>` +
		`<ds:Reference URI="#` + id + `"><ds:Transforms>` +
		`<ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"></ds:Transform>` +
		`<ds:Transform Algorithm="` + samlExcC14N + `"></ds:Transform></ds:Transforms>` +
		`<ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"></ds:DigestMethod>` +
		`<ds:DigestValue>` + base64.StdEncoding.EncodeToString(sum[:]) + `</ds:DigestValue></ds:Reference>`

	canonInfo := `<ds:SignedInfo xmlns:ds="` + samlNSDSig + `">` + info + `</ds:SignedInfo>`
	infoSum := sha256.Sum256([]byte(canonInfo))

	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, infoSum[:])
	if err != nil {
		panic(err)
	}

	sigXML := `<ds:Signature xmlns:ds="` + samlNSDSig + `"><ds:SignedInfo>` + info + `</ds:SignedInfo>` +
		`<ds:SignatureValue>` + base64.StdEncoding.EncodeToString(sig) + `</ds:SignatureValue></ds:Signature>`

	return open + before + sigXML + after + closeTag
}

func (i *samlIDP) response(requestID, acs, poolID string, o *samlOpts) string {
	now := time.Now().UTC()
	stamp := func(d time.Duration) string { return now.Add(d).Format(time.RFC3339) }

	o.fill(requestID, poolID, i.key)

	body := `<saml:Subject><saml:NameID>` + o.nameID + `</saml:NameID>` +
		`<saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">` +
		`<saml:SubjectConfirmationData` + o.irt(o.inResponse) + ` NotOnOrAfter="` + stamp(o.expires) +
		`" Recipient="` + acs + `"></saml:SubjectConfirmationData></saml:SubjectConfirmation></saml:Subject>` +
		`<saml:Conditions NotBefore="` + stamp(-time.Minute) + `" NotOnOrAfter="` + stamp(o.expires) + `">` +
		`<saml:AudienceRestriction><saml:Audience>` + o.audience + `</saml:Audience></saml:AudienceRestriction>` +
		o.extraRestriction() +
		`</saml:Conditions><saml:AttributeStatement><saml:Attribute Name="` + samlEmailAttr + `">` +
		o.emailXML() + `</saml:Attribute></saml:AttributeStatement>`

	aOpen := `<saml:Assertion xmlns:saml="` + samlNSAssert + `" ID="_assert1" IssueInstant="` + stamp(0) +
		`" Version="2.0">`
	aBefore := `<saml:Issuer>` + o.issuer + `</saml:Issuer>`
	aClose := `</saml:Assertion>`

	dest := ` Destination="` + acs + `"`
	if o.noDest {
		dest = ""
	}

	rOpen := `<samlp:Response xmlns:samlp="` + samlNSProtocol + `"` + dest +
		` ID="_resp1"` + o.irt(requestID) + ` IssueInstant="` + stamp(0) + `" Version="2.0">`
	status := `<samlp:Status><samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success">` +
		`</samlp:StatusCode></samlp:Status>`

	var doc string

	switch o.sign {
	case "assertion":
		signedA := i.signed(o.signKey, "_assert1", aOpen, aBefore, body, aClose)
		doc = rOpen + status + o.wrap(signedA) + `</samlp:Response>`
	case "response":
		doc = i.signed(o.signKey, "_resp1", rOpen, status, o.wrap(aOpen+aBefore+body+aClose), `</samlp:Response>`)
	default:
		doc = rOpen + status + o.wrap(aOpen+aBefore+body+aClose) + `</samlp:Response>`
	}

	if o.mutate != nil {
		doc = o.mutate(doc)
	}

	return base64.StdEncoding.EncodeToString([]byte(doc))
}

func (o *samlOpts) emailXML() string {
	if len(o.emailVals) == 0 {
		return `<saml:AttributeValue>alice@corp.example</saml:AttributeValue>`
	}

	var sb strings.Builder
	for _, v := range o.emailVals {
		sb.WriteString(`<saml:AttributeValue>` + v + `</saml:AttributeValue>`)
	}

	return sb.String()
}

func (o *samlOpts) irt(id string) string {
	if o.unsolicit {
		return ""
	}

	return ` InResponseTo="` + id + `"`
}

func (o *samlOpts) wrap(assertion string) string {
	if o.encryptTo == nil {
		return assertion
	}

	return encryptAssertionXML(o.encryptTo, assertion, o.encMode)
}

func (o *samlOpts) extraRestriction() string {
	if o.extraAud == "" {
		return ""
	}

	return `<saml:AudienceRestriction><saml:Audience>` + o.extraAud + `</saml:Audience></saml:AudienceRestriction>`
}

func (o *samlOpts) fill(requestID, poolID string, def *rsa.PrivateKey) {
	if o.signKey == nil {
		o.signKey = def
	}

	if o.audience == "" {
		o.audience = "urn:amazon:cognito:sp:" + poolID
	}

	if o.issuer == "" {
		o.issuer = samlEntityID
	}

	if o.extraAud == "self" {
		o.extraAud = o.audience
	}

	if o.inResponse == "" {
		o.inResponse = requestID
	}

	if o.nameID == "" {
		o.nameID = samlNameID
	}

	if o.sign == "" {
		o.sign = "assertion"
	}

	if o.expires == 0 {
		o.expires = 5 * time.Minute
	}
}

type samlEnv struct {
	*oauthEnv

	idp      *samlIDP
	clientID string
}

func newSAMLEnv(t *testing.T, byURL bool) *samlEnv {
	t.Helper()

	return newSAMLEnvWith(t, byURL, nil, nil)
}

// newSAMLEnvWith builds a SAML pool; tweak adjusts the test IdP and extra is merged into ProviderDetails.
func newSAMLEnvWith(t *testing.T, byURL bool, tweak func(*samlIDP), extra map[string]string) *samlEnv {
	t.Helper()

	idp := newSAMLIDP(t)
	if tweak != nil {
		tweak(idp)
	}

	env := newOAuthEnv(t)

	details := map[string]string{detailMetadataFile: idp.metadata()}
	if byURL {
		details = map[string]string{"MetadataURL": idp.srv.URL + "/metadata"}
	}

	maps.Copy(details, extra)

	_, err := env.backend.CreateIdentityProviderFull(env.poolID, fedProvider, "SAML", details,
		map[string]string{"email": samlEmailAttr}, nil)
	require.NoError(t, err)

	client, err := env.backend.CreateUserPoolClientWithOpts(env.poolID, "saml", cognitoidp.UserPoolClientOptions{
		AllowedOAuthFlows: []string{"code"}, AllowedOAuthFlowsUserPoolClient: true,
		AllowedOAuthScopes: []string{"openid", "email", "profile"}, CallbackURLs: []string{oauthRedirect},
		LogoutURLs:                 []string{oauthLogout},
		SupportedIdentityProviders: []string{fedProvider},
	})
	require.NoError(t, err)

	return &samlEnv{oauthEnv: env, idp: idp, clientID: client.ClientID}
}

const detailMetadataFile = "MetadataFile"

// start runs authorize and returns the IdP redirect's request ID, relay state and decoded AuthnRequest.
func (e *samlEnv) start(t *testing.T) (string, string, string) {
	t.Helper()

	id, relay, xmlReq, _ := e.startWith(t, nil)

	return id, relay, xmlReq
}

func (e *samlEnv) startWith(t *testing.T, hdr map[string]string) (string, string, string, *http.Response) {
	t.Helper()

	q := authorizeQuery(e.clientID, "")
	q.Set("identity_provider", fedProvider)

	resp, _ := e.get(t, "/oauth2/authorize?"+q.Encode(), hdr)
	require.Equal(t, http.StatusFound, resp.StatusCode)

	loc, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, e.idp.srv.URL+"/sso", loc.Scheme+"://"+loc.Host+loc.Path, loc.String())

	raw, err := base64.StdEncoding.DecodeString(loc.Query().Get("SAMLRequest"))
	require.NoError(t, err)

	xmlReq, err := io.ReadAll(flate.NewReader(bytes.NewReader(raw)))
	require.NoError(t, err)

	m := regexp.MustCompile(`ID="([^"]+)"`).FindStringSubmatch(string(xmlReq))
	require.Len(t, m, 2)

	return m[1], loc.Query().Get("RelayState"), string(xmlReq), resp
}

func (e *samlEnv) acs() string { return e.srv.URL + "/saml2/idpresponse" }

func (e *samlEnv) post(t *testing.T, relay, samlResponse string) *http.Response {
	t.Helper()

	resp, _ := e.postForm(
		t,
		"/saml2/idpresponse",
		url.Values{"SAMLResponse": {samlResponse}, "RelayState": {relay}},
		nil,
	)

	return resp
}

func TestSAMLFederation(t *testing.T) {
	t.Parallel()

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		name      string
		errSubstr string
		opts      samlOpts
		byURL     bool
		wantOK    bool
	}{
		{name: "signed assertion", wantOK: true},
		{name: "signed response", opts: samlOpts{sign: "response"}, wantOK: true},
		{name: "metadata url", byURL: true, wantOK: true},
		{
			name: "unsigned root attrs ignored when only assertion signed",
			opts: samlOpts{mutate: func(s string) string {
				s = regexp.MustCompile(`Destination="[^"]*"`).ReplaceAllString(s, `Destination="https://evil.example/"`)

				s = regexp.MustCompile(`(<samlp:Response[^>]*?) InResponseTo="[^"]*"`).
					ReplaceAllString(s, `${1} InResponseTo="_forged"`)

				return strings.Replace(s, "status:Success", "status:Responder", 1)
			}},
			wantOK: true,
		},
		{
			name: "signed response without destination", errSubstr: "destination",
			opts: samlOpts{sign: "response", noDest: true},
		},
		{name: "every restriction names the sp", opts: samlOpts{extraAud: "self"}, wantOK: true},
		{
			name: "second restriction missing audience", errSubstr: "audience",
			opts: samlOpts{extraAud: "urn:amazon:cognito:sp:other"},
		},
		{name: "unsigned", opts: samlOpts{sign: "none"}, errSubstr: "not signed"},
		{name: "wrong key", opts: samlOpts{signKey: otherKey}, errSubstr: "does not verify"},
		{
			name: "tampered name id",
			opts: samlOpts{mutate: func(s string) string {
				return regexp.MustCompile(`alice@corp`).ReplaceAllString(s, "mallory@corp")
			}},
			errSubstr: "digest mismatch",
		},
		{name: "expired", opts: samlOpts{expires: -time.Hour}, errSubstr: "subject confirmation"},
		{name: "wrong audience", opts: samlOpts{audience: "urn:amazon:cognito:sp:other"}, errSubstr: "audience"},
		{name: "wrong issuer", opts: samlOpts{issuer: "https://evil.example"}, errSubstr: "issuer"},
		{name: "wrong request", opts: samlOpts{inResponse: "_other"}, errSubstr: "subject confirmation"},
		{
			name: "second assertion",
			opts: samlOpts{mutate: func(s string) string {
				return regexp.MustCompile(`</samlp:Status>`).
					ReplaceAllString(s, `</samlp:Status><saml:Assertion xmlns:saml="`+
						samlNSAssert+`" ID="_evil"><saml:Issuer>x</saml:Issuer></saml:Assertion>`)
			}},
			errSubstr: "exactly one assertion",
		},
		{
			name: "doctype",
			opts: samlOpts{mutate: func(s string) string {
				return `<!DOCTYPE x [<!ENTITY e "v">]>` + s
			}},
			errSubstr: "directives",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMLEnv(t, tt.byURL)
			reqID, relay, xmlReq := env.start(t)
			assert.Contains(t, xmlReq, "urn:amazon:cognito:sp:"+env.poolID)
			assert.Contains(t, xmlReq, env.acs())

			opts := tt.opts
			doc := env.idp.response(reqID, env.acs(), env.poolID, &opts)
			raw, derr := base64.StdEncoding.DecodeString(doc)
			require.NoError(t, derr)

			verr := cognitoidp.ValidateSAMLResponseForTest(
				raw, []byte(env.idp.metadata()), env.poolID, env.acs(), reqID, time.Now())
			if tt.wantOK {
				require.NoError(t, verr)
			} else {
				require.ErrorContains(t, verr, tt.errSubstr)
			}

			resp := env.post(t, relay, doc)
			q := locationQuery(t, resp)

			if !tt.wantOK {
				assert.Equal(t, "invalid_request", q.Get("error"))
				assert.Equal(t, "SAML response validation failed", q.Get("error_description"))
				assert.Equal(t, "st8", q.Get("state"))
				assert.Empty(t, q.Get("code"))

				users, lerr := env.backend.ListUsers(env.poolID)
				require.NoError(t, lerr)

				for _, u := range users {
					assert.NotEqual(t, fedProvider+"_"+samlNameID, u.Username)
				}

				return
			}

			require.NotEmpty(t, q.Get("code"), q.Encode())

			form, body := env.postForm(t, "/oauth2/token", url.Values{
				"grant_type": {"authorization_code"}, "code": {q.Get("code")},
				"redirect_uri": {oauthRedirect}, "client_id": {env.clientID},
			}, nil)
			require.Equal(t, http.StatusOK, form.StatusCode, string(body))

			var tok map[string]any
			require.NoError(t, json.Unmarshal(body, &tok))

			claims := env.verifyAgainstJWKS(t, tok["id_token"].(string))
			assert.Equal(t, fedProvider+"_"+samlNameID, claims["cognito:username"])
			assert.Equal(t, samlNameID, claims["email"])

			idents := claims["identities"].([]any)
			require.Len(t, idents, 1)
			assert.Equal(t, "SAML", idents[0].(map[string]any)["providerType"])
			assert.Equal(t, samlEntityID, idents[0].(map[string]any)["issuer"])
		})
	}
}

func TestSAMLMultiValuedAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		vals []string
	}{
		{name: "single", vals: []string{"alice@corp.example"}, want: "alice@corp.example"},
		{name: "multi", vals: []string{"a", "b", "c"}, want: "[a,b,c]"},
		{name: "multi encoded", vals: []string{"a b", "x@y.z", "k_1-*"}, want: "[a+b,x%40y.z,k_1-*]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMLEnv(t, false)
			reqID, relay, _ := env.start(t)

			opts := samlOpts{emailVals: tt.vals}
			doc := env.idp.response(reqID, env.acs(), env.poolID, &opts)
			q := locationQuery(t, env.post(t, relay, doc))
			require.NotEmpty(t, q.Get("code"), q.Encode())

			resp, body := env.postForm(t, "/oauth2/token", url.Values{
				"grant_type": {"authorization_code"}, "code": {q.Get("code")},
				"redirect_uri": {oauthRedirect}, "client_id": {env.clientID},
			}, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

			var tok map[string]any
			require.NoError(t, json.Unmarshal(body, &tok))

			claims := env.verifyAgainstJWKS(t, tok["id_token"].(string))
			assert.Equal(t, tt.want, claims["email"])
		})
	}
}

func TestSAMLReplayAndBadState(t *testing.T) {
	t.Parallel()

	env := newSAMLEnv(t, false)
	reqID, relay, _ := env.start(t)
	doc := env.idp.response(reqID, env.acs(), env.poolID, &samlOpts{})

	first := locationQuery(t, env.post(t, relay, doc))
	require.NotEmpty(t, first.Get("code"))

	resp, _ := env.postForm(t, "/saml2/idpresponse",
		url.Values{"SAMLResponse": {doc}, "RelayState": {relay}}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "replayed relay state must be rejected")

	resp, _ = env.postForm(t, "/saml2/idpresponse", url.Values{"SAMLResponse": {doc}, "RelayState": {"nope"}}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func bindingCookie(resp *http.Response) *http.Cookie {
	for _, c := range resp.Cookies() {
		if strings.HasPrefix(c.Name, "gs_idp_") {
			return c
		}
	}

	return nil
}

// bareDo sends req on a cookie-less client that does not follow redirects.
func bareDo(t *testing.T, req *http.Request) *http.Response {
	t.Helper()

	c := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	resp, err := c.Do(req)
	require.NoError(t, err)

	_ = resp.Body.Close()

	return resp
}

func TestSAMLBrowserBinding(t *testing.T) {
	t.Parallel()

	https := map[string]string{"X-Forwarded-Proto": "https"}

	tests := []struct {
		name       string
		hdr        map[string]string
		cookie     string
		wantStatus int
		wantNone   bool
		wantSecure bool
	}{
		{name: "http valid cookie", cookie: "good", wantStatus: http.StatusFound},
		{name: "http no cookie falls back", cookie: "", wantStatus: http.StatusFound},
		{name: "http wrong cookie", cookie: "bad", wantStatus: http.StatusBadRequest},
		{
			name: "https valid cookie", hdr: https, cookie: "good", wantStatus: http.StatusFound,
			wantNone: true, wantSecure: true,
		},
		{
			name: "https no cookie", hdr: https, cookie: "", wantStatus: http.StatusBadRequest,
			wantNone: true, wantSecure: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMLEnv(t, false)
			reqID, relay, _, resp := env.startWith(t, tt.hdr)

			ck := bindingCookie(resp)
			require.NotNil(t, ck)
			assert.True(t, ck.HttpOnly)
			assert.Equal(t, "/saml2/idpresponse", ck.Path)
			assert.Equal(t, tt.wantSecure, ck.Secure)

			if tt.wantNone {
				assert.Equal(t, http.SameSiteNoneMode, ck.SameSite)
			} else {
				assert.Equal(t, http.SameSiteLaxMode, ck.SameSite)
			}

			doc := env.idp.response(reqID, env.acs(), env.poolID, &samlOpts{})
			req, err := http.NewRequest(http.MethodPost, env.srv.URL+"/saml2/idpresponse",
				strings.NewReader(url.Values{"SAMLResponse": {doc}, "RelayState": {relay}}.Encode()))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			for k, v := range tt.hdr {
				req.Header.Set(k, v)
			}

			switch tt.cookie {
			case "good":
				req.AddCookie(ck)
			case "bad":
				req.AddCookie(&http.Cookie{Name: ck.Name, Value: "forged"})
			}

			assert.Equal(t, tt.wantStatus, bareDo(t, req).StatusCode)
		})
	}
}

func TestSAMLMetadataKeyUse(t *testing.T) {
	t.Parallel()

	idp := newSAMLIDP(t)
	cert := base64.StdEncoding.EncodeToString(idp.cert)

	build := func(kd string) []byte {
		return []byte(`<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="e">` +
			`<md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">` + kd +
			`<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://i/sso"/>` +
			`</md:IDPSSODescriptor></md:EntityDescriptor>`)
	}

	keyDesc := func(use string) string {
		attr := ""
		if use != "" {
			attr = ` use="` + use + `"`
		}

		return `<md:KeyDescriptor` + attr + `><ds:KeyInfo xmlns:ds="` + samlNSDSig + `"><ds:X509Data>` +
			`<ds:X509Certificate>` + cert + `</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor>`
	}

	tests := []struct {
		name    string
		kd      string
		want    int
		wantErr bool
	}{
		{name: "signing", kd: keyDesc("signing"), want: 1},
		{name: "unspecified use", kd: keyDesc(""), want: 1},
		{name: "encryption only", kd: keyDesc("encryption"), wantErr: true},
		{name: "encryption ignored beside signing", kd: keyDesc("encryption") + keyDesc("signing"), want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n, err := cognitoidp.SAMLSigningKeyCount(build(tt.kd))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, n)
		})
	}
}
