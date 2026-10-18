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
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
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
	expires    time.Duration
}

type samlIDP struct {
	srv  *httptest.Server
	key  *rsa.PrivateKey
	cert []byte
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
	return `<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="` + samlEntityID + `">` +
		`<md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">` +
		`<md:KeyDescriptor use="signing"><ds:KeyInfo xmlns:ds="` + samlNSDSig + `"><ds:X509Data><ds:X509Certificate>` +
		base64.StdEncoding.EncodeToString(
			i.cert,
		) + `</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor>` +
		`<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="` +
		i.srv.URL + `/sso"/></md:IDPSSODescriptor></md:EntityDescriptor>`
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
		`<saml:SubjectConfirmationData InResponseTo="` + o.inResponse + `" NotOnOrAfter="` + stamp(o.expires) +
		`" Recipient="` + acs + `"></saml:SubjectConfirmationData></saml:SubjectConfirmation></saml:Subject>` +
		`<saml:Conditions NotBefore="` + stamp(-time.Minute) + `" NotOnOrAfter="` + stamp(o.expires) + `">` +
		`<saml:AudienceRestriction><saml:Audience>` + o.audience + `</saml:Audience></saml:AudienceRestriction>` +
		`</saml:Conditions><saml:AttributeStatement><saml:Attribute Name="` + samlEmailAttr + `">` +
		`<saml:AttributeValue>alice@corp.example</saml:AttributeValue></saml:Attribute></saml:AttributeStatement>`

	aOpen := `<saml:Assertion xmlns:saml="` + samlNSAssert + `" ID="_assert1" IssueInstant="` + stamp(0) +
		`" Version="2.0">`
	aBefore := `<saml:Issuer>` + o.issuer + `</saml:Issuer>`
	aClose := `</saml:Assertion>`

	rOpen := `<samlp:Response xmlns:samlp="` + samlNSProtocol + `" Destination="` + acs +
		`" ID="_resp1" InResponseTo="` + requestID + `" IssueInstant="` + stamp(0) + `" Version="2.0">`
	status := `<samlp:Status><samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success">` +
		`</samlp:StatusCode></samlp:Status>`

	var doc string

	switch o.sign {
	case "assertion":
		doc = rOpen + status + i.signed(o.signKey, "_assert1", aOpen, aBefore, body, aClose) + `</samlp:Response>`
	case "response":
		doc = i.signed(o.signKey, "_resp1", rOpen, status, aOpen+aBefore+body+aClose, `</samlp:Response>`)
	default:
		doc = rOpen + status + aOpen + aBefore + body + aClose + `</samlp:Response>`
	}

	if o.mutate != nil {
		doc = o.mutate(doc)
	}

	return base64.StdEncoding.EncodeToString([]byte(doc))
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

	idp := newSAMLIDP(t)
	env := newOAuthEnv(t)

	details := map[string]string{detailMetadataFile: idp.metadata()}
	if byURL {
		details = map[string]string{"MetadataURL": idp.srv.URL + "/metadata"}
	}

	_, err := env.backend.CreateIdentityProviderFull(env.poolID, fedProvider, "SAML", details,
		map[string]string{"email": samlEmailAttr}, nil)
	require.NoError(t, err)

	client, err := env.backend.CreateUserPoolClientWithOpts(env.poolID, "saml", cognitoidp.UserPoolClientOptions{
		AllowedOAuthFlows: []string{"code"}, AllowedOAuthFlowsUserPoolClient: true,
		AllowedOAuthScopes: []string{"openid", "email", "profile"}, CallbackURLs: []string{oauthRedirect},
		SupportedIdentityProviders: []string{fedProvider},
	})
	require.NoError(t, err)

	return &samlEnv{oauthEnv: env, idp: idp, clientID: client.ClientID}
}

const detailMetadataFile = "MetadataFile"

// start runs authorize and returns the IdP redirect's request ID, relay state and decoded AuthnRequest.
func (e *samlEnv) start(t *testing.T) (string, string, string) {
	t.Helper()

	q := authorizeQuery(e.clientID, "")
	q.Set("identity_provider", fedProvider)

	resp, _ := e.get(t, "/oauth2/authorize?"+q.Encode(), nil)
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

	return m[1], loc.Query().Get("RelayState"), string(xmlReq)
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
			resp := env.post(t, relay, env.idp.response(reqID, env.acs(), env.poolID, &opts))
			q := locationQuery(t, resp)

			if !tt.wantOK {
				assert.Equal(t, "invalid_request", q.Get("error"))
				assert.Contains(t, q.Get("error_description"), tt.errSubstr)
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
