package cognitoidp

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	pathSAMLIDPResponse = "/saml2/idpresponse"
	opSAMLIDPResponse   = "SAML2IDPResponse"
	pathSAMLLogout      = "/saml2/logout"
	opSAMLLogout        = "SAML2Logout"
	pathSAMLMetadata    = "/saml2/metadata"
	opSAMLMetadata      = "SAML2Metadata"

	nsSAMLMetadata  = "urn:oasis:names:tc:SAML:2.0:metadata"
	nsSAMLProtocol  = "urn:oasis:names:tc:SAML:2.0:protocol"
	nsSAMLAssertion = "urn:oasis:names:tc:SAML:2.0:assertion"

	bindingRedirect  = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
	bindingPost      = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
	samlStatusOK     = "urn:oasis:names:tc:SAML:2.0:status:Success"
	samlConfirmation = "urn:oasis:names:tc:SAML:2.0:cm:bearer"
	samlSPPrefix     = "urn:amazon:cognito:sp:"

	detailMetadataURL  = "MetadataURL"
	detailMetadataFile = "MetadataFile"
	samlClockSkew      = 2 * time.Minute
	samlRequestIDBytes = 16
	maxSAMLResponse    = 1 << 20
	claimNameID        = "NameID"
)

var errSAML = errors.New("saml error")

func samlErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSAML, fmt.Sprintf(format, args...))
}

type samlMetadata struct {
	entityID        string
	ssoRedirect     string
	ssoPost         string
	sloRedirect     string
	sloPost         string
	keys            []*rsa.PublicKey
	wantAuthnSigned bool
}

// sloEndpoint prefers the redirect binding; the bool reports whether it is the redirect one.
func (md *samlMetadata) sloEndpoint() (string, bool, bool) {
	switch {
	case md.sloRedirect != "":
		return md.sloRedirect, true, true
	case md.sloPost != "":
		return md.sloPost, false, true
	default:
		return "", false, false
	}
}

func idpFetchRaw(ctx context.Context, target string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, idpErrorf("building request: %v", err)
	}

	client := &http.Client{
		Timeout:       idpHTTPTimeout,
		Transport:     idpTransport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, idpErrorf("request to %s failed: %v", req.URL.Host, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, idpErrorf("%s returned status %d", req.URL.Host, resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxIDPDocBytes))
	if err != nil {
		return nil, idpErrorf("reading %s: %v", req.URL.Host, err)
	}

	return data, nil
}

// loadSAMLMetadata reads IdP metadata from MetadataFile (inline XML) or MetadataURL.
func loadSAMLMetadata(ctx context.Context, details map[string]string) (*samlMetadata, error) {
	raw := []byte(details[detailMetadataFile])

	if len(raw) == 0 {
		target := details[detailMetadataURL]
		if !httpURL(target) {
			return nil, samlErrorf("provider requires MetadataFile or an http(s) MetadataURL")
		}

		if strings.HasPrefix(strings.ToLower(target), "http://") {
			logger.Load(ctx).WarnContext(ctx, "cognitoidp: SAML MetadataURL is plain http; use https in production")
		}

		var err error
		if raw, err = idpFetchRaw(ctx, target); err != nil {
			return nil, err
		}
	}

	return parseSAMLMetadata(raw)
}

func parseSAMLMetadata(raw []byte) (*samlMetadata, error) {
	root, err := parseXML(raw)
	if err != nil {
		return nil, samlErrorf("metadata: %v", err)
	}

	var idpDesc *xnode

	root.walk(func(n *xnode) {
		if idpDesc == nil && n.is(nsSAMLMetadata, "IDPSSODescriptor") {
			idpDesc = n
		}
	})

	if idpDesc == nil || idpDesc.parent == nil {
		return nil, samlErrorf("metadata has no IDPSSODescriptor")
	}

	md := &samlMetadata{
		entityID: idpDesc.parent.attr("entityID"), keys: signingKeys(idpDesc),
		wantAuthnSigned: strings.EqualFold(idpDesc.attr("WantAuthnRequestsSigned"), "true"),
	}

	for _, s := range idpDesc.childrenNamed(nsSAMLMetadata, "SingleLogoutService") {
		switch s.attr("Binding") {
		case bindingRedirect:
			md.sloRedirect = s.attr("Location")
		case bindingPost:
			md.sloPost = s.attr("Location")
		}
	}

	for _, s := range idpDesc.childrenNamed(nsSAMLMetadata, "SingleSignOnService") {
		switch s.attr("Binding") {
		case bindingRedirect:
			md.ssoRedirect = s.attr("Location")
		case bindingPost:
			md.ssoPost = s.attr("Location")
		}
	}

	return md, md.validate()
}

func (md *samlMetadata) validate() error {
	if md.entityID == "" || len(md.keys) == 0 || (md.ssoRedirect == "" && md.ssoPost == "") {
		return samlErrorf("metadata needs an entityID, a signing certificate and a SingleSignOnService")
	}

	for _, u := range []string{md.ssoRedirect, md.ssoPost, md.sloRedirect, md.sloPost} {
		if u != "" && !httpURL(u) {
			return samlErrorf("IdP endpoint %q is not an http(s) URL", u)
		}
	}

	return nil
}

// signingKeys collects the RSA public keys of KeyDescriptors usable for signing (use absent or "signing").
func signingKeys(idpDesc *xnode) []*rsa.PublicKey {
	var keys []*rsa.PublicKey

	var certs []*xnode

	for _, kd := range idpDesc.childrenNamed(nsSAMLMetadata, "KeyDescriptor") {
		if use := kd.attr("use"); use != "" && use != "signing" {
			continue
		}

		kd.walk(func(n *xnode) {
			if n.is(nsDSig, "X509Certificate") {
				certs = append(certs, n)
			}
		})
	}

	for _, n := range certs {
		der, err := base64.StdEncoding.DecodeString(stripSpace(n.textContent()))
		if err != nil {
			continue
		}

		cert, err := x509.ParseCertificate(der)
		if err != nil {
			continue
		}

		if pub, ok := cert.PublicKey.(*rsa.PublicKey); ok {
			keys = append(keys, pub)
		}
	}

	return keys
}

func samlACSURL(r *http.Request) string {
	return samlBaseURL(r) + pathSAMLIDPResponse
}

func samlSLOURL(r *http.Request) string {
	return samlBaseURL(r) + pathSAMLLogout
}

func samlBaseURL(r *http.Request) string {
	return strings.TrimSuffix(idpCallbackURL(r), pathOAuthIDPResponse)
}

func buildAuthnRequest(id, poolID, destination, acs string, now time.Time) string {
	return `<samlp:AuthnRequest xmlns:samlp="` + nsSAMLProtocol + `" xmlns:saml="` + nsSAMLAssertion +
		`" ID="` + id + `" Version="2.0" IssueInstant="` + now.UTC().Format(time.RFC3339) +
		`" Destination="` + html.EscapeString(destination) + `" ProtocolBinding="` + bindingPost +
		`" AssertionConsumerServiceURL="` + html.EscapeString(acs) + `"><saml:Issuer>` + samlSPPrefix + poolID +
		`</saml:Issuer><samlp:NameIDPolicy AllowCreate="true"/></samlp:AuthnRequest>`
}

func deflateBase64(data string) (string, error) {
	var buf bytes.Buffer

	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return "", fmt.Errorf("deflate: %w", err)
	}

	if _, err = w.Write([]byte(data)); err != nil {
		return "", fmt.Errorf("deflate: %w", err)
	}

	if err = w.Close(); err != nil {
		return "", fmt.Errorf("deflate: %w", err)
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// samlOutbound describes one SP-to-IdP protocol message and how to deliver it.
type samlOutbound struct {
	sign     *rsa.PrivateKey
	endpoint string
	param    string
	relay    string
	redirect bool
}

// signingKey returns the pool key when the provider (or its metadata) wants signed SP messages.
func (b *InMemoryBackend) signingKey(
	poolID string,
	details map[string]string,
	md *samlMetadata,
) (*rsa.PrivateKey, error) {
	if details[detailRequestSigning] != signAlgRSASHA256 && !md.wantAuthnSigned {
		return nil, nil //nolint:nilnil // nil key means unsigned
	}

	sp, err := b.spIdentity(poolID)
	if err != nil {
		return nil, err
	}

	return sp.key, nil
}

// sendSAML delivers xmlMsg by HTTP-Redirect (deflated, query-signed) or auto-submitted HTTP-POST form.
func sendSAML(c *echo.Context, out *samlOutbound, xmlMsg string) error {
	if out.redirect {
		return redirectSAML(c, out, xmlMsg)
	}

	if out.sign != nil {
		signed, err := signEnveloped(xmlMsg, "</saml:Issuer>", out.sign)
		if err != nil {
			return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
		}

		xmlMsg = signed
	}

	page := `<!doctype html><html><body onload="document.forms[0].submit()"><form method="post" action="` +
		html.EscapeString(out.endpoint) + `"><input type="hidden" name="` + out.param + `" value="` +
		base64.StdEncoding.EncodeToString([]byte(xmlMsg)) + `"><input type="hidden" name="RelayState" value="` +
		html.EscapeString(
			out.relay,
		) + `"><noscript><button type="submit">Continue</button></noscript></form></body></html>`

	return c.HTML(http.StatusOK, page)
}

func redirectSAML(c *echo.Context, out *samlOutbound, xmlMsg string) error {
	encoded, err := deflateBase64(xmlMsg)
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	if out.sign != nil {
		loc, serr := signedRedirectURL(out.endpoint, out.param, encoded, out.relay, out.sign)
		if serr != nil {
			return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
		}

		return c.Redirect(http.StatusFound, loc)
	}

	target, err := url.Parse(out.endpoint)
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	q := target.Query()
	q.Set(out.param, encoded)

	if out.relay != "" {
		q.Set(paramRelayState, out.relay)
	}

	target.RawQuery = q.Encode()

	return c.Redirect(http.StatusFound, target.String())
}

func (h *Handler) startSAML(c *echo.Context, req *authorizeRequest, idp *IdentityProvider) error {
	fail := func(desc string) error {
		return (&authorizeFailure{
			redirect: req.redirectURI, code: errServerError, desc: desc, state: req.state,
		}).respond(c)
	}

	md, err := loadSAMLMetadata(c.Request().Context(), idp.ProviderDetails)
	if err != nil {
		return fail("identity provider is misconfigured")
	}

	signKey, err := h.Backend.signingKey(req.client.UserPoolID, idp.ProviderDetails, md)
	if err != nil {
		return fail("")
	}

	rawID := make([]byte, samlRequestIDBytes)
	if err = readRandom(rawID); err != nil {
		return fail("")
	}

	requestID := "_" + hex.EncodeToString(rawID)
	acs := samlACSURL(c.Request())

	key, err := h.Backend.storeIDPState(&idpState{
		PoolID: req.client.UserPoolID, ClientID: req.client.ClientID, ProviderName: idp.ProviderName,
		RedirectURI: req.redirectURI, AppState: req.state, CodeChallenge: req.codeChallenge,
		AppNonce: req.nonce, ResponseType: req.responseType, Scopes: req.scopes,
		IDPRedirect: acs, SAMLRequestID: requestID,
	})
	if err != nil {
		return fail("")
	}

	setIDPBinding(c, key, pathSAMLIDPResponse, true)
	c.Response().Header().Set("Cache-Control", "no-store")

	out := &samlOutbound{param: paramSAMLRequest, relay: key, sign: signKey}
	endpoint := md.ssoRedirect

	if out.redirect = endpoint != ""; !out.redirect {
		endpoint = md.ssoPost
	}

	out.endpoint = endpoint

	return sendSAML(c, out, buildAuthnRequest(requestID, req.client.UserPoolID, endpoint, acs, time.Now()))
}

// idpInitRelay reports whether a RelayState carries IdP-initiated app parameters rather than a state key.
func idpInitRelay(relay string) bool {
	return strings.Contains(relay, "client_id=")
}

func (h *Handler) handleSAMLResponse(c *echo.Context) error {
	r := c.Request()
	r.Body = http.MaxBytesReader(c.Response(), r.Body, maxSAMLResponse)

	if err := r.ParseForm(); err != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "malformed form body")
	}

	relay := r.PostForm.Get(paramRelayState)
	if idpInitRelay(relay) {
		return h.handleIDPInitiated(c, relay)
	}

	if !verifyIDPBinding(r, relay, true) {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "state does not match this browser session")
	}

	st, ok := h.Backend.consumeIDPState(relay)
	if !ok || st.SAMLRequestID == "" || st.Logout {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "state is invalid or has expired")
	}

	fail := func(desc string) error {
		return (&authorizeFailure{
			redirect: st.RedirectURI, code: errInvalidRequest, desc: truncateDesc(desc), state: st.AppState,
		}).respond(c)
	}

	raw, err := base64.StdEncoding.DecodeString(stripSpace(r.PostForm.Get(paramSAMLResponse)))
	if err != nil || len(raw) == 0 {
		return fail("SAMLResponse is missing or not base64")
	}

	idp, ok := h.Backend.federationProvider(st.PoolID, st.ProviderName, "")
	if !ok || idp.ProviderType != providerTypeSAML {
		return fail("identity provider no longer exists")
	}

	md, err := loadSAMLMetadata(r.Context(), idp.ProviderDetails)
	if err != nil {
		logger.Load(r.Context()).WarnContext(r.Context(), "cognitoidp: SAML metadata load failed",
			"pool", st.PoolID, "provider", st.ProviderName, "error", err)

		return fail("identity provider metadata could not be loaded")
	}

	exp := h.samlExpectation(st.PoolID, st.IDPRedirect, st.SAMLRequestID)

	res, err := validateSAMLResponse(raw, md, exp, time.Now())
	if err != nil {
		logger.Load(r.Context()).WarnContext(r.Context(), "cognitoidp: SAML response rejected",
			"pool", st.PoolID, "provider", st.ProviderName, "error", err)

		return fail("SAML response validation failed")
	}

	return h.finishSAMLLogin(c, st, idp, md, res)
}

// samlExpectation binds a Response to this endpoint and request, with the pool key for decryption.
func (h *Handler) samlExpectation(poolID, acs, requestID string) *samlExpect {
	exp := &samlExpect{poolID: poolID, acs: acs, requestID: requestID}
	if sp, err := h.Backend.spIdentity(poolID); err == nil {
		exp.sp = sp.key
	}

	return exp
}

// finishSAMLLogin burns the assertion ID, signs the federated user in and resumes the app's flow.
func (h *Handler) finishSAMLLogin(
	c *echo.Context, st *idpState, idp *IdentityProvider, md *samlMetadata, res *samlResult,
) error {
	fail := func(desc string) error {
		return (&authorizeFailure{
			redirect: st.RedirectURI, code: errInvalidRequest, desc: truncateDesc(desc), state: st.AppState,
		}).respond(c)
	}

	if !h.Backend.claimSAMLAssertion(st.PoolID, res.assertionID, res.validUntil) {
		logger.Load(c.Request().Context()).WarnContext(c.Request().Context(),
			"cognitoidp: SAML assertion replayed", "pool", st.PoolID, "provider", st.ProviderName)

		return fail("SAML response validation failed")
	}

	username, err := h.Backend.federatedSignIn(st.PoolID, st.ClientID, idp, res.claims, md.entityID)
	if err != nil {
		return fail(strings.TrimSpace(err.Error()))
	}

	link := &samlLink{provider: idp.ProviderName, nameID: res.nameID, sessionIndex: res.sessionIndex}

	if id, serr := h.Backend.createHostedSessionFor(st.PoolID, username, link); serr == nil {
		setSessionCookie(c.Response(), c.Request(), id, hostedSessionTTL)
	}

	return h.completeFederatedLogin(c, st, username)
}
