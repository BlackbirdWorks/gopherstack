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
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	pathSAMLIDPResponse = "/saml2/idpresponse"
	opSAMLIDPResponse   = "SAML2IDPResponse"

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
	entityID    string
	ssoRedirect string
	ssoPost     string
	keys        []*rsa.PublicKey
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

	md := &samlMetadata{entityID: idpDesc.parent.attr("entityID"), keys: signingKeys(idpDesc)}

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

	for _, u := range []string{md.ssoRedirect, md.ssoPost} {
		if u != "" && !httpURL(u) {
			return samlErrorf("SingleSignOnService %q is not an http(s) URL", u)
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
	return strings.TrimSuffix(idpCallbackURL(r), pathOAuthIDPResponse) + pathSAMLIDPResponse
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

func (h *Handler) startSAML(c *echo.Context, req *authorizeRequest, idp *IdentityProvider) error {
	fail := func(code, desc string) error {
		return (&authorizeFailure{redirect: req.redirectURI, code: code, desc: desc, state: req.state}).respond(c)
	}

	md, err := loadSAMLMetadata(c.Request().Context(), idp.ProviderDetails)
	if err != nil {
		return fail(errServerError, "identity provider is misconfigured")
	}

	rawID := make([]byte, samlRequestIDBytes)
	if _, err = readRandom(rawID); err != nil {
		return fail(errServerError, "")
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
		return fail(errServerError, "")
	}

	setIDPBinding(c, key, pathSAMLIDPResponse, true)
	c.Response().Header().Set("Cache-Control", "no-store")

	if md.ssoRedirect != "" {
		return redirectAuthnRequest(c, md.ssoRedirect, req.client.UserPoolID, requestID, acs, key)
	}

	xmlReq := buildAuthnRequest(requestID, req.client.UserPoolID, md.ssoPost, acs, time.Now())

	page := `<!doctype html><html><body onload="document.forms[0].submit()"><form method="post" action="` +
		html.EscapeString(md.ssoPost) + `"><input type="hidden" name="SAMLRequest" value="` +
		base64.StdEncoding.EncodeToString([]byte(xmlReq)) + `"><input type="hidden" name="RelayState" value="` +
		html.EscapeString(key) + `"><noscript><button type="submit">Continue</button></noscript></form></body></html>`

	return c.HTML(http.StatusOK, page)
}

func redirectAuthnRequest(c *echo.Context, sso, poolID, requestID, acs, relay string) error {
	encoded, err := deflateBase64(buildAuthnRequest(requestID, poolID, sso, acs, time.Now()))
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	target, err := url.Parse(sso)
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	q := target.Query()
	q.Set("SAMLRequest", encoded)
	q.Set("RelayState", relay)
	target.RawQuery = q.Encode()

	return c.Redirect(http.StatusFound, target.String())
}

func (h *Handler) handleSAMLResponse(c *echo.Context) error {
	r := c.Request()
	r.Body = http.MaxBytesReader(c.Response(), r.Body, maxSAMLResponse)

	if err := r.ParseForm(); err != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "malformed form body")
	}

	if !verifyIDPBinding(r, r.PostForm.Get("RelayState"), true) {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "state does not match this browser session")
	}

	st, ok := h.Backend.consumeIDPState(r.PostForm.Get("RelayState"))
	if !ok || st.SAMLRequestID == "" {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "state is invalid or has expired")
	}

	fail := func(desc string) error {
		return (&authorizeFailure{
			redirect: st.RedirectURI, code: errInvalidRequest, desc: truncateDesc(desc), state: st.AppState,
		}).respond(c)
	}

	raw, err := base64.StdEncoding.DecodeString(stripSpace(r.PostForm.Get("SAMLResponse")))
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

	claims, err := validateSAMLResponse(raw, md, st, time.Now())
	if err != nil {
		logger.Load(r.Context()).WarnContext(r.Context(), "cognitoidp: SAML response rejected",
			"pool", st.PoolID, "provider", st.ProviderName, "error", err)

		return fail("SAML response validation failed")
	}

	username, err := h.Backend.federatedSignIn(st.PoolID, st.ClientID, idp, claims, md.entityID)
	if err != nil {
		return fail(strings.TrimSpace(err.Error()))
	}

	return h.completeFederatedLogin(c, st, username)
}

// validateSAMLResponse verifies the XML signature against the pinned IdP certificates, then the
// issuer, destination, request binding, audience and validity window, using only signed content.
func validateSAMLResponse(raw []byte, md *samlMetadata, st *idpState, now time.Time) (map[string]any, error) {
	root, err := parseXML(raw)
	if err != nil {
		return nil, samlErrorf("%v", err)
	}

	if !root.is(nsSAMLProtocol, "Response") {
		return nil, samlErrorf("root element is not a SAML Response")
	}

	signed, err := verifiedElements(root, md.keys)
	if err != nil {
		return nil, err
	}

	if slices.Contains(signed, root) {
		if err = checkSignedResponse(root, st); err != nil {
			return nil, err
		}
	}

	if len(root.childrenNamed(nsSAMLAssertion, "EncryptedAssertion")) > 0 {
		return nil, samlErrorf("encrypted assertions are not supported")
	}

	assertion, err := signedAssertion(root, signed)
	if err != nil {
		return nil, err
	}

	return assertionClaims(assertion, md, st, now)
}

// checkSignedResponse validates the Response-level attributes, which are only trustworthy when the
// Response element itself is covered by a verified signature.
func checkSignedResponse(root *xnode, st *idpState) error {
	if root.attr("Destination") != st.IDPRedirect {
		return samlErrorf("response destination does not match this endpoint")
	}

	if root.attr("InResponseTo") != st.SAMLRequestID {
		return samlErrorf("response is not a reply to the pending request")
	}

	status := root.child(nsSAMLProtocol, "Status")
	if status == nil || status.child(nsSAMLProtocol, "StatusCode") == nil ||
		status.child(nsSAMLProtocol, "StatusCode").attr("Value") != samlStatusOK {
		return samlErrorf("identity provider returned a non-success status")
	}

	return nil
}

// signedAssertion returns the single assertion whose content is covered by a verified signature.
func signedAssertion(root *xnode, signed []*xnode) (*xnode, error) {
	assertions := root.childrenNamed(nsSAMLAssertion, "Assertion")
	if len(assertions) != 1 {
		return nil, samlErrorf("response must contain exactly one assertion")
	}

	a := assertions[0]
	if !slices.Contains(signed, a) && !slices.Contains(signed, root) {
		return nil, samlErrorf("assertion is not covered by a valid signature")
	}

	return a, nil
}

func assertionClaims(a *xnode, md *samlMetadata, st *idpState, now time.Time) (map[string]any, error) {
	if iss := a.child(nsSAMLAssertion, "Issuer"); iss == nil || iss.textContent() != md.entityID {
		return nil, samlErrorf("assertion issuer does not match the identity provider")
	}

	subj := a.child(nsSAMLAssertion, "Subject")
	if subj == nil {
		return nil, samlErrorf("assertion has no subject")
	}

	if err := checkConfirmation(subj, st, now); err != nil {
		return nil, err
	}

	if err := checkConditions(a, st.PoolID, now); err != nil {
		return nil, err
	}

	nameID := subj.child(nsSAMLAssertion, "NameID")
	if nameID == nil || nameID.textContent() == "" {
		return nil, samlErrorf("assertion has no NameID")
	}

	claims := map[string]any{}

	if stmt := a.child(nsSAMLAssertion, "AttributeStatement"); stmt != nil {
		for _, attr := range stmt.childrenNamed(nsSAMLAssertion, "Attribute") {
			if vals := attr.childrenNamed(nsSAMLAssertion, "AttributeValue"); len(vals) > 0 && attr.attr("Name") != "" {
				claims[attr.attr("Name")] = vals[0].textContent()
			}
		}
	}

	claims[claimSub] = nameID.textContent()
	claims[claimNameID] = nameID.textContent()

	return claims, nil
}

func checkConfirmation(subj *xnode, st *idpState, now time.Time) error {
	for _, sc := range subj.childrenNamed(nsSAMLAssertion, "SubjectConfirmation") {
		if sc.attr("Method") != samlConfirmation {
			continue
		}

		data := sc.child(nsSAMLAssertion, "SubjectConfirmationData")
		if data == nil || data.attr("Recipient") != st.IDPRedirect || data.attr("InResponseTo") != st.SAMLRequestID {
			continue
		}

		notAfter, err := time.Parse(time.RFC3339, data.attr("NotOnOrAfter"))
		if err != nil || !now.Before(notAfter.Add(samlClockSkew)) {
			continue
		}

		return nil
	}

	return samlErrorf("assertion has no valid bearer subject confirmation")
}

func checkConditions(a *xnode, poolID string, now time.Time) error {
	cond := a.child(nsSAMLAssertion, "Conditions")
	if cond == nil {
		return samlErrorf("assertion has no conditions")
	}

	notAfter, err := time.Parse(time.RFC3339, cond.attr("NotOnOrAfter"))
	if err != nil || !now.Before(notAfter.Add(samlClockSkew)) {
		return samlErrorf("assertion has expired")
	}

	if nb := cond.attr("NotBefore"); nb != "" {
		notBefore, perr := time.Parse(time.RFC3339, nb)
		if perr != nil || now.Add(samlClockSkew).Before(notBefore) {
			return samlErrorf("assertion is not yet valid")
		}
	}

	want := samlSPPrefix + poolID

	restrictions := cond.childrenNamed(nsSAMLAssertion, "AudienceRestriction")
	if len(restrictions) == 0 {
		return samlErrorf("assertion audience is not %s", want)
	}

	for _, ar := range restrictions {
		if !slices.ContainsFunc(ar.childrenNamed(nsSAMLAssertion, "Audience"), func(a *xnode) bool {
			return a.textContent() == want
		}) {
			return samlErrorf("assertion audience is not %s", want)
		}
	}

	return nil
}
