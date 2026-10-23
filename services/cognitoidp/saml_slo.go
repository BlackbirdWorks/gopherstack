package cognitoidp

import (
	"encoding/hex"
	"html"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	samlLogoutMaxAge = 10 * time.Minute
	samlLogoutTTL    = 5 * time.Minute
)

func buildLogoutRequest(id, poolID, destination, nameID, sessionIndex string, now time.Time) string {
	idx := ""
	if sessionIndex != "" {
		idx = `<samlp:SessionIndex>` + html.EscapeString(sessionIndex) + `</samlp:SessionIndex>`
	}

	return `<samlp:LogoutRequest xmlns:samlp="` + nsSAMLProtocol + `" xmlns:saml="` + nsSAMLAssertion +
		`" ID="` + id + `" Version="2.0" IssueInstant="` + now.UTC().Format(time.RFC3339) +
		`" Destination="` + html.EscapeString(destination) + `" NotOnOrAfter="` +
		now.Add(samlLogoutTTL).UTC().Format(time.RFC3339) + `"><saml:Issuer>` + samlSPPrefix + poolID +
		`</saml:Issuer><saml:NameID>` + html.EscapeString(nameID) + `</saml:NameID>` + idx + `</samlp:LogoutRequest>`
}

func buildLogoutResponse(id, inResponseTo, poolID, destination string, now time.Time) string {
	return `<samlp:LogoutResponse xmlns:samlp="` + nsSAMLProtocol + `" xmlns:saml="` + nsSAMLAssertion +
		`" ID="` + id + `" InResponseTo="` + html.EscapeString(inResponseTo) + `" Version="2.0" IssueInstant="` +
		now.UTC().Format(time.RFC3339) + `" Destination="` + html.EscapeString(destination) + `"><saml:Issuer>` +
		samlSPPrefix + poolID + `</saml:Issuer><samlp:Status><samlp:StatusCode Value="` + samlStatusOK +
		`"></samlp:StatusCode></samlp:Status></samlp:LogoutResponse>`
}

func newSAMLID() (string, error) {
	raw := make([]byte, samlRequestIDBytes)
	if err := readRandom(raw); err != nil {
		return "", err
	}

	return "_" + hex.EncodeToString(raw), nil
}

// idpLogout sends a signed LogoutRequest to the IdP of the user's SAML login when IDPSignout is on.
// It reports false (after logging) when nothing was sent so the caller can finish a local logout.
func (h *Handler) idpLogout(c *echo.Context, client *UserPoolClient, link *samlLink, returnTo string) (bool, error) {
	ctx := c.Request().Context()

	idp, ok := h.Backend.federationProvider(client.UserPoolID, link.provider, "")
	if !ok || idp.ProviderType != providerTypeSAML || !detailTrue(idp.ProviderDetails, detailIDPSignout) {
		return false, nil
	}

	md, err := loadSAMLMetadata(ctx, idp.ProviderDetails)
	endpoint, redirect, has := samlEndpoint(md, err)

	if !has {
		return false, nil
	}

	signKey, err := h.Backend.signingKey(client.UserPoolID, idp.ProviderDetails, md)
	if err != nil {
		return false, nil //nolint:nilerr // fall back to local logout
	}

	id, err := newSAMLID()
	if err != nil {
		return false, nil //nolint:nilerr // fall back to local logout
	}

	relay, err := h.Backend.storeIDPState(&idpState{
		PoolID: client.UserPoolID, ClientID: client.ClientID, ProviderName: idp.ProviderName,
		RedirectURI: returnTo, SAMLRequestID: id, IDPRedirect: samlSLOURL(c.Request()), Logout: true,
	})
	if err != nil {
		return false, nil //nolint:nilerr // fall back to local logout
	}

	logger.Load(ctx).InfoContext(ctx, "cognitoidp: sending SAML LogoutRequest", "pool", client.UserPoolID,
		"provider", idp.ProviderName)

	out := &samlOutbound{endpoint: endpoint, param: paramSAMLRequest, relay: relay, redirect: redirect, sign: signKey}
	msg := buildLogoutRequest(id, client.UserPoolID, endpoint, link.nameID, link.sessionIndex, time.Now())

	return true, sendSAML(c, out, msg)
}

func samlEndpoint(md *samlMetadata, err error) (string, bool, bool) {
	if err != nil || md == nil {
		return "", false, false
	}

	return md.sloEndpoint()
}

// handleSAMLLogout is the SP single-logout endpoint: IdP LogoutRequest or LogoutResponse, via redirect or POST.
func (h *Handler) handleSAMLLogout(c *echo.Context) error {
	r := c.Request()
	r.Body = http.MaxBytesReader(c.Response(), r.Body, maxSAMLResponse)

	if err := r.ParseForm(); err != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "malformed form body")
	}

	has := func(name string) bool {
		if r.Method == http.MethodGet {
			return r.URL.Query().Has(name)
		}

		return r.PostForm.Has(name)
	}

	switch {
	case has(paramSAMLRequest):
		return h.handleLogoutRequest(c)
	case has(paramSAMLResponse):
		return h.handleLogoutResponse(c)
	default:
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "no SAML message")
	}
}

type logoutRequest struct {
	nameID  string
	id      string
	indexes []string
}

// checkLogoutRequest validates a LogoutRequest against its (already signature-verified) provider.
func checkLogoutRequest(root *xnode, md *samlMetadata, slo string, now time.Time) (*logoutRequest, error) {
	if !root.is(nsSAMLProtocol, "LogoutRequest") {
		return nil, samlErrorf("message is not a LogoutRequest")
	}

	if iss := root.child(nsSAMLAssertion, "Issuer"); iss == nil || iss.textContent() != md.entityID {
		return nil, samlErrorf("logout issuer does not match the identity provider")
	}

	if root.attr("Destination") != slo || root.attr(attrNameID) == "" {
		return nil, samlErrorf("logout destination does not match this endpoint")
	}

	if err := checkLogoutTime(root, now); err != nil {
		return nil, err
	}

	nameID := root.child(nsSAMLAssertion, "NameID")
	if nameID == nil || nameID.textContent() == "" {
		return nil, samlErrorf("logout has no NameID")
	}

	req := &logoutRequest{nameID: nameID.textContent(), id: root.attr(attrNameID)}
	for _, si := range root.childrenNamed(nsSAMLProtocol, "SessionIndex") {
		req.indexes = append(req.indexes, si.textContent())
	}

	return req, nil
}

func checkLogoutTime(root *xnode, now time.Time) error {
	issued, err := time.Parse(time.RFC3339, root.attr("IssueInstant"))
	if err != nil || now.Add(samlClockSkew).Before(issued) || now.After(issued.Add(samlLogoutMaxAge)) {
		return samlErrorf("logout request is not fresh")
	}

	if na := root.attr("NotOnOrAfter"); na != "" {
		notAfter, perr := time.Parse(time.RFC3339, na)
		if perr != nil || !now.Before(notAfter.Add(samlClockSkew)) {
			return samlErrorf("logout request has expired")
		}
	}

	return nil
}

func (h *Handler) handleLogoutRequest(c *echo.Context) error {
	r := c.Request()
	ctx := r.Context()

	msg, err := readSAMLMessage(r, paramSAMLRequest)
	if err != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "SAMLRequest is invalid")
	}

	issuer := ""
	if iss := msg.root.child(nsSAMLAssertion, "Issuer"); iss != nil {
		issuer = iss.textContent()
	}

	for _, idp := range h.Backend.samlProviders(h.hostPool(c)) {
		if !detailTrue(idp.ProviderDetails, detailIDPSignout) {
			continue
		}

		md, lerr := loadSAMLMetadata(ctx, idp.ProviderDetails)
		if lerr != nil || md.entityID != issuer {
			continue
		}

		if serr := msg.verify(paramSAMLRequest, md.keys); serr != nil {
			logger.Load(ctx).WarnContext(ctx, "cognitoidp: SAML LogoutRequest signature rejected",
				"provider", idp.ProviderName, "error", serr)

			continue
		}

		return h.applyLogoutRequest(c, idp, md, msg)
	}

	return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "LogoutRequest could not be validated")
}

func (h *Handler) applyLogoutRequest(c *echo.Context, idp *IdentityProvider, md *samlMetadata, msg *samlInbound) error {
	r := c.Request()

	req, err := checkLogoutRequest(msg.root, md, samlSLOURL(r), time.Now())
	if err != nil {
		logger.Load(r.Context()).WarnContext(r.Context(), "cognitoidp: SAML LogoutRequest rejected",
			"provider", idp.ProviderName, "error", err)

		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "LogoutRequest could not be validated")
	}

	if !h.Backend.claimSAMLAssertion(idp.UserPoolID, "logout:"+req.id, time.Now().Add(samlLogoutMaxAge)) {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "LogoutRequest was already processed")
	}

	h.Backend.endSAMLSessions(idp.UserPoolID, idp.ProviderName, req.nameID, req.indexes)
	h.endSession(c)

	endpoint, redirect, has := md.sloEndpoint()
	if !has {
		return c.HTML(http.StatusOK, "<!doctype html><html><body>Signed out</body></html>")
	}

	signKey, err := h.Backend.signingKey(idp.UserPoolID, idp.ProviderDetails, md)
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	id, err := newSAMLID()
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	out := &samlOutbound{
		endpoint: endpoint,
		param:    paramSAMLResponse,
		relay:    msg.relay,
		redirect: redirect,
		sign:     signKey,
	}

	return sendSAML(c, out, buildLogoutResponse(id, req.id, idp.UserPoolID, endpoint, time.Now()))
}

func (h *Handler) handleLogoutResponse(c *echo.Context) error {
	r := c.Request()
	ctx := r.Context()
	bad := func() error {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "LogoutResponse could not be validated")
	}

	msg, err := readSAMLMessage(r, paramSAMLResponse)
	if err != nil {
		return bad()
	}

	st, ok := h.Backend.consumeIDPState(msg.relay)
	if !ok || !st.Logout {
		return bad()
	}

	idp, ok := h.Backend.federationProvider(st.PoolID, st.ProviderName, "")
	if !ok || idp.ProviderType != providerTypeSAML {
		return bad()
	}

	md, err := loadSAMLMetadata(ctx, idp.ProviderDetails)
	if err == nil {
		err = msg.verify(paramSAMLResponse, md.keys)
	}

	if err == nil {
		err = checkLogoutResponse(msg.root, md, st)
	}

	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cognitoidp: SAML LogoutResponse rejected",
			"pool", st.PoolID, "provider", st.ProviderName, "error", err)

		return bad()
	}

	return c.Redirect(http.StatusFound, st.RedirectURI)
}

func checkLogoutResponse(root *xnode, md *samlMetadata, st *idpState) error {
	if !root.is(nsSAMLProtocol, "LogoutResponse") {
		return samlErrorf("message is not a LogoutResponse")
	}

	if iss := root.child(nsSAMLAssertion, "Issuer"); iss == nil || iss.textContent() != md.entityID {
		return samlErrorf("logout issuer does not match the identity provider")
	}

	if root.attr("Destination") != st.IDPRedirect || root.attr("InResponseTo") != st.SAMLRequestID {
		return samlErrorf("logout response is not a reply to the pending request at this endpoint")
	}

	return nil
}
