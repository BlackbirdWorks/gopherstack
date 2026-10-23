package cognitoidp

import (
	"encoding/base64"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// idpInitQuery reads the app parameters (client_id, redirect_uri, response_type, state ...) an IdP puts in
// RelayState, either bare or as the query of an /oauth2/authorize URL.
func idpInitQuery(relay string) url.Values {
	if _, after, found := strings.Cut(relay, "?"); found {
		relay = after
	}

	q, err := url.ParseQuery(relay)
	if err != nil {
		return url.Values{}
	}

	return q
}

// idpInitProviders lists the client's SAML providers that opted in to IdP-initiated sign-in.
func (h *Handler) idpInitProviders(client *UserPoolClient) []*IdentityProvider {
	var out []*IdentityProvider

	for _, idp := range h.Backend.samlProviders(client.UserPoolID) {
		if detailTrue(idp.ProviderDetails, detailIDPInit) &&
			slices.Contains(client.SupportedIdentityProviders, idp.ProviderName) {
			out = append(out, idp)
		}
	}

	return out
}

// handleIDPInitiated completes an unsolicited Response. RelayState is untrusted, so it only selects the
// app client and a redirect_uri that must be registered for that client; the signed assertion authenticates.
func (h *Handler) handleIDPInitiated(c *echo.Context, relay string) error {
	r := c.Request()

	req, bad := h.validateAuthorize(idpInitQuery(relay), h.hostPool(c))
	if bad != nil {
		return bad.respond(c)
	}

	fail := func(desc string) error {
		return (&authorizeFailure{
			redirect: req.redirectURI, code: errInvalidRequest, desc: desc, state: req.state,
		}).respond(c)
	}

	idps := h.idpInitProviders(req.client)
	if len(idps) == 0 {
		return fail("IdP-initiated sign-in is not enabled for this client")
	}

	raw, err := base64.StdEncoding.DecodeString(stripSpace(r.PostForm.Get(paramSAMLResponse)))
	if err != nil || len(raw) == 0 {
		return fail("SAMLResponse is missing or not base64")
	}

	exp := h.samlExpectation(req.client.UserPoolID, samlACSURL(r), "")

	idp, md, res := h.matchIDPInitiated(c, idps, raw, exp)
	if res == nil {
		return fail("SAML response validation failed")
	}

	st := &idpState{
		PoolID: req.client.UserPoolID, ClientID: req.client.ClientID, ProviderName: idp.ProviderName,
		RedirectURI: req.redirectURI, AppState: req.state, CodeChallenge: req.codeChallenge,
		AppNonce: req.nonce, ResponseType: req.responseType, Scopes: req.scopes,
	}

	return h.finishSAMLLogin(c, st, idp, md, res)
}

// matchIDPInitiated returns the first opted-in provider whose metadata validates raw as an unsolicited response.
func (h *Handler) matchIDPInitiated(
	c *echo.Context, idps []*IdentityProvider, raw []byte, exp *samlExpect,
) (*IdentityProvider, *samlMetadata, *samlResult) {
	ctx := c.Request().Context()

	for _, idp := range idps {
		md, err := loadSAMLMetadata(ctx, idp.ProviderDetails)
		if err != nil {
			continue
		}

		res, verr := validateSAMLResponse(raw, md, exp, time.Now())
		if verr != nil {
			logger.Load(ctx).WarnContext(ctx, "cognitoidp: IdP-initiated SAML response rejected",
				"pool", exp.poolID, "provider", idp.ProviderName, "error", verr)

			continue
		}

		return idp, md, res
	}

	return nil, nil, nil
}
