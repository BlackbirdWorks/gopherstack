package bedrockagentcore

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	flowM2M          = "M2M"
	flowUserFed      = "USER_FEDERATION"
	maxTokenResponse = 1 << 20
)

func (b *InMemoryBackend) getResourceOAuth2Token(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	t, err := b.authorize(strOf(in, "workloadIdentityToken"))
	if err != nil {
		return nil, 0, err
	}

	p, err := b.providerFor(t, ProviderOAuth2, strOf(in, "resourceCredentialProviderName"))
	if err != nil {
		return nil, 0, err
	}

	switch strOf(in, "oauth2Flow") {
	case flowM2M:
	case flowUserFed:
		return b.userFederation(ctx, t, in)
	default:
		return nil, 0, invalid("oauth2Flow must be %s or %s", flowM2M, flowUserFed)
	}

	token, err := b.clientCredentials(ctx, p, in)
	if err != nil {
		return nil, 0, err
	}

	return doc{"accessToken": token}, 0, nil
}

func (b *InMemoryBackend) clientCredentials(ctx context.Context, p ProviderInfo, in doc) (string, error) {
	secret, ok := b.ctl().SecretValue(p.SecretARN, p.SecretJSONID)
	if !ok {
		return "", notFound("the client secret of provider %q is unavailable", p.Name)
	}

	endpoint, err := b.tokenEndpoint(ctx, p)
	if err != nil {
		return "", err
	}

	form := url.Values{"grant_type": {"client_credentials"}}
	if scopes := strList(in, "scopes"); len(scopes) > 0 {
		form.Set("scope", strings.Join(scopes, " "))
	}

	for _, a := range strList(in, "audiences") {
		form.Add("audience", a)
	}

	for _, r := range strList(in, "resources") {
		form.Add("resource", r)
	}

	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", invalid("the token endpoint %q is not a valid URL", endpoint)
	}

	hreq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	hreq.SetBasicAuth(url.QueryEscape(strOf(p.Config, "clientId")), url.QueryEscape(secret))

	return b.exchange(hreq)
}

func (b *InMemoryBackend) exchange(hreq *http.Request) (string, error) {
	resp, err := b.engine.cfg.HTTP.Do(hreq)
	if err != nil {
		return "", invalid("the token endpoint could not be reached")
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponse))
	if resp.StatusCode/100 != 2 { //nolint:mnd // 2xx
		return "", invalid("the token endpoint returned HTTP %d", resp.StatusCode)
	}

	var out struct {
		AccessToken string `json:"access_token"`
	}

	if err = json.Unmarshal(body, &out, jsonx.V1()); err != nil || out.AccessToken == "" {
		return "", invalid("the token endpoint did not return an access_token")
	}

	return out.AccessToken, nil
}

// tokenEndpoint resolves the provider's token endpoint from its stored metadata or discovery document.
func (b *InMemoryBackend) tokenEndpoint(ctx context.Context, p ProviderInfo) (string, error) {
	disc := objOf(p.Config, "oauthDiscovery")
	if te := strOf(objOf(disc, "authorizationServerMetadata"), "tokenEndpoint"); te != "" {
		return te, nil
	}

	du := strOf(disc, "discoveryUrl")
	if du == "" {
		return "", invalid("provider %q (%s) has no token endpoint the emulator can reach", p.Name, p.Vendor)
	}

	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, du, http.NoBody)
	if err != nil {
		return "", invalid("the discovery URL %q is not valid", du)
	}

	resp, err := b.engine.cfg.HTTP.Do(hreq)
	if err != nil {
		return "", invalid("the discovery URL could not be reached")
	}

	defer resp.Body.Close()

	var meta struct {
		TokenEndpoint string `json:"token_endpoint"`
	}

	if err = json.UnmarshalDecode(
		jsontext.NewDecoder(io.LimitReader(resp.Body, maxTokenResponse)),
		&meta,
		jsonx.V1(),
	); err != nil ||
		meta.TokenEndpoint == "" {
		return "", invalid("the discovery document of provider %q has no token_endpoint", p.Name)
	}

	return meta.TokenEndpoint, nil
}

func (b *InMemoryBackend) userFederation(ctx context.Context, t *AccessToken, in doc) (any, int, error) {
	if t.UserID == "" {
		return nil, 0, invalid("the workload access token carries no user identity for the USER_FEDERATION flow")
	}

	res, err := b.ctl().FederatedToken(ctx, Scope{Account: t.Account, Region: t.Region}, FederationRequest{
		Provider: strOf(in, "resourceCredentialProviderName"), Workload: t.Workload,
		UserID: t.UserID, Scopes: strList(in, "scopes"), Audiences: strList(in, "audiences"),
		Resources:        strList(in, "resources"),
		CustomParameters: stringMapOf(objOf(in, "customParameters")), ReturnURL: strOf(in, "resourceOauth2ReturnUrl"),
		CustomState: strOf(in, "customState"), SessionURI: strOf(in, "sessionUri"),
		Force: boolOf(in, "forceAuthentication", false),
	})
	if err != nil {
		return nil, 0, federationError(err)
	}

	out := doc{}

	switch {
	case res.AccessToken != "":
		out["accessToken"] = res.AccessToken
	case res.Failed:
		out["sessionUri"], out["sessionStatus"] = res.SessionURI, "FAILED"
	default:
		out["authorizationUrl"], out["sessionUri"], out["sessionStatus"] = res.AuthorizationURL, res.SessionURI, "IN_PROGRESS"
	}

	return out, 0, nil
}

func federationError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return notFound("the authorization session or credential provider was not found")
	case errors.Is(err, ErrInvalidFederation):
		return invalid("%v", err)
	default:
		return invalid("the authorization session does not belong to this user")
	}
}

func (b *InMemoryBackend) completeResourceTokenAuth(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	ident := objOf(in, "userIdentifier")
	if strOf(in, "sessionUri") == "" || (strOf(ident, "userId") == "" && strOf(ident, "userToken") == "") {
		return nil, 0, invalid("sessionUri and a userIdentifier are required")
	}

	c := b.ctl()
	if c == nil {
		return nil, 0, internal("the control plane is not wired")
	}

	if err = c.CompleteFederation(ctx, b.scopeOf(ctx), strOf(in, "sessionUri"),
		UserIdentifier{UserID: strOf(ident, "userId"), UserToken: strOf(ident, "userToken")}); err != nil {
		return nil, 0, federationError(err)
	}

	return doc{}, 0, nil
}
