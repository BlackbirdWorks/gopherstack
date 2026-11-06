package bedrockagentcorecontrol

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

const (
	outboundTimeout  = 30 * time.Second
	maxOutboundBytes = 4 << 20
	grantTypeCC      = "CLIENT_CREDENTIALS"
	authTypeKey      = "API_KEY"
	authTypeOAuth    = "OAUTH"
	locQuery         = "QUERY_PARAMETER"
)

var (
	errDestinationNotPermitted = errors.New("destination is outside loopback and external gateway targets are disabled")
	errNoRedirects             = errors.New("redirects are not followed")
	errOAuthUnsupported        = errors.New("OAuth credential provider is not usable for this target")
	errProviderNotFound        = errors.New("credential provider not found")
	errAPIKeyUnavailable       = errors.New("API key is unavailable")
	errNoAccessToken           = errors.New("token endpoint returned no access_token")
	errBadStatus               = errors.New("unexpected status")
)

// hostSwitch skips certificate verification only for loopback IP literals (emulator-run targets).
type hostSwitch struct {
	verified, loopback http.RoundTripper
}

func (h hostSwitch) RoundTrip(r *http.Request) (*http.Response, error) {
	next := h.verified
	if httpclient.IsLoopbackHost(r.URL.Hostname()) {
		next = h.loopback
	}

	return next.RoundTrip(r)
}

func newOutboundClient(allow func() bool) *http.Client {
	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = httpclient.LoopbackDialer(allow, errDestinationNotPermitted).DialContext

	loop := tr.Clone()
	loop.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // loopback IP literals only
		MinVersion:         tls.VersionTLS12,
	}

	return &http.Client{
		Timeout:       outboundTimeout,
		Transport:     hostSwitch{verified: tr, loopback: loop},
		CheckRedirect: func(*http.Request, []*http.Request) error { return errNoRedirects },
	}
}

// authRT applies outbound credentials to every request of a client.
type authRT struct {
	next  http.RoundTripper
	apply func(*http.Request)
}

func (a authRT) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	a.apply(r)

	return a.next.RoundTrip(r)
}

// targetClient returns the outbound client with the target's credentials applied.
func (b *InMemoryBackend) targetClient(ctx context.Context, t targetView) (*http.Client, error) {
	apply, err := b.outboundCredentials(ctx, t)
	if err != nil {
		return nil, err
	}

	if apply == nil {
		return b.outbound, nil
	}

	c := *b.outbound
	c.Transport = authRT{next: b.outbound.Transport, apply: apply}

	return &c, nil
}

func (b *InMemoryBackend) outboundCredentials(ctx context.Context, t targetView) (func(*http.Request), error) {
	var applies []func(*http.Request)

	for _, c := range listOf(t.doc, "credentialProviderConfigurations") {
		cm := mapOf(c)
		prov := objOf(cm, "credentialProvider")

		switch strOf(cm, "credentialProviderType") {
		case authTypeKey:
			fn, err := b.apiKeyCredential(t.sc, objOf(prov, "apiKeyCredentialProvider"))
			if err != nil {
				return nil, err
			}

			applies = append(applies, fn)
		case authTypeOAuth:
			fn, err := b.oauthCredential(ctx, t.sc, objOf(prov, "oauthCredentialProvider"))
			if err != nil {
				return nil, err
			}

			applies = append(applies, fn)
		}
	}

	if len(applies) == 0 {
		return nil, nil //nolint:nilnil // no credentials configured
	}

	return func(r *http.Request) {
		for _, fn := range applies {
			fn(r)
		}
	}, nil
}

func (b *InMemoryBackend) providerInfo(
	sc scope, kind dataplane.ProviderKind, providerARN string,
) (dataplane.ProviderInfo, error) {
	b.mu.RLock("providerInfo")
	r, ok := b.byARN(sc, providerARN)
	name := ""

	if ok {
		name = r.ID
	}

	b.mu.RUnlock()

	if !ok {
		return dataplane.ProviderInfo{}, fmt.Errorf("%w: %s", errProviderNotFound, providerARN)
	}

	info, found := dataSeam{b: b}.Provider(dataplane.Scope{Account: sc.account, Region: sc.region}, kind, name)
	if !found {
		return dataplane.ProviderInfo{}, fmt.Errorf("%w: %s", errProviderNotFound, providerARN)
	}

	return info, nil
}

func (b *InMemoryBackend) apiKeyCredential(sc scope, cfg map[string]any) (func(*http.Request), error) {
	info, err := b.providerInfo(sc, dataplane.ProviderAPIKey, strOf(cfg, "providerArn"))
	if err != nil {
		return nil, err
	}

	key, ok := dataSeam{b: b}.SecretValue(info.SecretARN, info.SecretJSONID)
	if !ok {
		return nil, fmt.Errorf("%w for credential provider %q", errAPIKeyUnavailable, info.Name)
	}

	name := strOf(cfg, "credentialParameterName")
	value := strOf(cfg, "credentialPrefix") + key

	if strOf(cfg, "credentialLocation") == locQuery {
		return func(r *http.Request) {
			q := r.URL.Query()
			q.Set(name, value)
			r.URL.RawQuery = q.Encode()
		}, nil
	}

	if name == "" {
		name = "Authorization"
	}

	return func(r *http.Request) { r.Header.Set(name, value) }, nil
}

func (b *InMemoryBackend) oauthCredential(
	ctx context.Context, sc scope, cfg map[string]any,
) (func(*http.Request), error) {
	grant := strOf(cfg, "grantType")
	if grant != "" && grant != grantTypeCC {
		return nil, fmt.Errorf("%w: grant type %s needs an interactive consent flow", errOAuthUnsupported, grant)
	}

	info, err := b.providerInfo(sc, dataplane.ProviderOAuth2, strOf(cfg, "providerArn"))
	if err != nil {
		return nil, err
	}

	custom := mapOf(info.Config["customOauth2ProviderConfig"])
	if custom == nil {
		custom = info.Config
	}

	endpoint, err := b.tokenEndpoint(ctx, objOf(custom, "oauthDiscovery"))
	if err != nil {
		return nil, err
	}

	secret, _ := dataSeam{b: b}.SecretValue(info.SecretARN, info.SecretJSONID)
	form := url.Values{"grant_type": {oauthGrantForm}, "client_id": {strOf(custom, "clientId")}}

	if secret != "" {
		form.Set("client_secret", secret)
	}

	if scopes := stringsOf(cfg["scopes"]); len(scopes) > 0 {
		form.Set("scope", strings.Join(scopes, " "))
	}

	var tok struct {
		AccessToken string `json:"access_token"`
	}

	if err = b.postForm(ctx, endpoint, form, &tok); err != nil {
		return nil, fmt.Errorf("token request for credential provider %q: %w", info.Name, err)
	}

	if tok.AccessToken == "" {
		return nil, fmt.Errorf("%w (credential provider %q)", errNoAccessToken, info.Name)
	}

	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+tok.AccessToken) }, nil
}

const oauthGrantForm = "client_credentials"

func (b *InMemoryBackend) tokenEndpoint(ctx context.Context, disc map[string]any) (string, error) {
	if meta := objOf(disc, "authorizationServerMetadata"); meta != nil {
		if ep := strOf(meta, "tokenEndpoint"); ep != "" {
			return ep, nil
		}
	}

	du := strOf(disc, "discoveryUrl")
	if du == "" {
		return "", fmt.Errorf("%w: provider has no discovery URL", errOAuthUnsupported)
	}

	var doc struct {
		TokenEndpoint string `json:"token_endpoint"`
	}

	if err := b.getJSON(ctx, du, &doc); err != nil {
		return "", fmt.Errorf("fetch OAuth discovery document: %w", err)
	}

	if doc.TokenEndpoint == "" {
		return "", fmt.Errorf("%w: discovery document has no token_endpoint", errOAuthUnsupported)
	}

	return doc.TokenEndpoint, nil
}

func stringsOf(v any) []string {
	var out []string

	if l, ok := v.([]any); ok {
		for _, e := range l {
			if s, isStr := e.(string); isStr {
				out = append(out, s)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) getJSON(ctx context.Context, target string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	return b.doJSON(req, out)
}

func (b *InMemoryBackend) postForm(ctx context.Context, target string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	return b.doJSON(req, out)
}

func (b *InMemoryBackend) doJSON(req *http.Request, out any) error {
	resp, err := b.outbound.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxOutboundBytes))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode/100 != 2 { //nolint:mnd // status class
		return fmt.Errorf("%w %d", errBadStatus, resp.StatusCode)
	}

	if err = json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
