package bedrockagentcorecontrol

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

const (
	grantAuthCode      = "AUTHORIZATION_CODE"
	sessionURIPrefix   = "urn:ietf:params:oauth:request_uri:"
	callbackPathPrefix = "/identities/oauth2/callback/"
	opOAuthCallback    = "OAuth2Callback"
	sessionInProgress  = "IN_PROGRESS"
	sessionCompleted   = "COMPLETED"
	sessionFailed      = "FAILED"
	sessionExchanging  = "EXCHANGING"
	sessionTTL         = 10 * time.Minute
	sessionKindTarget  = "target"
	sessionKindUser    = "user"
	targetFailed       = "FAILED"
	tokenSkew          = 30 * time.Second
	sessionIDLen       = 32
	paramClientID      = "client_id"
	paramGrantType     = "grant_type"
)

var (
	errAuthPending      = errors.New("the target needs the end user's authorization first")
	errNoAuthEndpoint   = errors.New("the OAuth provider has no authorization endpoint the emulator can reach")
	errSessionMismatch  = errors.New("the session does not belong to this user")
	errNoIssuer         = errors.New("the token has no iss claim")
	errInvalidUserToken = errors.New("invalid user token")
	errIssuerMismatch   = errors.New("the discovery document issuer does not match the token issuer")
)

// AuthSession is one three-legged OAuth authorization of a gateway target or of a workload user.
type AuthSession struct {
	Created      time.Time `json:"created"`
	Expires      time.Time `json:"expires"`
	ID           string    `json:"id"`
	State        string    `json:"state"`
	Account      string    `json:"account"`
	Region       string    `json:"region"`
	Kind         string    `json:"kind"`
	ProviderID   string    `json:"providerId"`
	GatewayID    string    `json:"gatewayId,omitempty"`
	TargetID     string    `json:"targetId,omitempty"`
	UserID       string    `json:"userId"`
	ReturnURL    string    `json:"returnUrl,omitempty"`
	CustomState  string    `json:"customState,omitempty"`
	Status       string    `json:"status"`
	AccessToken  string    `json:"accessToken,omitempty"`
	RefreshToken string    `json:"refreshToken,omitempty"`
	Scopes       []string  `json:"scopes,omitempty"`
	Bound        bool      `json:"bound,omitempty"`
}

type oauthProv struct {
	disc        map[string]any
	id          string
	callbackURL string
	clientID    string
	secretARN   string
	secretKey   string
}

type prepKey struct{}

// authPrep carries authorization endpoints resolved before the backend lock is taken.
type authPrep struct {
	endpoints map[string]string
	errs      map[string]string
}

func (b *InMemoryBackend) oauthProvider(sc scope, ref string) (oauthProv, bool) {
	b.mu.RLock("oauthProvider")
	defer b.mu.RUnlock()

	return b.oauthProviderLocked(sc, ref)
}

func (b *InMemoryBackend) oauthProviderLocked(sc scope, ref string) (oauthProv, bool) {
	r, ok := b.byARN(sc, ref)
	if !ok {
		r, ok = b.find(sc, b.kinds[kindOAuthProvider], "", ref)
	}

	if !ok || r.Kind != kindOAuthProvider {
		return oauthProv{}, false
	}

	var inner map[string]any
	for _, v := range objOf(r.Doc, "oauth2ProviderConfigOutput") {
		inner = mapOf(v)
	}

	return oauthProv{
		id: r.ID, callbackURL: strOf(r.Doc, "callbackUrl"), clientID: strOf(inner, "clientId"),
		disc:      objOf(inner, "oauthDiscovery"),
		secretARN: strOf(objOf(r.Doc, "clientSecretArn"), "secretArn"), secretKey: strOf(r.Doc, "clientSecretJsonKey"),
	}, true
}

func (b *InMemoryBackend) endpointFromDiscovery(
	ctx context.Context, disc map[string]any, meta, field string,
) (string, error) {
	if m := objOf(disc, "authorizationServerMetadata"); strOf(m, meta) != "" {
		return strOf(m, meta), nil
	}

	du := strOf(disc, "discoveryUrl")
	if du == "" {
		return "", errNoAuthEndpoint
	}

	doc := map[string]any{}
	if err := b.getJSON(ctx, du, &doc); err != nil {
		return "", fmt.Errorf("fetch OAuth discovery document: %w", err)
	}

	if v := strOf(doc, field); v != "" {
		return v, nil
	}

	return "", errNoAuthEndpoint
}

func authCodeConfigs(cfgs []any) []map[string]any {
	var out []map[string]any

	for _, c := range cfgs {
		p := objOf(objOf(mapOf(c), "credentialProvider"), "oauthCredentialProvider")
		if strOf(mapOf(c), "credentialProviderType") == authTypeOAuth && strOf(p, "grantType") == grantAuthCode {
			out = append(out, p)
		}
	}

	return out
}

// prepareAuth resolves the authorization endpoints a gateway target write needs; it runs before the lock because
// it may fetch a discovery document.
func (b *InMemoryBackend) prepareAuth(ctx context.Context, sc scope, k *kindSpec, in map[string]any) context.Context {
	if k.Name != kindGatewayTarget {
		return ctx
	}

	return b.prepareAuthDocs(ctx, sc, []map[string]any{in})
}

func (b *InMemoryBackend) prepareAuthDocs(ctx context.Context, sc scope, docs []map[string]any) context.Context {
	prep := &authPrep{endpoints: map[string]string{}, errs: map[string]string{}}

	provs := make([]map[string]any, 0, len(docs))

	for _, in := range docs {
		provs = append(provs, authCodeConfigs(listOf(in, "credentialProviderConfigurations"))...)
	}

	for _, p := range provs {
		ref := strOf(p, "providerArn")

		prov, ok := b.oauthProvider(sc, ref)
		if !ok {
			continue
		}

		ep, err := b.endpointFromDiscovery(ctx, prov.disc, "authorizationEndpoint", "authorization_endpoint")
		if err != nil {
			prep.errs[ref] = err.Error()

			continue
		}

		prep.endpoints[ref] = ep
	}

	return context.WithValue(ctx, prepKey{}, prep)
}

func prepOf(ctx context.Context) *authPrep {
	p, _ := ctx.Value(prepKey{}).(*authPrep)
	if p == nil {
		p = &authPrep{endpoints: map[string]string{}, errs: map[string]string{}}
	}

	return p
}

func (b *InMemoryBackend) newSession(sc scope, kind, providerID, userID string, scopes []string) *AuthSession {
	now := b.now()

	return &AuthSession{
		ID: sessionURIPrefix + awsid.NewN("", sessionIDLen), State: awsid.NewN("", sessionIDLen),
		Account: sc.account, Region: sc.region, Kind: kind, ProviderID: providerID, UserID: userID,
		Scopes: scopes, Status: sessionInProgress, Created: now, Expires: now,
	}
}

// authorizationURL is the provider authorize URL the end user's browser visits (RFC 6749 section 4.1.1).
func authorizationURL(endpoint string, p oauthProv, s *AuthSession, custom map[string]string, extra url.Values) string {
	q := url.Values{}
	for k, v := range custom {
		q.Set(k, v)
	}

	for k, vs := range extra {
		for _, v := range vs {
			q.Add(k, v)
		}
	}

	q.Set("response_type", "code")
	q.Set(paramClientID, p.clientID)
	q.Set("redirect_uri", p.callbackURL)
	q.Set("state", s.State)

	if len(s.Scopes) > 0 {
		q.Set("scope", strings.Join(s.Scopes, " "))
	}

	sep := "?"
	if strings.Contains(endpoint, "?") {
		sep = "&"
	}

	return endpoint + sep + q.Encode()
}

func stringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))

	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}

	return out
}

// requireTargetAuth puts a gateway target whose OAuth credentials use AUTHORIZATION_CODE into the pending
// authorization state until the end user completes the flow (TargetStatus *_PENDING_AUTH, types/enums.go).
func (b *InMemoryBackend) requireTargetAuth(ctx context.Context, r *Resource, pending string) {
	sc := scope{account: r.Account, region: r.Region}
	prep := prepOf(ctx)

	for _, s := range b.sessions.All() {
		if s.TargetID == r.ID && s.Status == sessionInProgress {
			b.sessions.Delete(s.ID)
		}
	}

	delete(r.Doc, "authorizationData")

	for _, p := range authCodeConfigs(listOf(r.Doc, "credentialProviderConfigurations")) {
		ref := strOf(p, "providerArn")

		prov, ok := b.oauthProviderLocked(sc, ref)
		if !ok || b.hasCompletedSession(r.ID, prov.id) {
			continue
		}

		if msg := prep.errs[ref]; msg != "" || prep.endpoints[ref] == "" {
			r.Status = targetFailed
			r.Doc["statusReasons"] = []any{"Authorization endpoint unavailable: " + msg}

			return
		}

		s := b.newSession(sc, sessionKindTarget, prov.id, awsid.NewN("", sessionIDLen), stringsOf(p["scopes"]))
		s.GatewayID, s.TargetID, s.ReturnURL = r.Parent, r.ID, strOf(p, "defaultReturnUrl")
		b.putSession(s)

		custom := stringMap(mapOf(p["customParameters"]))
		r.Status = pending
		r.Doc["authorizationData"] = map[string]any{"oauth2": map[string]any{
			"authorizationUrl": authorizationURL(prep.endpoints[ref], prov, s, custom, nil),
			"userId":           s.UserID,
		}}

		return
	}

	if strings.HasSuffix(r.Status, "_PENDING_AUTH") || r.Status == targetFailed {
		r.Status = stateReady
	}
}

func (b *InMemoryBackend) hasCompletedSession(targetID, providerID string) bool {
	return slices.ContainsFunc(b.sessions.All(), func(s *AuthSession) bool {
		return s.TargetID == targetID && s.ProviderID == providerID && s.Status == sessionCompleted
	})
}

type tokenGrant struct {
	access  string
	refresh string
	ttl     time.Duration
}

// exchangeCode redeems an authorization code at the provider's token endpoint (RFC 6749 section 4.1.3).
func (b *InMemoryBackend) exchangeCode(ctx context.Context, p oauthProv, code string) (tokenGrant, error) {
	endpoint, err := b.tokenEndpoint(ctx, p.disc)
	if err != nil {
		return tokenGrant{}, err
	}

	form := url.Values{
		paramGrantType: {"authorization_code"}, "code": {code}, "redirect_uri": {p.callbackURL},
		paramClientID: {p.clientID},
	}

	return b.tokenRequest(ctx, p, endpoint, form)
}

func (b *InMemoryBackend) tokenRequest(
	ctx context.Context, p oauthProv, endpoint string, form url.Values,
) (tokenGrant, error) {
	if secret, ok := (dataSeam{b: b}).SecretValue(p.secretARN, p.secretKey); ok && secret != "" {
		form.Set("client_secret", secret)
	}

	var tok struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}

	if err := b.postForm(ctx, endpoint, form, &tok); err != nil {
		return tokenGrant{}, fmt.Errorf("token request: %w", err)
	}

	if tok.AccessToken == "" {
		return tokenGrant{}, errNoAccessToken
	}

	return tokenGrant{
		access: tok.AccessToken, refresh: tok.RefreshToken,
		ttl: time.Duration(max(tok.ExpiresIn, 0)) * time.Second,
	}, nil
}

func (b *InMemoryBackend) sessionByState(state string) (*AuthSession, bool) {
	if state == "" {
		return nil, false
	}

	var found *AuthSession

	for _, s := range b.sessions.All() {
		if subtle.ConstantTimeCompare([]byte(s.State), []byte(state)) == 1 {
			found = s
		}
	}

	return found, found != nil
}

// putSession stores s after dropping sessions that can no longer complete; the caller holds the write lock.
func (b *InMemoryBackend) putSession(s *AuthSession) {
	now := b.now()

	for _, old := range b.sessions.All() {
		pending := old.Status != sessionCompleted || (old.Kind == sessionKindUser && !old.Bound)
		stale := now.After(old.Created.Add(sessionTTL))

		if (stale && pending) || b.orphanedSession(old) {
			b.sessions.Delete(old.ID)
		}
	}

	b.sessions.Put(s)
}

func (b *InMemoryBackend) orphanedSession(s *AuthSession) bool {
	if s.TargetID == "" {
		return false
	}

	t, ok := b.resources.Get(makeKey(s.Account, s.Region, kindGatewayTarget, s.GatewayID, s.TargetID))

	return !ok || !b.live(t)
}

// handleOAuthCallback completes a session from the provider's redirect (RFC 6749 section 4.1.2).
func (b *InMemoryBackend) handleOAuthCallback(
	ctx context.Context, callbackID string, q url.Values,
) (int, string, string) {
	b.mu.Lock("oauthCallback")
	s, found := b.sessionByState(q.Get("state"))

	var cp *AuthSession

	var prov oauthProv

	if found {
		prov, _ = b.oauthProviderLocked(scope{account: s.Account, region: s.Region}, s.ProviderID)
		found = s.Status == sessionInProgress && !b.now().After(s.Created.Add(sessionTTL)) &&
			strings.HasSuffix(prov.callbackURL, callbackPathPrefix+callbackID)
	}

	if found {
		s.Status = sessionExchanging
		c := *s
		cp = &c
	}

	b.mu.Unlock()

	if !found {
		return http.StatusBadRequest, "unknown authorization session", ""
	}

	status, grant := sessionFailed, tokenGrant{}

	if code := q.Get("code"); code != "" && q.Get("error") == "" {
		if g, err := b.exchangeCode(ctx, prov, code); err == nil {
			status, grant = sessionCompleted, g
		}
	}

	b.mu.Lock("oauthCallback")
	defer b.mu.Unlock()

	live, ok := b.sessions.Get(cp.ID)
	if !ok {
		return http.StatusBadRequest, "unknown authorization session", ""
	}

	live.Status, live.AccessToken, live.RefreshToken = status, grant.access, grant.refresh
	live.Expires = expiry(live.Created, b.now(), grant.ttl)

	if live.TargetID != "" && status == sessionCompleted {
		b.markTargetAuthorized(live)
	}

	if status != sessionCompleted {
		return http.StatusBadRequest, "authorization failed", ""
	}

	return http.StatusOK, "authorization complete", returnLocation(live)
}

func returnLocation(s *AuthSession) string {
	if s.ReturnURL == "" {
		return ""
	}

	u, err := url.Parse(s.ReturnURL)
	if err != nil || (u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS) || u.Host == "" {
		return ""
	}

	if s.CustomState != "" {
		q := u.Query()
		q.Set("state", s.CustomState)
		u.RawQuery = q.Encode()
	}

	return u.String()
}

func (b *InMemoryBackend) markTargetAuthorized(s *AuthSession) {
	t, ok := b.resources.Get(makeKey(s.Account, s.Region, kindGatewayTarget, s.GatewayID, s.TargetID))
	if !ok || !strings.HasSuffix(t.Status, "_PENDING_AUTH") {
		return
	}

	t.Status = stateReady
	t.Updated = b.now()

	delete(t.Doc, "authorizationData")
}

// sessionToken returns the access token of the completed session of a target, refreshing it when it expired.
func (b *InMemoryBackend) sessionToken(ctx context.Context, sc scope, targetID, providerRef string) (string, error) {
	prov, ok := b.oauthProvider(sc, providerRef)
	if !ok {
		return "", fmt.Errorf("%w: %s", errProviderNotFound, providerRef)
	}

	b.mu.RLock("sessionToken")

	var cp *AuthSession

	for _, s := range b.sessions.All() {
		if s.TargetID == targetID && s.ProviderID == prov.id && s.Status == sessionCompleted {
			c := *s
			cp = &c
		}
	}

	now := b.now()
	b.mu.RUnlock()

	if cp == nil {
		return "", errAuthPending
	}

	if cp.Expires.IsZero() || now.Add(tokenSkew).Before(cp.Expires) || cp.Expires.Equal(cp.Created) {
		return cp.AccessToken, nil
	}

	return b.refreshSession(ctx, prov, cp)
}

func (b *InMemoryBackend) refreshSession(ctx context.Context, prov oauthProv, s *AuthSession) (string, error) {
	if s.RefreshToken == "" {
		return "", errAuthPending
	}

	endpoint, err := b.tokenEndpoint(ctx, prov.disc)
	if err != nil {
		return "", err
	}

	grant, err := b.tokenRequest(ctx, prov, endpoint, url.Values{
		paramGrantType: {"refresh_token"}, "refresh_token": {s.RefreshToken}, paramClientID: {prov.clientID},
	})
	if err != nil {
		return "", err
	}

	b.mu.Lock("refreshSession")
	defer b.mu.Unlock()

	if live, ok := b.sessions.Get(s.ID); ok {
		live.AccessToken = grant.access
		live.Expires = expiry(live.Created, b.now(), grant.ttl)

		if grant.refresh != "" {
			live.RefreshToken = grant.refresh
		}
	}

	return grant.access, nil
}

// FederatedToken serves the USER_FEDERATION flow of GetResourceOauth2Token.
func (d dataSeam) FederatedToken(
	ctx context.Context, sc dataplane.Scope, req dataplane.FederationRequest,
) (dataplane.FederationResult, error) {
	return d.b.federatedToken(ctx, dsc(sc), req)
}

func (b *InMemoryBackend) federatedToken(
	ctx context.Context, sc scope, req dataplane.FederationRequest,
) (dataplane.FederationResult, error) {
	prov, ok := b.oauthProvider(sc, req.Provider)
	if !ok {
		return dataplane.FederationResult{}, dataplane.ErrNotFound
	}

	if err := b.checkReturnURL(sc, req); err != nil {
		return dataplane.FederationResult{}, err
	}

	if req.SessionURI != "" {
		return b.resumeSession(ctx, prov, req)
	}

	if !req.Force {
		if tok, ok2 := b.cachedUserToken(ctx, prov, req); ok2 {
			return dataplane.FederationResult{AccessToken: tok}, nil
		}
	}

	endpoint, err := b.endpointFromDiscovery(ctx, prov.disc, "authorizationEndpoint", "authorization_endpoint")
	if err != nil {
		return dataplane.FederationResult{}, fmt.Errorf("%w: %w", dataplane.ErrInvalidFederation, err)
	}

	s := b.newSession(sc, sessionKindUser, prov.id, req.UserID, req.Scopes)
	s.ReturnURL, s.CustomState = req.ReturnURL, req.CustomState

	extra := url.Values{}
	for _, a := range req.Audiences {
		extra.Add("audience", a)
	}

	for _, r := range req.Resources {
		extra.Add("resource", r)
	}

	b.mu.Lock("federatedToken")
	b.putSession(s)
	b.mu.Unlock()

	return dataplane.FederationResult{
		AuthorizationURL: authorizationURL(endpoint, prov, s, req.CustomParameters, extra),
		SessionURI:       s.ID, InProgress: true,
	}, nil
}

func (b *InMemoryBackend) checkReturnURL(sc scope, req dataplane.FederationRequest) error {
	if req.ReturnURL == "" {
		return nil
	}

	b.mu.RLock("checkReturnURL")
	defer b.mu.RUnlock()

	wl, ok := b.find(sc, b.kinds[kindWorkloadIdentity], "", req.Workload)
	if !ok {
		return nil
	}

	if !slices.Contains(stringsOf(wl.Doc["allowedResourceOauth2ReturnUrls"]), req.ReturnURL) {
		return fmt.Errorf("%w: resourceOauth2ReturnUrl is not allowed for the workload identity",
			dataplane.ErrInvalidFederation)
	}

	return nil
}

func (b *InMemoryBackend) cachedUserToken(
	ctx context.Context, prov oauthProv, req dataplane.FederationRequest,
) (string, bool) {
	b.mu.RLock("cachedUserToken")

	var cp *AuthSession

	for _, s := range b.sessions.All() {
		if reusable(s, prov.id, req) {
			c := *s
			cp = &c
		}
	}

	now := b.now()
	b.mu.RUnlock()

	if cp == nil {
		return "", false
	}

	if cp.Expires.Equal(cp.Created) || now.Add(tokenSkew).Before(cp.Expires) {
		return cp.AccessToken, true
	}

	if tok, err := b.refreshSession(ctx, prov, cp); err == nil {
		return tok, true
	}

	return "", false
}

func (b *InMemoryBackend) resumeSession(
	ctx context.Context, prov oauthProv, req dataplane.FederationRequest,
) (dataplane.FederationResult, error) {
	b.mu.RLock("resumeSession")
	s, ok := b.sessions.Get(req.SessionURI)

	var cp AuthSession

	if ok {
		cp = *s
	}

	b.mu.RUnlock()

	if !ok || cp.ProviderID != prov.id || cp.Kind != sessionKindUser {
		return dataplane.FederationResult{}, dataplane.ErrNotFound
	}

	if cp.Status != sessionCompleted && b.now().After(cp.Created.Add(sessionTTL)) {
		return dataplane.FederationResult{}, dataplane.ErrNotFound
	}

	if cp.UserID != req.UserID {
		return dataplane.FederationResult{}, errSessionMismatch
	}

	switch {
	case cp.Status == sessionFailed:
		return dataplane.FederationResult{SessionURI: cp.ID, Failed: true}, nil
	case cp.Status == sessionCompleted && cp.Bound:
		return dataplane.FederationResult{AccessToken: cp.AccessToken}, nil
	}

	endpoint, err := b.endpointFromDiscovery(ctx, prov.disc, "authorizationEndpoint", "authorization_endpoint")
	if err != nil {
		return dataplane.FederationResult{}, fmt.Errorf("%w: %w", dataplane.ErrInvalidFederation, err)
	}

	return dataplane.FederationResult{
		AuthorizationURL: authorizationURL(endpoint, prov, &cp, req.CustomParameters, nil),
		SessionURI:       cp.ID, InProgress: true,
	}, nil
}

// CompleteFederation binds the end user to an authorization session (CompleteResourceTokenAuth).
func (d dataSeam) CompleteFederation(
	ctx context.Context, sc dataplane.Scope, sessionURI string, userIdentifier dataplane.UserIdentifier,
) error {
	b := d.b

	userID := userIdentifier.UserID
	if userIdentifier.UserToken != "" {
		sub, err := b.VerifyUserJWT(ctx, userIdentifier.UserToken)
		if err != nil {
			return fmt.Errorf("%w: %w", dataplane.ErrInvalidFederation, err)
		}

		userID = sub
	}

	b.mu.Lock("CompleteFederation")
	defer b.mu.Unlock()

	s, ok := b.sessions.Get(sessionURI)
	if !ok || s.Account != sc.Account || s.Region != sc.Region || s.Kind != sessionKindUser {
		return dataplane.ErrNotFound
	}

	if s.UserID != userID {
		return errSessionMismatch
	}

	s.Bound = true

	return nil
}

// expiry is created (the "never expires" marker) when the provider returned no expires_in.
func expiry(created, now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return created
	}

	return now.Add(ttl)
}

// VerifyUserJWT verifies a user token's signature against the JWKS of its issuer and returns its subject. The
// issuer is read from the unverified token, so the JWKS is fetched only from loopback unless external targets
// are enabled.
func (b *InMemoryBackend) VerifyUserJWT(ctx context.Context, token string) (string, error) {
	claims := jwt.MapClaims{}

	keyfunc := func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errSigningMethod
		}

		iss, _ := claims["iss"].(string)
		if iss == "" {
			return nil, errNoIssuer
		}

		kid, _ := t.Header["kid"].(string)
		key, docIssuer, err := b.signingKey(ctx, strings.TrimSuffix(iss, "/")+discoverySuffixOIDC, iss, kid)
		if err == nil && docIssuer != iss {
			return nil, errIssuerMismatch
		}

		return key, err
	}

	tok, err := jwt.ParseWithClaims(strings.TrimSpace(token), claims, keyfunc, jwt.WithExpirationRequired())
	if err != nil || !tok.Valid {
		return "", fmt.Errorf("%w: %w", errInvalidUserToken, err)
	}

	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", fmt.Errorf("%w: no sub claim", errInvalidUserToken)
	}

	return sub, nil
}

// VerifyUserJWT verifies a user token for the data plane.
func (d dataSeam) VerifyUserJWT(ctx context.Context, token string) (string, error) {
	sub, err := d.b.VerifyUserJWT(ctx, token)
	if err != nil {
		return "", fmt.Errorf("%w: %w", dataplane.ErrInvalidFederation, err)
	}

	return sub, nil
}

// awaitingAuthorization reports whether a target uses AUTHORIZATION_CODE credentials nobody has authorized yet.
func (b *InMemoryBackend) awaitingAuthorization(t targetView) bool {
	b.mu.RLock("awaitingAuthorization")
	defer b.mu.RUnlock()

	for _, p := range authCodeConfigs(listOf(t.doc, "credentialProviderConfigurations")) {
		prov, ok := b.oauthProviderLocked(t.sc, strOf(p, "providerArn"))
		if ok && !b.hasCompletedSession(t.id, prov.id) {
			return true
		}
	}

	return false
}

// oauthCallbackRoute reports the provider callback id of GET /identities/oauth2/callback/<id>.
func oauthCallbackRoute(r *http.Request) (string, bool) {
	if r.Method != http.MethodGet {
		return "", false
	}

	id, ok := strings.CutPrefix(r.URL.Path, callbackPathPrefix)

	return id, ok && id != "" && !strings.Contains(id, "/")
}

func (h *Handler) serveOAuthCallback(c *echo.Context, callbackID string) error {
	status, msg, location := h.Backend.handleOAuthCallback(c.Request().Context(), callbackID, c.Request().URL.Query())
	if location != "" {
		return c.Redirect(http.StatusFound, location)
	}

	return writeJSON(c, status, gatewayMsg(msg))
}

// reusable reports whether a bound, completed session of the user already covers the requested scopes.
func reusable(s *AuthSession, providerID string, req dataplane.FederationRequest) bool {
	if s.Kind != sessionKindUser || s.ProviderID != providerID || s.UserID != req.UserID {
		return false
	}

	covers := !slices.ContainsFunc(req.Scopes, func(sc string) bool { return !slices.Contains(s.Scopes, sc) })

	return s.Status == sessionCompleted && s.Bound && covers
}
