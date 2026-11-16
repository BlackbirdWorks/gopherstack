package gcpidentitytoolkit

import (
	"encoding/json/v2"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// idpResolved is a configured, enabled provider.
type idpResolved struct {
	assert  func(url.Values, *InMemoryBackend) (*idpAssertion, error)
	id      string
	authURI string
	client  string
	scope   string
}

//nolint:gochecknoglobals // immutable catalog
var idpAuthEndpoints = map[string]string{
	"google.com":    "https://accounts.google.com/o/oauth2/auth",
	"facebook.com":  "https://www.facebook.com/dialog/oauth",
	"github.com":    "https://github.com/login/oauth/authorize",
	"twitter.com":   "https://twitter.com/i/oauth2/authorize",
	"microsoft.com": "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
	"apple.com":     "https://appleid.apple.com/auth/authorize",
	"yahoo.com":     "https://api.login.yahoo.com/oauth2/request_auth",
	"linkedin.com":  "https://www.linkedin.com/oauth/v2/authorization",
}

func (b *InMemoryBackend) idpLocked(sc *scope, providerID string) (*idpResolved, error) {
	p, tenant := sc.proj, sc.tenant
	key := idpKey(tenant, providerID)

	switch {
	case strings.HasPrefix(providerID, oidcPrefix):
		cfg := p.OAuthIdps[key]
		if cfg == nil || !cfg.Enabled {
			return nil, idtk("OPERATION_NOT_ALLOWED", "The identity provider is not configured or disabled.")
		}

		return &idpResolved{
			id:      providerID,
			client:  cfg.ClientId,
			scope:   "openid",
			authURI: strings.TrimSuffix(cfg.Issuer, "/") + "/authorize",
			assert: func(v url.Values, bk *InMemoryBackend) (*idpAssertion, error) {
				return oauthAssertion(v, cfg.Issuer, cfg.ClientId, bk.now())
			},
		}, nil
	case strings.HasPrefix(providerID, samlPrefix):
		cfg := p.SamlIdps[key]
		if cfg == nil || !cfg.Enabled || cfg.IdpConfig == nil || cfg.SpConfig == nil {
			return nil, idtk("OPERATION_NOT_ALLOWED", "The identity provider is not configured or disabled.")
		}

		return &idpResolved{
			id: providerID, authURI: cfg.IdpConfig.SsoUrl,
			assert: func(v url.Values, bk *InMemoryBackend) (*idpAssertion, error) {
				return samlAssertion(v, cfg.IdpConfig.IdpEntityId, cfg.SpConfig.SpEntityId, bk.now())
			},
		}, nil
	}

	cfg := p.DefaultIdps[key]
	if cfg == nil || !cfg.Enabled {
		return nil, idtk("OPERATION_NOT_ALLOWED", "The identity provider is not configured or disabled.")
	}

	return &idpResolved{
		id: providerID, client: cfg.ClientId, scope: "openid email profile", authURI: idpAuthEndpoints[providerID],
		assert: func(v url.Values, bk *InMemoryBackend) (*idpAssertion, error) {
			return oauthAssertion(v, "", "", bk.now())
		},
	}, nil
}

// IdpSignInResponse is the accounts:signInWithIdp response body.
type IdpSignInResponse struct {
	Kind                 string    `json:"kind"`
	FederatedID          string    `json:"federatedId,omitempty"`
	ProviderID           string    `json:"providerId,omitempty"`
	LocalID              string    `json:"localId,omitempty"`
	Email                string    `json:"email,omitempty"`
	OriginalEmail        string    `json:"originalEmail,omitempty"`
	InputEmail           string    `json:"inputEmail,omitempty"`
	DisplayName          string    `json:"displayName,omitempty"`
	FullName             string    `json:"fullName,omitempty"`
	FirstName            string    `json:"firstName,omitempty"`
	LastName             string    `json:"lastName,omitempty"`
	PhotoURL             string    `json:"photoUrl,omitempty"`
	IDToken              string    `json:"idToken,omitempty"`
	RefreshToken         string    `json:"refreshToken,omitempty"`
	ExpiresIn            string    `json:"expiresIn,omitempty"`
	RawUserInfo          string    `json:"rawUserInfo,omitempty"`
	OauthIDToken         string    `json:"oauthIdToken,omitempty"`
	OauthAccessToken     string    `json:"oauthAccessToken,omitempty"`
	TenantID             string    `json:"tenantId,omitempty"`
	MfaPendingCredential string    `json:"mfaPendingCredential,omitempty"`
	VerifiedProvider     []string  `json:"verifiedProvider,omitempty"`
	MfaInfo              []mfaWire `json:"mfaInfo,omitempty"`
	EmailVerified        bool      `json:"emailVerified,omitempty"`
	IsNewUser            bool      `json:"isNewUser,omitempty"`
	NeedConfirmation     bool      `json:"needConfirmation,omitempty"`
}

func providerFromBody(r *authRequest) (url.Values, string, error) {
	body, err := url.ParseQuery(r.PostBody)
	if err != nil {
		return nil, "", idpInvalid("The post body is not a query string.")
	}

	pid := body.Get("providerId")
	if pid == "" {
		return nil, "", idpInvalid("The post body carries no providerId.")
	}

	return body, pid, nil
}

// SignInWithIdp exchanges an IdP credential for an Identity Platform session, creating or linking accounts.
func (b *InMemoryBackend) SignInWithIdp(c call, r *authRequest) (*IdpSignInResponse, error) {
	if r.PostBody == "" || r.RequestURI == "" {
		return nil, idtk("INVALID_REQUEST_URI")
	}

	body, pid, err := providerFromBody(r)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("SignInWithIdp")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	prov, err := b.idpLocked(sc, pid)
	if err != nil {
		return nil, err
	}

	a, err := prov.assert(body, b)
	if err != nil {
		return nil, err
	}

	u, isNew, pending, err := b.resolveIdpUserLocked(sc, r, pid, a, c.ip)
	if err != nil {
		return nil, err
	}

	out := idpProfile(sc, pid, a)
	if r.ReturnIdpCred {
		out.OauthIDToken, out.OauthAccessToken = a.idToken, a.accessToken
	}

	if pending {
		out.NeedConfirmation, out.VerifiedProvider = true, []string{pid}

		return out, nil
	}

	session, err := b.blockLocked(sc, evBeforeSignIn, pid, u, a)
	if err != nil {
		return nil, err
	}

	if u.Disabled {
		return nil, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
	}

	if pend, ok := b.mfaPendingLocked(sc, u, pid); ok {
		out.LocalID, out.MfaPendingCredential, out.MfaInfo = u.UID, pend.MfaPendingCredential, pend.MfaInfo
		out.IsNewUser, out.Email = isNew, u.Email

		return out, nil
	}

	b.touchLoginLocked(u)

	s, err := b.issueLocked(sc, u, pid, session)
	if err != nil {
		return nil, err
	}

	out.LocalID, out.IDToken, out.RefreshToken, out.ExpiresIn = u.UID, s.IDToken, s.RefreshToken, expiresInSecs
	out.IsNewUser, out.Email, out.EmailVerified = isNew, u.Email, u.EmailVerified

	return out, nil
}

func idpProfile(sc *scope, pid string, a *idpAssertion) *IdpSignInResponse {
	raw, _ := json.Marshal(a.raw, jsonx.V1())

	return &IdpSignInResponse{
		Kind: "identitytoolkit#VerifyAssertionResponse", ProviderID: pid, FederatedID: a.subject,
		Email: a.email, OriginalEmail: a.email, InputEmail: a.email, DisplayName: a.name, FullName: a.name,
		FirstName: a.givenName, LastName: a.familyName, PhotoURL: a.picture, RawUserInfo: string(raw),
		EmailVerified: a.emailVerified, TenantID: sc.tenant,
	}
}

func (b *InMemoryBackend) resolveIdpUserLocked(
	sc *scope, r *authRequest, pid string, a *idpAssertion, ip string,
) (*userRec, bool, bool, error) {
	rec := providerRec{
		ProviderID: pid, RawID: a.subject, Email: a.email, DisplayName: a.name, PhotoURL: a.picture,
		FederatedID: a.subject,
	}

	if r.IDToken != "" {
		u, err := b.linkIdpLocked(sc, r.IDToken, rec)

		return u, false, false, err
	}

	if u := sc.byProvider(pid, a.subject); u != nil {
		if u.Disabled {
			return nil, false, false, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
		}

		return u, false, false, nil
	}

	if a.email != "" {
		if existing := sc.byEmail(a.email); existing != nil {
			return b.mergeByEmailLocked(existing, rec, a)
		}
	}

	if r.AutoCreate != nil && !*r.AutoCreate {
		return nil, false, false, idtk("USER_NOT_FOUND")
	}

	if err := b.signUpAllowedLocked(sc, ip); err != nil {
		return nil, false, false, err
	}

	u, err := b.createClientUserLocked(sc, newUser{
		email: a.email, displayName: a.name, photoURL: a.picture, emailVerified: a.emailVerified,
		providers: []providerRec{rec},
	}, pid, a)

	return u, true, false, err
}

func (b *InMemoryBackend) mergeByEmailLocked(
	existing *userRec, rec providerRec, a *idpAssertion,
) (*userRec, bool, bool, error) {
	if existing.Disabled {
		return nil, false, false, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
	}

	if !a.emailVerified {
		return existing, false, true, nil
	}

	if !existing.EmailVerified {
		existing.PasswordHash, existing.Salt, existing.Hash = nil, nil, nil
		existing.EmailVerified = true
		syncProviders(existing)
	}

	existing.Providers = append(existing.Providers, rec)

	return existing, false, false, nil
}

func (b *InMemoryBackend) linkIdpLocked(sc *scope, idToken string, rec providerRec) (*userRec, error) {
	u, _, err := b.userForTokenLocked(sc, idToken)
	if err != nil {
		return nil, err
	}

	if o := sc.byProvider(rec.ProviderID, rec.RawID); o != nil && o.UID != u.UID {
		return nil, idtk("FEDERATED_USER_ID_ALREADY_IN_USE")
	}

	if u.Anonymous {
		u.Anonymous = false
	}

	if rec.Email != "" && u.Email == "" && sc.byEmail(rec.Email) == nil {
		u.Email = strs.Fold(rec.Email)
	}

	u.Providers = append(u.Providers[:0:0], u.Providers...)
	if sc.byProvider(rec.ProviderID, rec.RawID) == nil {
		u.Providers = append(u.Providers, rec)
	}

	return u, nil
}

func idpAuthURI(prov *idpResolved, continueURI, sessionID string) string {
	q := url.Values{}
	if prov.client != "" {
		q.Set("client_id", prov.client)
		q.Set("response_type", "code")
		q.Set("scope", prov.scope)
	}

	q.Set("redirect_uri", continueURI)
	q.Set("state", sessionID)

	sep := "?"
	if strings.Contains(prov.authURI, "?") {
		sep = "&"
	}

	return prov.authURI + sep + q.Encode()
}

func (b *InMemoryBackend) federatedAuthURILocked(sc *scope, r *authRequest) (map[string]any, error) {
	if r.ContinueURI == "" {
		return nil, idtk("MISSING_CONTINUE_URI")
	}

	prov, err := b.idpLocked(sc, r.ProviderID)
	if err != nil {
		return nil, err
	}

	sid := randomToken(sessionBytes)

	return map[string]any{
		keyKind: "identitytoolkit#CreateAuthUriResponse", "providerId": r.ProviderID, "sessionId": sid,
		"authUri": idpAuthURI(prov, r.ContinueURI, sid), "registered": false,
		"forExistingProvider": false, "captchaRequired": false, "allProviders": []string{},
		"signinMethods": []string{},
	}, nil
}
