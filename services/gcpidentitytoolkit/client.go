package gcpidentitytoolkit

import (
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// call identifies the project, tenant and caller class of one request.
type call struct {
	project string
	tenant  string
	host    string
	admin   bool
}

func tokenResp(kind string, u *userRec, s *session) *tokenResponse {
	return &tokenResponse{
		Kind: kind, LocalID: u.UID, Email: u.Email, DisplayName: u.DisplayName,
		IDToken: s.IDToken, RefreshToken: s.RefreshToken, ExpiresIn: expiresInSecs,
	}
}

func (b *InMemoryBackend) touchLoginLocked(u *userRec) { u.LastLoginAt = b.nowMillis() }

// SignUp creates an email/password or anonymous account, or links email/password to an anonymous one.
func (b *InMemoryBackend) SignUp(c call, r *authRequest) (*tokenResponse, error) {
	b.mu.Lock("SignUp")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	email, pw := r.Email.first(), r.Password

	if r.IDToken != "" {
		return b.linkAnonymousLocked(sc, r, email, pw)
	}

	if email == "" && pw == "" {
		if !sc.anon {
			return nil, idtk("ADMIN_ONLY_OPERATION")
		}

		anon, cErr := b.createUserLocked(sc, newUser{anonymous: true}, false)
		if cErr != nil {
			return nil, cErr
		}

		return b.finishSignInLocked(sc, anon, anonProvider, "identitytoolkit#SignupNewUserResponse")
	}

	if !sc.password {
		return nil, idtk("OPERATION_NOT_ALLOWED", "Password sign-in is disabled for this project.")
	}

	if email == "" {
		return nil, idtk("MISSING_EMAIL")
	}

	if pw == "" {
		return nil, idtk("MISSING_PASSWORD")
	}

	u, err := b.createUserLocked(sc, newUser{email: email, password: pw, displayName: deref(r.DisplayName)}, false)
	if err != nil {
		return nil, err
	}

	return b.finishSignInLocked(sc, u, passwordProv, "identitytoolkit#SignupNewUserResponse")
}

func deref(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func effectiveTenant(c call, r *authRequest) string {
	if c.tenant != "" {
		return c.tenant
	}

	return r.TenantID
}

func (b *InMemoryBackend) finishSignInLocked(sc *scope, u *userRec, provider, kind string) (*tokenResponse, error) {
	b.touchLoginLocked(u)

	s, err := b.issueLocked(sc, u, provider, nil)
	if err != nil {
		return nil, err
	}

	return tokenResp(kind, u, s), nil
}

func (b *InMemoryBackend) linkAnonymousLocked(sc *scope, r *authRequest, email, pw string) (*tokenResponse, error) {
	u, _, err := b.userForTokenLocked(sc, r.IDToken)
	if err != nil {
		return nil, err
	}

	if !sc.password {
		return nil, idtk("OPERATION_NOT_ALLOWED", "Password sign-in is disabled for this project.")
	}

	if email == "" || pw == "" {
		return nil, idtk("MISSING_EMAIL")
	}

	if !validEmail(strs.Fold(email)) {
		return nil, idtk("INVALID_EMAIL")
	}

	if o := sc.byEmail(email); o != nil && o.UID != u.UID {
		return nil, idtk("EMAIL_EXISTS")
	}

	if err = checkPassword(sc, pw); err != nil {
		return nil, err
	}

	u.Email, u.Anonymous = strs.Fold(email), false

	if err = b.setPasswordLocked(sc, u, pw); err != nil {
		return nil, err
	}

	syncProviders(u)

	return b.finishSignInLocked(sc, u, passwordProv, "identitytoolkit#SignupNewUserResponse")
}

// SignInWithPassword authenticates by email and password.
func (b *InMemoryBackend) SignInWithPassword(c call, r *authRequest) (*tokenResponse, error) {
	email := strs.Fold(r.Email.first())

	b.mu.Lock("SignInWithPassword")
	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))

	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if !sc.password {
		b.mu.Unlock()

		return nil, idtk("OPERATION_NOT_ALLOWED", "Password sign-in is disabled for this project.")
	}

	if email == "" {
		b.mu.Unlock()

		return nil, idtk("MISSING_EMAIL")
	}

	if r.Password == "" {
		b.mu.Unlock()

		return nil, idtk("MISSING_PASSWORD")
	}

	u := sc.byEmail(email)
	privacy := sc.privacy

	var snap userRec

	var spec hashSpec

	if u != nil && len(u.PasswordHash) > 0 {
		snap = *u
		spec = sc.proj.Hash

		if u.Hash != nil {
			spec = *u.Hash
		}
	}
	b.mu.Unlock()

	if u == nil {
		return nil, loginFailure(privacy, "EMAIL_NOT_FOUND")
	}

	if len(snap.PasswordHash) == 0 || !spec.verify(r.Password, snap.Salt, snap.PasswordHash) {
		return nil, loginFailure(privacy, "INVALID_PASSWORD")
	}

	return b.completePasswordSignIn(c, effectiveTenant(c, r), snap.UID, r.Password)
}

func loginFailure(privacy bool, code string) *APIError {
	if privacy {
		return idtk("INVALID_LOGIN_CREDENTIALS")
	}

	return idtk(code)
}

func (b *InMemoryBackend) completePasswordSignIn(c call, tenant, uid, password string) (*tokenResponse, error) {
	b.mu.Lock("completePasswordSignIn")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, err
	}

	u, ok := sc.users[uid]
	if !ok {
		return nil, idtk("INVALID_LOGIN_CREDENTIALS")
	}

	if u.Disabled {
		return nil, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
	}

	if u.Hash != nil {
		changedAt := u.PasswordAt
		if err = b.setPasswordLocked(sc, u, password); err == nil {
			u.PasswordAt = changedAt
		}
	}

	resp, err := b.finishSignInLocked(sc, u, passwordProv, "identitytoolkit#VerifyPasswordResponse")
	if err != nil {
		return nil, err
	}

	resp.Registered = true

	return resp, nil
}

// SignInWithCustomToken exchanges a custom token for an ID token, creating the account on first use.
func (b *InMemoryBackend) SignInWithCustomToken(c call, r *authRequest) (*tokenResponse, error) {
	b.mu.Lock("SignInWithCustomToken")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if r.Token == "" {
		return nil, idtk("MISSING_CUSTOM_TOKEN")
	}

	uid, dev, err := b.customTokenLocked(r.Token, sc)
	if err != nil {
		return nil, err
	}

	u, exists := sc.users[uid]
	if !exists {
		if u, err = b.createUserLocked(sc, newUser{uid: uid}, false); err != nil {
			return nil, err
		}
	}

	if u.Disabled {
		return nil, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
	}

	b.touchLoginLocked(u)

	s, err := b.issueLocked(sc, u, customProv, dev)
	if err != nil {
		return nil, err
	}

	return &tokenResponse{
		Kind: "identitytoolkit#VerifyCustomTokenResponse", IDToken: s.IDToken, RefreshToken: s.RefreshToken,
		ExpiresIn: expiresInSecs, IsNewUser: !exists,
	}, nil
}

// Lookup returns accounts by ID token (client) or by uid, email, phone or provider id (admin).
func (b *InMemoryBackend) Lookup(c call, r *authRequest) (map[string]any, error) {
	b.mu.Lock("Lookup")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	var found []*userRec

	if r.IDToken != "" {
		u, _, tErr := b.userForTokenLocked(sc, r.IDToken)
		if tErr != nil {
			return nil, tErr
		}

		found = append(found, u)
	} else {
		if !c.admin {
			return nil, idtk("MISSING_ID_TOKEN")
		}

		found = lookupAdmin(sc, r)
	}

	users := make([]userWire, 0, len(found))
	for _, u := range found {
		users = append(users, b.toWire(sc, u, c.admin && r.IDToken == ""))
	}

	out := map[string]any{keyKind: "identitytoolkit#GetAccountInfoResponse"}
	if len(users) > 0 {
		out["users"] = users
	}

	return out, nil
}

func lookupAdmin(sc *scope, r *authRequest) []*userRec {
	var found []*userRec

	add := func(u *userRec) {
		if u != nil && !slices.Contains(found, u) {
			found = append(found, u)
		}
	}

	for _, id := range r.LocalID {
		add(sc.users[id])
	}

	for _, e := range r.Email {
		add(sc.byEmail(e))
	}

	for _, p := range r.PhoneNumber {
		add(sc.byPhone(p))
	}

	for _, f := range r.FederatedUserID {
		add(sc.byProvider(f.ProviderID, f.RawID))
	}

	return found
}

// DeleteAccount deletes the caller's account (ID token) or any account (admin, by uid).
func (b *InMemoryBackend) DeleteAccount(c call, r *authRequest) error {
	b.mu.Lock("DeleteAccount")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return err
	}

	uid := r.LocalID.first()

	if r.IDToken != "" {
		u, _, tErr := b.userForTokenLocked(sc, r.IDToken)
		if tErr != nil {
			return tErr
		}

		uid = u.UID
	} else if !c.admin {
		return idtk("MISSING_ID_TOKEN")
	}

	if uid == "" {
		return idtk("MISSING_LOCAL_ID")
	}

	if _, ok := sc.users[uid]; !ok {
		return idtk("USER_NOT_FOUND")
	}

	delete(sc.users, uid)

	return nil
}

// Refresh exchanges a refresh token for a new ID token (securetoken v1/token).
func (b *InMemoryBackend) Refresh(grantType, token string) (map[string]string, error) {
	b.mu.Lock("Refresh")
	defer b.mu.Unlock()

	if grantType != "refresh_token" {
		return nil, idtk("INVALID_GRANT_TYPE")
	}

	if token == "" {
		return nil, idtk("MISSING_REFRESH_TOKEN")
	}

	p, ok := b.parseRefreshLocked(token)
	if !ok {
		return nil, idtk("INVALID_REFRESH_TOKEN")
	}

	sc, err := b.scopeLocked(p.Project, p.Tenant)
	if err != nil {
		return nil, idtk("INVALID_REFRESH_TOKEN")
	}

	u, ok := sc.users[p.UID]
	if !ok {
		return nil, idtk("USER_NOT_FOUND")
	}

	if u.Disabled {
		return nil, idtk("USER_DISABLED")
	}

	if u.ValidSince > 0 && p.Issued < u.ValidSince {
		return nil, idtk("TOKEN_EXPIRED")
	}

	id, err := b.signJWTLocked(b.tokenClaims(sc, u, p.Provider, p.AuthTime, securetokenIssuer, p.Claims))
	if err != nil {
		return nil, err
	}

	u.LastRefreshAt = b.nowMillis()

	return map[string]string{
		"access_token": id, "id_token": id, "refresh_token": token, "expires_in": expiresInSecs,
		"token_type": "Bearer", "user_id": u.UID, "project_id": projectNumber(p.Project),
	}, nil
}

func projectNumber(project string) string {
	var h uint64 = 14695981039346656037

	for i := range len(project) {
		h ^= uint64(project[i])
		h *= 1099511628211
	}

	return strconv.FormatUint(h%projectNumberMod+projectNumberFloor, 10)
}

// CreateAuthURI reports sign-in methods for an identifier, honouring email enumeration protection.
func (b *InMemoryBackend) CreateAuthURI(c call, r *authRequest) (map[string]any, error) {
	b.mu.Lock("CreateAuthURI")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if r.ProviderID != "" {
		return nil, idtk("OPERATION_NOT_ALLOWED", "Federated sign-in is not supported by this emulator.")
	}

	if r.Identifier == "" {
		return nil, idtk("MISSING_IDENTIFIER")
	}

	if r.ContinueURI == "" {
		return nil, idtk("MISSING_CONTINUE_URI")
	}

	if !validEmail(strs.Fold(r.Identifier)) {
		return nil, idtk("INVALID_IDENTIFIER")
	}

	out := map[string]any{keyKind: "identitytoolkit#CreateAuthUriResponse", "sessionId": randomToken(sessionBytes)}
	if sc.privacy {
		return out, nil
	}

	out["registered"] = false

	if u := sc.byEmail(r.Identifier); u != nil {
		methods := []string{}

		if len(u.PasswordHash) > 0 {
			methods = append(methods, passwordProv)
		}

		if sc.link {
			methods = append(methods, "emailLink")
		}

		out["registered"] = true
		out["allProviders"] = methods
		out["signinMethods"] = methods
	}

	return out, nil
}

const (
	sessionBytes       = 8
	projectNumberMod   = 1_000_000_000_000
	projectNumberFloor = 100_000_000_000
	minCookieTTL       = 5 * time.Minute
	maxCookieTTL       = 14 * 24 * time.Hour
)

// CreateSessionCookie mints a session cookie from an ID token.
func (b *InMemoryBackend) CreateSessionCookie(c call, r *authRequest) (map[string]string, error) {
	dur := int64(r.ValidDuration)
	if dur == 0 {
		dur = int64(maxCookieTTL.Seconds())
	}

	if dur < int64(minCookieTTL.Seconds()) || dur > int64(maxCookieTTL.Seconds()) {
		return nil, idtk("INVALID_DURATION")
	}

	b.mu.Lock("CreateSessionCookie")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	if r.IDToken == "" {
		return nil, idtk("MISSING_ID_TOKEN")
	}

	u, vt, err := b.userForTokenLocked(sc, r.IDToken)
	if err != nil {
		return nil, err
	}

	provider := passwordProv

	if fb, ok := vt.claims["firebase"].(map[string]any); ok {
		if p, _ := fb["sign_in_provider"].(string); p != "" {
			provider = p
		}
	}

	authTime := b.now().Unix()
	if f, ok := vt.claims["auth_time"].(float64); ok {
		authTime = int64(f)
	}

	claims := b.tokenClaims(sc, u, provider, authTime, sessionIssuer, nil)
	claims["exp"] = b.now().Add(time.Duration(dur) * time.Second).Unix()

	cookie, err := b.signJWTLocked(claims)
	if err != nil {
		return nil, err
	}

	return map[string]string{"sessionCookie": cookie}, nil
}

func oobLink(host, mode, code, continueURL, tenant string) string {
	q := url.Values{}
	q.Set("mode", mode)
	q.Set("lang", "en")
	q.Set("oobCode", code)
	q.Set("apiKey", fakeAPIKey)

	if continueURL != "" {
		q.Set("continueUrl", continueURL)
	}

	if tenant != "" {
		q.Set("tenantId", tenant)
	}

	return "http://" + host + "/emulator/action?" + q.Encode()
}
