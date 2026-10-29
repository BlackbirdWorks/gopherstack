package gcpidentitytoolkit

import (
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	securetokenIssuer = "https://securetoken.google.com/" //nolint:gosec // public URL, not a credential
	sessionIssuer     = "https://session.firebase.google.com/"
	customTokenAud    = "https://identitytoolkit.googleapis.com/google.identity.identitytoolkit.v1.IdentityToolkit" //nolint:gosec,lll // public audience
	maxCustomTokenAge = time.Hour
	clockSkew         = 5 * time.Minute
)

//nolint:gochecknoglobals // immutable lookup table
var reservedClaims = map[string]bool{
	"acr": true, "amr": true, "at_hash": true, "aud": true, "auth_time": true, "azp": true, "cnf": true,
	"c_hash": true, "exp": true, "firebase": true, "iat": true, "iss": true, "jti": true, "nbf": true,
	"nonce": true, "sub": true,
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// refreshPayload is the signed content of a refresh token.
type refreshPayload struct {
	Claims   map[string]any `json:"c,omitempty"`
	UID      string         `json:"u"`
	Project  string         `json:"p"`
	Tenant   string         `json:"t,omitempty"`
	Provider string         `json:"s"`
	Issued   int64          `json:"i"`
	AuthTime int64          `json:"a"`
}

func (b *InMemoryBackend) signRefreshLocked(p refreshPayload) (string, error) {
	_, keys, err := b.ensureKeysLocked()
	if err != nil {
		return "", err
	}

	body, err := json.Marshal(p)
	if err != nil {
		return "", err //nolint:wrapcheck // unreachable
	}

	mac := hmac.New(sha256.New, keys.Secret)
	mac.Write(body)

	return b64url(body) + "." + b64url(mac.Sum(nil)), nil
}

func (b *InMemoryBackend) parseRefreshLocked(tok string) (*refreshPayload, bool) {
	if b.keys == nil {
		return nil, false
	}

	bodyB64, sigB64, ok := strings.Cut(tok, ".")
	if !ok {
		return nil, false
	}

	body, err1 := base64.RawURLEncoding.DecodeString(bodyB64)
	sig, err2 := base64.RawURLEncoding.DecodeString(sigB64)

	if err1 != nil || err2 != nil {
		return nil, false
	}

	mac := hmac.New(sha256.New, b.keys.Secret)
	mac.Write(body)

	if !hmac.Equal(sig, mac.Sum(nil)) {
		return nil, false
	}

	var p refreshPayload
	if json.Unmarshal(body, &p) != nil {
		return nil, false
	}

	return &p, true
}

// identities lists the user's identifiers per provider, as the token's firebase.identities claim.
func identities(u *userRec) map[string][]string {
	out := map[string][]string{}

	if u.Email != "" {
		out["email"] = []string{u.Email}
	}

	if u.Phone != "" {
		out[phoneProv] = []string{u.Phone}
	}

	for _, p := range u.Providers {
		if p.ProviderID == passwordProv || p.ProviderID == phoneProv {
			continue
		}

		out[p.ProviderID] = append(out[p.ProviderID], p.RawID)
	}

	return out
}

func (b *InMemoryBackend) tokenClaims(
	sc *scope,
	u *userRec,
	provider string,
	authTime int64,
	issuer string,
	extra map[string]any,
) jwt.MapClaims {
	claims := jwt.MapClaims{}

	if u.CustomAttrs != "" {
		var custom map[string]any
		if json.Unmarshal([]byte(u.CustomAttrs), &custom) == nil {
			maps.Copy(claims, custom)
		}
	}

	maps.Copy(claims, extra)

	if u.DisplayName != "" {
		claims["name"] = u.DisplayName
	}

	if u.PhotoURL != "" {
		claims["picture"] = u.PhotoURL
	}

	fb := map[string]any{"identities": identities(u), "sign_in_provider": provider}
	if sc.tenant != rootTenantKey {
		fb["tenant"] = sc.tenant
	}

	now := b.now()
	claims["iss"] = issuer + sc.project
	claims["aud"] = sc.project
	claims["auth_time"] = authTime
	claims["user_id"] = u.UID
	claims["sub"] = u.UID
	claims["iat"] = now.Unix()
	claims["exp"] = now.Add(idTokenTTL).Unix()

	if u.Email != "" {
		claims["email"] = u.Email
		claims["email_verified"] = u.EmailVerified
	}

	if u.Phone != "" {
		claims["phone_number"] = u.Phone
	}

	claims["firebase"] = fb

	return claims
}

func (b *InMemoryBackend) signJWTLocked(claims jwt.MapClaims) (string, error) {
	priv, keys, err := b.ensureKeysLocked()
	if err != nil {
		return "", err
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = keys.KID

	return tok.SignedString(priv)
}

// session is one sign-in's token pair.
type session struct {
	IDToken      string
	RefreshToken string
}

// issueLocked mints an ID token and refresh token for a fresh sign-in.
func (b *InMemoryBackend) issueLocked(sc *scope, u *userRec, provider string, extra map[string]any) (*session, error) {
	now := b.now().Unix()

	refresh, err := b.signRefreshLocked(refreshPayload{
		UID: u.UID, Project: sc.project, Tenant: sc.tenant, Provider: provider,
		Issued: now, AuthTime: now, Claims: extra,
	})
	if err != nil {
		return nil, err
	}

	id, err := b.signJWTLocked(b.tokenClaims(sc, u, provider, now, securetokenIssuer, extra))
	if err != nil {
		return nil, err
	}

	u.LastRefreshAt = b.nowMillis()

	return &session{IDToken: id, RefreshToken: refresh}, nil
}

// verifiedToken is a validated ID token.
type verifiedToken struct {
	claims jwt.MapClaims
	uid    string
	tenant string
}

// verifyJWTLocked checks signature, issuer, audience and expiry of a token this backend signed.
func (b *InMemoryBackend) verifyJWTLocked(raw, project, issuer string) (*verifiedToken, error) {
	if b.priv == nil && b.keys == nil {
		return nil, idtk("INVALID_ID_TOKEN")
	}

	priv, _, err := b.ensureKeysLocked()
	if err != nil {
		return nil, err
	}

	claims := jwt.MapClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{algRS256}),
		jwt.WithTimeFunc(b.now),
		jwt.WithExpirationRequired(),
	)

	_, err = parser.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return &priv.PublicKey, nil })
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, idtk("TOKEN_EXPIRED")
		}

		return nil, idtk("INVALID_ID_TOKEN")
	}

	if iss, _ := claims["iss"].(string); iss != issuer+project {
		return nil, idtk("INVALID_ID_TOKEN")
	}

	if aud, _ := claims["aud"].(string); aud != project {
		return nil, idtk("INVALID_ID_TOKEN")
	}

	uid, _ := claims["sub"].(string)
	tenant := ""

	if fb, ok := claims["firebase"].(map[string]any); ok {
		tenant, _ = fb["tenant"].(string)
	}

	return &verifiedToken{claims: claims, uid: uid, tenant: tenant}, nil
}

// userForTokenLocked resolves and checks the account behind an ID token.
func (b *InMemoryBackend) userForTokenLocked(sc *scope, raw string) (*userRec, *verifiedToken, error) {
	vt, err := b.verifyJWTLocked(raw, sc.project, securetokenIssuer)
	if err != nil {
		return nil, nil, err
	}

	if vt.tenant != sc.tenant {
		return nil, nil, idtk("INVALID_ID_TOKEN", "tenant mismatch")
	}

	u, ok := sc.users[vt.uid]
	if !ok {
		return nil, nil, idtk("USER_NOT_FOUND")
	}

	iat, _ := vt.claims.GetIssuedAt()
	if u.ValidSince > 0 && iat != nil && iat.Unix() < u.ValidSince {
		return nil, nil, idtk("TOKEN_EXPIRED")
	}

	if u.Disabled {
		return nil, nil, idtk("USER_DISABLED", "The user account has been disabled by an administrator.")
	}

	return u, vt, nil
}

func (b *InMemoryBackend) customTokenLocked(raw string, sc *scope) (string, map[string]any, error) {
	claims := jwt.MapClaims{}

	tok, _, err := jwt.NewParser().ParseUnverified(raw, claims)
	if err != nil {
		return "", nil, idtk("INVALID_CUSTOM_TOKEN", "Invalid token format.")
	}

	if err = b.checkCustomSignature(raw, tok.Method.Alg(), claims); err != nil {
		return "", nil, err
	}

	return b.checkCustomClaims(claims, sc)
}

func (b *InMemoryBackend) checkCustomSignature(raw, alg string, claims jwt.MapClaims) error {
	bad := func(detail string) error { return idtk("INVALID_CUSTOM_TOKEN", detail) }

	switch alg {
	case "none":
		if parts := strings.Split(raw, "."); len(parts) != 3 || parts[2] != "" {
			return bad("Unsigned token carries a signature.")
		}
	case algRS256:
		if !b.verifyCustomSignature(raw, claims) {
			return bad("Token signature could not be verified against a known service account key.")
		}
	default:
		return bad("Unsupported algorithm.")
	}

	return nil
}

func (b *InMemoryBackend) checkCustomClaims(claims jwt.MapClaims, sc *scope) (string, map[string]any, error) {
	bad := func(detail string) error { return idtk("INVALID_CUSTOM_TOKEN", detail) }

	if aud, _ := claims["aud"].(string); aud != customTokenAud {
		return "", nil, bad("Invalid audience.")
	}

	now := b.now()

	exp, err := claims.GetExpirationTime()
	if err != nil || exp == nil || now.After(exp.Add(clockSkew)) {
		return "", nil, bad("Token has expired.")
	}

	iat, err := claims.GetIssuedAt()
	if err != nil || iat == nil || iat.After(now.Add(clockSkew)) || exp.Sub(iat.Time) > maxCustomTokenAge+clockSkew {
		return "", nil, bad("Invalid issue time.")
	}

	uid, _ := claims["uid"].(string)
	if uid == "" || len(uid) > maxUIDLen {
		return "", nil, bad("Invalid uid.")
	}

	if t, _ := claims["tenant_id"].(string); t != sc.tenant {
		return "", nil, idtk("TENANT_ID_MISMATCH")
	}

	dev, _ := claims["claims"].(map[string]any)
	for k := range dev {
		if reservedClaims[k] {
			return "", nil, idtk("FORBIDDEN_CLAIM", k)
		}
	}

	return uid, dev, nil
}

func (b *InMemoryBackend) verifyCustomSignature(raw string, claims jwt.MapClaims) bool {
	if b.customKeys == nil {
		return false
	}

	iss, _ := claims["iss"].(string)

	for _, key := range b.customKeys(iss) {
		parser := jwt.NewParser(jwt.WithValidMethods([]string{algRS256}), jwt.WithoutClaimsValidation())
		if _, err := parser.Parse(raw, func(*jwt.Token) (any, error) { return key, nil }); err == nil {
			return true
		}
	}

	return false
}

// publicKeyCert returns the signing certificate and key id, creating keys on first use.
func (b *InMemoryBackend) publicKeyCert() (string, string, *rsa.PublicKey, error) {
	b.mu.Lock("publicKeyCert")
	defer b.mu.Unlock()

	priv, keys, err := b.ensureKeysLocked()
	if err != nil {
		return "", "", nil, err
	}

	return keys.KID, keys.Cert, &priv.PublicKey, nil
}
