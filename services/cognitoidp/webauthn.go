package cognitoidp

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
)

const (
	webauthnRegSessionPrefix = "webauthn-reg:"
	webauthnConfigRPID       = "RelyingPartyId"
	webauthnConfigUV         = "UserVerification"
	webauthnConfigFactor     = "FactorConfiguration"
	webauthnFactorMFA        = "MULTI_FACTOR_WITH_USER_VERIFICATION"
	webauthnCredentialParam  = "CREDENTIAL"
	webauthnRequestOptsParam = "CREDENTIAL_REQUEST_OPTIONS"
)

// validateWebAuthnConfiguration checks a SetUserPoolMfaConfig WebAuthnConfiguration member.
func validateWebAuthnConfiguration(cfg map[string]any) error {
	if rp, ok := cfg[webauthnConfigRPID].(string); ok && rp != "" {
		if err := protocol.ValidateRPID(rp); err != nil {
			return fmt.Errorf("%w: RelyingPartyId: %w", ErrInvalidParameter, err)
		}
	}

	if uv, ok := cfg[webauthnConfigUV].(string); ok && uv != "" &&
		uv != string(protocol.VerificationRequired) && uv != string(protocol.VerificationPreferred) {
		return fmt.Errorf("%w: UserVerification must be required or preferred", ErrInvalidParameter)
	}

	if f, ok := cfg[webauthnConfigFactor].(string); ok && f != "" &&
		f != webauthnFactorMFA && f != "SINGLE_FACTOR" {
		return fmt.Errorf("%w: FactorConfiguration %q is not valid", ErrInvalidParameter, f)
	}

	return nil
}

// passkeyRP is a pool's resolved passkey relying party configuration.
type passkeyRP struct {
	id   string
	name string
	uv   protocol.UserVerificationRequirement
	mfa  bool
}

// passkeyUser adapts a pool user and its stored credentials to the go-webauthn User.
type passkeyUser struct {
	handle []byte
	name   string
	creds  []webauthn.Credential
}

func (u *passkeyUser) WebAuthnID() []byte                         { return u.handle }
func (u *passkeyUser) WebAuthnName() string                       { return u.name }
func (u *passkeyUser) WebAuthnDisplayName() string                { return u.name }
func (u *passkeyUser) WebAuthnCredentials() []webauthn.Credential { return u.creds }

func (b *InMemoryBackend) passkeyUserLocked(user *User) *passkeyUser {
	stored := b.webauthnCredentials[userStateKey(user.UserPoolID, user.Username)]
	ids := make([]string, 0, len(stored))

	for id, c := range stored {
		if c.Credential != nil {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	creds := make([]webauthn.Credential, 0, len(ids))
	for _, id := range ids {
		creds = append(creds, *stored[id].Credential)
	}

	return &passkeyUser{handle: []byte(user.Sub), name: user.Username, creds: creds}
}

// passkeyRPLocked resolves the pool's passkey settings. WebAuthnNotEnabled when the pool neither
// configured passkeys nor allows WEB_AUTHN as a first factor; WebAuthnConfigurationMissing when there
// is no relying party ID and no domain to default it from.
func (b *InMemoryBackend) passkeyRPLocked(pool *UserPool) (passkeyRP, error) {
	var cfg map[string]any
	if full := b.poolMfaConfigs[pool.ID]; full != nil {
		cfg = full.WebAuthnConfiguration
	}

	allowed := pool.SignInPolicy != nil && slices.Contains(pool.SignInPolicy.AllowedFirstAuthFactors, authFactorWebAuthn)
	if len(cfg) == 0 && !allowed {
		return passkeyRP{}, fmt.Errorf("%w: passkeys are not enabled for pool %q", ErrWebAuthnNotEnabled, pool.ID)
	}

	rp := passkeyRP{name: pool.Name, uv: protocol.VerificationPreferred}
	if rp.name == "" {
		rp.name = pool.ID
	}

	rp.id, _ = cfg[webauthnConfigRPID].(string)
	if rp.id == "" {
		rp.id = b.poolDomainHostLocked(pool.ID)
	}

	if rp.id == "" {
		return passkeyRP{}, fmt.Errorf(
			"%w: pool %q has no relying party ID or user pool domain", ErrWebAuthnConfigurationMissing, pool.ID,
		)
	}

	if uv, _ := cfg[webauthnConfigUV].(string); uv == string(protocol.VerificationRequired) {
		rp.uv = protocol.VerificationRequired
	}

	factor, _ := cfg[webauthnConfigFactor].(string)
	rp.mfa = factor == webauthnFactorMFA

	return rp, nil
}

// poolDomainHostLocked is the host of the pool's domain, or "" when it has none.
func (b *InMemoryBackend) poolDomainHostLocked(poolID string) string {
	d := b.findPoolDomainLocked(poolID)

	switch {
	case d == nil:
		return ""
	case d.CertificateArn != "":
		return d.Domain
	default:
		return d.Domain + ".auth." + b.region + ".amazoncognito.com"
	}
}

// userHasPasskeysLocked reports whether the user can complete a WEB_AUTHN challenge.
func (b *InMemoryBackend) userHasPasskeysLocked(pool *UserPool, user *User) bool {
	if _, err := b.passkeyRPLocked(pool); err != nil {
		return false
	}

	return len(b.passkeyUserLocked(user).creds) > 0
}

// originAllowed accepts an https origin (http on loopback) whose host is the relying party ID or a subdomain.
func originAllowed(origin, rpID string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}

	host := strings.ToLower(u.Hostname())
	loopback := host == "localhost" || host == "127.0.0.1" || host == "::1"

	if u.Scheme != "https" && (u.Scheme != "http" || !loopback) {
		return false
	}

	rpID = strings.ToLower(rpID)

	return host == rpID || strings.HasSuffix(host, "."+rpID)
}

func rpIDHashMatches(rpID string, got []byte) bool {
	want := sha256.Sum256([]byte(rpID))

	return bytes.Equal(want[:], got)
}

func (rp passkeyRP) engine(origin string) (*webauthn.WebAuthn, error) {
	w, err := webauthn.New(&webauthn.Config{RPID: rp.id, RPDisplayName: rp.name, RPOrigins: []string{origin}})
	if err != nil {
		return nil, fmt.Errorf("%w: relying party %q: %w", ErrWebAuthnConfigurationMissing, rp.id, err)
	}

	return w, nil
}

func descriptorsOf(creds []webauthn.Credential) []protocol.CredentialDescriptor {
	out := make([]protocol.CredentialDescriptor, 0, len(creds))
	for i := range creds {
		out = append(out, creds[i].Descriptor())
	}

	return out
}

func toJSONMap(v any) (map[string]any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encoding webauthn options: %w", err)
	}

	var out map[string]any
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decoding webauthn options: %w", err)
	}

	return out, nil
}

// StartWebAuthnRegistration returns the PublicKeyCredentialCreationOptions for the authenticated
// user's next CompleteWebAuthnRegistration call and remembers the challenge for it.
func (b *InMemoryBackend) StartWebAuthnRegistration(accessToken string) (map[string]any, error) {
	b.mu.Lock("StartWebAuthnRegistration")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	pool, ok := b.pools.Get(user.UserPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, user.UserPoolID)
	}

	rp, err := b.passkeyRPLocked(pool)
	if err != nil {
		return nil, err
	}

	engine, err := rp.engine("https://" + rp.id)
	if err != nil {
		return nil, err
	}

	pu := b.passkeyUserLocked(user)

	creation, session, err := engine.BeginRegistration(pu,
		webauthn.WithExclusions(descriptorsOf(pu.creds)),
		webauthn.WithAuthenticatorSelection(protocol.AuthenticatorSelection{
			ResidentKey: protocol.ResidentKeyRequirementPreferred, UserVerification: rp.uv,
		}),
		webauthn.WithConveyancePreference(protocol.PreferNoAttestation),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: beginning registration: %w", ErrWebAuthnConfigurationMissing, err)
	}

	opts, err := toJSONMap(creation.Response)
	if err != nil {
		return nil, err
	}

	b.mfaSessions[webauthnRegSessionPrefix+userStateKey(user.UserPoolID, user.Username)] = &mfaSessionEntry{
		PoolID: pool.ID, ClientID: accessTokenClientID(pool, accessToken), Username: user.Username,
		ChallengeType: "WEBAUTHN_REGISTRATION", WebAuthn: session, ExpiresAt: b.now().Add(mfaSessionTTL),
	}

	return opts, nil
}

// CompleteWebAuthnRegistration verifies a RegistrationResponseJSON against the challenge issued by
// StartWebAuthnRegistration and stores the resulting credential.
func (b *InMemoryBackend) CompleteWebAuthnRegistration(
	accessToken string, credential map[string]any,
) (*WebAuthnCredential, error) {
	b.mu.Lock("CompleteWebAuthnRegistration")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	pool, ok := b.pools.Get(user.UserPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, user.UserPoolID)
	}

	rp, err := b.passkeyRPLocked(pool)
	if err != nil {
		return nil, err
	}

	sessKey := webauthnRegSessionPrefix + userStateKey(user.UserPoolID, user.Username)
	entry, ok := b.mfaSessions[sessKey]
	delete(b.mfaSessions, sessKey)

	if !ok || entry.WebAuthn == nil || !entry.ExpiresAt.After(b.now()) {
		return nil, fmt.Errorf("%w: start a new registration", ErrWebAuthnChallengeNotFound)
	}

	if entry.ClientID != accessTokenClientID(pool, accessToken) {
		return nil, fmt.Errorf("%w: access token is for a different app client", ErrWebAuthnClientMismatch)
	}

	raw, err := json.Marshal(credential)
	if err != nil {
		return nil, fmt.Errorf("%w: credential: %w", ErrInvalidParameter, err)
	}

	parsed, err := protocol.ParseCredentialCreationResponseBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: credential: %w", ErrInvalidParameter, err)
	}

	origin := parsed.Response.CollectedClientData.Origin
	if !originAllowed(origin, rp.id) {
		return nil, fmt.Errorf("%w: origin %q is not allowed for %q", ErrWebAuthnOriginNotAllowed, origin, rp.id)
	}

	if !rpIDHashMatches(rp.id, parsed.Response.AttestationObject.AuthData.RPIDHash) {
		return nil, fmt.Errorf("%w: credential is not bound to %q", ErrWebAuthnRelyingPartyMismatch, rp.id)
	}

	engine, err := rp.engine(origin)
	if err != nil {
		return nil, err
	}

	verified, err := engine.CreateCredential(b.passkeyUserLocked(user), *entry.WebAuthn, parsed)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWebAuthnCredentialNotSupported, err)
	}

	return b.storePasskeyLocked(user, rp, credential, verified)
}

func (b *InMemoryBackend) storePasskeyLocked(
	user *User, rp passkeyRP, credential map[string]any, verified *webauthn.Credential,
) (*WebAuthnCredential, error) {
	key := userStateKey(user.UserPoolID, user.Username)
	id := base64.RawURLEncoding.EncodeToString(verified.ID)

	if _, dup := b.webauthnCredentials[key][id]; dup {
		return nil, fmt.Errorf("%w: credential %q is already registered", ErrInvalidParameter, id)
	}

	if b.webauthnCredentials[key] == nil {
		b.webauthnCredentials[key] = make(map[string]*WebAuthnCredential)
	}

	attachment, _ := credential["authenticatorAttachment"].(string)
	transports := credentialTransports(credential)

	if transports == nil {
		transports = []string{}
	}

	rec := &WebAuthnCredential{
		Credential:              verified,
		CredentialID:            id,
		FriendlyName:            fmt.Sprintf("Passkey %d", len(b.webauthnCredentials[key])+1),
		RelyingPartyID:          rp.id,
		AuthenticatorAttachment: attachment,
		AuthenticatorTransports: transports,
		CreatedAt:               b.now(),
	}
	b.webauthnCredentials[key][id] = rec

	cp := *rec

	return &cp, nil
}

// beginPasskeyLoginLocked issues a WEB_AUTHN challenge for the user.
func (b *InMemoryBackend) beginPasskeyLoginLocked(
	pool *UserPool, clientID string, user *User,
) (*AuthResult, error) {
	rp, err := b.passkeyRPLocked(pool)
	if err != nil {
		return nil, err
	}

	pu := b.passkeyUserLocked(user)
	if len(pu.creds) == 0 {
		return nil, fmt.Errorf("%w: user %q has no registered passkey", ErrInvalidParameter, user.Username)
	}

	engine, err := rp.engine("https://" + rp.id)
	if err != nil {
		return nil, err
	}

	assertion, session, err := engine.BeginLogin(pu, webauthn.WithUserVerification(rp.uv))
	if err != nil {
		return nil, fmt.Errorf("%w: beginning passkey login: %w", ErrInvalidParameter, err)
	}

	opts, err := json.Marshal(assertion.Response)
	if err != nil {
		return nil, fmt.Errorf("encoding credential request options: %w", err)
	}

	token := randomAlphanumeric(mfaSessionLen)
	b.mfaSessions[token] = &mfaSessionEntry{
		PoolID: pool.ID, ClientID: clientID, Username: user.Username, ChallengeType: authFactorWebAuthn,
		FirstFactor: true, WebAuthn: session, ExpiresAt: b.now().Add(mfaSessionTTL),
	}

	return &AuthResult{
		MFASession: token, ChallengeName: authFactorWebAuthn,
		ChallengeParameters: map[string]string{webauthnRequestOptsParam: string(opts)},
	}, nil
}

// verifyPasskeyLocked checks an AuthenticationResponseJSON against the pending challenge. It returns
// whether the assertion verified the user and satisfies the pool's passkey-as-MFA setting.
func (b *InMemoryBackend) verifyPasskeyLocked(
	pool *UserPool, user *User, entry *mfaSessionEntry, responses map[string]string,
) (bool, error) {
	rp, err := b.passkeyRPLocked(pool)
	if err != nil {
		return false, err
	}

	parsed, err := protocol.ParseCredentialRequestResponseBytes([]byte(responses[webauthnCredentialParam]))
	if err != nil {
		return false, fmt.Errorf("%w: CREDENTIAL: %w", ErrNotAuthorized, err)
	}

	origin := parsed.Response.CollectedClientData.Origin
	if !originAllowed(origin, rp.id) {
		return false, fmt.Errorf("%w: origin %q is not allowed for %q", ErrWebAuthnOriginNotAllowed, origin, rp.id)
	}

	if !rpIDHashMatches(rp.id, parsed.Response.AuthenticatorData.RPIDHash) {
		return false, fmt.Errorf("%w: credential is not bound to %q", ErrWebAuthnRelyingPartyMismatch, rp.id)
	}

	engine, err := rp.engine(origin)
	if err != nil {
		return false, err
	}

	verified, err := engine.ValidateLogin(b.passkeyUserLocked(user), *entry.WebAuthn, parsed)
	if err != nil || verified.Authenticator.CloneWarning {
		return false, fmt.Errorf("%w: passkey assertion rejected", ErrNotAuthorized)
	}

	id := base64.RawURLEncoding.EncodeToString(verified.ID)
	if rec := b.webauthnCredentials[userStateKey(user.UserPoolID, user.Username)][id]; rec != nil {
		rec.Credential = verified
	}

	uv := parsed.Response.AuthenticatorData.Flags.HasUserVerified()

	return rp.mfa && uv, nil
}

// ListWebAuthnCredentials returns a page of WebAuthn credentials for the authenticated user.
func (b *InMemoryBackend) ListWebAuthnCredentials(
	accessToken string,
	limit int,
	nextToken string,
) ([]*WebAuthnCredential, string, error) {
	b.mu.RLock("ListWebAuthnCredentials")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, "", err
	}

	creds := b.webauthnCredentials[userStateKey(user.UserPoolID, user.Username)]
	all := make([]*WebAuthnCredential, 0, len(creds))

	for _, c := range creds {
		cp := *c
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CredentialID < all[j].CredentialID })

	startIdx := 0

	if nextToken != "" {
		startIdx = len(all)

		for i, c := range all {
			if c.CredentialID == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit >= len(all) {
		return all, "", nil
	}

	return all[:limit], all[limit].CredentialID, nil
}

// DeleteWebAuthnCredential removes a WebAuthn credential for the authenticated user.
func (b *InMemoryBackend) DeleteWebAuthnCredential(accessToken, credentialID string) error {
	b.mu.Lock("DeleteWebAuthnCredential")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if _, ok := b.webauthnCredentials[key][credentialID]; !ok {
		return fmt.Errorf("%w: credential %q not found", ErrWebAuthnCredentialNotFound, credentialID)
	}

	delete(b.webauthnCredentials[key], credentialID)

	return nil
}
