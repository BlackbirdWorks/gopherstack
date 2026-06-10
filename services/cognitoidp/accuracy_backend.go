package cognitoidp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// Challenge name constants for auth challenge flows.
const (
	challengeSoftwareTokenMFA    = "SOFTWARE_TOKEN_MFA"
	challengeNewPasswordRequired = "NEW_PASSWORD_REQUIRED"
	challengeSMSMFA              = "SMS_MFA"
	challengeEmailOTP            = "EMAIL_OTP"
)

// confirmCodeTTL is the lifetime of a confirmation or reset code before it expires.
const confirmCodeTTL = 24 * time.Hour

// totpSecretLen is the number of random bytes for a TOTP secret (160-bit secret per RFC 6238).
const totpSecretLen = 20

// ResourceServerScope defines a single OAuth scope on a resource server.
type ResourceServerScope struct {
	ScopeName        string `json:"ScopeName,omitempty"`
	ScopeDescription string `json:"ScopeDescription,omitempty"`
}

// ResourceServer represents an OAuth 2.0 resource server registered to a user pool.
type ResourceServer struct {
	UserPoolID string                `json:"UserPoolId,omitempty"`
	Identifier string                `json:"Identifier,omitempty"`
	Name       string                `json:"Name,omitempty"`
	Scopes     []ResourceServerScope `json:"Scopes,omitempty"`
}

// UserPoolOptions holds optional parameters for CreateUserPoolWithOpts.
type UserPoolOptions struct {
	PasswordPolicy         *PasswordPolicy `json:"passwordPolicy,omitempty"`
	AutoVerifiedAttributes []string        `json:"autoVerifiedAttributes,omitempty"`
}

// UserPoolClientOptions holds optional parameters for CreateUserPoolClientWithOpts and UpdateUserPoolClientWithOpts.
type UserPoolClientOptions struct {
	AllowedOAuthFlows     []string `json:"allowedOAuthFlows,omitempty"`
	AllowedOAuthScopes    []string `json:"allowedOAuthScopes,omitempty"`
	ExplicitAuthFlows     []string `json:"explicitAuthFlows,omitempty"`
	GenerateSecret        bool     `json:"generateSecret,omitempty"`
	EnableTokenRevocation bool     `json:"enableTokenRevocation,omitempty"`
}

// userGroupsLocked returns the group names for a user in a pool, sorted by group precedence ascending
// (lowest precedence value = highest priority), then alphabetically. Caller must hold at least a read lock.
func (b *InMemoryBackend) userGroupsLocked(poolID, username string) []string {
	type gp struct {
		name       string
		precedence int32
	}

	var matched []gp

	for groupName, members := range b.groupMembers[poolID] {
		if _, isMember := members[username]; !isMember {
			continue
		}
		if g, ok := b.groups[poolID][groupName]; ok {
			matched = append(matched, gp{name: groupName, precedence: g.Precedence})
		}
	}

	slices.SortFunc(matched, func(a, b gp) int {
		if a.precedence != b.precedence {
			if a.precedence < b.precedence {
				return -1
			}

			return 1
		}

		if a.name < b.name {
			return -1
		}

		if a.name > b.name {
			return 1
		}

		return 0
	})

	out := make([]string, len(matched))
	for i, g := range matched {
		out[i] = g.name
	}

	return out
}

// validatePasswordPolicy checks AWS-imposed bounds on a PasswordPolicy at
// pool create / update time. AWS rejects MinimumLength outside [6, 99] and
// TemporaryPasswordValidityDays outside [0, 365] with InvalidParameterException.
func validatePasswordPolicy(pp *PasswordPolicy) error {
	if pp == nil {
		return nil
	}

	const (
		minPasswordLength   = 6
		maxPasswordLength   = 99
		maxTempPwdValidDays = 365
	)

	if pp.MinimumLength != 0 && (pp.MinimumLength < minPasswordLength || pp.MinimumLength > maxPasswordLength) {
		return fmt.Errorf("%w: MinimumLength must be in [%d, %d]",
			ErrInvalidParameter, minPasswordLength, maxPasswordLength)
	}

	if pp.TemporaryPasswordValidityDays < 0 || pp.TemporaryPasswordValidityDays > maxTempPwdValidDays {
		return fmt.Errorf("%w: TemporaryPasswordValidityDays must be in [0, %d]",
			ErrInvalidParameter, maxTempPwdValidDays)
	}

	return nil
}

// validatePassword checks the proposed password against the pool's PasswordPolicy.
// Returns ErrInvalidPassword if the policy is violated.
func validatePassword(policy *PasswordPolicy, password string) error {
	if policy == nil {
		return nil
	}

	minLen := policy.MinimumLength
	if minLen <= 0 {
		minLen = 8
	}

	if len(password) < minLen {
		return fmt.Errorf("%w: password must be at least %d characters", ErrInvalidPassword, minLen)
	}

	if policy.RequireUppercase && !strings.ContainsFunc(password, unicode.IsUpper) {
		return fmt.Errorf("%w: password must contain at least one uppercase letter", ErrInvalidPassword)
	}

	if policy.RequireLowercase && !strings.ContainsFunc(password, unicode.IsLower) {
		return fmt.Errorf("%w: password must contain at least one lowercase letter", ErrInvalidPassword)
	}

	if policy.RequireNumbers && !strings.ContainsFunc(password, unicode.IsDigit) {
		return fmt.Errorf("%w: password must contain at least one number", ErrInvalidPassword)
	}

	if policy.RequireSymbols {
		isSymbol := func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsSpace(r) }
		if !strings.ContainsFunc(password, isSymbol) {
			return fmt.Errorf("%w: password must contain at least one symbol", ErrInvalidPassword)
		}
	}

	return nil
}

// CreateUserPoolWithOpts creates a user pool with optional PasswordPolicy and AutoVerifiedAttributes.
func (b *InMemoryBackend) CreateUserPoolWithOpts(name string, opts UserPoolOptions) (*UserPool, error) {
	b.mu.Lock("CreateUserPoolWithOpts")
	defer b.mu.Unlock()

	if _, ok := b.poolsByName[name]; ok {
		return nil, fmt.Errorf("%w: pool %q already exists", ErrUserPoolAlreadyExists, name)
	}

	poolID := b.region + "_" + randomAlphanumeric(poolIDSuffixLen)
	issuerURL := fmt.Sprintf("%s/%s", b.endpoint, poolID)

	issuer, err := newTokenIssuer(issuerURL)
	if err != nil {
		return nil, fmt.Errorf("creating token issuer: %w", err)
	}

	autoVerified := make([]string, len(opts.AutoVerifiedAttributes))
	copy(autoVerified, opts.AutoVerifiedAttributes)

	pool := &UserPool{
		ID:                     poolID,
		Name:                   name,
		ARN:                    fmt.Sprintf("arn:aws:cognito-idp:%s:%s:userpool/%s", b.region, b.accountID, poolID),
		CreatedAt:              time.Now(),
		issuer:                 issuer,
		AutoVerifiedAttributes: autoVerified,
		PasswordPolicy:         opts.PasswordPolicy,
	}

	b.pools[poolID] = pool
	b.poolsByName[name] = pool
	b.users[poolID] = make(map[string]*User)

	cp := *pool

	return &cp, nil
}

// CreateUserPoolClientWithOpts creates an app client with full OAuth and flow configuration.
func (b *InMemoryBackend) CreateUserPoolClientWithOpts(
	userPoolID, clientName string,
	opts UserPoolClientOptions,
) (*UserPoolClient, error) {
	b.mu.Lock("CreateUserPoolClientWithOpts")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	flows := make([]string, len(opts.AllowedOAuthFlows))
	copy(flows, opts.AllowedOAuthFlows)
	scopes := make([]string, len(opts.AllowedOAuthScopes))
	copy(scopes, opts.AllowedOAuthScopes)
	explicitFlows := make([]string, len(opts.ExplicitAuthFlows))
	copy(explicitFlows, opts.ExplicitAuthFlows)

	client := &UserPoolClient{
		ClientID:              randomAlphanumeric(clientIDLen),
		ClientName:            clientName,
		UserPoolID:            userPoolID,
		CreatedAt:             time.Now(),
		AllowedOAuthFlows:     flows,
		AllowedOAuthScopes:    scopes,
		ExplicitAuthFlows:     explicitFlows,
		EnableTokenRevocation: opts.EnableTokenRevocation,
	}

	if opts.GenerateSecret {
		client.ClientSecret = randomAlphanumeric(clientSecretLen)
	}

	b.clients[client.ClientID] = client
	if b.clientsByPool[userPoolID] == nil {
		b.clientsByPool[userPoolID] = make(map[string]*UserPoolClient)
	}
	b.clientsByPool[userPoolID][client.ClientID] = client

	cp := *client

	return &cp, nil
}

// UpdateUserPoolClientWithOpts updates app client fields including OAuth flows and scopes.
func (b *InMemoryBackend) UpdateUserPoolClientWithOpts(
	userPoolID, clientID, clientName string,
	opts UserPoolClientOptions,
) (*UserPoolClient, error) {
	b.mu.Lock("UpdateUserPoolClientWithOpts")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients[clientID]
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q does not belong to pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	if clientName != "" {
		client.ClientName = clientName
	}

	if opts.AllowedOAuthFlows != nil {
		flows := make([]string, len(opts.AllowedOAuthFlows))
		copy(flows, opts.AllowedOAuthFlows)
		client.AllowedOAuthFlows = flows
	}

	if opts.AllowedOAuthScopes != nil {
		scopes := make([]string, len(opts.AllowedOAuthScopes))
		copy(scopes, opts.AllowedOAuthScopes)
		client.AllowedOAuthScopes = scopes
	}

	if opts.ExplicitAuthFlows != nil {
		ef := make([]string, len(opts.ExplicitAuthFlows))
		copy(ef, opts.ExplicitAuthFlows)
		client.ExplicitAuthFlows = ef
	}

	client.EnableTokenRevocation = opts.EnableTokenRevocation
	cp := *client

	return &cp, nil
}

// UpdateUserPoolWithOpts updates user pool PasswordPolicy, AutoVerifiedAttributes, and MFA config.
func (b *InMemoryBackend) UpdateUserPoolWithOpts(
	userPoolID, mfaConfiguration string,
	opts UserPoolOptions,
) error {
	b.mu.Lock("UpdateUserPoolWithOpts")
	defer b.mu.Unlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if mfaConfiguration != "" {
		pool.MfaConfiguration = mfaConfiguration
	}

	if opts.PasswordPolicy != nil {
		pp := *opts.PasswordPolicy
		pool.PasswordPolicy = &pp
	}

	if opts.AutoVerifiedAttributes != nil {
		av := make([]string, len(opts.AutoVerifiedAttributes))
		copy(av, opts.AutoVerifiedAttributes)
		pool.AutoVerifiedAttributes = av
	}

	return nil
}

// SignUpWithValidation is like SignUp but enforces the pool's PasswordPolicy and
// automatically verifies attributes configured in AutoVerifiedAttributes.
func (b *InMemoryBackend) SignUpWithValidation(
	clientID, username, password string,
	userAttributes map[string]string,
) (*User, error) {
	b.mu.Lock("SignUpWithValidation")
	defer b.mu.Unlock()

	client, ok := b.clients[clientID]
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools[client.UserPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	if err := validatePassword(pool.PasswordPolicy, password); err != nil {
		return nil, err
	}

	poolUsers := b.users[client.UserPoolID]
	if _, exists := poolUsers[username]; exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUsernameExists, username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hashing password: %w", err)
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	// Auto-verify attributes that are configured on the pool.
	autoConfirmed := false

	for _, attr := range pool.AutoVerifiedAttributes {
		if _, hasAttr := attrs[attr]; hasAttr {
			attrs[attr+"_verified"] = attrVerifiedTrue
			autoConfirmed = true
		}
	}

	status := UserStatusUnconfirmed
	var confirmCode string
	var confirmExpiry time.Time

	if autoConfirmed {
		status = UserStatusConfirmed
	} else {
		confirmCode = randomAlphanumeric(confirmCodeLen)
		confirmExpiry = time.Now().Add(confirmCodeTTL)
	}

	user := &User{
		Sub:                  uuid.New().String(),
		Username:             username,
		UserPoolID:           client.UserPoolID,
		PasswordHash:         string(hash),
		Status:               status,
		Attributes:           attrs,
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
		Enabled:              true,
		ConfirmCode:          confirmCode,
		ConfirmCodeExpiresAt: confirmExpiry,
	}

	poolUsers[username] = user
	b.usersBySub[client.UserPoolID+":"+user.Sub] = username

	cp := *user

	return &cp, nil
}

// RespondToNewPasswordRequired allows a user in FORCE_CHANGE_PASSWORD status to set a
// permanent password and receive tokens. The session token is from authenticate().
func (b *InMemoryBackend) RespondToNewPasswordRequired(
	clientID, session, newPassword string,
) (*TokenResult, error) {
	b.mu.Lock("RespondToNewPasswordRequired")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok {
		return nil, fmt.Errorf("%w: session not found or expired", ErrNotAuthorized)
	}

	if entry.ChallengeType != challengeNewPasswordRequired {
		return nil, fmt.Errorf("%w: session is not a NEW_PASSWORD_REQUIRED challenge", ErrNotAuthorized)
	}

	if entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: session was issued for a different client", ErrNotAuthorized)
	}

	pool, ok := b.pools[entry.PoolID]
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	if err := validatePassword(pool.PasswordPolicy, newPassword); err != nil {
		return nil, err
	}

	user, ok := b.users[entry.PoolID][entry.Username]
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hashing password: %w", err)
	}

	user.PasswordHash = string(hash)
	user.Status = UserStatusConfirmed
	user.UpdatedAt = time.Now()
	delete(b.mfaSessions, session)

	result, err := b.issueTokensLocked(pool, clientID, user)
	if err != nil {
		return nil, err
	}

	return result.Tokens, nil
}

// AssociateSoftwareToken generates a new TOTP secret for the authenticated user and stores it.
// Returns the base32-encoded secret for use with TOTP authenticator apps (RFC 6238).
func (b *InMemoryBackend) AssociateSoftwareToken(accessToken string) (string, error) {
	b.mu.Lock("AssociateSoftwareToken")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return "", err
	}

	secretBytes := make([]byte, totpSecretLen)
	if _, err = rand.Read(secretBytes); err != nil {
		return "", fmt.Errorf("generating TOTP secret: %w", err)
	}

	secret := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(secretBytes)
	user.TOTPSecret = secret
	user.TOTPVerified = false

	return secret, nil
}

// VerifySoftwareToken marks the TOTP token as verified. In simulation mode any 6-digit code
// is accepted; the secret was already stored by AssociateSoftwareToken.
func (b *InMemoryBackend) VerifySoftwareToken(accessToken, userCode string) error {
	b.mu.Lock("VerifySoftwareToken")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	if user.TOTPSecret == "" {
		return fmt.Errorf("%w: no TOTP secret associated; call AssociateSoftwareToken first", ErrNotAuthorized)
	}

	if len(userCode) != totpCodeLen {
		return fmt.Errorf("%w: TOTP code must be %d digits", ErrCodeMismatch, totpCodeLen)
	}

	for _, ch := range userCode {
		if ch < '0' || ch > '9' {
			return fmt.Errorf("%w: TOTP code must contain only digits", ErrCodeMismatch)
		}
	}

	user.TOTPVerified = true

	return nil
}

// SetUserMFAPreference sets the preferred MFA method for the authenticated user.
func (b *InMemoryBackend) SetUserMFAPreference(
	accessToken string,
	smsMFAEnabled, softwareTokenEnabled bool,
	preferredMFA string,
) error {
	b.mu.Lock("SetUserMFAPreference")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	return b.applyMFAPreferenceLocked(user, smsMFAEnabled, softwareTokenEnabled, preferredMFA)
}

// AdminSetUserMFASetting sets the MFA configuration for a specific user (admin operation).
func (b *InMemoryBackend) AdminSetUserMFASetting(
	userPoolID, username string,
	smsMFAEnabled, softwareTokenEnabled bool,
	preferredMFA string,
) error {
	b.mu.Lock("AdminSetUserMFASetting")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := b.users[userPoolID][username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	return b.applyMFAPreferenceLocked(user, smsMFAEnabled, softwareTokenEnabled, preferredMFA)
}

// applyMFAPreferenceLocked updates the MFA setting list and preferred MFA on a user.
// Caller must hold the write lock.
func (b *InMemoryBackend) applyMFAPreferenceLocked(
	user *User,
	smsMFAEnabled, softwareTokenEnabled bool,
	preferredMFA string,
) error {
	var settings []string

	if smsMFAEnabled {
		settings = append(settings, challengeSMSMFA)
	}

	if softwareTokenEnabled {
		settings = append(settings, challengeSoftwareTokenMFA)
	}

	sort.Strings(settings)
	user.UserMFASettingList = settings

	if preferredMFA != "" {
		found := slices.Contains(settings, preferredMFA)
		if !found && len(settings) > 0 {
			return fmt.Errorf(
				"%w: preferred MFA %q is not in the enabled MFA list",
				ErrInvalidUserPoolConfig,
				preferredMFA,
			)
		}
		user.PreferredMfaSetting = preferredMFA
	} else if len(settings) == 0 {
		user.PreferredMfaSetting = ""
	}

	user.UpdatedAt = time.Now()

	return nil
}

// EvictExpiredMFASessions removes mfaSession entries that have expired or whose pool or
// user no longer exists. The janitor calls this periodically to prevent unbounded accumulation.
func (b *InMemoryBackend) EvictExpiredMFASessions() {
	b.mu.Lock("EvictExpiredMFASessions")
	defer b.mu.Unlock()

	now := time.Now()

	for token, entry := range b.mfaSessions {
		if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
			delete(b.mfaSessions, token)

			continue
		}

		poolUsers, poolExists := b.users[entry.PoolID]
		if !poolExists {
			delete(b.mfaSessions, token)

			continue
		}

		if _, userExists := poolUsers[entry.Username]; !userExists {
			delete(b.mfaSessions, token)
		}
	}
}

// ---- Resource Server CRUD ----

// CreateResourceServer creates an OAuth 2.0 resource server for a user pool.
func (b *InMemoryBackend) CreateResourceServer(
	userPoolID, identifier, name string,
	scopes []ResourceServerScope,
) (*ResourceServer, error) {
	b.mu.Lock("CreateResourceServer")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if b.resourceServers[userPoolID] == nil {
		b.resourceServers[userPoolID] = make(map[string]*ResourceServer)
	}

	if _, exists := b.resourceServers[userPoolID][identifier]; exists {
		return nil, fmt.Errorf(
			"%w: resource server %q already exists in pool %q",
			ErrAlreadyExists,
			identifier,
			userPoolID,
		)
	}

	scopesCopy := make([]ResourceServerScope, len(scopes))
	copy(scopesCopy, scopes)

	rs := &ResourceServer{
		UserPoolID: userPoolID,
		Identifier: identifier,
		Name:       name,
		Scopes:     scopesCopy,
	}
	b.resourceServers[userPoolID][identifier] = rs

	cp := *rs

	return &cp, nil
}

// DescribeResourceServer returns a resource server by pool ID and identifier.
func (b *InMemoryBackend) DescribeResourceServer(userPoolID, identifier string) (*ResourceServer, error) {
	b.mu.RLock("DescribeResourceServer")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	rs, ok := b.resourceServers[userPoolID][identifier]
	if !ok {
		return nil, fmt.Errorf(
			"%w: resource server %q not found in pool %q",
			ErrUserPoolNotFound,
			identifier,
			userPoolID,
		)
	}

	cp := *rs

	return &cp, nil
}

// ListResourceServers returns all resource servers for a user pool sorted by identifier.
func (b *InMemoryBackend) ListResourceServers(userPoolID string) ([]*ResourceServer, error) {
	b.mu.RLock("ListResourceServers")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolServers := b.resourceServers[userPoolID]
	out := make([]*ResourceServer, 0, len(poolServers))

	for _, rs := range poolServers {
		cp := *rs
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Identifier < out[j].Identifier })

	return out, nil
}

// UpdateResourceServer updates the name and scopes of an existing resource server.
func (b *InMemoryBackend) UpdateResourceServer(
	userPoolID, identifier, name string,
	scopes []ResourceServerScope,
) (*ResourceServer, error) {
	b.mu.Lock("UpdateResourceServer")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	rs, ok := b.resourceServers[userPoolID][identifier]
	if !ok {
		return nil, fmt.Errorf(
			"%w: resource server %q not found in pool %q",
			ErrUserPoolNotFound,
			identifier,
			userPoolID,
		)
	}

	if name != "" {
		rs.Name = name
	}

	if scopes != nil {
		scopesCopy := make([]ResourceServerScope, len(scopes))
		copy(scopesCopy, scopes)
		rs.Scopes = scopesCopy
	}

	cp := *rs

	return &cp, nil
}

// DeleteResourceServer removes a resource server from a user pool.
func (b *InMemoryBackend) DeleteResourceServer(userPoolID, identifier string) error {
	b.mu.Lock("DeleteResourceServer")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.resourceServers[userPoolID][identifier]; !ok {
		return fmt.Errorf(
			"%w: resource server %q not found in pool %q",
			ErrUserPoolNotFound,
			identifier,
			userPoolID,
		)
	}

	delete(b.resourceServers[userPoolID], identifier)

	return nil
}

// RespondToSRPChallenge completes the USER_SRP_AUTH two-step flow. The session token
// was issued by authenticate() after credentials were verified; this call issues tokens.
func (b *InMemoryBackend) RespondToSRPChallenge(clientID, session string) (*TokenResult, error) {
	b.mu.Lock("RespondToSRPChallenge")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok {
		return nil, fmt.Errorf("%w: SRP session not found or expired", ErrNotAuthorized)
	}

	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		delete(b.mfaSessions, session)

		return nil, fmt.Errorf("%w: SRP session not found or expired", ErrNotAuthorized)
	}

	if entry.ChallengeType != challengePasswordVerifier {
		return nil, fmt.Errorf("%w: session is not a PASSWORD_VERIFIER challenge", ErrNotAuthorized)
	}

	if entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: session was issued for a different client", ErrNotAuthorized)
	}

	pool, ok := b.pools[entry.PoolID]
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	user, ok := b.users[entry.PoolID][entry.Username]
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	delete(b.mfaSessions, session)

	result, err := b.issueTokensLocked(pool, clientID, user)
	if err != nil {
		return nil, err
	}

	return result.Tokens, nil
}

// AdminCreateUserWithPolicy creates a new user in the pool with FORCE_CHANGE_PASSWORD status,
// enforcing the pool's PasswordPolicy on the temporary password.
func (b *InMemoryBackend) AdminCreateUserWithPolicy(
	userPoolID, username, tempPassword string,
	userAttributes map[string]string,
) (*User, error) {
	b.mu.Lock("AdminCreateUserWithPolicy")
	defer b.mu.Unlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := poolUsers[username]; exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, username)
	}

	if tempPassword != "" {
		if err := validatePassword(pool.PasswordPolicy, tempPassword); err != nil {
			return nil, err
		}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(tempPassword), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hashing password: %w", err)
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	if tempPassword != "" {
		attrs["custom:temporaryPassword"] = tempPassword
	}

	user := &User{
		Sub:          uuid.New().String(),
		Username:     username,
		UserPoolID:   userPoolID,
		PasswordHash: string(hash),
		Status:       UserStatusForceChangePassword,
		Attributes:   attrs,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Enabled:      true,
	}

	poolUsers[username] = user
	b.usersBySub[userPoolID+":"+user.Sub] = username

	cp := *user

	return &cp, nil
}

// ValidateSecretHash validates the SECRET_HASH field for a client that has a secret.
// AWS computes SecretHash = BASE64(HMAC-SHA256(username + clientId, clientSecret)).
// When a client has no secret, an empty hash is accepted (and a non-empty hash is rejected).
// When a client has a secret, the hash must be present and match.
func (b *InMemoryBackend) ValidateSecretHash(clientID, username, providedHash string) error {
	b.mu.RLock("ValidateSecretHash")
	defer b.mu.RUnlock()

	client, ok := b.clients[clientID]
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.ClientSecret == "" {
		if providedHash != "" {
			return fmt.Errorf(
				"%w: SecretHash provided but client %q has no secret",
				ErrInvalidParameter,
				clientID,
			)
		}

		return nil
	}

	if providedHash == "" {
		return fmt.Errorf(
			"%w: SecretHash required for client %q which has a secret",
			ErrInvalidParameter,
			clientID,
		)
	}

	mac := hmac.New(sha256.New, []byte(client.ClientSecret))
	mac.Write([]byte(username + clientID))
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(providedHash), []byte(expected)) {
		return fmt.Errorf("%w: SecretHash validation failed for client %q", ErrNotAuthorized, clientID)
	}

	return nil
}

// AdminSetUserMFAPreference sets the MFA preferences for a specific user (admin operation).
// This is the modern preferred API (vs AdminSetUserSettings). It delegates to AdminSetUserMFASetting.
func (b *InMemoryBackend) AdminSetUserMFAPreference(
	userPoolID, username string,
	smsMFAEnabled, softwareTokenEnabled bool,
	preferredMFA string,
) error {
	return b.AdminSetUserMFASetting(userPoolID, username, smsMFAEnabled, softwareTokenEnabled, preferredMFA)
}
