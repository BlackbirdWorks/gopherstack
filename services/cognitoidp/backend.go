package cognitoidp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	attrEmail = "email"
)

const (
	// bcryptCost is the bcrypt cost used for password hashing.
	bcryptCost = 10

	// poolIDSuffixLen is the length of the random suffix in pool IDs.
	poolIDSuffixLen = 8

	// clientIDLen is the length of randomly generated client IDs.
	clientIDLen = 26

	// clientSecretLen is the length of randomly generated client secrets.
	clientSecretLen = 51

	// alphanumChars contains characters used for random ID generation.
	alphanumChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// UserStatusUnconfirmed indicates the user has signed up but not confirmed their account.
	UserStatusUnconfirmed = "UNCONFIRMED"

	// UserStatusConfirmed indicates the user has confirmed their account.
	UserStatusConfirmed = "CONFIRMED"

	// UserStatusForceChangePassword indicates the user must change their password on next login.
	UserStatusForceChangePassword = "FORCE_CHANGE_PASSWORD"

	// defaultRefreshTokenTTL is the lifetime for refresh tokens.
	defaultRefreshTokenTTL = 30 * 24 * time.Hour
)

// SchemaAttribute represents a custom attribute definition for a user pool.
type SchemaAttribute struct {
	Name                     string  `json:"Name,omitempty"`
	AttributeDataType        string  `json:"AttributeDataType,omitempty"`
	StringAttributeMinLength int64   `json:"StringAttributeMinLength,omitempty"`
	StringAttributeMaxLength int64   `json:"StringAttributeMaxLength,omitempty"`
	NumberAttributeMinValue  float64 `json:"NumberAttributeMinValue,omitempty"`
	NumberAttributeMaxValue  float64 `json:"NumberAttributeMaxValue,omitempty"`
	Mutable                  bool    `json:"Mutable,omitempty"`
	Required                 bool    `json:"Required,omitempty"`
	DeveloperOnlyAttribute   bool    `json:"DeveloperOnlyAttribute,omitempty"`
}

// PasswordPolicy holds the password-complexity requirements for a user pool.
type PasswordPolicy struct {
	MinimumLength                 int  `json:"MinimumLength,omitempty"`
	RequireUppercase              bool `json:"RequireUppercase,omitempty"`
	RequireLowercase              bool `json:"RequireLowercase,omitempty"`
	RequireNumbers                bool `json:"RequireNumbers,omitempty"`
	RequireSymbols                bool `json:"RequireSymbols,omitempty"`
	TemporaryPasswordValidityDays int  `json:"TemporaryPasswordValidityDays,omitempty"`
}

// deletionProtectionActive is the UserPool.DeletionProtection value that makes
// DeleteUserPool refuse to delete the pool.
const deletionProtectionActive = "ACTIVE"

// UserPool represents a Cognito User Pool.
type UserPool struct {
	CreatedAt              time.Time `json:"createdAt"`
	UpdatedAt              time.Time `json:"updatedAt"`
	issuer                 *tokenIssuer
	LambdaConfig           map[string]any    `json:"lambdaConfig,omitempty"`
	EmailConfiguration     map[string]any    `json:"emailConfiguration,omitempty"`
	AccountRecoverySetting map[string]any    `json:"accountRecoverySetting,omitempty"`
	PasswordPolicy         *PasswordPolicy   `json:"passwordPolicy,omitempty"`
	ID                     string            `json:"id,omitempty"`
	Name                   string            `json:"name,omitempty"`
	ARN                    string            `json:"arn,omitempty"`
	MfaConfiguration       string            `json:"mfaConfiguration,omitempty"`
	DeletionProtection     string            `json:"deletionProtection,omitempty"`
	CustomAttributes       []SchemaAttribute `json:"customAttributes,omitempty"`
	AutoVerifiedAttributes []string          `json:"autoVerifiedAttributes,omitempty"`
}

// UserPoolClient represents an app client registered to a user pool.
type UserPoolClient struct {
	CreatedAt                       time.Time         `json:"createdAt"`
	UpdatedAt                       time.Time         `json:"updatedAt"`
	TokenValidityUnits              map[string]string `json:"tokenValidityUnits,omitempty"`
	ClientID                        string            `json:"clientId,omitempty"`
	ClientName                      string            `json:"clientName,omitempty"`
	UserPoolID                      string            `json:"userPoolId,omitempty"`
	ClientSecret                    string            `json:"clientSecret,omitempty"`
	PreventUserExistenceErrors      string            `json:"preventUserExistenceErrors,omitempty"`
	AllowedOAuthScopes              []string          `json:"allowedOAuthScopes,omitempty"`
	ExplicitAuthFlows               []string          `json:"explicitAuthFlows,omitempty"`
	CallbackURLs                    []string          `json:"callbackURLs,omitempty"`
	LogoutURLs                      []string          `json:"logoutURLs,omitempty"`
	SupportedIdentityProviders      []string          `json:"supportedIdentityProviders,omitempty"`
	AllowedOAuthFlows               []string          `json:"allowedOAuthFlows,omitempty"`
	AccessTokenValidity             int32             `json:"accessTokenValidity,omitempty"`
	IDTokenValidity                 int32             `json:"idTokenValidity,omitempty"`
	RefreshTokenValidity            int32             `json:"refreshTokenValidity,omitempty"`
	EnableTokenRevocation           bool              `json:"enableTokenRevocation,omitempty"`
	AllowedOAuthFlowsUserPoolClient bool              `json:"allowedOAuthFlowsUserPoolClient,omitempty"`
}

// User represents a Cognito user within a pool.
type User struct {
	CreatedAt            time.Time         `json:"createdAt"`
	UpdatedAt            time.Time         `json:"updatedAt"`
	ConfirmCodeExpiresAt time.Time         `json:"confirmCodeExpiresAt"`
	LastAuthTime         time.Time         `json:"lastAuthTime"`
	Attributes           map[string]string `json:"attributes,omitempty"`
	UserPoolID           string            `json:"userPoolID,omitempty"`
	Sub                  string            `json:"sub,omitempty"`
	Username             string            `json:"username,omitempty"`
	PasswordHash         string            `json:"passwordHash,omitempty"`
	Status               string            `json:"status,omitempty"`
	ConfirmCode          string            `json:"confirmCode,omitempty"`
	PreferredMfaSetting  string            `json:"preferredMfaSetting,omitempty"`
	TOTPSecret           string            `json:"totpSecret,omitempty"`
	UserMFASettingList   []string          `json:"userMFASettingList,omitempty"`
	MFAOptions           []MFAOptionType   `json:"mfaOptions,omitempty"`
	LinkedProviders      []ProviderLink    `json:"linkedProviders,omitempty"`
	Enabled              bool              `json:"enabled,omitempty"`
	TOTPVerified         bool              `json:"totpVerified,omitempty"`
}

// Group represents a Cognito User Pool group.
type Group struct {
	CreatedAt      time.Time `json:"createdAt"`
	LastModifiedAt time.Time `json:"lastModifiedAt"`
	GroupName      string    `json:"groupName,omitempty"`
	UserPoolID     string    `json:"userPoolId,omitempty"`
	Description    string    `json:"description,omitempty"`
	RoleArn        string    `json:"roleArn,omitempty"`
	Precedence     int32     `json:"precedence,omitempty"`
}

// InMemoryBackend is the in-memory store for Cognito IDP resources.
//
// Most resource collections are *store.Table[T] registered on b.registry (see
// store_setup.go for keyFns/composite keys and the full per-field rationale).
// A handful of fields remain plain maps because their value carries no pure
// identity for a store.Table key; store_setup.go's registerAllTables doc
// comment lists each one and why.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry
	pools    *store.Table[UserPool]
	// poolsByName is a secondary index on pools keyed by Name.
	poolsByName *store.Index[UserPool]
	clients     *store.Table[UserPoolClient]
	// clientsByPool is a secondary index on clients keyed by UserPoolID, for
	// efficient per-pool listing and deletes.
	clientsByPool *store.Index[UserPoolClient]
	// users is keyed by the composite userKey(poolID, username).
	users *store.Table[User]
	// usersByPool is a secondary index on users keyed by UserPoolID.
	usersByPool *store.Index[User]
	// usersBySub is a secondary index on users keyed by userSubKey(poolID,
	// sub), for O(1) access token resolution.
	usersBySub *store.Index[User]
	// refreshTokens maps refresh token → poolID/username for REFRESH_TOKEN_AUTH flow.
	refreshTokens map[string]*refreshTokenEntry
	// refreshTokensByClient maps clientID -> refreshToken set for efficient client cleanup.
	refreshTokensByClient map[string]map[string]struct{}
	// refreshTokensByUser maps poolID+":"+username → refreshToken set for efficient per-user token cleanup.
	refreshTokensByUser map[string]map[string]struct{}
	// mfaSessions maps session token → pending challenge context (MFA or NEW_PASSWORD_REQUIRED).
	mfaSessions map[string]*mfaSessionEntry
	// groups is keyed by the composite groupKey(poolID, groupName).
	groups *store.Table[Group]
	// groupsByPool is a secondary index on groups keyed by UserPoolID.
	groupsByPool *store.Index[Group]
	// groupMembers maps poolID → groupName → set of usernames
	groupMembers map[string]map[string]map[string]struct{}
	// resourceServers is keyed by the composite resourceServerKey(poolID, identifier).
	resourceServers *store.Table[ResourceServer]
	// resourceServersByPool is a secondary index on resourceServers keyed by UserPoolID.
	resourceServersByPool *store.Index[ResourceServer]
	// tokenRevokedBefore maps poolID+":"+username → revocation time for GlobalSignOut.
	// Access tokens with auth_time before this timestamp are rejected.
	tokenRevokedBefore map[string]time.Time
	// identityProviders is keyed by the composite identityProviderKey(poolID, providerName).
	identityProviders *store.Table[IdentityProvider]
	// identityProvidersByPool is a secondary index on identityProviders keyed by UserPoolID.
	identityProvidersByPool *store.Index[IdentityProvider]
	// domains maps domain → UserPoolDomain (domain names are globally unique in Cognito)
	domains *store.Table[UserPoolDomain]
	// resourceTags maps ARN → tag key → tag value
	resourceTags map[string]map[string]string
	// riskConfigurations maps poolID+":"+clientID → RiskConfiguration (clientID="" for pool-level)
	riskConfigurations map[string]*RiskConfiguration
	// logDeliveryConfigs maps poolID → LogDeliveryConfig
	logDeliveryConfigs map[string]*LogDeliveryConfig
	// uiCustomizations is keyed by the composite uiKey(poolID, clientID).
	uiCustomizations *store.Table[UICustomization]
	// managedLoginBrandings is keyed by the composite managedLoginBrandingKey(poolID, brandingID).
	managedLoginBrandings *store.Table[ManagedLoginBranding]
	// managedLoginBrandingsByPool is a secondary index on managedLoginBrandings keyed by UserPoolID.
	managedLoginBrandingsByPool *store.Index[ManagedLoginBranding]
	// terms maps poolID → Terms
	terms *store.Table[Terms]
	// userImportJobs is keyed by the composite userImportJobKey(poolID, jobID).
	userImportJobs *store.Table[UserImportJob]
	// userImportJobsByPool is a secondary index on userImportJobs keyed by UserPoolID.
	userImportJobsByPool *store.Index[UserImportJob]
	// poolMfaConfigs maps poolID → full MFA config (SMS/TOTP/Email sub-configs)
	poolMfaConfigs map[string]*UserPoolMfaFullConfig
	// attrVerificationCodes maps poolID+":"+username+":"+attrName → pending verification entry
	attrVerificationCodes map[string]*attrVerificationEntry
	// typedRiskConfigurations maps poolID+":"+clientID → typed risk configuration
	typedRiskConfigurations *store.Table[TypedRiskConfiguration]
	// devices maps poolID+":"+username → deviceKey → *Device (device tracking / "remember this device").
	devices map[string]map[string]*Device
	// webauthnCredentials maps poolID+":"+username → credentialID → *WebAuthnCredential.
	webauthnCredentials map[string]map[string]*WebAuthnCredential
	// authEvents maps poolID+":"+username → eventID → *AuthEvent (adaptive-auth event feedback tracking).
	authEvents map[string]map[string]*AuthEvent
	accountID  string
	region     string
	endpoint   string
}

// refreshTokenEntry holds the pool/user context for a refresh token.
type refreshTokenEntry struct {
	ExpiresAt time.Time `json:"expiresAt"`
	PoolID    string    `json:"poolId,omitempty"`
	ClientID  string    `json:"clientId,omitempty"`
	Username  string    `json:"username,omitempty"`
	// AuthTime is the original authentication time (Unix seconds) of the
	// session that minted this refresh-token chain. AWS Cognito preserves
	// auth_time across REFRESH_TOKEN_AUTH; it is not reset on each refresh.
	AuthTime int64 `json:"authTime,omitempty"`
}

// mfaSessionTTL is the lifetime of an MFA or challenge session token.
const mfaSessionTTL = 3 * time.Minute

// mfaSessionEntry holds the pending challenge context (MFA or NEW_PASSWORD_REQUIRED).
type mfaSessionEntry struct {
	ExpiresAt     time.Time `json:"expiresAt"`
	PoolID        string    `json:"poolID,omitempty"`
	ClientID      string    `json:"clientID,omitempty"`
	Username      string    `json:"username,omitempty"`
	ChallengeType string    `json:"challengeType,omitempty"` // "SOFTWARE_TOKEN_MFA", "NEW_PASSWORD_REQUIRED" ...
	// SRPPassword holds the user's password for USER_SRP_AUTH second-step validation.
	SRPPassword string `json:"srpPassword,omitempty"`
	// Code holds the one-time code generated for SMS_MFA/EMAIL_OTP challenges. Unlike
	// SOFTWARE_TOKEN_MFA (verified cryptographically against the user's TOTP secret), SMS
	// and email codes have no client-held secret to re-derive from, so — exactly like
	// ForgotPassword/ConfirmSignUp confirmation codes elsewhere in this backend — the code
	// is generated once here and the challenge response must match it exactly.
	Code string `json:"code,omitempty"`
}

// AuthResult is the result of a successful authentication or a pending challenge.
type AuthResult struct {
	// Tokens is set when authentication is complete.
	Tokens *TokenResult `json:"tokens,omitempty"`
	// MFASession is set when a challenge is required; the caller must respond to it.
	MFASession string `json:"mfaSession,omitempty"`
	// ChallengeName identifies the type of challenge (SOFTWARE_TOKEN_MFA, NEW_PASSWORD_REQUIRED, etc.).
	ChallengeName string `json:"challengeName,omitempty"`
}

// mfaSessionLen is the character length of randomly generated MFA session tokens.
const mfaSessionLen = 64

// totpCodeLen is the expected length of a TOTP code.
const totpCodeLen = 6

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                    lockmetrics.New("cognitoidp"),
		registry:              store.NewRegistry(),
		refreshTokens:         make(map[string]*refreshTokenEntry),
		refreshTokensByClient: make(map[string]map[string]struct{}),
		refreshTokensByUser:   make(map[string]map[string]struct{}),
		mfaSessions:           make(map[string]*mfaSessionEntry),
		groupMembers:          make(map[string]map[string]map[string]struct{}),
		tokenRevokedBefore:    make(map[string]time.Time),
		resourceTags:          make(map[string]map[string]string),
		riskConfigurations:    make(map[string]*RiskConfiguration),
		logDeliveryConfigs:    make(map[string]*LogDeliveryConfig),
		poolMfaConfigs:        make(map[string]*UserPoolMfaFullConfig),
		attrVerificationCodes: make(map[string]*attrVerificationEntry),
		devices:               make(map[string]map[string]*Device),
		webauthnCredentials:   make(map[string]map[string]*WebAuthnCredential),
		authEvents:            make(map[string]map[string]*AuthEvent),
		accountID:             accountID,
		region:                region,
		endpoint:              endpoint,
	}

	registerAllTables(b)

	return b
}

// CreateUserPool creates a new user pool with the given name.
func (b *InMemoryBackend) CreateUserPool(name string) (*UserPool, error) {
	b.mu.Lock("CreateUserPool")
	defer b.mu.Unlock()

	if b.poolNameExists(name) {
		return nil, fmt.Errorf("%w: pool %q already exists", ErrUserPoolAlreadyExists, name)
	}

	poolID := b.region + "_" + randomAlphanumeric(poolIDSuffixLen)
	issuerURL := fmt.Sprintf("%s/%s", b.endpoint, poolID)

	issuer, err := newTokenIssuer(issuerURL)
	if err != nil {
		return nil, fmt.Errorf("creating token issuer: %w", err)
	}

	pool := &UserPool{
		ID:        poolID,
		Name:      name,
		ARN:       arn.Build("cognito-idp", b.region, b.accountID, fmt.Sprintf("userpool/%s", poolID)),
		CreatedAt: time.Now(),
		issuer:    issuer,
	}

	b.pools.Put(pool)

	cp := *pool

	return &cp, nil
}

// DescribeUserPool returns the user pool with the given ID.
func (b *InMemoryBackend) DescribeUserPool(userPoolID string) (*UserPool, error) {
	b.mu.RLock("DescribeUserPool")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	cp := *pool

	return &cp, nil
}

// DeleteUserPool removes the user pool with the given ID and all of its associated clients.
func (b *InMemoryBackend) DeleteUserPool(userPoolID string) error {
	b.mu.Lock("DeleteUserPool")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	// AWS refuses to delete a user pool with deletion protection ACTIVE; the caller must
	// first UpdateUserPool to set DeletionProtection back to INACTIVE.
	if pool.DeletionProtection == deletionProtectionActive {
		return fmt.Errorf(
			"%w: User pool cannot be deleted because deletion protection is activated, "+
				"set deletion protection to INACTIVE and retry to delete the user pool",
			ErrInvalidParameter,
		)
	}

	b.pools.Delete(userPoolID)

	// Copy the index result before deleting: Delete mutates the byPool index's
	// backing slice in place, so ranging directly over it while deleting would
	// skip entries.
	for _, u := range slices.Clone(b.usersByPool.Get(userPoolID)) {
		b.users.Delete(userKey(userPoolID, u.Username))
	}

	for _, client := range slices.Clone(b.clientsByPool.Get(userPoolID)) {
		b.clients.Delete(client.ClientID)
		b.deleteRefreshTokensForClientAndUserIndexLocked(client.ClientID)
	}

	// Clean up groups and group memberships for this pool.
	for _, g := range slices.Clone(b.groupsByPool.Get(userPoolID)) {
		b.groups.Delete(groupKey(userPoolID, g.GroupName))
	}

	delete(b.groupMembers, userPoolID)

	return nil
}

// DeleteUserPoolClient removes the app client with the given client ID from the given pool.
// If userPoolID is empty the pool ownership check is skipped.
func (b *InMemoryBackend) DeleteUserPoolClient(userPoolID, clientID string) error {
	b.mu.Lock("DeleteUserPoolClient")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if userPoolID != "" && client.UserPoolID != userPoolID {
		return fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	b.clients.Delete(clientID)

	// Clean up any refresh tokens issued by this client to prevent leaks.
	b.deleteRefreshTokensForClientAndUserIndexLocked(clientID)

	return nil
}

// ListUserPools returns all user pools sorted by name.
func (b *InMemoryBackend) ListUserPools() []*UserPool {
	b.mu.RLock("ListUserPools")
	defer b.mu.RUnlock()

	pools := b.pools.All()
	out := make([]*UserPool, 0, len(pools))

	for _, p := range pools {
		cp := *p
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// ListUserPoolClients returns all app clients for the given user pool sorted by client name.
func (b *InMemoryBackend) ListUserPoolClients(userPoolID string) ([]*UserPoolClient, error) {
	b.mu.RLock("ListUserPoolClients")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolClients := b.clientsByPool.Get(userPoolID)
	out := make([]*UserPoolClient, 0, len(poolClients))

	for _, c := range poolClients {
		cp := *c
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ClientName < out[j].ClientName })

	return out, nil
}

// CreateUserPoolClient creates a new app client for the given user pool.
func (b *InMemoryBackend) CreateUserPoolClient(userPoolID, clientName string) (*UserPoolClient, error) {
	b.mu.Lock("CreateUserPoolClient")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client := &UserPoolClient{
		ClientID:   randomAlphanumeric(clientIDLen),
		ClientName: clientName,
		UserPoolID: userPoolID,
		CreatedAt:  time.Now(),
	}

	b.clients.Put(client)

	cp := *client

	return &cp, nil
}

// DescribeUserPoolClient returns the app client with the given client ID.
func (b *InMemoryBackend) DescribeUserPoolClient(userPoolID, clientID string) (*UserPoolClient, error) {
	b.mu.RLock("DescribeUserPoolClient")
	defer b.mu.RUnlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	cp := *client

	return &cp, nil
}

// SignUp registers a new user with UNCONFIRMED status.
func (b *InMemoryBackend) SignUp(clientID, username, password string, userAttributes map[string]string) (*User, error) {
	b.mu.Lock("SignUp")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if _, poolOK := b.pools.Get(client.UserPoolID); !poolOK {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	if _, exists := b.users.Get(userKey(client.UserPoolID, username)); exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUsernameExists, username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hashing password: %w", err)
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	user := &User{
		Sub:          uuid.New().String(),
		Username:     username,
		UserPoolID:   client.UserPoolID,
		PasswordHash: string(hash),
		Status:       UserStatusUnconfirmed,
		Attributes:   attrs,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Enabled:      true,
		// Generate a confirmation code (simulates the code sent via email/SMS).
		ConfirmCode:          randomAlphanumeric(confirmCodeLen),
		ConfirmCodeExpiresAt: time.Now().Add(confirmCodeTTL),
	}

	b.users.Put(user)

	cp := *user

	return &cp, nil
}

// ConfirmSignUp confirms a user's registration by validating the confirmation code.
func (b *InMemoryBackend) ConfirmSignUp(clientID, username, confirmationCode string) error {
	b.mu.Lock("ConfirmSignUp")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if _, poolOK := b.pools.Get(client.UserPoolID); !poolOK {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if confirmationCode == "" {
		return fmt.Errorf("%w: confirmation code is required", ErrCodeMismatch)
	}

	// Re-confirming an already-confirmed user is idempotent (the stored code is
	// cleared on first confirmation). Short-circuit before code matching so a
	// cleared code does not look like an empty-code bypass.
	if user.Status == UserStatusConfirmed {
		return nil
	}

	// Check expiry before a code mismatch so an expired code surfaces
	// ExpiredCodeException rather than CodeMismatchException (AWS ordering).
	if !user.ConfirmCodeExpiresAt.IsZero() && time.Now().After(user.ConfirmCodeExpiresAt) {
		return fmt.Errorf("%w: confirmation code has expired", ErrExpiredCode)
	}

	// If no code was ever stored for an unconfirmed user, there is nothing to
	// match against — any supplied code is a mismatch. Without this guard an
	// empty stored code would let an arbitrary code confirm the user.
	if user.ConfirmCode == "" || confirmationCode != user.ConfirmCode {
		return fmt.Errorf("%w: invalid confirmation code", ErrCodeMismatch)
	}

	user.Status = UserStatusConfirmed
	user.ConfirmCode = ""
	user.ConfirmCodeExpiresAt = time.Time{}

	return nil
}

// InitiateAuth authenticates a user using the specified auth flow.
func (b *InMemoryBackend) InitiateAuth(clientID, authFlow, username, password string) (*AuthResult, error) {
	b.mu.Lock("InitiateAuth")
	defer b.mu.Unlock()

	user, pool, err := b.findUserByClientID(clientID, username)
	if err != nil {
		return nil, err
	}

	return b.authenticate(pool, clientID, authFlow, user, password)
}

// AdminInitiateAuth authenticates a user as an admin using the specified auth flow.
func (b *InMemoryBackend) AdminInitiateAuth(
	userPoolID, clientID, authFlow, username, password string,
) (*AuthResult, error) {
	b.mu.Lock("AdminInitiateAuth")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok || client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	user, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	return b.authenticate(pool, clientID, authFlow, user, password)
}

// AdminCreateUser creates a new user in the pool with FORCE_CHANGE_PASSWORD status.
func (b *InMemoryBackend) AdminCreateUser(
	userPoolID, username, tempPassword string,
	userAttributes map[string]string,
) (*User, error) {
	b.mu.Lock("AdminCreateUser")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.users.Get(userKey(userPoolID, username)); exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(tempPassword), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hashing password: %w", err)
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	// Simulate email-based temporary password delivery: store the temp password
	// in a synthetic attribute so integrations can retrieve it without SMTP.
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

	b.users.Put(user)

	cp := *user

	return &cp, nil
}

// AdminSetUserPassword sets the password for a user in a pool.
func (b *InMemoryBackend) AdminSetUserPassword(userPoolID, username, password string, permanent bool) error {
	b.mu.Lock("AdminSetUserPassword")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	// AWS enforces the pool's password policy on AdminSetUserPassword, just as
	// it does on ConfirmForgotPassword. An invalid password is rejected with
	// InvalidPasswordException.
	if err := validatePassword(pool.PasswordPolicy, password); err != nil {
		return err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return fmt.Errorf("hashing password: %w", err)
	}

	user.PasswordHash = string(hash)
	user.UpdatedAt = time.Now()

	if permanent {
		user.Status = UserStatusConfirmed
	}

	return nil
}

// AdminConfirmSignUp confirms a user's registration without requiring a confirmation code.
// This is an admin operation that bypasses the normal confirmation flow.
func (b *InMemoryBackend) AdminConfirmSignUp(userPoolID, username string) error {
	b.mu.Lock("AdminConfirmSignUp")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	user.Status = UserStatusConfirmed
	user.ConfirmCode = ""

	return nil
}

// AdminGetUser returns a user from a pool by username.
func (b *InMemoryBackend) AdminGetUser(userPoolID, username string) (*User, error) {
	b.mu.RLock("AdminGetUser")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	cp := *user

	return &cp, nil
}

// AdminDeleteUser deletes a user from a pool by username.
func (b *InMemoryBackend) AdminDeleteUser(userPoolID, username string) error {
	b.mu.Lock("AdminDeleteUser")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	b.users.Delete(userKey(userPoolID, username))

	b.deleteRefreshTokensForUserLocked(userPoolID, username)

	return nil
}

// ListUsers returns all users in a pool sorted by username.
func (b *InMemoryBackend) ListUsers(userPoolID string) ([]*User, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolUsers := b.usersByPool.Get(userPoolID)
	out := make([]*User, 0, len(poolUsers))

	for _, u := range poolUsers {
		cp := *u
		cp.Attributes = maps.Clone(u.Attributes)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })

	return out, nil
}

// ForgotPassword initiates a password reset for a user.
// In this mock the reset code is generated and stored on the user.
func (b *InMemoryBackend) ForgotPassword(clientID, username string) (string, error) {
	b.mu.Lock("ForgotPassword")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return "", fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if _, poolOK := b.pools.Get(client.UserPoolID); !poolOK {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		if client.PreventUserExistenceErrors == preventUserExistenceEnabled {
			// AWS never reveals UserNotFoundException here when masking is enabled: the
			// caller gets the same success response (fabricated CodeDeliveryDetails) it
			// would get for a real account. The code is not stored anywhere, so
			// ConfirmForgotPassword will still fail for this username -- matching AWS,
			// which never actually delivers anything for a nonexistent account either.
			return randomAlphanumeric(confirmCodeLen), nil
		}

		return "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if !user.Enabled {
		return "", fmt.Errorf("%w: User is disabled", ErrNotAuthorized)
	}

	if user.Status == UserStatusUnconfirmed || user.Status == UserStatusForceChangePassword {
		return "", fmt.Errorf(
			"%w: Cannot reset password for the user as there is no registered/verified"+
				" email or phone_number",
			ErrInvalidParameter,
		)
	}

	code := randomAlphanumeric(confirmCodeLen)
	user.ConfirmCode = code
	user.ConfirmCodeExpiresAt = time.Now().Add(confirmCodeTTL)

	return code, nil
}

// ConfirmForgotPassword resets a user's password using the code generated by ForgotPassword.
func (b *InMemoryBackend) ConfirmForgotPassword(clientID, username, code, newPassword string) error {
	b.mu.Lock("ConfirmForgotPassword")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if _, poolOK := b.pools.Get(client.UserPoolID); !poolOK {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if !user.ConfirmCodeExpiresAt.IsZero() && time.Now().After(user.ConfirmCodeExpiresAt) {
		return fmt.Errorf("%w: password reset code has expired", ErrExpiredCode)
	}

	if user.ConfirmCode == "" || user.ConfirmCode != code {
		return fmt.Errorf("%w: invalid reset code", ErrCodeMismatch)
	}

	pool, ok2 := b.pools.Get(client.UserPoolID)
	if ok2 {
		if err2 := validatePassword(pool.PasswordPolicy, newPassword); err2 != nil {
			return err2
		}
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcryptCost)
	if err != nil {
		return fmt.Errorf("hashing password: %w", err)
	}

	user.PasswordHash = string(hash)
	user.ConfirmCode = ""
	user.ConfirmCodeExpiresAt = time.Time{}
	user.Status = UserStatusConfirmed

	return nil
}

// GetUser returns user attributes for an authenticated user (via access token).
func (b *InMemoryBackend) GetUser(accessToken string) (*User, error) {
	b.mu.RLock("GetUser")
	defer b.mu.RUnlock()

	u, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	cp := *u
	cp.Attributes = maps.Clone(u.Attributes)

	return &cp, nil
}

// ChangePassword changes the password for an authenticated user (via access token).
// The pool's PasswordPolicy is enforced on the proposed password.
func (b *InMemoryBackend) ChangePassword(accessToken, previousPassword, proposedPassword string) error {
	b.mu.Lock("ChangePassword")
	defer b.mu.Unlock()

	u, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	if err2 := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(previousPassword)); err2 != nil {
		return fmt.Errorf("%w: previous password is incorrect", ErrNotAuthorized)
	}

	if pool, ok := b.pools.Get(u.UserPoolID); ok {
		if err3 := validatePassword(pool.PasswordPolicy, proposedPassword); err3 != nil {
			return err3
		}
	}

	hash, err4 := bcrypt.GenerateFromPassword([]byte(proposedPassword), bcryptCost)
	if err4 != nil {
		return fmt.Errorf("hashing password: %w", err4)
	}

	u.PasswordHash = string(hash)

	return nil
}

// findUserByAccessTokenLocked finds the live *User for a given access token.
// It uses the usersBySub secondary index for O(1) lookup after JWT parsing.
// The caller must hold b.mu (either read or write lock).
func (b *InMemoryBackend) findUserByAccessTokenLocked(accessToken string) (*User, error) {
	for _, pool := range b.pools.All() {
		claims, err := pool.issuer.ParseAccessToken(accessToken)
		if err != nil {
			continue
		}

		sub, _ := claims["sub"].(string)
		if sub == "" {
			continue
		}

		// O(1) lookup via secondary index.
		u, found := b.userBySub(pool.ID, sub)
		if !found {
			continue
		}

		// Check per-user token revocation: reject tokens issued before GlobalSignOut.
		if revokedBefore, ok2 := b.tokenRevokedBefore[pool.ID+":"+u.Username]; ok2 {
			authTime, _ := claims["auth_time"].(float64)
			if time.Unix(int64(authTime), 0).Before(revokedBefore) {
				continue
			}
		}

		return u, nil
	}

	return nil, fmt.Errorf("%w: access token is invalid or expired", ErrNotAuthorized)
}

// GetSigningCertificate returns a deterministic, PEM-encoded self-signed X.509
// certificate for the user pool's JWT signing key. The certificate is cached on the
// pool's token issuer, so repeated calls for the same pool return a stable PEM.
func (b *InMemoryBackend) GetSigningCertificate(userPoolID string) (string, error) {
	b.mu.RLock("GetSigningCertificate")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	cert, err := pool.issuer.SigningCertificatePEM()
	if err != nil {
		return "", fmt.Errorf("signing certificate for pool %q: %w", userPoolID, err)
	}

	return cert, nil
}

// GetUserPoolJWKS returns the JSON Web Key Set for the given user pool.
func (b *InMemoryBackend) GetUserPoolJWKS(userPoolID string) (*JWKSResponse, error) {
	b.mu.RLock("GetUserPoolJWKS")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	jwks := pool.issuer.JWKS()

	return &jwks, nil
}

// ErrJWTKeyNotFound is returned when a JWT key ID is not found for a known issuer.
var ErrJWTKeyNotFound = errors.New("JWT key ID not found for issuer")

// ErrJWTIssuerUnknown is returned when no pool matches the given issuer URL.
var ErrJWTIssuerUnknown = errors.New("JWT issuer not managed by this emulator")

// GetJWTPublicKey returns the RSA public key for the user pool whose issuerURL
// matches and whose key ID equals kid. Returns nil, nil when no pool matches
// (caller should reject the token as unauthorized).
func (b *InMemoryBackend) GetJWTPublicKey(issuerURL, kid string) (*rsa.PublicKey, error) {
	b.mu.RLock("GetJWTPublicKey")
	defer b.mu.RUnlock()

	for _, pool := range b.pools.All() {
		if pool.issuer == nil || pool.issuer.issuerURL != issuerURL {
			continue
		}

		key, ok := pool.issuer.PublicKeyForKID(kid)
		if !ok {
			return nil, fmt.Errorf("%w: %q for issuer %q", ErrJWTKeyNotFound, kid, issuerURL)
		}

		return key, nil
	}

	return nil, ErrJWTIssuerUnknown
}

// findUserByClientID finds a user and their pool using the clientID. This backs the
// non-admin InitiateAuth path, so an unknown username is reported via
// unknownUserAuthError, which honors the client's PreventUserExistenceErrors setting.
// AdminInitiateAuth looks users up separately and always reveals UserNotFoundException,
// matching AWS (existence-error masking only applies to the non-admin, unauthenticated API).
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findUserByClientID(clientID, username string) (*User, *UserPool, error) {
	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		return nil, nil, unknownUserAuthError(client, username)
	}

	return user, pool, nil
}

// unknownUserAuthError returns the error InitiateAuth surfaces when the username does not
// exist. When the app client's PreventUserExistenceErrors is "ENABLED" (the AWS-recommended
// setting), Cognito masks the distinction behind the same NotAuthorizedException a wrong
// password produces, using the identical message, so a caller cannot enumerate valid
// usernames by comparing error types/text. "LEGACY" (the default when unset) reveals
// UserNotFoundException, matching Cognito's pre-2019 behavior kept for backward
// compatibility. Only the non-admin InitiateAuth API applies this masking; AdminInitiateAuth
// always reveals the real error since the caller already has admin-level AWS credentials.
func unknownUserAuthError(client *UserPoolClient, username string) error {
	if client.PreventUserExistenceErrors == preventUserExistenceEnabled {
		return fmt.Errorf("%w: incorrect username or password", ErrNotAuthorized)
	}

	return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
}

// challengePasswordVerifier is returned for USER_SRP_AUTH after credentials are validated.
const challengePasswordVerifier = "PASSWORD_VERIFIER"

// isAuthFlowAllowed checks whether the given flow is permitted by the client's ExplicitAuthFlows list.
// Returns true when no restriction is configured.
func (b *InMemoryBackend) isAuthFlowAllowed(clientID, authFlow string) bool {
	client, ok := b.clients.Get(clientID)
	if !ok || len(client.ExplicitAuthFlows) == 0 {
		return true
	}

	for _, f := range client.ExplicitAuthFlows {
		if f == authFlow || f == "ALLOW_"+authFlow {
			return true
		}
	}

	return false
}

// newMFASession stores a new session entry and returns an AuthResult with the challenge.
// For SMS_MFA/EMAIL_OTP a one-time numeric code is generated and stored on the session so
// RespondToMFAChallenge can require an exact match, the same way ForgotPassword/SignUp
// confirmation codes are validated elsewhere in this backend (there is no real SMS/email
// gateway to deliver the code out of band, so simulation is the best available proxy — but
// unlike a bare format check, "any 6 digits" is no longer accepted).
func (b *InMemoryBackend) newMFASession(pool *UserPool, clientID, username, challengeType string) *AuthResult {
	sessionToken := randomAlphanumeric(mfaSessionLen)

	entry := &mfaSessionEntry{
		PoolID:        pool.ID,
		ClientID:      clientID,
		Username:      username,
		ChallengeType: challengeType,
		ExpiresAt:     time.Now().Add(mfaSessionTTL),
	}

	if challengeType == challengeSMSMFA || challengeType == challengeEmailOTP {
		entry.Code = randomNumeric(totpCodeLen)
	}

	b.mfaSessions[sessionToken] = entry

	return &AuthResult{
		MFASession:    sessionToken,
		ChallengeName: challengeType,
	}
}

// mfaChallengeType returns the challenge type to use given pool config and user preference.
func mfaChallengeType(_ *UserPool, user *User) string {
	if user.PreferredMfaSetting != "" {
		return user.PreferredMfaSetting
	}

	return challengeSoftwareTokenMFA
}

// authenticate validates a user's credentials and returns tokens or a challenge.
// Caller must hold the write lock.
func (b *InMemoryBackend) authenticate(
	pool *UserPool,
	clientID, authFlow string,
	user *User,
	password string,
) (*AuthResult, error) {
	switch authFlow {
	case "USER_PASSWORD_AUTH", "ADMIN_USER_PASSWORD_AUTH", "ADMIN_NO_SRP_AUTH", "USER_SRP_AUTH":
		// valid flows; ADMIN_NO_SRP_AUTH is a legacy alias for ADMIN_USER_PASSWORD_AUTH
	default:
		return nil, fmt.Errorf("%w: unsupported auth flow %q", ErrInvalidUserPoolConfig, authFlow)
	}

	if !b.isAuthFlowAllowed(clientID, authFlow) {
		return nil, fmt.Errorf(
			"%w: auth flow %q is not in client ExplicitAuthFlows",
			ErrInvalidUserPoolConfig,
			authFlow,
		)
	}

	if user.Status == UserStatusUnconfirmed {
		return nil, fmt.Errorf("%w: user %q is not confirmed", ErrUserNotConfirmed, user.Username)
	}

	if !user.Enabled {
		return nil, fmt.Errorf("%w: user %q account is disabled", ErrNotAuthorized, user.Username)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, fmt.Errorf("%w: incorrect username or password", ErrNotAuthorized)
	}

	// USER_SRP_AUTH: credentials verified; return PASSWORD_VERIFIER so client completes handshake.
	if authFlow == "USER_SRP_AUTH" {
		return b.newMFASession(pool, clientID, user.Username, challengePasswordVerifier), nil
	}

	// Issue NEW_PASSWORD_REQUIRED challenge when user must set a permanent password.
	if user.Status == UserStatusForceChangePassword {
		return b.newMFASession(pool, clientID, user.Username, challengeNewPasswordRequired), nil
	}

	// MFA enforcement: if the pool requires or offers MFA, issue an MFA challenge.
	mfaConfig := pool.MfaConfiguration
	if mfaConfig == "ON" || mfaConfig == "OPTIONAL" {
		return b.newMFASession(pool, clientID, user.Username, mfaChallengeType(pool, user)), nil
	}

	return b.issueTokensLocked(pool, clientID, user)
}

// issueTokensLocked issues tokens for a confirmed user. Caller must hold the write lock.
func (b *InMemoryBackend) issueTokensLocked(pool *UserPool, clientID string, user *User) (*AuthResult, error) {
	now := time.Now()
	user.LastAuthTime = now

	groups := b.userGroupsLocked(pool.ID, user.Username)

	var scopes []string
	refreshTTL := defaultRefreshTokenTTL
	var accessExpiry, idExpiry time.Duration
	if client, ok := b.clients.Get(clientID); ok {
		scopes = client.AllowedOAuthScopes
		if d := tokenExpiryFor(client, "AccessToken"); d > 0 {
			accessExpiry = d
		}
		if d := tokenExpiryFor(client, "IdToken"); d > 0 {
			idExpiry = d
		}
		if d := tokenExpiryFor(client, "RefreshToken"); d > 0 {
			refreshTTL = d
		}
	}

	tokens, err := pool.issuer.Issue(TokenParams{
		ClientID:          clientID,
		Username:          user.Username,
		UserSub:           user.Sub,
		Groups:            groups,
		AuthTime:          now.Unix(),
		Scopes:            scopes,
		Attributes:        user.Attributes,
		AccessTokenExpiry: accessExpiry,
		IDTokenExpiry:     idExpiry,
	})
	if err != nil {
		return nil, fmt.Errorf("issuing tokens: %w", err)
	}

	// Store the refresh token so REFRESH_TOKEN_AUTH can validate it.
	b.storeRefreshTokenLocked(tokens.RefreshToken, &refreshTokenEntry{
		PoolID:    pool.ID,
		ClientID:  clientID,
		Username:  user.Username,
		AuthTime:  now.Unix(),
		ExpiresAt: now.UTC().Add(refreshTTL),
	})

	return &AuthResult{Tokens: tokens}, nil
}

// InitiateAuthRefreshToken exchanges a valid refresh token for new ID/Access tokens.
func (b *InMemoryBackend) InitiateAuthRefreshToken(clientID, refreshToken string) (*TokenResult, error) {
	b.mu.Lock("InitiateAuthRefreshToken")
	defer b.mu.Unlock()

	if refreshToken == "" {
		return nil, fmt.Errorf("%w: Missing required parameter REFRESH_TOKEN", ErrInvalidParameter)
	}

	entry, ok := b.refreshTokens[refreshToken]
	if !ok {
		return nil, fmt.Errorf("%w: refresh token not found or expired", ErrNotAuthorized)
	}
	if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(time.Now().UTC()) {
		b.deleteRefreshTokenLocked(refreshToken)

		return nil, fmt.Errorf("%w: refresh token not found or expired", ErrNotAuthorized)
	}

	if entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: refresh token was issued for a different client", ErrNotAuthorized)
	}

	pool, ok := b.pools.Get(entry.PoolID)
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	user, ok := b.users.Get(userKey(entry.PoolID, entry.Username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	if !user.Enabled {
		return nil, fmt.Errorf("%w: user %q account is disabled", ErrNotAuthorized, entry.Username)
	}

	now := time.Now()
	groups := b.userGroupsLocked(entry.PoolID, user.Username)

	var scopes []string
	refreshTTL := defaultRefreshTokenTTL
	var accessExpiry, idExpiry time.Duration
	if c, cok := b.clients.Get(clientID); cok {
		scopes = c.AllowedOAuthScopes
		if d := tokenExpiryFor(c, "AccessToken"); d > 0 {
			accessExpiry = d
		}
		if d := tokenExpiryFor(c, "IdToken"); d > 0 {
			idExpiry = d
		}
		if d := tokenExpiryFor(c, "RefreshToken"); d > 0 {
			refreshTTL = d
		}
	}

	// Preserve the original authentication time across refresh; AWS Cognito
	// does not reset auth_time on REFRESH_TOKEN_AUTH. Legacy entries minted
	// before AuthTime was tracked fall back to the refresh moment.
	authTime := entry.AuthTime
	if authTime == 0 {
		authTime = now.Unix()
		entry.AuthTime = authTime
	}

	tokens, err := pool.issuer.Issue(TokenParams{
		ClientID:          clientID,
		Username:          user.Username,
		UserSub:           user.Sub,
		Groups:            groups,
		AuthTime:          authTime,
		Scopes:            scopes,
		AccessTokenExpiry: accessExpiry,
		IDTokenExpiry:     idExpiry,
	})
	if err != nil {
		return nil, fmt.Errorf("issuing tokens: %w", err)
	}

	// Rotate the refresh token: invalidate old, store new.
	b.deleteRefreshTokenLocked(refreshToken)
	entry.ExpiresAt = now.UTC().Add(refreshTTL)
	b.storeRefreshTokenLocked(tokens.RefreshToken, entry)

	return tokens, nil
}

// RevokeToken revokes a refresh token, preventing further use.
func (b *InMemoryBackend) RevokeToken(token, clientID string) error {
	b.mu.Lock("RevokeToken")
	defer b.mu.Unlock()

	entry, ok := b.refreshTokens[token]
	if !ok {
		// AWS Cognito silently succeeds when revoking an already-revoked/unknown token.
		return nil
	}

	if entry.ClientID != clientID {
		return fmt.Errorf("%w: token was issued for a different client", ErrNotAuthorized)
	}

	b.deleteRefreshTokenLocked(token)

	return nil
}

// RespondToMFAChallenge validates an MFA session and the user-supplied code, then issues
// tokens. SOFTWARE_TOKEN_MFA is verified as a real RFC 6238 TOTP code against the user's
// AssociateSoftwareToken secret; SMS_MFA/EMAIL_OTP are verified against the one-time code
// generated when the challenge session was created (see newMFASession). A wrong code
// returns CodeMismatchException without consuming the session, so the caller may retry
// until the session expires — matching real Cognito.
func (b *InMemoryBackend) RespondToMFAChallenge(clientID, session, code string) (*TokenResult, error) {
	b.mu.Lock("RespondToMFAChallenge")
	defer b.mu.Unlock()

	if len(code) != totpCodeLen {
		return nil, fmt.Errorf("%w: code must be %d digits", ErrCodeMismatch, totpCodeLen)
	}

	for _, ch := range code {
		if ch < '0' || ch > '9' {
			return nil, fmt.Errorf("%w: code must contain only digits", ErrCodeMismatch)
		}
	}

	entry, ok := b.mfaSessions[session]
	if !ok {
		return nil, fmt.Errorf("%w: MFA session not found or expired", ErrNotAuthorized)
	}

	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		delete(b.mfaSessions, session)

		return nil, fmt.Errorf("%w: MFA session not found or expired", ErrNotAuthorized)
	}

	if entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: MFA session was issued for a different client", ErrNotAuthorized)
	}

	pool, ok := b.pools.Get(entry.PoolID)
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	user, ok := b.users.Get(userKey(entry.PoolID, entry.Username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	if err := verifyMFAChallengeCode(entry, user, code); err != nil {
		// Do not consume the session on a wrong code: the caller may retry until it expires.
		return nil, err
	}

	// Consume the session (one-time use).
	delete(b.mfaSessions, session)

	result, err := b.issueTokensLocked(pool, clientID, user)
	if err != nil {
		return nil, err
	}

	return result.Tokens, nil
}

// verifyMFAChallengeCode validates code against the challenge type recorded on the MFA
// session. SOFTWARE_TOKEN_MFA is verified cryptographically (RFC 6238 TOTP) against the
// user's secret; SMS_MFA/EMAIL_OTP are verified against the code generated by
// newMFASession, since there is no client-held secret to re-derive those from.
func verifyMFAChallengeCode(entry *mfaSessionEntry, user *User, code string) error {
	switch entry.ChallengeType {
	case challengeSoftwareTokenMFA:
		if user.TOTPSecret == "" {
			return fmt.Errorf("%w: no TOTP secret associated; call AssociateSoftwareToken first", ErrNotAuthorized)
		}

		if !verifyTOTPCode(user.TOTPSecret, code, time.Now()) {
			return fmt.Errorf("%w: invalid software token code", ErrCodeMismatch)
		}

		return nil

	case challengeSMSMFA, challengeEmailOTP:
		if entry.Code == "" || !hmac.Equal([]byte(entry.Code), []byte(code)) {
			return fmt.Errorf("%w: invalid MFA code", ErrCodeMismatch)
		}

		return nil

	default:
		return fmt.Errorf("%w: unexpected challenge type %q for MFA response", ErrCodeMismatch, entry.ChallengeType)
	}
}

// CreateGroup creates a group in a user pool.
func (b *InMemoryBackend) CreateGroup(userPoolID, groupName, description string, precedence int32) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.groups.Get(groupKey(userPoolID, groupName)); exists {
		return nil, fmt.Errorf("%w: group %q already exists in pool %q", ErrAlreadyExists, groupName, userPoolID)
	}

	g := &Group{
		GroupName:   groupName,
		UserPoolID:  userPoolID,
		Description: description,
		Precedence:  precedence,
		CreatedAt:   time.Now().UTC(),
	}
	b.groups.Put(g)

	cp := *g

	return &cp, nil
}

// DeleteGroup removes a group from a user pool.
func (b *InMemoryBackend) DeleteGroup(userPoolID, groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return fmt.Errorf("%w: group %q not found in pool %q", ErrGroupNotFound, groupName, userPoolID)
	}

	b.groups.Delete(groupKey(userPoolID, groupName))

	if b.groupMembers[userPoolID] != nil {
		delete(b.groupMembers[userPoolID], groupName)
	}

	return nil
}

// ListGroups returns all groups in a user pool sorted by group name.
func (b *InMemoryBackend) ListGroups(userPoolID string) ([]*Group, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolGroups := b.groupsByPool.Get(userPoolID)
	out := make([]*Group, 0, len(poolGroups))

	for _, g := range poolGroups {
		cp := *g
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].GroupName < out[j].GroupName })

	return out, nil
}

// AdminAddUserToGroup adds a user to a group.
func (b *InMemoryBackend) AdminAddUserToGroup(userPoolID, username, groupName string) error {
	b.mu.Lock("AdminAddUserToGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if b.groupMembers[userPoolID] == nil {
		b.groupMembers[userPoolID] = make(map[string]map[string]struct{})
	}

	if b.groupMembers[userPoolID][groupName] == nil {
		b.groupMembers[userPoolID][groupName] = make(map[string]struct{})
	}

	b.groupMembers[userPoolID][groupName][username] = struct{}{}

	return nil
}

// AdminRemoveUserFromGroup removes a user from a group.
func (b *InMemoryBackend) AdminRemoveUserFromGroup(userPoolID, username, groupName string) error {
	b.mu.Lock("AdminRemoveUserFromGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if b.groupMembers[userPoolID] != nil && b.groupMembers[userPoolID][groupName] != nil {
		delete(b.groupMembers[userPoolID][groupName], username)
	}

	return nil
}

// AdminListGroupsForUser returns the groups a user belongs to, sorted by group name.
func (b *InMemoryBackend) AdminListGroupsForUser(userPoolID, username string) ([]*Group, error) {
	b.mu.RLock("AdminListGroupsForUser")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	out := make([]*Group, 0, len(b.groupMembers[userPoolID]))

	for groupName, members := range b.groupMembers[userPoolID] {
		if _, isMember := members[username]; !isMember {
			continue
		}

		if g, ok := b.groups.Get(groupKey(userPoolID, groupName)); ok {
			cp := *g
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].GroupName < out[j].GroupName })

	return out, nil
}

// UpdateUserAttributes updates the attributes of an authenticated user.
func (b *InMemoryBackend) UpdateUserAttributes(accessToken string, attributes map[string]string) error {
	b.mu.Lock("UpdateUserAttributes")
	defer b.mu.Unlock()

	u, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	if u.Attributes == nil {
		u.Attributes = make(map[string]string)
	}

	maps.Copy(u.Attributes, attributes)
	u.UpdatedAt = time.Now()

	return nil
}

// AdminUpdateUserAttributes updates attributes for a user in a pool.
func (b *InMemoryBackend) AdminUpdateUserAttributes(userPoolID, username string, attributes map[string]string) error {
	b.mu.Lock("AdminUpdateUserAttributes")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if u.Attributes == nil {
		u.Attributes = make(map[string]string)
	}

	maps.Copy(u.Attributes, attributes)
	u.UpdatedAt = time.Now()

	return nil
}

// AddCustomAttributes adds custom attribute definitions to a user pool schema.
// All attribute names must start with the "custom:" prefix as required by AWS Cognito.
func (b *InMemoryBackend) AddCustomAttributes(userPoolID string, attrs []SchemaAttribute) error {
	b.mu.Lock("AddCustomAttributes")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	for _, a := range attrs {
		if !strings.HasPrefix(a.Name, "custom:") {
			return fmt.Errorf(
				"%w: attribute name %q must start with 'custom:' prefix",
				ErrInvalidUserPoolConfig,
				a.Name,
			)
		}
	}

	pool.CustomAttributes = append(pool.CustomAttributes, attrs...)

	return nil
}

// AddUserPoolClientSecret generates and stores a client secret for the given app client.
func (b *InMemoryBackend) AddUserPoolClientSecret(userPoolID, clientID string) (string, error) {
	b.mu.Lock("AddUserPoolClientSecret")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok {
		return "", fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return "", fmt.Errorf("%w: client %q does not belong to pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	secret := randomAlphanumeric(clientSecretLen)
	client.ClientSecret = secret

	return secret, nil
}

// AdminDeleteUserAttributes removes specific attributes from a user in a pool.
func (b *InMemoryBackend) AdminDeleteUserAttributes(userPoolID, username string, attrNames []string) error {
	b.mu.Lock("AdminDeleteUserAttributes")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	for _, name := range attrNames {
		delete(u.Attributes, name)
	}

	return nil
}

// AdminDisableProviderForUser prevents a federated identity from signing in for a user.
// Since this mock does not track federated identity providers, this validates the pool exists
// and returns success (matching AWS behaviour for unknown provider links).
func (b *InMemoryBackend) AdminDisableProviderForUser(userPoolID string) error {
	b.mu.RLock("AdminDisableProviderForUser")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	return nil
}

// AdminDisableUser disables a user account in a pool.
func (b *InMemoryBackend) AdminDisableUser(userPoolID, username string) error {
	b.mu.Lock("AdminDisableUser")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	u.Enabled = false

	return nil
}

// AdminEnableUser re-enables a previously disabled user account in a pool.
func (b *InMemoryBackend) AdminEnableUser(userPoolID, username string) error {
	b.mu.Lock("AdminEnableUser")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	u.Enabled = true

	return nil
}

// AdminForgetDevice removes a tracked device for a user. A device that is on
// record (registered via ConfirmDevice) is really deleted. A missing
// deviceKey is treated as a no-op rather than ResourceNotFoundException:
// pre-existing callers invoke this operation without ever having confirmed a
// device, and historically received success once the user was found; this
// keeps that contract while making the operation state-aware for devices
// that do exist.
func (b *InMemoryBackend) AdminForgetDevice(userPoolID, username, deviceKey string) error {
	b.mu.Lock("AdminForgetDevice")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if key := userStateKey(userPoolID, username); b.devices[key] != nil {
		delete(b.devices[key], deviceKey)
	}

	return nil
}

// ValidateAccessToken verifies that the supplied access token is valid and resolves to a
// live user, returning NotAuthorizedException otherwise. It is used by access-token-scoped
// operations that have no persistent state to mutate but must still authenticate the token.
func (b *InMemoryBackend) ValidateAccessToken(accessToken string) error {
	b.mu.RLock("ValidateAccessToken")
	defer b.mu.RUnlock()

	if _, err := b.findUserByAccessTokenLocked(accessToken); err != nil {
		return err
	}

	return nil
}

// ValidatePoolUser validates that a pool and a user within it both exist. It is used by
// operations that have nothing to mutate but must still reject unknown pools/users with
// the AWS-accurate error shape.
func (b *InMemoryBackend) ValidatePoolUser(userPoolID, username string) error {
	b.mu.RLock("ValidatePoolUser")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	return nil
}

// ListUsersInGroup returns all users belonging to a group, sorted by username.
func (b *InMemoryBackend) ListUsersInGroup(userPoolID, groupName string) ([]*User, error) {
	b.mu.RLock("ListUsersInGroup")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	members := b.groupMembers[userPoolID][groupName]
	out := make([]*User, 0, len(members))

	for username := range members {
		u, ok := b.users.Get(userKey(userPoolID, username))
		if !ok {
			continue
		}
		cp := *u
		cp.Attributes = maps.Clone(u.Attributes)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })

	return out, nil
}

// AdminUserGlobalSignOut signs out a user from all sessions by revoking their refresh tokens
// and setting a per-user revocation timestamp so previously-issued access tokens are invalidated.
func (b *InMemoryBackend) AdminUserGlobalSignOut(userPoolID, username string) error {
	b.mu.Lock("AdminUserGlobalSignOut")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	b.deleteRefreshTokensForUserLocked(userPoolID, username)
	b.tokenRevokedBefore[userPoolID+":"+username] = time.Now().UTC()

	return nil
}

// GlobalSignOut signs out the authenticated user by revoking their refresh tokens
// and setting a per-user revocation timestamp so previously-issued access tokens are invalidated.
func (b *InMemoryBackend) GlobalSignOut(accessToken string) error {
	b.mu.Lock("GlobalSignOut")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	b.deleteRefreshTokensForUserLocked(user.UserPoolID, user.Username)
	b.tokenRevokedBefore[user.UserPoolID+":"+user.Username] = time.Now().UTC()

	return nil
}

// ResendConfirmationCode generates a new confirmation code for an unconfirmed user.
func (b *InMemoryBackend) ResendConfirmationCode(clientID, username string) (string, error) {
	b.mu.Lock("ResendConfirmationCode")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return "", fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if _, poolOK := b.pools.Get(client.UserPoolID); !poolOK {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		if client.PreventUserExistenceErrors == preventUserExistenceEnabled {
			// Same masking rationale as ForgotPassword above: fabricate a success
			// response instead of revealing that the account does not exist. The code
			// is never stored, so a subsequent ConfirmSignUp for this username still
			// fails.
			return randomAlphanumeric(confirmCodeLen), nil
		}

		return "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if user.Status != UserStatusUnconfirmed {
		return "", fmt.Errorf("%w: user is already confirmed", ErrInvalidParameter)
	}

	code := randomAlphanumeric(confirmCodeLen)
	user.ConfirmCode = code
	user.ConfirmCodeExpiresAt = time.Now().Add(confirmCodeTTL)

	return code, nil
}

// SetUserPoolMfaConfig sets the MFA configuration for a user pool.
func (b *InMemoryBackend) SetUserPoolMfaConfig(userPoolID, mfaConfig string) error {
	b.mu.Lock("SetUserPoolMfaConfig")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	pool.MfaConfiguration = mfaConfig

	return nil
}

// UpdateGroup updates a group's description and precedence.
func (b *InMemoryBackend) UpdateGroup(userPoolID, groupName, description string, precedence int32) (*Group, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	g, ok := b.groups.Get(groupKey(userPoolID, groupName))
	if !ok {
		return nil, fmt.Errorf("%w: group %q not found in pool %q", ErrGroupNotFound, groupName, userPoolID)
	}

	g.Description = description
	g.Precedence = precedence

	cp := *g

	return &cp, nil
}

// Reset clears all backend state. Useful for test isolation.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()

	b.refreshTokens = make(map[string]*refreshTokenEntry)
	b.refreshTokensByClient = make(map[string]map[string]struct{})
	b.refreshTokensByUser = make(map[string]map[string]struct{})
	b.groupMembers = make(map[string]map[string]map[string]struct{})
	b.tokenRevokedBefore = make(map[string]time.Time)
	b.resourceTags = make(map[string]map[string]string)
	b.riskConfigurations = make(map[string]*RiskConfiguration)
	b.logDeliveryConfigs = make(map[string]*LogDeliveryConfig)
	b.devices = make(map[string]map[string]*Device)
	b.webauthnCredentials = make(map[string]map[string]*WebAuthnCredential)
	b.authEvents = make(map[string]map[string]*AuthEvent)
}

// UpdateUserPool updates mutable properties of an existing user pool.
func (b *InMemoryBackend) UpdateUserPool(userPoolID, mfaConfiguration string) error {
	b.mu.Lock("UpdateUserPool")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if mfaConfiguration != "" {
		pool.MfaConfiguration = mfaConfiguration
	}

	pool.UpdatedAt = time.Now()

	return nil
}

// UpdateUserPoolClient updates mutable properties of an app client.
func (b *InMemoryBackend) UpdateUserPoolClient(userPoolID, clientID, clientName string) (*UserPoolClient, error) {
	b.mu.Lock("UpdateUserPoolClient")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q does not belong to pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	if clientName != "" {
		client.ClientName = clientName
	}

	client.UpdatedAt = time.Now()
	cp := *client

	return &cp, nil
}

// AdminResetUserPassword resets a user back to FORCE_CHANGE_PASSWORD status so they
// must set a new password on next login.
func (b *InMemoryBackend) AdminResetUserPassword(userPoolID, username string) error {
	b.mu.Lock("AdminResetUserPassword")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	u.Status = UserStatusForceChangePassword
	u.UpdatedAt = time.Now()

	// Revoke all existing refresh tokens for the user so active sessions are invalidated.
	b.deleteRefreshTokensForUserLocked(userPoolID, username)

	return nil
}

// GetGroup returns a single group from a user pool by name.
func (b *InMemoryBackend) GetGroup(userPoolID, groupName string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	g, ok := b.groups.Get(groupKey(userPoolID, groupName))
	if !ok {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	cp := *g

	return &cp, nil
}

// DeleteUser deletes the currently authenticated user (self-service).
func (b *InMemoryBackend) DeleteUser(accessToken string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	u, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	poolID := u.UserPoolID
	username := u.Username

	b.users.Delete(userKey(poolID, username))
	b.deleteRefreshTokensForUserLocked(poolID, username)

	return nil
}

// DeleteUserAttributes removes specific attributes from the authenticated user (self-service).
func (b *InMemoryBackend) DeleteUserAttributes(accessToken string, attrNames []string) error {
	b.mu.Lock("DeleteUserAttributes")
	defer b.mu.Unlock()

	u, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	for _, name := range attrNames {
		delete(u.Attributes, name)
	}

	u.UpdatedAt = time.Now()

	return nil
}

// VerifyUserAttribute is a no-op stub: the mock does not send verification codes so
// all attributes are considered already verified. Returns success for any code.
func (b *InMemoryBackend) VerifyUserAttribute(accessToken, attributeName, _ string) error {
	b.mu.RLock("VerifyUserAttribute")
	defer b.mu.RUnlock()

	if _, err := b.findUserByAccessTokenLocked(accessToken); err != nil {
		return err
	}

	// Attribute name validation: must be a known verifiable attribute.
	switch attributeName {
	case attrEmail, attrPhoneNumber:
		// valid
	default:
		return fmt.Errorf("%w: attribute %q is not verifiable", ErrInvalidUserPoolConfig, attributeName)
	}

	return nil
}

// ListUsersFiltered returns users matching an optional AWS-style filter string.
// Supported filter form: "username = \"prefix*\"" or "username ^= \"prefix\"".
// If filter is empty all users are returned (same as ListUsers).
func (b *InMemoryBackend) ListUsersFiltered(userPoolID, filter string) ([]*User, error) {
	b.mu.RLock("ListUsersFiltered")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolUsers := b.usersByPool.Get(userPoolID)
	prefix, attrFilter := parseListUsersFilter(filter)

	out := make([]*User, 0, len(poolUsers))

	for _, u := range poolUsers {
		if !userMatchesFilter(u, prefix, attrFilter) {
			continue
		}

		cp := *u
		cp.Attributes = maps.Clone(u.Attributes)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })

	return out, nil
}

// parseListUsersFilter parses a simplified Cognito filter expression.
// Returns a username prefix (may be "") and an optional attribute=value pair.
func parseListUsersFilter(filter string) (string, [2]string) {
	if filter == "" {
		return "", [2]string{}
	}

	// Trim whitespace and quotes.
	f := strings.TrimSpace(filter)

	// Common form: `username ^= "prefix"` or `username = "value"`
	for _, sep := range []string{" ^= ", " = "} {
		before, after, ok := strings.Cut(f, sep)
		if !ok {
			continue
		}

		attr := strings.TrimSpace(before)
		val := strings.Trim(strings.TrimSpace(after), `"`)

		if attr == "username" {
			val = strings.TrimSuffix(val, "*")

			return val, [2]string{}
		}

		return "", [2]string{attr, val}
	}

	return "", [2]string{}
}

// userMatchesFilter returns true if the user satisfies the filter criteria.
func userMatchesFilter(u *User, usernamePrefix string, attrFilter [2]string) bool {
	if usernamePrefix != "" && !strings.HasPrefix(u.Username, usernamePrefix) {
		return false
	}

	if attrFilter[0] != "" {
		attrVal, exists := u.Attributes[attrFilter[0]]
		if !exists || attrVal != attrFilter[1] {
			return false
		}
	}

	return true
}

// PoolMetrics holds aggregate statistics for a user pool.
type PoolMetrics struct {
	UserCount        int `json:"userCount,omitempty"`
	ClientCount      int `json:"clientCount,omitempty"`
	GroupCount       int `json:"groupCount,omitempty"`
	ActiveTokenCount int `json:"activeTokenCount,omitempty"`
}

// AddUserPoolInternal seeds a user pool directly into the backend, bypassing normal
// creation logic. Intended for use in tests only.
func (b *InMemoryBackend) AddUserPoolInternal(pool *UserPool) {
	b.mu.Lock("AddUserPoolInternal")
	defer b.mu.Unlock()

	b.pools.Put(pool)
}

// AddUserPoolClientInternal seeds a user pool client directly into the backend.
// Intended for use in tests only.
func (b *InMemoryBackend) AddUserPoolClientInternal(client *UserPoolClient) {
	b.mu.Lock("AddUserPoolClientInternal")
	defer b.mu.Unlock()

	b.clients.Put(client)
}

// deleteRefreshTokenLocked deletes a refresh token and updates secondary indexes.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) deleteRefreshTokenLocked(token string) {
	entry, ok := b.refreshTokens[token]
	if !ok {
		return
	}

	delete(b.refreshTokens, token)

	clientTokens, cok := b.refreshTokensByClient[entry.ClientID]
	if cok {
		delete(clientTokens, token)
		if len(clientTokens) == 0 {
			delete(b.refreshTokensByClient, entry.ClientID)
		}
	}

	userKey := entry.PoolID + ":" + entry.Username
	userTokens, foundUserTokens := b.refreshTokensByUser[userKey]
	if foundUserTokens {
		delete(userTokens, token)
		if len(userTokens) == 0 {
			delete(b.refreshTokensByUser, userKey)
		}
	}
}

// deleteRefreshTokensForClientAndUserIndexLocked deletes all refresh tokens issued for a client
// and keeps both secondary indexes (refreshTokensByClient, refreshTokensByUser) consistent.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) deleteRefreshTokensForClientAndUserIndexLocked(clientID string) {
	clientTokens, ok := b.refreshTokensByClient[clientID]
	if !ok {
		return
	}

	for token := range clientTokens {
		entry, exists := b.refreshTokens[token]
		if !exists {
			continue
		}

		// Also clean up the user index to prevent memory leaks.
		userKey := entry.PoolID + ":" + entry.Username
		userTokens, foundUserTokens := b.refreshTokensByUser[userKey]
		if foundUserTokens {
			delete(userTokens, token)
			if len(userTokens) == 0 {
				delete(b.refreshTokensByUser, userKey)
			}
		}

		delete(b.refreshTokens, token)
	}

	delete(b.refreshTokensByClient, clientID)
}

// deleteRefreshTokensForUserLocked deletes all refresh tokens for a user in a pool.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) deleteRefreshTokensForUserLocked(poolID, username string) {
	userKey := poolID + ":" + username
	userTokens, ok := b.refreshTokensByUser[userKey]
	if !ok {
		return
	}

	for token := range userTokens {
		entry, exists := b.refreshTokens[token]
		if !exists {
			continue
		}
		clientTokens, cok := b.refreshTokensByClient[entry.ClientID]
		if cok {
			delete(clientTokens, token)
			if len(clientTokens) == 0 {
				delete(b.refreshTokensByClient, entry.ClientID)
			}
		}
		delete(b.refreshTokens, token)
	}

	delete(b.refreshTokensByUser, userKey)
}

// storeRefreshTokenLocked stores a refresh token and updates secondary indexes.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) storeRefreshTokenLocked(token string, entry *refreshTokenEntry) {
	b.refreshTokens[token] = entry

	if b.refreshTokensByClient[entry.ClientID] == nil {
		b.refreshTokensByClient[entry.ClientID] = make(map[string]struct{})
	}

	b.refreshTokensByClient[entry.ClientID][token] = struct{}{}

	userKey := entry.PoolID + ":" + entry.Username
	if b.refreshTokensByUser[userKey] == nil {
		b.refreshTokensByUser[userKey] = make(map[string]struct{})
	}
	b.refreshTokensByUser[userKey][token] = struct{}{}
}

// AddUserInternal seeds a user directly into the backend, bypassing normal sign-up.
// Intended for use in tests only. The pool must already exist.
func (b *InMemoryBackend) AddUserInternal(user *User) {
	b.mu.Lock("AddUserInternal")
	defer b.mu.Unlock()

	b.users.Put(user)
}

// GetPoolMetrics returns aggregate statistics for a user pool.
func (b *InMemoryBackend) GetPoolMetrics(userPoolID string) (*PoolMetrics, error) {
	b.mu.RLock("GetPoolMetrics")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	tokenCount := 0
	now := time.Now().UTC()
	for _, entry := range b.refreshTokens {
		if entry.PoolID == userPoolID && (entry.ExpiresAt.IsZero() || entry.ExpiresAt.After(now)) {
			tokenCount++
		}
	}

	return &PoolMetrics{
		UserCount:        len(b.usersByPool.Get(userPoolID)),
		ClientCount:      len(b.clientsByPool.Get(userPoolID)),
		GroupCount:       len(b.groupsByPool.Get(userPoolID)),
		ActiveTokenCount: tokenCount,
	}, nil
}

// sortedCustomAttributes returns a copy of the pool's custom attributes sorted by name.
func sortedCustomAttributes(attrs []SchemaAttribute) []SchemaAttribute {
	if len(attrs) == 0 {
		return nil
	}

	cp := slices.Clone(attrs)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Name < cp[j].Name })

	return cp
}

// randomAlphanumeric returns a random alphanumeric string of length n.
func randomAlphanumeric(n int) string {
	b := make([]byte, n)
	for i := range b {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphanumChars))))
		if err != nil {
			b[i] = alphanumChars[0]

			continue
		}

		b[i] = alphanumChars[idx.Int64()]
	}

	return string(b)
}

// numericChars contains the digits used for random numeric code generation
// (SMS_MFA / EMAIL_OTP one-time codes).
const numericChars = "0123456789"

// randomNumeric returns a random numeric string of length n.
func randomNumeric(n int) string {
	b := make([]byte, n)
	for i := range b {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(numericChars))))
		if err != nil {
			b[i] = numericChars[0]

			continue
		}

		b[i] = numericChars[idx.Int64()]
	}

	return string(b)
}
