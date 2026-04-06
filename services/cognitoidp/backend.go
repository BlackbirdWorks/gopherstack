package cognitoidp

import (
	"crypto/rand"
	"fmt"
	"maps"
	"math/big"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
)

// SchemaAttribute represents a custom attribute definition for a user pool.
type SchemaAttribute struct {
	Name                     string  `json:"Name"`
	AttributeDataType        string  `json:"AttributeDataType,omitempty"`
	StringAttributeMinLength string  `json:"StringAttributeMinLength,omitempty"`
	StringAttributeMaxLength string  `json:"StringAttributeMaxLength,omitempty"`
	NumberAttributeMinValue  float64 `json:"NumberAttributeMinValue,omitempty"`
	NumberAttributeMaxValue  float64 `json:"NumberAttributeMaxValue,omitempty"`
	Mutable                  bool    `json:"Mutable"`
	Required                 bool    `json:"Required"`
	DeveloperOnlyAttribute   bool    `json:"DeveloperOnlyAttribute"`
}

// UserPool represents a Cognito User Pool.
type UserPool struct {
	CreatedAt        time.Time
	issuer           *tokenIssuer
	ID               string
	Name             string
	ARN              string
	CustomAttributes []SchemaAttribute
}

// UserPoolClient represents an app client registered to a user pool.
type UserPoolClient struct {
	CreatedAt    time.Time `json:"createdAt"`
	ClientID     string    `json:"clientId"`
	ClientName   string    `json:"clientName"`
	UserPoolID   string    `json:"userPoolId"`
	ClientSecret string    `json:"clientSecret,omitempty"`
}

// User represents a Cognito user within a pool.
type User struct {
	CreatedAt    time.Time
	Attributes   map[string]string
	Sub          string
	Username     string
	UserPoolID   string
	PasswordHash string
	Status       string
	ConfirmCode  string
	Enabled      bool
}

// Group represents a Cognito User Pool group.
type Group struct {
	CreatedAt   time.Time `json:"createdAt"`
	GroupName   string    `json:"groupName"`
	UserPoolID  string    `json:"userPoolId"`
	Description string    `json:"description,omitempty"`
	Precedence  int32     `json:"precedence"`
}

// InMemoryBackend is the in-memory store for Cognito IDP resources.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	pools       map[string]*UserPool
	poolsByName map[string]*UserPool
	clients     map[string]*UserPoolClient
	users       map[string]map[string]*User
	// refreshTokens maps refresh token → poolID/username for REFRESH_TOKEN_AUTH flow.
	refreshTokens map[string]*refreshTokenEntry
	// groups maps poolID → groupName → Group
	groups map[string]map[string]*Group
	// groupMembers maps poolID → groupName → set of usernames
	groupMembers map[string]map[string]map[string]struct{}
	accountID    string
	region       string
	endpoint     string
}

// refreshTokenEntry holds the pool/user context for a refresh token.
type refreshTokenEntry struct {
	PoolID   string `json:"poolId"`
	ClientID string `json:"clientId"`
	Username string `json:"username"`
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:            lockmetrics.New("cognitoidp"),
		pools:         make(map[string]*UserPool),
		poolsByName:   make(map[string]*UserPool),
		clients:       make(map[string]*UserPoolClient),
		users:         make(map[string]map[string]*User),
		refreshTokens: make(map[string]*refreshTokenEntry),
		groups:        make(map[string]map[string]*Group),
		groupMembers:  make(map[string]map[string]map[string]struct{}),
		accountID:     accountID,
		region:        region,
		endpoint:      endpoint,
	}
}

// CreateUserPool creates a new user pool with the given name.
func (b *InMemoryBackend) CreateUserPool(name string) (*UserPool, error) {
	b.mu.Lock("CreateUserPool")
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

	pool := &UserPool{
		ID:        poolID,
		Name:      name,
		ARN:       fmt.Sprintf("arn:aws:cognito-idp:%s:%s:userpool/%s", b.region, b.accountID, poolID),
		CreatedAt: time.Now(),
		issuer:    issuer,
	}

	b.pools[poolID] = pool
	b.poolsByName[name] = pool
	b.users[poolID] = make(map[string]*User)

	cp := *pool

	return &cp, nil
}

// DescribeUserPool returns the user pool with the given ID.
func (b *InMemoryBackend) DescribeUserPool(userPoolID string) (*UserPool, error) {
	b.mu.RLock("DescribeUserPool")
	defer b.mu.RUnlock()

	pool, ok := b.pools[userPoolID]
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

	pool, ok := b.pools[userPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	delete(b.poolsByName, pool.Name)
	delete(b.pools, userPoolID)
	delete(b.users, userPoolID)

	maps.DeleteFunc(b.clients, func(_ string, client *UserPoolClient) bool {
		return client.UserPoolID == userPoolID
	})

	// Clean up any refresh tokens issued for users in this pool to prevent leaks.
	maps.DeleteFunc(b.refreshTokens, func(_ string, entry *refreshTokenEntry) bool {
		return entry.PoolID == userPoolID
	})

	// Clean up groups and group memberships for this pool.
	delete(b.groups, userPoolID)
	delete(b.groupMembers, userPoolID)

	return nil
}

// DeleteUserPoolClient removes the app client with the given client ID from the given pool.
// If userPoolID is empty the pool ownership check is skipped.
func (b *InMemoryBackend) DeleteUserPoolClient(userPoolID, clientID string) error {
	b.mu.Lock("DeleteUserPoolClient")
	defer b.mu.Unlock()

	client, ok := b.clients[clientID]
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if userPoolID != "" && client.UserPoolID != userPoolID {
		return fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	delete(b.clients, clientID)

	// Clean up any refresh tokens issued by this client to prevent leaks.
	maps.DeleteFunc(b.refreshTokens, func(_ string, entry *refreshTokenEntry) bool {
		return entry.ClientID == clientID
	})

	return nil
}

// ListUserPools returns all user pools.
func (b *InMemoryBackend) ListUserPools() []*UserPool {
	b.mu.RLock("ListUserPools")
	defer b.mu.RUnlock()

	out := make([]*UserPool, 0, len(b.pools))
	for _, p := range b.pools {
		cp := *p
		out = append(out, &cp)
	}

	return out
}

// ListUserPoolClients returns all app clients for the given user pool.
func (b *InMemoryBackend) ListUserPoolClients(userPoolID string) ([]*UserPoolClient, error) {
	b.mu.RLock("ListUserPoolClients")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	var out []*UserPoolClient

	for _, c := range b.clients {
		if c.UserPoolID == userPoolID {
			cp := *c
			out = append(out, &cp)
		}
	}

	return out, nil
}

// CreateUserPoolClient creates a new app client for the given user pool.
func (b *InMemoryBackend) CreateUserPoolClient(userPoolID, clientName string) (*UserPoolClient, error) {
	b.mu.Lock("CreateUserPoolClient")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client := &UserPoolClient{
		ClientID:   randomAlphanumeric(clientIDLen),
		ClientName: clientName,
		UserPoolID: userPoolID,
		CreatedAt:  time.Now(),
	}

	b.clients[client.ClientID] = client

	cp := *client

	return &cp, nil
}

// DescribeUserPoolClient returns the app client with the given client ID.
func (b *InMemoryBackend) DescribeUserPoolClient(userPoolID, clientID string) (*UserPoolClient, error) {
	b.mu.RLock("DescribeUserPoolClient")
	defer b.mu.RUnlock()

	client, ok := b.clients[clientID]
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

	client, ok := b.clients[clientID]
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	poolUsers, ok := b.users[client.UserPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	if _, exists := poolUsers[username]; exists {
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
		Enabled:      true,
		// Generate a confirmation code (simulates the code sent via email/SMS).
		ConfirmCode: randomAlphanumeric(confirmCodeLen),
	}

	poolUsers[username] = user

	cp := *user

	return &cp, nil
}

// ConfirmSignUp confirms a user's registration by validating the confirmation code.
func (b *InMemoryBackend) ConfirmSignUp(clientID, username, confirmationCode string) error {
	b.mu.Lock("ConfirmSignUp")
	defer b.mu.Unlock()

	client, ok := b.clients[clientID]
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	poolUsers, ok := b.users[client.UserPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := poolUsers[username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if confirmationCode == "" {
		return fmt.Errorf("%w: confirmation code is required", ErrCodeMismatch)
	}

	if user.ConfirmCode != "" && confirmationCode != user.ConfirmCode {
		return fmt.Errorf("%w: invalid confirmation code", ErrCodeMismatch)
	}

	user.Status = UserStatusConfirmed
	user.ConfirmCode = ""

	return nil
}

// InitiateAuth authenticates a user using the specified auth flow.
func (b *InMemoryBackend) InitiateAuth(clientID, authFlow, username, password string) (*TokenResult, error) {
	b.mu.RLock("InitiateAuth")
	defer b.mu.RUnlock()

	user, pool, err := b.findUserByClientID(clientID, username)
	if err != nil {
		return nil, err
	}

	return b.authenticate(pool, clientID, authFlow, user, password)
}

// AdminInitiateAuth authenticates a user as an admin using the specified auth flow.
func (b *InMemoryBackend) AdminInitiateAuth(
	userPoolID, clientID, authFlow, username, password string,
) (*TokenResult, error) {
	b.mu.RLock("AdminInitiateAuth")
	defer b.mu.RUnlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients[clientID]
	if !ok || client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := poolUsers[username]
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

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := poolUsers[username]; exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(tempPassword), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hashing password: %w", err)
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	user := &User{
		Sub:          uuid.New().String(),
		Username:     username,
		UserPoolID:   userPoolID,
		PasswordHash: string(hash),
		Status:       UserStatusForceChangePassword,
		Attributes:   attrs,
		CreatedAt:    time.Now(),
		Enabled:      true,
	}

	poolUsers[username] = user

	cp := *user

	return &cp, nil
}

// AdminSetUserPassword sets the password for a user in a pool.
func (b *InMemoryBackend) AdminSetUserPassword(userPoolID, username, password string, permanent bool) error {
	b.mu.Lock("AdminSetUserPassword")
	defer b.mu.Unlock()

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := poolUsers[username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return fmt.Errorf("hashing password: %w", err)
	}

	user.PasswordHash = string(hash)

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

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := poolUsers[username]
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

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := poolUsers[username]
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

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok2 := poolUsers[username]; !ok2 {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	delete(poolUsers, username)

	// Revoke any refresh tokens that belong to this user.
	for token, entry := range b.refreshTokens {
		if entry.PoolID == userPoolID && entry.Username == username {
			delete(b.refreshTokens, token)
		}
	}

	return nil
}

// ListUsers returns all users in a pool.
func (b *InMemoryBackend) ListUsers(userPoolID string) ([]*User, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	poolUsers, ok := b.users[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	out := make([]*User, 0, len(poolUsers))

	for _, u := range poolUsers {
		cp := *u
		cp.Attributes = maps.Clone(u.Attributes)
		out = append(out, &cp)
	}

	return out, nil
}

// ForgotPassword initiates a password reset for a user.
// In this mock the reset code is generated and stored on the user.
func (b *InMemoryBackend) ForgotPassword(clientID, username string) (string, error) {
	b.mu.Lock("ForgotPassword")
	defer b.mu.Unlock()

	client, ok := b.clients[clientID]
	if !ok {
		return "", fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	poolUsers, ok := b.users[client.UserPoolID]
	if !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := poolUsers[username]
	if !ok {
		return "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	code := randomAlphanumeric(confirmCodeLen)
	user.ConfirmCode = code

	return code, nil
}

// ConfirmForgotPassword resets a user's password using the code generated by ForgotPassword.
func (b *InMemoryBackend) ConfirmForgotPassword(clientID, username, code, newPassword string) error {
	b.mu.Lock("ConfirmForgotPassword")
	defer b.mu.Unlock()

	client, ok := b.clients[clientID]
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	poolUsers, ok := b.users[client.UserPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := poolUsers[username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if user.ConfirmCode == "" || user.ConfirmCode != code {
		return fmt.Errorf("%w: invalid reset code", ErrCodeMismatch)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcryptCost)
	if err != nil {
		return fmt.Errorf("hashing password: %w", err)
	}

	user.PasswordHash = string(hash)
	user.ConfirmCode = ""
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

	hash, err3 := bcrypt.GenerateFromPassword([]byte(proposedPassword), bcryptCost)
	if err3 != nil {
		return fmt.Errorf("hashing password: %w", err3)
	}

	u.PasswordHash = string(hash)

	return nil
}

// findUserByAccessTokenLocked finds the live *User for a given access token.
// The caller must hold b.mu (either read or write lock).
func (b *InMemoryBackend) findUserByAccessTokenLocked(accessToken string) (*User, error) {
	for _, pool := range b.pools {
		claims, err := pool.issuer.ParseAccessToken(accessToken)
		if err != nil {
			continue
		}

		sub, _ := claims["sub"].(string)
		if sub == "" {
			continue
		}

		for _, u := range b.users[pool.ID] {
			if u.Sub == sub {
				return u, nil
			}
		}
	}

	return nil, fmt.Errorf("%w: access token is invalid or expired", ErrNotAuthorized)
}

// GetUserPoolJWKS returns the JSON Web Key Set for the given user pool.
func (b *InMemoryBackend) GetUserPoolJWKS(userPoolID string) (*JWKSResponse, error) {
	b.mu.RLock("GetUserPoolJWKS")
	defer b.mu.RUnlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	jwks := pool.issuer.JWKS()

	return &jwks, nil
}

// findUserByClientID finds a user and their pool using the clientID.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findUserByClientID(clientID, username string) (*User, *UserPool, error) {
	client, ok := b.clients[clientID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools[client.UserPoolID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	poolUsers, ok := b.users[client.UserPoolID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := poolUsers[username]
	if !ok {
		return nil, nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	return user, pool, nil
}

// authenticate validates a user's credentials and returns tokens. Caller must hold at least a read lock.
func (b *InMemoryBackend) authenticate(
	pool *UserPool,
	clientID, authFlow string,
	user *User,
	password string,
) (*TokenResult, error) {
	switch authFlow {
	case "USER_PASSWORD_AUTH", "ADMIN_USER_PASSWORD_AUTH", "USER_SRP_AUTH":
		// fall through to password validation
	default:
		return nil, fmt.Errorf("%w: unsupported auth flow %q", ErrInvalidUserPoolConfig, authFlow)
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

	tokens, err := pool.issuer.Issue(clientID, user.Username, user.Sub)
	if err != nil {
		return nil, fmt.Errorf("issuing tokens: %w", err)
	}

	// Store the refresh token so REFRESH_TOKEN_AUTH can validate it.
	b.refreshTokens[tokens.RefreshToken] = &refreshTokenEntry{
		PoolID:   pool.ID,
		ClientID: clientID,
		Username: user.Username,
	}

	return tokens, nil
}

// InitiateAuthRefreshToken exchanges a valid refresh token for new ID/Access tokens.
func (b *InMemoryBackend) InitiateAuthRefreshToken(clientID, refreshToken string) (*TokenResult, error) {
	b.mu.Lock("InitiateAuthRefreshToken")
	defer b.mu.Unlock()

	entry, ok := b.refreshTokens[refreshToken]
	if !ok {
		return nil, fmt.Errorf("%w: refresh token not found or expired", ErrNotAuthorized)
	}

	if entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: refresh token was issued for a different client", ErrNotAuthorized)
	}

	pool, ok := b.pools[entry.PoolID]
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	poolUsers, ok := b.users[entry.PoolID]
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	user, ok := poolUsers[entry.Username]
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	tokens, err := pool.issuer.Issue(clientID, user.Username, user.Sub)
	if err != nil {
		return nil, fmt.Errorf("issuing tokens: %w", err)
	}

	// Rotate the refresh token: invalidate old, store new.
	delete(b.refreshTokens, refreshToken)
	b.refreshTokens[tokens.RefreshToken] = entry

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

	delete(b.refreshTokens, token)

	return nil
}

// CreateGroup creates a group in a user pool.
func (b *InMemoryBackend) CreateGroup(userPoolID, groupName, description string, precedence int32) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if b.groups[userPoolID] == nil {
		b.groups[userPoolID] = make(map[string]*Group)
	}

	if _, exists := b.groups[userPoolID][groupName]; exists {
		return nil, fmt.Errorf("%w: group %q already exists in pool %q", ErrAlreadyExists, groupName, userPoolID)
	}

	g := &Group{
		GroupName:   groupName,
		UserPoolID:  userPoolID,
		Description: description,
		Precedence:  precedence,
		CreatedAt:   time.Now().UTC(),
	}
	b.groups[userPoolID][groupName] = g

	cp := *g

	return &cp, nil
}

// DeleteGroup removes a group from a user pool.
func (b *InMemoryBackend) DeleteGroup(userPoolID, groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups[userPoolID][groupName]; !ok {
		return fmt.Errorf("%w: group %q not found in pool %q", ErrGroupNotFound, groupName, userPoolID)
	}

	delete(b.groups[userPoolID], groupName)

	if b.groupMembers[userPoolID] != nil {
		delete(b.groupMembers[userPoolID], groupName)
	}

	return nil
}

// ListGroups returns all groups in a user pool.
func (b *InMemoryBackend) ListGroups(userPoolID string) ([]*Group, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolGroups := b.groups[userPoolID]
	out := make([]*Group, 0, len(poolGroups))

	for _, g := range poolGroups {
		cp := *g
		out = append(out, &cp)
	}

	return out, nil
}

// AdminAddUserToGroup adds a user to a group.
func (b *InMemoryBackend) AdminAddUserToGroup(userPoolID, username, groupName string) error {
	b.mu.Lock("AdminAddUserToGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups[userPoolID][groupName]; !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	poolUsers := b.users[userPoolID]
	if _, ok := poolUsers[username]; !ok {
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

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups[userPoolID][groupName]; !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if b.groupMembers[userPoolID] != nil && b.groupMembers[userPoolID][groupName] != nil {
		delete(b.groupMembers[userPoolID][groupName], username)
	}

	return nil
}

// AdminListGroupsForUser returns the groups a user belongs to.
func (b *InMemoryBackend) AdminListGroupsForUser(userPoolID, username string) ([]*Group, error) {
	b.mu.RLock("AdminListGroupsForUser")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users[userPoolID][username]; !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	out := make([]*Group, 0)

	for groupName, members := range b.groupMembers[userPoolID] {
		if _, isMember := members[username]; !isMember {
			continue
		}

		if g, ok := b.groups[userPoolID][groupName]; ok {
			cp := *g
			out = append(out, &cp)
		}
	}

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

	return nil
}

// AdminUpdateUserAttributes updates attributes for a user in a pool.
func (b *InMemoryBackend) AdminUpdateUserAttributes(userPoolID, username string, attributes map[string]string) error {
	b.mu.Lock("AdminUpdateUserAttributes")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users[userPoolID][username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if u.Attributes == nil {
		u.Attributes = make(map[string]string)
	}

	maps.Copy(u.Attributes, attributes)

	return nil
}

// AddCustomAttributes adds custom attribute definitions to a user pool schema.
func (b *InMemoryBackend) AddCustomAttributes(userPoolID string, attrs []SchemaAttribute) error {
	b.mu.Lock("AddCustomAttributes")
	defer b.mu.Unlock()

	pool, ok := b.pools[userPoolID]
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	pool.CustomAttributes = append(pool.CustomAttributes, attrs...)

	return nil
}

// AddUserPoolClientSecret generates and stores a client secret for the given app client.
func (b *InMemoryBackend) AddUserPoolClientSecret(userPoolID, clientID string) (string, error) {
	b.mu.Lock("AddUserPoolClientSecret")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients[clientID]
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

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users[userPoolID][username]
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

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	return nil
}

// AdminDisableUser disables a user account in a pool.
func (b *InMemoryBackend) AdminDisableUser(userPoolID, username string) error {
	b.mu.Lock("AdminDisableUser")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users[userPoolID][username]
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

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users[userPoolID][username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	u.Enabled = true

	return nil
}

// AdminForgetDevice forgets a device for a user. Since this mock does not track devices,
// it validates the user exists and returns success.
func (b *InMemoryBackend) AdminForgetDevice(userPoolID, username string) error {
	b.mu.RLock("AdminForgetDevice")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users[userPoolID][username]; !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	return nil
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
