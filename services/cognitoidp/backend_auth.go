package cognitoidp

import (
	"crypto/rand"
	"fmt"
	"maps"
	"math/big"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

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

	pool, poolOK := b.pools.Get(client.UserPoolID)
	if !poolOK {
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

	// PostConfirmation is fire-and-observe: the response carries no fields that
	// mutate state, but real AWS still surfaces a trigger invocation error to the
	// ConfirmSignUp caller (the user's confirmation itself is NOT rolled back --
	// Cognito confirms first, then invokes the trigger, matching this ordering).
	if _, err := b.invokeLambdaTrigger(pool, triggerKeyPostConfirmation, triggerSourcePostConfirmationSignUp,
		clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
			eventKeyClientMetadata: map[string]any{},
		},
		map[string]any{},
	); err != nil {
		return err
	}

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

// AdminConfirmSignUp confirms a user's registration without requiring a confirmation code.
// This is an admin operation that bypasses the normal confirmation flow.
func (b *InMemoryBackend) AdminConfirmSignUp(userPoolID, username string) error {
	b.mu.Lock("AdminConfirmSignUp")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	user.Status = UserStatusConfirmed
	user.ConfirmCode = ""

	// Same trigger source and fire-and-observe semantics as ConfirmSignUp -- AWS
	// uses a single PostConfirmation_ConfirmSignUp trigger source for both the
	// self-service and admin confirmation paths. AdminConfirmSignUp has no app
	// client in scope, so callerContext.clientId is left empty (matches the
	// admin API not routing through a client).
	if _, err := b.invokeLambdaTrigger(pool, triggerKeyPostConfirmation, triggerSourcePostConfirmationSignUp,
		"", username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
			eventKeyClientMetadata: map[string]any{},
		},
		map[string]any{},
	); err != nil {
		return err
	}

	return nil
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

	return b.issueTokensLocked(pool, clientID, user, triggerSourceTokenGenAuthentication)
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

// confirmCodeTTL is the lifetime of a confirmation or reset code before it expires.
const confirmCodeTTL = 24 * time.Hour

// SignUpWithValidation is like SignUp but enforces the pool's PasswordPolicy and
// automatically verifies attributes configured in AutoVerifiedAttributes.
func (b *InMemoryBackend) SignUpWithValidation(
	clientID, username, password string,
	userAttributes map[string]string,
) (*User, error) {
	b.mu.Lock("SignUpWithValidation")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	if err := validatePassword(pool.PasswordPolicy, password); err != nil {
		return nil, err
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

	preSignUpResp, err := b.invokeLambdaTrigger(
		pool, triggerKeyPreSignUp, triggerSourcePreSignUpSignUp, clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(attrs),
			"validationData":       map[string]any{},
			eventKeyClientMetadata: map[string]any{},
		},
		map[string]any{"autoConfirmUser": false, "autoVerifyEmail": false, "autoVerifyPhone": false},
	)
	if err != nil {
		return nil, err
	}

	lambdaAutoConfirm, lambdaAutoVerifyEmail, lambdaAutoVerifyPhone := parsePreSignUpResponse(preSignUpResp)

	// Auto-verify attributes that are configured on the pool, or that the PreSignUp
	// trigger explicitly requested verification for.
	autoConfirmed := lambdaAutoConfirm

	for _, attr := range pool.AutoVerifiedAttributes {
		if _, hasAttr := attrs[attr]; hasAttr {
			attrs[attr+"_verified"] = attrVerifiedTrue
			autoConfirmed = true
		}
	}

	if lambdaAutoVerifyEmail {
		attrs[attrEmail+"_verified"] = attrVerifiedTrue
	}

	if lambdaAutoVerifyPhone {
		attrs["phone_number_verified"] = attrVerifiedTrue
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

	b.users.Put(user)

	cp := *user

	return &cp, nil
}
