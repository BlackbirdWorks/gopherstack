package cognitoidp

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
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

	if b.usernameExistsLocked(client.UserPoolID, username) {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUsernameExists, username)
	}

	hash, saltHex, verifierHex, err := hashAndSRP(client.UserPoolID, username, password)
	if err != nil {
		return nil, err
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	user := &User{
		Sub:          uuid.New().String(),
		Username:     username,
		UserPoolID:   client.UserPoolID,
		PasswordHash: hash,
		SRPSalt:      saltHex,
		SRPVerifier:  verifierHex,
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
func (b *InMemoryBackend) ConfirmSignUp(
	clientID, username, confirmationCode string, meta ...ClientMetadata,
) error {
	return b.ConfirmSignUpWithOptions(clientID, username, confirmationCode, ConfirmSignUpOptions{
		ClientMetadata: firstMetadata(meta),
	})
}

// ConfirmSignUpOptions carries the optional ConfirmSignUp request members.
type ConfirmSignUpOptions struct {
	ClientMetadata     map[string]string
	ForceAliasCreation bool
}

// ConfirmSignUpWithOptions is ConfirmSignUp with ForceAliasCreation: when a verified sign-in alias of
// the user already belongs to another user it fails with AliasExistsException, or moves the alias
// to this user when ForceAliasCreation is set.
func (b *InMemoryBackend) ConfirmSignUpWithOptions(
	clientID, username, confirmationCode string, opts ConfirmSignUpOptions,
) error {
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

	username = b.resolveLoginNameLocked(pool, username)

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		return unknownUserConfirmError(client, username)
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

	if aliasErr := b.claimAliasesLocked(pool, user, opts.ForceAliasCreation); aliasErr != nil {
		return aliasErr
	}

	user.Status = UserStatusConfirmed
	user.ConfirmCode = ""
	user.ConfirmCodeExpiresAt = time.Time{}

	// PostConfirmation is fire-and-observe: the response carries no fields that
	// mutate state, but real AWS still surfaces a trigger invocation error to the
	// ConfirmSignUp caller (the user's confirmation itself is NOT rolled back --
	// Cognito confirms first, then invokes the trigger, matching this ordering).
	if _, err := b.invokeTriggerUnlocked(pool, triggerKeyPostConfirmation, triggerSourcePostConfirmationSignUp,
		clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
			eventKeyClientMetadata: stringMapToAny(opts.ClientMetadata),
		},
		map[string]any{},
	); err != nil {
		return err
	}

	return nil
}

// InitiateAuth authenticates a user using the specified auth flow.
func (b *InMemoryBackend) InitiateAuth(
	clientID, authFlow, username, password string, meta ...ClientMetadata,
) (*AuthResult, error) {
	return b.InitiateAuthWithDevice(clientID, authFlow, username, password, "", firstMetadata(meta))
}

// InitiateAuthWithDevice is InitiateAuth for a request that carries AuthParameters DEVICE_KEY.
func (b *InMemoryBackend) InitiateAuthWithDevice(
	clientID, authFlow, username, password, deviceKey string, cm ClientMetadata,
) (*AuthResult, error) {
	b.mu.Lock("InitiateAuth")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	username = b.resolveLoginNameLocked(pool, username)

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		migrated, finalStatus, migErr := b.tryUserMigration(pool, clientID, authFlow, username, password, cm)
		if migErr != nil {
			return nil, migErr
		}

		if migrated == nil {
			return nil, unknownUserAuthError(client, username)
		}

		user = migrated

		result, authErr := b.authenticate(pool, clientID, authFlow, user, password, cm, deviceKey)
		b.applyPostMigrationFinalStatus(pool.ID, username, finalStatus)

		return result, authErr
	}

	return b.authenticate(pool, clientID, authFlow, user, password, cm, deviceKey)
}

// AdminInitiateAuth authenticates a user as an admin using the specified auth flow.
func (b *InMemoryBackend) AdminInitiateAuth(
	userPoolID, clientID, authFlow, username, password string, meta ...ClientMetadata,
) (*AuthResult, error) {
	return b.AdminInitiateAuthWithDevice(userPoolID, clientID, authFlow, username, password, "", firstMetadata(meta))
}

// AdminInitiateAuthWithDevice is AdminInitiateAuth for a request that carries AuthParameters DEVICE_KEY.
func (b *InMemoryBackend) AdminInitiateAuthWithDevice(
	userPoolID, clientID, authFlow, username, password, deviceKey string, cm ClientMetadata,
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

	username = b.resolveLoginNameLocked(pool, username)

	user, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		migrated, finalStatus, migErr := b.tryUserMigration(pool, clientID, authFlow, username, password, cm)
		if migErr != nil {
			return nil, migErr
		}

		if migrated == nil {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
		}

		user = migrated

		result, authErr := b.authenticate(pool, clientID, authFlow, user, password, cm, deviceKey)
		b.applyPostMigrationFinalStatus(pool.ID, username, finalStatus)

		return result, authErr
	}

	return b.authenticate(pool, clientID, authFlow, user, password, cm, deviceKey)
}

// AdminConfirmSignUp confirms a user's registration without requiring a confirmation code.
// This is an admin operation that bypasses the normal confirmation flow.
func (b *InMemoryBackend) AdminConfirmSignUp(userPoolID, username string, meta ...ClientMetadata) error {
	b.mu.Lock("AdminConfirmSignUp")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	username = b.resolveLoginNameLocked(pool, username)

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
	if _, err := b.invokeTriggerUnlocked(pool, triggerKeyPostConfirmation, triggerSourcePostConfirmationSignUp,
		"", username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
			eventKeyClientMetadata: stringMapToAny(firstMetadata(meta)),
		},
		map[string]any{},
	); err != nil {
		return err
	}

	return nil
}

// ForgotPassword initiates a password reset for a user.
// In this mock the reset code is generated and stored on the user.
func (b *InMemoryBackend) ForgotPassword(clientID, username string, meta ...ClientMetadata) (string, error) {
	b.mu.Lock("ForgotPassword")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return "", fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, poolOK := b.pools.Get(client.UserPoolID)
	if !poolOK {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, fabricatedCode, err := b.resolveForgotPasswordUser(pool, client, username, firstMetadata(meta))
	if err != nil {
		return "", err
	}

	if fabricatedCode != "" {
		// AWS never reveals UserNotFoundException here when masking is enabled: the
		// caller gets the same success response (fabricated CodeDeliveryDetails) it
		// would get for a real account. The code is not stored anywhere, so
		// ConfirmForgotPassword will still fail for this username -- matching AWS,
		// which never actually delivers anything for a nonexistent account either.
		return fabricatedCode, nil
	}

	code := randomAlphanumeric(confirmCodeLen)
	user.ConfirmCode = code
	user.ConfirmCodeExpiresAt = time.Now().Add(confirmCodeTTL)

	return code, nil
}

// resolveForgotPasswordUser resolves the user ForgotPassword should generate a reset
// code for: an existing enabled user in a resettable status, or a user freshly created
// by the UserMigration_ForgotPassword trigger (which has no verified-contact-info
// state to check yet -- the Lambda already vouched for it by supplying
// userAttributes). When neither applies and PreventUserExistenceErrors masking is on,
// it returns a fabricated code with no user, since there is nothing to attach a code
// to. Caller must hold b.mu.
func (b *InMemoryBackend) resolveForgotPasswordUser(
	pool *UserPool, client *UserPoolClient, username string, cm map[string]string,
) (*User, string, error) {
	username = b.resolveLoginNameLocked(pool, username)

	if u, ok := b.users.Get(userKey(pool.ID, username)); ok {
		if !u.Enabled {
			return nil, "", fmt.Errorf("%w: User is disabled", ErrNotAuthorized)
		}

		if u.Status == UserStatusUnconfirmed || u.Status == UserStatusForceChangePassword {
			return nil, "", fmt.Errorf(
				"%w: Cannot reset password for the user as there is no registered/verified"+
					" email or phone_number",
				ErrInvalidParameter,
			)
		}

		return u, "", nil
	}

	migrated, migErr := b.tryUserMigrationForgotPassword(pool, client.ClientID, username, cm)
	if migErr != nil {
		return nil, "", migErr
	}

	if migrated != nil {
		return migrated, "", nil
	}

	if client.PreventUserExistenceErrors == preventUserExistenceEnabled {
		return nil, randomAlphanumeric(confirmCodeLen), nil
	}

	return nil, "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
}

// ConfirmForgotPassword resets a user's password using the code generated by ForgotPassword.
func (b *InMemoryBackend) ConfirmForgotPassword(
	clientID, username, code, newPassword string, meta ...ClientMetadata,
) error {
	b.mu.Lock("ConfirmForgotPassword")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	cfpPool, poolOK := b.pools.Get(client.UserPoolID)
	if !poolOK {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	username = b.resolveLoginNameLocked(cfpPool, username)

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		return unknownUserConfirmError(client, username)
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

	hash, saltHex, verifierHex, err := hashAndSRP(client.UserPoolID, username, newPassword)
	if err != nil {
		return err
	}

	user.PasswordHash = hash
	user.TemporaryPassword = ""
	user.SRPSalt = saltHex
	user.SRPVerifier = verifierHex
	user.ConfirmCode = ""
	user.ConfirmCodeExpiresAt = time.Time{}
	user.Status = UserStatusConfirmed

	// PostConfirmation fires for a completed password reset too; like ConfirmSignUp, the
	// reset itself is not rolled back when the trigger errors.
	if pool != nil {
		if _, err = b.invokeTriggerUnlocked(
			pool, triggerKeyPostConfirmation, triggerSourcePostConfirmationForgotPwd, clientID, username,
			map[string]any{
				eventKeyUserAttributes: stringMapToAny(user.Attributes),
				eventKeyClientMetadata: stringMapToAny(firstMetadata(meta)),
			},
			map[string]any{},
		); err != nil {
			return err
		}
	}

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

	hash, saltHex, verifierHex, err4 := hashAndSRP(u.UserPoolID, u.Username, proposedPassword)
	if err4 != nil {
		return err4
	}

	u.PasswordHash = hash
	u.TemporaryPassword = ""
	u.SRPSalt = saltHex
	u.SRPVerifier = verifierHex

	return nil
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

// unknownUserConfirmError returns the error ConfirmSignUp/ConfirmForgotPassword surface
// when the username does not exist. When PreventUserExistenceErrors is "ENABLED", AWS
// masks the distinction behind the same CodeMismatchException an incorrect confirmation
// code produces for a real account -- a caller cannot distinguish "no such user" from
// "wrong code" by error type/text, closing the same enumeration vector unknownUserAuthError
// closes for InitiateAuth. "LEGACY" (the default when unset) reveals UserNotFoundException.
func unknownUserConfirmError(client *UserPoolClient, username string) error {
	if client.PreventUserExistenceErrors == preventUserExistenceEnabled {
		return fmt.Errorf("%w: invalid confirmation code", ErrCodeMismatch)
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

// precheckAuthLocked runs the flow/lambda/status checks common to every auth flow
// (password-based or SRP) before any credential is actually verified. Caller must hold
// the write lock.
func (b *InMemoryBackend) precheckAuthLocked(
	pool *UserPool, clientID, authFlow string, user *User, cm map[string]string,
) error {
	switch authFlow {
	case "USER_PASSWORD_AUTH", "ADMIN_USER_PASSWORD_AUTH", "ADMIN_NO_SRP_AUTH",
		"USER_SRP_AUTH", "ADMIN_USER_SRP_AUTH", "CUSTOM_AUTH", authFlowUserAuth:
		// valid flows; ADMIN_NO_SRP_AUTH is a legacy alias for ADMIN_USER_PASSWORD_AUTH
	default:
		return fmt.Errorf("%w: unsupported auth flow %q", ErrInvalidUserPoolConfig, authFlow)
	}

	if !b.isAuthFlowAllowed(clientID, authFlow) {
		return fmt.Errorf(
			"%w: auth flow %q is not in client ExplicitAuthFlows",
			ErrInvalidUserPoolConfig,
			authFlow,
		)
	}

	return b.precheckUserLocked(pool, clientID, user, cm)
}

// precheckUserLocked runs the PreAuthentication trigger and the user status checks shared by every
// sign-in path. Caller must hold the write lock.
func (b *InMemoryBackend) precheckUserLocked(
	pool *UserPool, clientID string, user *User, cm map[string]string,
) error {
	// PreAuthentication fires before any credential/status validation, matching AWS:
	// the Lambda only sees userAttributes/validationData (never the password), and can
	// reject the attempt outright by returning an error.
	if err := b.preAuthenticationCheck(pool, clientID, user, cm); err != nil {
		return err
	}

	if user.Status == UserStatusUnconfirmed {
		return fmt.Errorf("%w: user %q is not confirmed", ErrUserNotConfirmed, user.Username)
	}

	if !user.Enabled {
		return fmt.Errorf("%w: user %q account is disabled", ErrNotAuthorized, user.Username)
	}

	return nil
}

// defaultTempPasswordValidityDays is real Cognito's TemporaryPasswordValidityDays default
// when a pool's PasswordPolicy doesn't set one (0 in this backend's wire representation).
const defaultTempPasswordValidityDays = 7

// tempPasswordExpired reports whether user's temporary password has outlived pool's
// PasswordPolicy.TemporaryPasswordValidityDays. A zero TempPasswordIssuedAt (users
// persisted before this field existed) is treated as never-expiring.
func tempPasswordExpired(pool *UserPool, user *User) bool {
	if user.TempPasswordIssuedAt.IsZero() {
		return false
	}

	days := defaultTempPasswordValidityDays
	if pool.PasswordPolicy != nil && pool.PasswordPolicy.TemporaryPasswordValidityDays > 0 {
		days = pool.PasswordPolicy.TemporaryPasswordValidityDays
	}

	return time.Since(user.TempPasswordIssuedAt) > time.Duration(days)*24*time.Hour
}

// postCredentialCheckLocked runs once a caller's credential (password or SRP password
// claim) has been verified: it gates on FORCE_CHANGE_PASSWORD and pool MFA
// configuration before finally issuing tokens. Caller must hold the write lock.
func (b *InMemoryBackend) postCredentialCheckLocked(
	pool *UserPool, clientID string, user *User, cm map[string]string,
) (*AuthResult, error) {
	return b.postCredentialCheckDeviceLocked(pool, clientID, user, cm, "")
}

// postCredentialCheckDeviceLocked is postCredentialCheckLocked for a sign-in that named a
// DEVICE_KEY: a remembered device with a registered verifier gets a DEVICE_SRP_AUTH challenge.
func (b *InMemoryBackend) postCredentialCheckDeviceLocked(
	pool *UserPool, clientID string, user *User, cm map[string]string, deviceKey string,
) (*AuthResult, error) {
	if user.Status == UserStatusForceChangePassword {
		if tempPasswordExpired(pool, user) {
			return nil, fmt.Errorf(
				"%w: temporary password has expired and must be reset by an administrator",
				ErrNotAuthorized,
			)
		}

		return b.newMFASession(pool, clientID, user.Username, challengeNewPasswordRequired), nil
	}

	if challenge := b.deviceChallengeLocked(pool, clientID, user, deviceKey); challenge != nil {
		return challenge, nil
	}

	return b.finishFirstFactorLocked(pool, clientID, user, cm, false)
}

// finishFirstFactorLocked applies the pool MFA gate (skipped when a remembered device already
// authenticated) and issues tokens.
func (b *InMemoryBackend) finishFirstFactorLocked(
	pool *UserPool, clientID string, user *User, cm map[string]string, skipMFA bool,
) (*AuthResult, error) {
	mfaConfig := pool.MfaConfiguration
	if !skipMFA && (mfaConfig == "ON" || mfaConfig == "OPTIONAL") {
		return b.newMFASession(pool, clientID, user.Username, mfaChallengeType(pool, user)), nil
	}

	return b.issueTokensLocked(pool, clientID, user, triggerSourceTokenGenAuthentication, cm)
}

// authenticate validates a user's password-based credentials (or delegates to
// CUSTOM_AUTH) and returns tokens or a challenge. USER_SRP_AUTH/ADMIN_USER_SRP_AUTH are
// handled separately by startSRPAuthLocked -- a real SRP client never sends a plaintext
// password to InitiateAuth, so there is nothing for this function to check here. Caller
// must hold the write lock.
func (b *InMemoryBackend) authenticate(
	pool *UserPool,
	clientID, authFlow string,
	user *User,
	password string,
	cm map[string]string,
	deviceKey string,
) (*AuthResult, error) {
	if err := b.precheckAuthLocked(pool, clientID, authFlow, user, cm); err != nil {
		return nil, err
	}

	// A real SRP client never sends a plaintext password: routing USER_SRP_AUTH/
	// ADMIN_USER_SRP_AUTH here (instead of to startSRPAuthLocked) is always a caller
	// bug, not a credential to check.
	if authFlow == authFlowUserSRP || authFlow == authFlowAdminUserSRP {
		return nil, fmt.Errorf(
			"%w: %q must use InitiateAuthSRP/AdminInitiateAuthSRP", ErrInvalidUserPoolConfig, authFlow,
		)
	}

	// CUSTOM_AUTH never validates a password server-side -- that decision is fully
	// delegated to the pool's DefineAuthChallenge/CreateAuthChallenge/
	// VerifyAuthChallengeResponse Lambda chain (custom_auth.go).
	if authFlow == "CUSTOM_AUTH" {
		return b.startCustomAuth(pool, clientID, user, cm)
	}

	if err := b.verifyPasswordLocked(pool, user, password); err != nil {
		return nil, err
	}

	return b.postCredentialCheckDeviceLocked(pool, clientID, user, cm, deviceKey)
}

// verifyPasswordLocked bcrypt-checks password with b.mu released, then re-validates the user.
func (b *InMemoryBackend) verifyPasswordLocked(pool *UserPool, user *User, password string) error {
	hash := user.PasswordHash

	var cmpErr error

	b.releaseLocked("AuthBcrypt", func() {
		cmpErr = bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	})

	if err := b.authUserCurrentLocked(pool, user); err != nil {
		return err
	}

	if cmpErr != nil || user.PasswordHash != hash {
		return fmt.Errorf("%w: incorrect username or password", ErrNotAuthorized)
	}

	return nil
}

// startSRPAuthLocked begins the USER_SRP_AUTH/ADMIN_USER_SRP_AUTH handshake: given the
// client's public ephemeral value A, it picks a random server secret b, computes the
// public B = (k*v + g^b) mod N from the user's stored SRP verifier, and returns a
// PASSWORD_VERIFIER challenge carrying SALT/SRP_B/SECRET_BLOCK/USER_ID_FOR_SRP -- the
// four parameters a real SRP client needs to derive the same session key and complete
// the handshake via RespondToAuthChallenge. Caller must hold the write lock.
func (b *InMemoryBackend) startSRPAuthLocked(
	pool *UserPool, clientID, authFlow string, user *User, srpAHex string, cm map[string]string,
) (*AuthResult, error) {
	if err := b.precheckAuthLocked(pool, clientID, authFlow, user, cm); err != nil {
		return nil, err
	}

	if user.SRPSalt == "" || user.SRPVerifier == "" {
		return nil, fmt.Errorf("%w: user %q has no SRP credentials", ErrNotAuthorized, user.Username)
	}

	aPub, ok := new(big.Int).SetString(srpAHex, hexBase)
	if !ok || aPub.Sign() <= 0 {
		return nil, fmt.Errorf("%w: invalid SRP_A", ErrInvalidParameter)
	}

	if new(big.Int).Mod(aPub, srpN()).Sign() == 0 {
		return nil, fmt.Errorf("%w: SRP_A mod N cannot be zero", ErrInvalidParameter)
	}

	verifier, ok := new(big.Int).SetString(user.SRPVerifier, hexBase)
	if !ok {
		return nil, fmt.Errorf("%w: corrupt SRP verifier for user %q", ErrNotAuthorized, user.Username)
	}

	bPriv, err := srpRandomExponent()
	if err != nil {
		return nil, err
	}

	bPub := srpServerB(verifier, bPriv)

	secretBlock := make([]byte, srpSecretBlockLen)
	if _, readErr := rand.Read(secretBlock); readErr != nil {
		return nil, fmt.Errorf("generating SRP secret block: %w", readErr)
	}

	sessionToken := randomAlphanumeric(mfaSessionLen)
	entry := &mfaSessionEntry{
		PoolID:         pool.ID,
		ClientID:       clientID,
		Username:       user.Username,
		ChallengeType:  challengePasswordVerifier,
		ExpiresAt:      time.Now().Add(mfaSessionTTL),
		SRPA:           hex.EncodeToString(srpPadHex(aPub)),
		SRPb:           hex.EncodeToString(srpPadHex(bPriv)),
		SRPB:           hex.EncodeToString(srpPadHex(bPub)),
		SRPSecretBlock: base64.StdEncoding.EncodeToString(secretBlock),
	}
	b.mfaSessions[sessionToken] = entry

	return &AuthResult{
		MFASession:    sessionToken,
		ChallengeName: challengePasswordVerifier,
		ChallengeParameters: map[string]string{
			"SALT":            user.SRPSalt,
			"SRP_B":           entry.SRPB,
			"SECRET_BLOCK":    entry.SRPSecretBlock,
			"USERNAME":        user.Username,
			"USER_ID_FOR_SRP": user.Username,
		},
	}, nil
}

// srpRandomExponent picks a random SRP private exponent b in [1, N).
func srpRandomExponent() (*big.Int, error) {
	n := srpN()

	for {
		b, err := rand.Int(rand.Reader, n)
		if err != nil {
			return nil, fmt.Errorf("generating SRP exponent: %w", err)
		}

		if b.Sign() != 0 {
			return b, nil
		}
	}
}

// InitiateAuthSRP begins a USER_SRP_AUTH handshake.
func (b *InMemoryBackend) InitiateAuthSRP(
	clientID, authFlow, username, srpA string, meta ...ClientMetadata,
) (*AuthResult, error) {
	b.mu.Lock("InitiateAuthSRP")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	username = b.resolveLoginNameLocked(pool, username)

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		return nil, unknownUserAuthError(client, username)
	}

	return b.startSRPAuthLocked(pool, clientID, authFlow, user, srpA, firstMetadata(meta))
}

// AdminInitiateAuthSRP begins an ADMIN_USER_SRP_AUTH handshake.
func (b *InMemoryBackend) AdminInitiateAuthSRP(
	userPoolID, clientID, authFlow, username, srpA string, meta ...ClientMetadata,
) (*AuthResult, error) {
	b.mu.Lock("AdminInitiateAuthSRP")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok || client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	username = b.resolveLoginNameLocked(pool, username)

	user, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	return b.startSRPAuthLocked(pool, clientID, authFlow, user, srpA, firstMetadata(meta))
}

// ResendConfirmationCode generates a new confirmation code for an unconfirmed user.
func (b *InMemoryBackend) ResendConfirmationCode(clientID, username string) (string, error) {
	b.mu.Lock("ResendConfirmationCode")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return "", fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	resendPool, poolOK := b.pools.Get(client.UserPoolID)
	if !poolOK {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	username = b.resolveLoginNameLocked(resendPool, username)

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
	_, err := b.AdminResetUserPasswordCode(userPoolID, username)

	return err
}

// AdminResetUserPasswordCode is AdminResetUserPassword that also issues the reset code
// the user later confirms with ConfirmForgotPassword.
func (b *InMemoryBackend) AdminResetUserPasswordCode(userPoolID, username string) (string, error) {
	b.mu.Lock("AdminResetUserPassword")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	u, ok := b.users.Get(userKey(userPoolID, username))
	if !ok {
		return "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	u.Status = UserStatusForceChangePassword
	u.UpdatedAt = time.Now()
	u.TempPasswordIssuedAt = u.UpdatedAt

	code := randomAlphanumeric(confirmCodeLen)
	u.ConfirmCode = code
	u.ConfirmCodeExpiresAt = u.UpdatedAt.Add(confirmCodeTTL)

	// Revoke all existing refresh tokens for the user so active sessions are invalidated.
	b.deleteRefreshTokensForUserLocked(userPoolID, username)

	return code, nil
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
	return b.SignUpWithTriggerData(clientID, username, password, userAttributes, TriggerData{})
}

// SignUpWithTriggerData is SignUpWithValidation that also hands ClientMetadata and ValidationData to PreSignUp.
func (b *InMemoryBackend) SignUpWithTriggerData(
	clientID, username, password string,
	userAttributes map[string]string,
	td TriggerData,
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

	if allowOnly, _ := pool.Settings.AdminCreateUserConfig["AllowAdminCreateUserOnly"].(bool); allowOnly {
		return nil, fmt.Errorf("%w: SignUp is not permitted for this user pool", ErrNotAuthorized)
	}

	if err := validatePassword(pool.PasswordPolicy, password); err != nil {
		return nil, err
	}

	if b.usernameExistsLocked(client.UserPoolID, username) {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUsernameExists, username)
	}

	hash, saltHex, verifierHex, err := hashAndSRP(client.UserPoolID, username, password)
	if err != nil {
		return nil, err
	}

	attrs := make(map[string]string, len(userAttributes))
	maps.Copy(attrs, userAttributes)

	preSignUpResp, err := b.invokeTriggerUnlocked(
		pool, triggerKeyPreSignUp, triggerSourcePreSignUpSignUp, clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(attrs),
			eventKeyValidationData: stringMapToAny(td.ValidationData),
			eventKeyClientMetadata: stringMapToAny(td.ClientMetadata),
		},
		map[string]any{"autoConfirmUser": false, "autoVerifyEmail": false, "autoVerifyPhone": false},
	)
	if err != nil {
		return nil, err
	}

	if slotErr := b.newUserSlotFreeLocked(pool, username); slotErr != nil {
		return nil, slotErr
	}

	if err = validatePassword(pool.PasswordPolicy, password); err != nil {
		return nil, err
	}

	autoConfirmed := applyPreSignUpVerification(pool, attrs, preSignUpResp)

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
		PasswordHash:         hash,
		SRPSalt:              saltHex,
		SRPVerifier:          verifierHex,
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

// applyPreSignUpVerification marks auto-verified attributes and returns whether PreSignUp
// requested autoConfirmUser; AutoVerifiedAttributes alone never skips the confirmation code.
func applyPreSignUpVerification(pool *UserPool, attrs map[string]string, resp map[string]any) bool {
	autoConfirm, autoVerifyEmail, autoVerifyPhone := parsePreSignUpResponse(resp)

	for _, attr := range pool.AutoVerifiedAttributes {
		if _, hasAttr := attrs[attr]; hasAttr {
			attrs[attr+"_verified"] = attrVerifiedTrue
		}
	}

	if autoVerifyEmail {
		attrs[attrEmail+"_verified"] = attrVerifiedTrue
	}

	if autoVerifyPhone {
		attrs["phone_number_verified"] = attrVerifiedTrue
	}

	return autoConfirm
}
