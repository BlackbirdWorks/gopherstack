package cognitoidp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
)

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

	pool, ok := b.pools.Get(entry.PoolID)
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	if err := validatePassword(pool.PasswordPolicy, newPassword); err != nil {
		return nil, err
	}

	user, ok := b.users.Get(userKey(entry.PoolID, entry.Username))
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

	result, err := b.issueTokensLocked(pool, clientID, user, triggerSourceTokenGenNewPasswordFlow)
	if err != nil {
		return nil, err
	}

	return result.Tokens, nil
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

	pool, ok := b.pools.Get(entry.PoolID)
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	user, ok := b.users.Get(userKey(entry.PoolID, entry.Username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	delete(b.mfaSessions, session)

	result, err := b.issueTokensLocked(pool, clientID, user, triggerSourceTokenGenAuthentication)
	if err != nil {
		return nil, err
	}

	return result.Tokens, nil
}

// ValidateSecretHash validates the SECRET_HASH field for a client that has a secret.
// AWS computes SecretHash = BASE64(HMAC-SHA256(username + clientId, clientSecret)).
// When a client has no secret, an empty hash is accepted (and a non-empty hash is rejected).
// When a client has a secret, the hash must be present and match.
func (b *InMemoryBackend) ValidateSecretHash(clientID, username, providedHash string) error {
	b.mu.RLock("ValidateSecretHash")
	defer b.mu.RUnlock()

	client, ok := b.clients.Get(clientID)
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

// bcryptHashPassword hashes a password with bcrypt.
func bcryptHashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", fmt.Errorf("hashing password: %w", err)
	}

	return string(hash), nil
}
