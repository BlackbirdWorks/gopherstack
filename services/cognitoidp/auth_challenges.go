package cognitoidp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
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

	hash, saltHex, verifierHex, err := hashAndSRP(entry.PoolID, entry.Username, newPassword)
	if err != nil {
		return nil, err
	}

	user.PasswordHash = hash
	user.SRPSalt = saltHex
	user.SRPVerifier = verifierHex
	user.Status = UserStatusConfirmed
	user.UpdatedAt = time.Now()
	delete(b.mfaSessions, session)

	result, err := b.issueTokensLocked(pool, clientID, user, triggerSourceTokenGenNewPasswordFlow)
	if err != nil {
		return nil, err
	}

	return result.Tokens, nil
}

// RespondToSRPChallenge completes the USER_SRP_AUTH/ADMIN_USER_SRP_AUTH handshake
// started by startSRPAuthLocked. It recomputes the SRP shared secret from the server's
// stored (A, b, B, v) session state, derives the same HKDF session key a real SRP
// client derives, and verifies the client's PASSWORD_CLAIM_SIGNATURE -- the actual
// zero-knowledge proof that the client knows the password without ever having sent it.
// On success this runs the same FORCE_CHANGE_PASSWORD/MFA gate any other credential
// check runs, so it may return a further challenge rather than tokens.
func (b *InMemoryBackend) RespondToSRPChallenge(
	clientID, session string, challengeResponses map[string]string,
) (*AuthResult, error) {
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

	if err := verifySRPPasswordClaim(entry, user, challengeResponses); err != nil {
		return nil, err
	}

	delete(b.mfaSessions, session)

	return b.postCredentialCheckLocked(pool, clientID, user)
}

// verifySRPPasswordClaim checks a client's PASSWORD_CLAIM_SECRET_BLOCK/
// PASSWORD_CLAIM_SIGNATURE/TIMESTAMP against the server-side SRP session state stored
// in entry, matching CognitoUser.js's authenticateUserDefaultAuth computation exactly.
func verifySRPPasswordClaim(entry *mfaSessionEntry, user *User, challengeResponses map[string]string) error {
	secretBlockB64 := challengeResponses["PASSWORD_CLAIM_SECRET_BLOCK"]
	if secretBlockB64 == "" || secretBlockB64 != entry.SRPSecretBlock {
		return fmt.Errorf("%w: PASSWORD_CLAIM_SECRET_BLOCK mismatch", ErrNotAuthorized)
	}

	secretBlock, err := base64.StdEncoding.DecodeString(secretBlockB64)
	if err != nil {
		return fmt.Errorf("%w: invalid PASSWORD_CLAIM_SECRET_BLOCK", ErrNotAuthorized)
	}

	timestamp := challengeResponses["TIMESTAMP"]
	if timestamp == "" {
		return fmt.Errorf("%w: missing TIMESTAMP", ErrNotAuthorized)
	}

	providedSig, err := base64.StdEncoding.DecodeString(challengeResponses["PASSWORD_CLAIM_SIGNATURE"])
	if err != nil {
		return fmt.Errorf("%w: invalid PASSWORD_CLAIM_SIGNATURE", ErrNotAuthorized)
	}

	aPub, aOK := new(big.Int).SetString(entry.SRPA, hexBase)
	bPriv, bOK := new(big.Int).SetString(entry.SRPb, hexBase)
	bPub, bpOK := new(big.Int).SetString(entry.SRPB, hexBase)
	verifier, vOK := new(big.Int).SetString(user.SRPVerifier, hexBase)

	if !aOK || !bOK || !bpOK || !vOK {
		return fmt.Errorf("%w: corrupt SRP session state", ErrNotAuthorized)
	}

	u := srpCalculateU(aPub, bPub)
	if u.Sign() == 0 {
		return fmt.Errorf("%w: SRP U cannot be zero", ErrNotAuthorized)
	}

	s := srpServerS(aPub, verifier, u, bPriv)
	hkdfKey := srpHKDF(s, u)

	poolName := srpPoolName(entry.PoolID)
	expectedSig := srpComputeSignature(hkdfKey, poolName, entry.Username, secretBlock, timestamp)

	if !hmac.Equal(expectedSig, providedSig) {
		return fmt.Errorf("%w: incorrect username or password", ErrNotAuthorized)
	}

	return nil
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
