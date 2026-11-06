package cognitoidp

import (
	"fmt"
	"slices"
	"time"
)

const (
	challengeSignUp          = "SIGN_UP"
	challengeSignUpConfirmed = "SIGN_UP_CONFIRMED"
	defaultAuthSessionMins   = 3
)

// authSessionTTL is the client's AuthSessionValidity (minutes), 3 when unset.
func authSessionTTL(client *UserPoolClient) time.Duration {
	mins := int(client.AuthSessionValidity)
	if mins <= 0 {
		mins = defaultAuthSessionMins
	}

	return time.Duration(mins) * time.Minute
}

// NewSignUpSession returns the Session SignUp hands back for a later ConfirmSignUp, or "" when the
// app client does not enable ALLOW_USER_AUTH (the only flow that can consume it).
func (b *InMemoryBackend) NewSignUpSession(clientID, username string) string {
	b.mu.Lock("NewSignUpSession")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok || !slices.Contains(client.ExplicitAuthFlows, "ALLOW_"+authFlowUserAuth) {
		return ""
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		return ""
	}

	token := randomAlphanumeric(mfaSessionLen)
	b.mfaSessions[token] = &mfaSessionEntry{
		PoolID: pool.ID, ClientID: clientID, Username: b.resolveLoginNameLocked(pool, username),
		ChallengeType: challengeSignUp, ExpiresAt: b.now().Add(authSessionTTL(client)),
	}

	return token
}

// ConfirmSignUpSession marks a SignUp session as confirmed after a successful ConfirmSignUp and returns
// the session for InitiateAuth(USER_AUTH). An unknown, expired or foreign session yields "".
func (b *InMemoryBackend) ConfirmSignUpSession(clientID, username, session string) string {
	b.mu.Lock("ConfirmSignUpSession")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok || entry.ChallengeType != challengeSignUp || entry.ClientID != clientID ||
		!entry.ExpiresAt.After(b.now()) {
		return ""
	}

	client, clientOK := b.clients.Get(clientID)
	pool, poolOK := b.pools.Get(entry.PoolID)

	if !clientOK || !poolOK || entry.Username != b.resolveLoginNameLocked(pool, username) {
		return ""
	}

	entry.ChallengeType = challengeSignUpConfirmed
	entry.ExpiresAt = b.now().Add(authSessionTTL(client))

	return session
}

// autoSignInLocked signs in a user straight after sign-up confirmation: the Session from ConfirmSignUp
// stands in for the first factor, and the pool's MFA still applies.
func (b *InMemoryBackend) autoSignInLocked(
	pool *UserPool, clientID string, user *User, session string, cm map[string]string,
) (*AuthResult, error) {
	entry, ok := b.mfaSessions[session]
	if !ok || entry.ChallengeType != challengeSignUpConfirmed || entry.ClientID != clientID ||
		entry.Username != user.Username || !entry.ExpiresAt.After(b.now()) {
		delete(b.mfaSessions, session)

		return nil, fmt.Errorf("%w: sign-up session is invalid or expired", ErrNotAuthorized)
	}

	if err := b.precheckAuthLocked(pool, clientID, authFlowUserAuth, user, cm); err != nil {
		return nil, err
	}

	delete(b.mfaSessions, session)

	return b.postCredentialCheckLocked(pool, clientID, user, cm)
}
