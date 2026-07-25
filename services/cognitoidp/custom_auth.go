package cognitoidp

// custom_auth.go implements the CUSTOM_AUTH authentication flow: Cognito's
// Lambda-driven state machine (DefineAuthChallenge / CreateAuthChallenge /
// VerifyAuthChallengeResponse) for authentication methods with no built-in Cognito
// support (CAPTCHA, custom OTP delivery, passwordless links, ...). Unlike
// USER_PASSWORD_AUTH/USER_SRP_AUTH, Cognito itself never validates credentials for
// CUSTOM_AUTH -- every decision (issue tokens, fail, or present another challenge) is
// delegated to the pool's DefineAuthChallenge Lambda, round by round, driven by the
// accumulated challenge history in mfaSessionEntry.CustomAuthSession.

import (
	"fmt"
	"time"
)

// challengeCustomChallenge is the fixed ChallengeName Cognito always returns to the
// client for a CUSTOM_AUTH round, regardless of the Lambda-internal challenge name
// DefineAuthChallenge chose for its own bookkeeping (see
// mfaSessionEntry.CustomAuthChallengeName, which holds that internal name for the
// next round's session history -- it is never sent to the client).
const challengeCustomChallenge = "CUSTOM_CHALLENGE"

// startCustomAuth begins a CUSTOM_AUTH flow for an existing user by invoking
// DefineAuthChallenge with an empty round history (session: []), matching AWS's first
// call for a fresh InitiateAuth/AdminInitiateAuth CUSTOM_AUTH request. Caller must
// hold b.mu (authenticate does).
func (b *InMemoryBackend) startCustomAuth(pool *UserPool, clientID string, user *User) (*AuthResult, error) {
	return b.customAuthRound(pool, clientID, user, nil)
}

// customAuthRound drives one iteration of the CUSTOM_AUTH state machine: it invokes
// DefineAuthChallenge with the round history so far and, per its decision, either
// issues tokens, fails the authentication, or invokes CreateAuthChallenge to build the
// next challenge and returns it to the caller as a pending session (the same
// MFASession/ChallengeName/ChallengeParameters shape InitiateAuth uses for any other
// challenge type). Caller must hold b.mu.
func (b *InMemoryBackend) customAuthRound(
	pool *UserPool, clientID string, user *User, session []customAuthChallengeResult,
) (*AuthResult, error) {
	challengeName, issueTokens, failAuthentication, err := b.defineAuthChallenge(
		pool, clientID, user.Username, user.Attributes, session, false,
	)
	if err != nil {
		return nil, err
	}

	if failAuthentication {
		return nil, fmt.Errorf("%w: incorrect username or password", ErrNotAuthorized)
	}

	if issueTokens {
		return b.issueTokensLocked(pool, clientID, user, triggerSourceTokenGenAuthentication)
	}

	if challengeName == "" {
		return nil, fmt.Errorf(
			"%w: DefineAuthChallenge response set none of challengeName, issueTokens, or failAuthentication",
			ErrUnexpectedLambda,
		)
	}

	public, private, metadata, err := b.createAuthChallenge(
		pool, clientID, user.Username, user.Attributes, challengeName, session,
	)
	if err != nil {
		return nil, err
	}

	sessionToken := randomAlphanumeric(mfaSessionLen)
	b.mfaSessions[sessionToken] = &mfaSessionEntry{
		PoolID:                      pool.ID,
		ClientID:                    clientID,
		Username:                    user.Username,
		ChallengeType:               challengeCustomChallenge,
		ExpiresAt:                   time.Now().Add(mfaSessionTTL),
		CustomAuthChallengeName:     challengeName,
		CustomAuthChallengeMetadata: metadata,
		CustomAuthPrivateParams:     private,
		CustomAuthSession:           session,
	}

	return &AuthResult{
		MFASession:          sessionToken,
		ChallengeName:       challengeCustomChallenge,
		ChallengeParameters: public,
	}, nil
}

// RespondToCustomAuthChallenge verifies answer for the pending CUSTOM_AUTH session (via
// VerifyAuthChallengeResponse), appends the round's outcome to the challenge history,
// and re-invokes DefineAuthChallenge to decide the next step. This mirrors AWS: a wrong
// answer does not automatically fail the attempt -- VerifyAuthChallengeResponse's
// answerCorrect is just one more entry in the session history DefineAuthChallenge sees,
// and it alone decides whether to retry, present a new challenge, or fail (e.g. "fail
// after 3 wrong answers").
func (b *InMemoryBackend) RespondToCustomAuthChallenge(clientID, session, answer string) (*AuthResult, error) {
	b.mu.Lock("RespondToCustomAuthChallenge")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok {
		return nil, fmt.Errorf("%w: session not found or expired", ErrNotAuthorized)
	}

	if entry.ChallengeType != challengeCustomChallenge {
		return nil, fmt.Errorf("%w: session is not a CUSTOM_CHALLENGE", ErrNotAuthorized)
	}

	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		delete(b.mfaSessions, session)

		return nil, fmt.Errorf("%w: session not found or expired", ErrNotAuthorized)
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

	answerCorrect, err := b.verifyCustomAuthChallenge(
		pool, clientID, user.Username, user.Attributes, entry.CustomAuthPrivateParams, answer,
	)
	if err != nil {
		delete(b.mfaSessions, session)

		return nil, err
	}

	nextSession := make([]customAuthChallengeResult, 0, len(entry.CustomAuthSession)+1)
	nextSession = append(nextSession, entry.CustomAuthSession...)
	nextSession = append(nextSession, customAuthChallengeResult{
		ChallengeName:     entry.CustomAuthChallengeName,
		ChallengeResult:   answerCorrect,
		ChallengeMetadata: entry.CustomAuthChallengeMetadata,
	})

	// Consume this round's session; customAuthRound mints a fresh one if another
	// challenge follows.
	delete(b.mfaSessions, session)

	return b.customAuthRound(pool, clientID, user, nextSession)
}
