package cognitoidp

import (
	"fmt"
	"slices"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// USER_AUTH choice-based authentication (gopherstack-5f20).
//
// SDK citations (cognitoidentityprovider@v1.67.4, GOMODCACHE):
//   - AuthFlowType.USER_AUTH: types/enums.go:220.
//   - ChallengeNameType.SELECT_CHALLENGE/PASSWORD/PASSWORD_SRP/EMAIL_OTP/SMS_OTP/WEB_AUTHN:
//     types/enums.go:270,276,278,275,275(sic),277 -- full block at types/enums.go:259-304.
//   - AuthFactorType.PASSWORD/EMAIL_OTP/SMS_OTP/WEB_AUTHN/SOFTWARE_TOKEN: types/enums.go:184-207.
//   - SignInPolicyType.AllowedFirstAuthFactors: types/types.go:2039-2047 ("SOFTWARE_TOKEN is
//     not currently supported as a first auth factor. Do not include this value").
//   - UserPoolPolicyType.SignInPolicy: types/types.go:2894.
//   - ExplicitAuthFlowsType.ALLOW_USER_AUTH: types/enums.go:639.
//   - InitiateAuthInput.AuthParameters (USER_AUTH: USERNAME, optional PREFERRED_CHALLENGE)
//     and InitiateAuthOutput.AvailableChallenges/ChallengeName=SELECT_CHALLENGE:
//     api_op_InitiateAuth.go (doc comments on AuthFlow/AuthParameters/AvailableChallenges/
//     ChallengeName fields).
//   - RespondToAuthChallengeInput.ChallengeName/ChallengeResponses doc comment (same per-
//     challenge key table as InitiateAuthOutput.ChallengeName): api_op_RespondToAuthChallenge.go.
//     SELECT_CHALLENGE responds with ANSWER (one of AvailableChallenges) plus USERNAME.
//     PASSWORD responds with PASSWORD. EMAIL_OTP responds with EMAIL_OTP_CODE. SMS_OTP
//     responds with SMS_OTP_CODE.
//   - Error set (InitiateAuth/RespondToAuthChallenge deserializers.go): both model
//     InvalidParameterException and NotAuthorizedException among others -- used below for
//     "ANSWER/PREFERRED_CHALLENGE not offered" and "wrong PASSWORD/code" respectively.
//
// State machine:
//
//	InitiateAuth(USER_AUTH) --precheckAuthLocked--> offered := SignInPolicy ∩ user's factors
//	  offered == ∅                  -> InvalidUserPoolConfigurationException
//	  PREFERRED_CHALLENGE given
//	    not in offered               -> InvalidParameterException
//	    in offered                   -> [first-factor round] (skips SELECT_CHALLENGE)
//	  PREFERRED_CHALLENGE absent      -> ChallengeName=SELECT_CHALLENGE, AvailableChallenges=offered
//
//	RespondToAuthChallenge(SELECT_CHALLENGE, ANSWER=x)
//	  x not in the session's offered set -> InvalidParameterException
//	  x in offered                        -> [first-factor round for x]
//
//	[first-factor round] RespondToAuthChallenge(ChallengeName=x, ...)
//	  PASSWORD:   ChallengeResponses[PASSWORD] bcrypt-verified against User.PasswordHash,
//	              the same check USER_PASSWORD_AUTH performs (auth.go authenticate).
//	  EMAIL_OTP:  ChallengeResponses[EMAIL_OTP_CODE] compared to a generated one-time code,
//	              reusing mfa.go's verifyMFAChallengeCode (the same mechanism SMS_MFA/
//	              EMAIL_OTP MFA challenges already use -- no real delivery gateway either way).
//	  SMS_OTP:    ChallengeResponses[SMS_OTP_CODE], same mechanism as EMAIL_OTP above.
//	  wrong answer -> NotAuthorizedException (PASSWORD) or CodeMismatchException (OTP)
//	  right answer -> postCredentialCheckLocked (the SAME FORCE_CHANGE_PASSWORD / pool-MFA
//	                  continuation every other credential check in this backend runs --
//	                  the caller may get further MFA/NEW_PASSWORD_REQUIRED challenges here,
//	                  exactly as USER_PASSWORD_AUTH/USER_SRP_AUTH do today)
//
// Disclosed, not modeled (PARITY.md):
//   - WEB_AUTHN is never offered: it requires a WebAuthn/passkey relying-party ceremony this
//     emulator does not implement, so even a pool with WEB_AUTHN in AllowedFirstAuthFactors
//     never offers it (userCanUseFirstFactor's default case).
//   - PASSWORD_SRP (the zero-knowledge-proof alternative response to a PASSWORD first
//     factor) is not wired into the USER_AUTH round -- a caller must use plaintext PASSWORD
//     here. USER_SRP_AUTH itself is fully modeled (srp.go) and unaffected.
//   - USER_AUTH does not attempt UserMigration_Authentication: that trigger's contract
//     expects a password, and USER_AUTH's InitiateAuth call carries none (only USERNAME/
//     PREFERRED_CHALLENGE) -- an unknown username returns unknownUserAuthError directly.
const authFlowUserAuth = "USER_AUTH"

// challengeSelectChallenge is ChallengeNameType SELECT_CHALLENGE (types/enums.go:270).
const challengeSelectChallenge = "SELECT_CHALLENGE"

// offeredFirstFactors intersects the pool's SignInPolicy.AllowedFirstAuthFactors with what
// this specific user can actually complete, preserving the pool's configured order (the
// real API returns AvailableChallenges in that order, not alphabetically). A nil
// SignInPolicy (pool never configured choice-based auth) offers nothing.
func offeredFirstFactors(pool *UserPool, user *User) []string {
	if pool.SignInPolicy == nil {
		return nil
	}

	offered := make([]string, 0, len(pool.SignInPolicy.AllowedFirstAuthFactors))

	for _, factor := range pool.SignInPolicy.AllowedFirstAuthFactors {
		if userCanUseFirstFactor(user, factor) {
			offered = append(offered, factor)
		}
	}

	return offered
}

// userCanUseFirstFactor reports whether user has the credential/verified contact info a
// USER_AUTH first factor requires: PASSWORD needs a stored password hash, EMAIL_OTP needs a
// verified email attribute, SMS_OTP needs a verified phone_number attribute. WEB_AUTHN is
// never satisfiable -- see the design comment above.
func userCanUseFirstFactor(user *User, factor string) bool {
	switch factor {
	case authFactorPassword:
		return user.PasswordHash != ""
	case challengeEmailOTP:
		return user.Attributes[attrEmail+"_verified"] == attrVerifiedTrue
	case authFactorSMSOTP:
		return user.Attributes[attrPhoneNumber+"_verified"] == attrVerifiedTrue
	default:
		return false
	}
}

// initiateUserAuthLocked is the shared USER_AUTH entry point for InitiateAuth and
// AdminInitiateAuth: run the common flow/lambda/status checks (precheckAuthLocked), then
// either negotiate (SELECT_CHALLENGE) or jump straight to the caller's PREFERRED_CHALLENGE.
// Caller must hold the write lock.
func (b *InMemoryBackend) initiateUserAuthLocked(
	pool *UserPool, clientID, username, preferredChallenge string, user *User,
) (*AuthResult, error) {
	if err := b.precheckAuthLocked(pool, clientID, authFlowUserAuth, user); err != nil {
		return nil, err
	}

	offered := offeredFirstFactors(pool, user)
	if len(offered) == 0 {
		return nil, fmt.Errorf(
			"%w: user pool %q has no USER_AUTH first factor available to user %q",
			ErrInvalidUserPoolConfig, pool.ID, username,
		)
	}

	if preferredChallenge != "" {
		if !slices.Contains(offered, preferredChallenge) {
			return nil, fmt.Errorf(
				"%w: PREFERRED_CHALLENGE %q is not offered to user %q",
				ErrInvalidParameter, preferredChallenge, username,
			)
		}

		return b.newFirstFactorChallengeSession(pool, clientID, username, preferredChallenge), nil
	}

	return b.newSelectChallengeSession(pool, clientID, username, offered), nil
}

// InitiateUserAuth begins USER_AUTH choice-based authentication for a non-admin caller.
func (b *InMemoryBackend) InitiateUserAuth(clientID, username, preferredChallenge string) (*AuthResult, error) {
	b.mu.Lock("InitiateUserAuth")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, ok := b.users.Get(userKey(client.UserPoolID, username))
	if !ok {
		return nil, unknownUserAuthError(client, username)
	}

	return b.initiateUserAuthLocked(pool, clientID, username, preferredChallenge, user)
}

// AdminInitiateUserAuth is AdminInitiateAuth's USER_AUTH entry point: same negotiation as
// InitiateUserAuth, but resolves the pool directly by ID like every other Admin* auth op.
func (b *InMemoryBackend) AdminInitiateUserAuth(
	userPoolID, clientID, username, preferredChallenge string,
) (*AuthResult, error) {
	b.mu.Lock("AdminInitiateUserAuth")
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

	return b.initiateUserAuthLocked(pool, clientID, username, preferredChallenge, user)
}

// newSelectChallengeSession stores a pending SELECT_CHALLENGE round and returns the
// negotiation challenge (ChallengeName=SELECT_CHALLENGE, AvailableChallenges=offered) to
// send to the client.
func (b *InMemoryBackend) newSelectChallengeSession(
	pool *UserPool, clientID, username string, offered []string,
) *AuthResult {
	sessionToken := randomAlphanumeric(mfaSessionLen)

	b.mfaSessions[sessionToken] = &mfaSessionEntry{
		PoolID:              pool.ID,
		ClientID:            clientID,
		Username:            username,
		ChallengeType:       challengeSelectChallenge,
		AvailableChallenges: slices.Clone(offered),
		ExpiresAt:           time.Now().Add(mfaSessionTTL),
	}

	return &AuthResult{
		MFASession:          sessionToken,
		ChallengeName:       challengeSelectChallenge,
		AvailableChallenges: slices.Clone(offered),
	}
}

// newFirstFactorChallengeSession starts a USER_AUTH first-factor round (PASSWORD,
// EMAIL_OTP, or SMS_OTP). OTP factors reuse the exact one-time-code mechanism
// newMFASession uses for its MFA namesakes (mfaSessionEntry.Code, verified by
// verifyMFAChallengeCode in mfa.go) -- there is still no real SMS/email gateway to deliver
// the code out of band. FirstFactor=true is what tells RespondToFirstFactorChallenge to
// continue through postCredentialCheckLocked (further MFA / FORCE_CHANGE_PASSWORD) instead
// of issuing tokens immediately the way a second-factor MFA round does.
func (b *InMemoryBackend) newFirstFactorChallengeSession(
	pool *UserPool, clientID, username, factor string,
) *AuthResult {
	sessionToken := randomAlphanumeric(mfaSessionLen)

	entry := &mfaSessionEntry{
		PoolID:        pool.ID,
		ClientID:      clientID,
		Username:      username,
		ChallengeType: factor,
		FirstFactor:   true,
		ExpiresAt:     time.Now().Add(mfaSessionTTL),
	}

	if factor == challengeEmailOTP || factor == authFactorSMSOTP {
		entry.Code = randomNumeric(totpCodeLen)
	}

	b.mfaSessions[sessionToken] = entry

	return &AuthResult{MFASession: sessionToken, ChallengeName: factor}
}

// RespondToSelectChallenge validates a SELECT_CHALLENGE response's ANSWER against the
// AvailableChallenges offered by the matching InitiateAuth(USER_AUTH) call and starts that
// challenge's own round.
func (b *InMemoryBackend) RespondToSelectChallenge(clientID, session, answer string) (*AuthResult, error) {
	b.mu.Lock("RespondToSelectChallenge")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok || entry.ChallengeType != challengeSelectChallenge {
		return nil, fmt.Errorf("%w: session is not a SELECT_CHALLENGE challenge", ErrNotAuthorized)
	}

	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		delete(b.mfaSessions, session)

		return nil, fmt.Errorf("%w: SELECT_CHALLENGE session not found or expired", ErrNotAuthorized)
	}

	if entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: SELECT_CHALLENGE session was issued for a different client", ErrNotAuthorized)
	}

	if !slices.Contains(entry.AvailableChallenges, answer) {
		return nil, fmt.Errorf(
			"%w: ANSWER %q is not one of the offered challenges", ErrInvalidParameter, answer,
		)
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

	return b.newFirstFactorChallengeSession(pool, clientID, user.Username, answer), nil
}

// RespondToFirstFactorChallenge verifies a USER_AUTH first-factor round (PASSWORD,
// EMAIL_OTP, or SMS_OTP -- the three factors this backend can actually offer, see the
// design comment above) and, on success, runs the SAME FORCE_CHANGE_PASSWORD/pool-MFA
// continuation every other credential check in this backend runs (postCredentialCheckLocked),
// rather than issuing tokens directly the way RespondToMFAChallenge does for a second-factor
// MFA round.
func (b *InMemoryBackend) RespondToFirstFactorChallenge(
	clientID, session string, challengeResponses map[string]string,
) (*AuthResult, error) {
	b.mu.Lock("RespondToFirstFactorChallenge")
	defer b.mu.Unlock()

	entry, ok := b.mfaSessions[session]
	if !ok || !entry.FirstFactor {
		return nil, fmt.Errorf("%w: session not found or expired", ErrNotAuthorized)
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

	if err := verifyFirstFactorResponse(entry, user, challengeResponses); err != nil {
		return nil, err
	}

	delete(b.mfaSessions, session)

	return b.postCredentialCheckLocked(pool, clientID, user)
}

// verifyFirstFactorResponse checks challengeResponses against entry.ChallengeType: PASSWORD
// is bcrypt-verified against the stored hash, exactly like USER_PASSWORD_AUTH (auth.go's
// authenticate); EMAIL_OTP/SMS_OTP reuse verifyMFAChallengeCode's one-time-code comparison
// (mfa.go) since the underlying mechanism -- a code generated at challenge time with no
// client-held secret to re-derive -- is identical to the MFA versions of these challenges.
func verifyFirstFactorResponse(entry *mfaSessionEntry, user *User, challengeResponses map[string]string) error {
	switch entry.ChallengeType {
	case authFactorPassword:
		if err := bcrypt.CompareHashAndPassword(
			[]byte(user.PasswordHash), []byte(challengeResponses["PASSWORD"]),
		); err != nil {
			return fmt.Errorf("%w: incorrect username or password", ErrNotAuthorized)
		}

		return nil

	case challengeEmailOTP, authFactorSMSOTP:
		return verifyMFAChallengeCode(entry, user, challengeResponses[firstFactorCodeKey(entry.ChallengeType)])

	default:
		return fmt.Errorf(
			"%w: unexpected challenge type %q for USER_AUTH first factor",
			ErrInvalidParameter, entry.ChallengeType,
		)
	}
}

// firstFactorCodeKey returns the ChallengeResponses key holding the user-supplied code for
// a USER_AUTH OTP first factor (api_op_RespondToAuthChallenge.go: "EMAIL_OTP: ... as
// EMAIL_OTP_CODE", "SMS_OTP: ... as SMS_OTP_CODE" -- distinct from SMS_MFA's SMS_MFA_CODE).
func firstFactorCodeKey(challengeType string) string {
	switch challengeType {
	case challengeEmailOTP:
		return "EMAIL_OTP_CODE"
	case authFactorSMSOTP:
		return "SMS_OTP_CODE"
	default:
		return ""
	}
}
