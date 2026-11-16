package azuregraph

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	passwordIterations = 120000
	maxPasswordIters   = 1000000
	maxPasswordBytes   = 256
	userKeyPrefix      = "user:"
	changeKeyPrefix    = "userchange:"
	hashScheme         = "pbkdf2-sha256"
	lockoutThreshold   = 10
	lockoutDuration    = time.Minute
	maxTrackedSignIns  = 10000

	aadsNoSuchUser      = 50034
	aadsBadCredentials  = 50126
	aadsAccountDisabled = 50057
	aadsPasswordExpired = 50055
	aadsAccountLocked   = 50053
)

type signInState struct {
	lockedUntil time.Time
	fails       int
}

func userSecretKey(id string) string { return userKeyPrefix + lc(id) }
func userChangeKey(id string) string { return changeKeyPrefix + lc(id) }
func derivePassword(pw string, salt []byte, iters int) (string, bool) {
	key, err := pbkdf2.Key(sha256.New, pw, salt, iters, sha256.Size)
	if err != nil {
		return "", false
	}

	return hex.EncodeToString(key), true
}

// signInHooks let tests observe the unlocked window of a sign-in.
type signInHooks struct {
	burned   func()
	verified func()
}

// burnPasswordWork spends the PBKDF2 time of a real check so unknown and unusable accounts answer as slowly as
// known ones.
func (b *Backend) burnPasswordWork(pw string) {
	if b.hooks.burned != nil {
		b.hooks.burned()
	}

	if len(pw) > maxPasswordBytes {
		return
	}

	_, _ = derivePassword(pw, make([]byte, saltLen), passwordIterations)
}

func hashPassword(pw string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("%w: %w", errNoRand, err)
	}

	derived, ok := derivePassword(pw, salt, passwordIterations)
	if !ok {
		return "", errNoRand
	}

	return strings.Join([]string{
		hashScheme, strconv.Itoa(passwordIterations), hex.EncodeToString(salt), derived,
	}, "$"), nil
}

func passwordMatches(stored, pw string) bool {
	parts := strings.Split(stored, "$")
	if len(parts) != 4 || parts[0] != hashScheme || len(pw) > maxPasswordBytes {
		return false
	}

	iters, err := strconv.Atoi(parts[1])
	if err != nil || iters < 1 || iters > maxPasswordIters {
		return false
	}

	salt, err := hex.DecodeString(parts[2])
	if err != nil || len(salt) == 0 {
		return false
	}

	derived, ok := derivePassword(pw, salt, iters)

	return ok && subtle.ConstantTimeCompare([]byte(derived), []byte(parts[3])) == 1
}

// storePassword keeps a salted hash of the user's password and whether the next sign-in must change it.
// Callers hold b.mu.
func (b *Backend) storePassword(id string, p *PasswordProfile) error {
	hashed, err := hashPassword(p.Password)
	if err != nil {
		return err
	}

	b.secrets[userSecretKey(id)] = hashed

	if p.ForceChangePasswordNextSignIn || p.ForceChangePasswordNextMFA {
		b.secrets[userChangeKey(id)] = "1"
	} else {
		delete(b.secrets, userChangeKey(id))
	}

	delete(b.signIns, lc(id))

	return nil
}

func passwordError(code int, format string, args ...any) *ClientAuthError {
	return &ClientAuthError{
		Status:      http.StatusBadRequest,
		Code:        code,
		Name:        "invalid_grant",
		Description: fmt.Sprintf("AADSTS%d: ", code) + fmt.Sprintf(format, args...),
	}
}

type signInAttempt struct {
	stored string
	id     string
}

// beginSignIn resolves the user and refuses locked, disabled and password-less accounts before any hashing.
func (b *Backend) beginSignIn(username string) (signInAttempt, *ClientAuthError) {
	b.mu.RLock("AuthenticateUser")
	defer b.mu.RUnlock()

	u, err := b.findUser(Ref{ID: username})
	if err != nil {
		return signInAttempt{}, passwordError(
			aadsNoSuchUser,
			"The user account '%s' does not exist in the '%s' directory.",
			username,
			b.cfg.TenantID,
		)
	}

	if st := b.signIns[lc(u.ID)]; st != nil && b.now().Before(st.lockedUntil) {
		return signInAttempt{}, passwordError(
			aadsAccountLocked,
			"The account is locked because the user tried to sign in too many times with an incorrect user ID or password.",
		)
	}

	if !u.AccountEnabled {
		return signInAttempt{}, passwordError(aadsAccountDisabled, "The user account is disabled.")
	}

	stored := b.secrets[userSecretKey(u.ID)]
	if stored == "" {
		return signInAttempt{}, passwordError(
			aadsBadCredentials,
			"Error validating credentials: user '%s' has no stored password; set one with PATCH /users/%s passwordProfile.",
			u.UserPrincipalName,
			u.ID,
		)
	}

	return signInAttempt{id: u.ID, stored: stored}, nil
}

// AuthenticateUser verifies a resource-owner password sign-in and returns the user's object ID and user
// principal name. Repeated failures lock the account for a minute on the backend clock (Learn:
// howto-password-smart-lockout defaults); as in Entra, anyone who knows a user's name can trigger that lockout.
func (b *Backend) AuthenticateUser(username, password string) (string, string, error) {
	att, cerr := b.beginSignIn(username)
	if cerr != nil {
		if cerr.Code != aadsAccountLocked {
			b.burnPasswordWork(password)
		}

		return "", "", cerr
	}

	ok := passwordMatches(att.stored, password)

	if b.hooks.verified != nil {
		b.hooks.verified()
	}

	b.mu.Lock("AuthenticateUser")
	defer b.mu.Unlock()

	u, found := b.users.get(lc(att.id))
	if !found || b.secrets[userSecretKey(att.id)] != att.stored {
		return "", "", passwordError(
			aadsBadCredentials,
			"Error validating credentials due to invalid username or password.",
		)
	}

	if st := b.signIns[lc(att.id)]; st != nil && b.now().Before(st.lockedUntil) {
		return "", "", passwordError(
			aadsAccountLocked,
			"The account is locked because the user tried to sign in too many times with an incorrect user ID or password.",
		)
	}

	if !u.AccountEnabled {
		return "", "", passwordError(aadsAccountDisabled, "The user account is disabled.")
	}

	if !ok {
		b.recordFailure(att.id)

		return "", "", passwordError(
			aadsBadCredentials,
			"Error validating credentials due to invalid username or password.",
		)
	}

	delete(b.signIns, lc(att.id))

	if b.secrets[userChangeKey(att.id)] != "" {
		return "", "", passwordError(
			aadsPasswordExpired,
			"The password is expired; the user must change it at the next sign-in.",
		)
	}

	return att.id, u.UserPrincipalName, nil
}

func (b *Backend) recordFailure(id string) {
	now := b.now()

	if len(b.signIns) >= maxTrackedSignIns {
		for k, st := range b.signIns {
			if !now.Before(st.lockedUntil) {
				delete(b.signIns, k)
			}
		}
	}

	st := b.signIns[lc(id)]
	if st == nil {
		if len(b.signIns) >= maxTrackedSignIns {
			return
		}

		st = &signInState{}
		b.signIns[lc(id)] = st
	}

	if !st.lockedUntil.IsZero() && !now.Before(st.lockedUntil) {
		st.fails, st.lockedUntil = 0, time.Time{}
	}

	st.fails++
	if st.fails >= lockoutThreshold {
		st.lockedUntil = now.Add(lockoutDuration)
	}
}
