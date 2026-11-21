package ociidentity

import (
	"crypto/subtle"
	"encoding/hex"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

func uiPasswordMatches(u *User, password string) bool {
	salt, serr := hex.DecodeString(u.UIPasswordSalt)
	want, derr := hex.DecodeString(u.UIPasswordDigest)
	got := deriveAuthToken(password, salt)

	return serr == nil && derr == nil && len(want) > 0 && subtle.ConstantTimeCompare(got, want) == 1
}

type digestCandidate struct{ user, salt, digest string }

// matchDigest derives against every candidate, and once against a dummy salt when there are none, so timing
// does not reveal whether the user exists. Callers must not hold the backend lock.
func (b *InMemoryBackend) matchDigest(cands []digestCandidate, secret string) (string, bool) {
	if len(cands) == 0 {
		b.derive(secret, make([]byte, authTokenSaltBytes))

		return "", false
	}

	userID, matched := "", false

	for _, c := range cands {
		salt, serr := hex.DecodeString(c.salt)
		want, derr := hex.DecodeString(c.digest)
		got := b.derive(secret, salt)

		if serr == nil && derr == nil && len(want) > 0 && subtle.ConstantTimeCompare(got, want) == 1 {
			userID, matched = c.user, true
		}
	}

	return userID, matched
}

// VerifyUIPassword reports whether password is the current console password of the active user with that name.
func (b *InMemoryBackend) VerifyUIPassword(userName, password string) (string, bool) {
	var cands []digestCandidate

	b.mu.RLock("VerifyUIPassword")

	for _, u := range b.users {
		if u.LifecycleState == stateActive && strs.Equal(u.Name, userName) {
			cands = append(cands, digestCandidate{u.ID, u.UIPasswordSalt, u.UIPasswordDigest})
		}
	}

	b.mu.RUnlock()

	return b.matchDigest(cands, password)
}

// VerifySMTPCredential reports whether username and password are a current SMTP credential, and returns its user.
func (b *InMemoryBackend) VerifySMTPCredential(username, password string) (string, bool) {
	var cands []digestCandidate

	b.mu.RLock("VerifySMTPCredential")

	for _, u := range b.users {
		if u.LifecycleState != stateActive {
			continue
		}

		for _, c := range u.SMTP {
			if c.Username == username {
				cands = append(cands, digestCandidate{u.ID, c.Salt, c.Digest})
			}
		}
	}

	b.mu.RUnlock()

	return b.matchDigest(cands, password)
}

// CustomerSecretKey returns the owner and secret of a customer secret key by its access key id.
func (b *InMemoryBackend) CustomerSecretKey(accessKey string) (string, string, bool) {
	b.mu.RLock("CustomerSecretKey")
	defer b.mu.RUnlock()

	for _, u := range b.users {
		for _, c := range u.SecretKeys {
			if c.ID == accessKey && c.Secret != "" {
				return u.ID, c.Secret, true
			}
		}
	}

	return "", "", false
}

type replaySecret struct {
	at     time.Time
	secret string
}

// replaySecret returns the secret a replayed create answers with. Only customer secret keys keep their secret;
// every other kind keeps a digest, so its plaintext is held in memory for the retry window and never persisted.
func (b *InMemoryBackend) replaySecret(kind credKind, c *Credential) (string, bool) {
	if kind.name == kindSecretKey {
		return c.Secret, c.Secret != ""
	}

	r, ok := b.authReplay[c.ID]
	if !ok || b.nowUTC().Sub(r.at) >= ociretry.TTL {
		delete(b.authReplay, c.ID)

		return "", false
	}

	return r.secret, true
}

// pruneReplay drops expired in-memory replay secrets; callers hold the write lock.
func (b *InMemoryBackend) pruneReplay(now time.Time) {
	for k, r := range b.authReplay {
		if now.Sub(r.at) >= ociretry.TTL {
			delete(b.authReplay, k)
		}
	}
}
