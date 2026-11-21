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

// VerifyUIPassword reports whether password is the current console password of the active user with that name.
func (b *InMemoryBackend) VerifyUIPassword(userName, password string) (string, bool) {
	b.mu.RLock("VerifyUIPassword")
	defer b.mu.RUnlock()

	userID, matched := "", false

	for _, u := range b.users {
		if u.LifecycleState == stateActive && strs.Equal(u.Name, userName) && uiPasswordMatches(u, password) {
			userID, matched = u.ID, true
		}
	}

	return userID, matched
}

// VerifySMTPCredential reports whether username and password are a current SMTP credential, and returns its user.
func (b *InMemoryBackend) VerifySMTPCredential(username, password string) (string, bool) {
	b.mu.RLock("VerifySMTPCredential")
	defer b.mu.RUnlock()

	userID, matched := "", false

	for _, u := range b.users {
		if u.LifecycleState != stateActive {
			continue
		}

		for _, c := range u.SMTP {
			if c.Username == username && c.Secret != "" &&
				subtle.ConstantTimeCompare([]byte(c.Secret), []byte(password)) == 1 {
				userID, matched = u.ID, true
			}
		}
	}

	return userID, matched
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

// replaySecret returns the secret a replayed create answers with. Auth tokens keep only a digest, so their
// plaintext is held in memory for the retry window and never persisted; a replay after a restart is invalidated.
func (b *InMemoryBackend) replaySecret(kind credKind, c *Credential) (string, bool) {
	if kind.name != kindAuthToken {
		return c.Secret, c.Secret != ""
	}

	r, ok := b.authReplay[c.ID]
	if !ok || b.nowUTC().Sub(r.at) >= ociretry.TTL {
		delete(b.authReplay, c.ID)

		return "", false
	}

	return r.secret, true
}
