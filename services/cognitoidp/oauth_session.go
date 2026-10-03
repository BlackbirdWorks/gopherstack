package cognitoidp

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"
)

const (
	sessionCookie  = "cognito"
	sessionIDBytes = 32
	// hostedSessionTTL: the authorize docs give no figure for the session cookie; one hour is assumed.
	hostedSessionTTL  = time.Hour
	maxHostedSessions = 10000
)

// hostedSession is a runtime-only managed-login session, bound to a pool and a user record.
type hostedSession struct {
	ExpiresAt      time.Time
	PoolID         string
	Username       string
	Sub            string
	PasswordDigest [sha256.Size]byte
	Seq            int64
}

// sessionKey is the store key: only a digest of the cookie value is kept server-side.
func sessionKey(id string) string {
	sum := sha256.Sum256([]byte(id))

	return hex.EncodeToString(sum[:])
}

func (b *InMemoryBackend) createHostedSession(poolID, username string) (string, error) {
	raw := make([]byte, sessionIDBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating session id: %w", err)
	}

	id := base64.RawURLEncoding.EncodeToString(raw)

	b.mu.Lock("CreateHostedSession")
	defer b.mu.Unlock()

	user, ok := b.users.Get(userKey(poolID, username))
	if !ok {
		return "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	now := time.Now()
	if len(b.hostedSessions) >= maxHostedSessions {
		for k, s := range b.hostedSessions {
			if !s.ExpiresAt.After(now) {
				delete(b.hostedSessions, k)
			}
		}
	}

	if len(b.hostedSessions) >= maxHostedSessions {
		b.evictEarliestHostedSessionLocked()
	}

	b.tokenSeq++
	b.hostedSessions[sessionKey(id)] = &hostedSession{
		ExpiresAt: now.Add(hostedSessionTTL), PoolID: poolID, Username: user.Username, Sub: user.Sub,
		PasswordDigest: sha256.Sum256([]byte(user.PasswordHash)), Seq: b.tokenSeq,
	}

	return id, nil
}

func (b *InMemoryBackend) evictEarliestHostedSessionLocked() {
	var (
		oldestKey string
		oldest    time.Time
	)

	for k, s := range b.hostedSessions {
		if oldestKey == "" || s.ExpiresAt.Before(oldest) {
			oldestKey, oldest = k, s.ExpiresAt
		}
	}

	delete(b.hostedSessions, oldestKey)
}

// hostedSessionUser returns the signed-in user when id is a live session for clientID's pool.
func (b *InMemoryBackend) hostedSessionUser(id, clientID string) (string, bool) {
	b.mu.Lock("HostedSessionUser")
	defer b.mu.Unlock()

	key := sessionKey(id)

	sess, ok := b.hostedSessions[key]
	if !ok {
		return "", false
	}

	client, ok := b.clients.Get(clientID)
	if !ok || client.UserPoolID != sess.PoolID {
		return "", false
	}

	if !b.hostedSessionLiveLocked(sess) {
		delete(b.hostedSessions, key)

		return "", false
	}

	return sess.Username, true
}

// hostedSessionLiveLocked re-checks the user record so disable, delete, sign-out and password change end the session.
func (b *InMemoryBackend) hostedSessionLiveLocked(sess *hostedSession) bool {
	if !sess.ExpiresAt.After(time.Now()) {
		return false
	}

	if _, ok := b.pools.Get(sess.PoolID); !ok {
		return false
	}

	user, ok := b.users.Get(userKey(sess.PoolID, sess.Username))
	if !ok || !user.Enabled || user.Status != UserStatusConfirmed || user.Sub != sess.Sub {
		return false
	}

	digest := sha256.Sum256([]byte(user.PasswordHash))
	if subtle.ConstantTimeCompare(digest[:], sess.PasswordDigest[:]) != 1 {
		return false
	}

	revoked, signedOut := b.tokenRevokedBeforeSeq[sess.PoolID+":"+sess.Username]

	return !signedOut || sess.Seq > revoked
}

func (b *InMemoryBackend) deleteHostedSession(id string) {
	b.mu.Lock("DeleteHostedSession")
	defer b.mu.Unlock()

	delete(b.hostedSessions, sessionKey(id))
}

// dropHostedSessionsLocked ends every session of the user; caller holds the write lock.
func (b *InMemoryBackend) dropHostedSessionsLocked(poolID, username string) {
	for k, s := range b.hostedSessions {
		if s.PoolID == poolID && s.Username == username {
			delete(b.hostedSessions, k)
		}
	}
}

func setSessionCookie(w http.ResponseWriter, r *http.Request, id string, ttl time.Duration) {
	c := &http.Cookie{
		Name: sessionCookie, Value: id, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode,
		Secure: true, MaxAge: int(ttl.Seconds()),
	}
	c.Secure = cookieSecure(r)
	if ttl <= 0 {
		c.MaxAge = -1
	}

	http.SetCookie(w, c)
}
