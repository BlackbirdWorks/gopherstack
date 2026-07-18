package sts

import (
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// defaultSTSRegion is the AWS region for the STS backend (STS is global, defaults to us-east-1).
const defaultSTSRegion = "us-east-1"

// RoleLookup is implemented by services (e.g. IAM) that can provide role metadata
// to STS for ExternalId validation and MaxSessionDuration enforcement.
type RoleLookup interface {
	GetRoleByArn(arn string) (*RoleMeta, error)
}

// OIDCLookup is implemented by services (e.g. IAM) that can validate OIDC providers
// for AssumeRoleWithWebIdentity.
type OIDCLookup interface {
	// OIDCProviderExists returns true if an OIDC provider with the given issuer URL exists.
	OIDCProviderExists(issuerURL string) bool
}

// RoleMeta carries the role properties that STS needs during AssumeRole.
type RoleMeta struct {
	// TrustPolicy is the raw JSON of the role's trust (assume-role) policy document.
	TrustPolicy string
	// MaxSessionDuration is the maximum session duration (in seconds) for this role.
	// A value of 0 means the system default maximum (MaxDurationSeconds) applies.
	MaxSessionDuration int32
}

// InMemoryBackend is a stateful in-memory STS backend.
type InMemoryBackend struct {
	roleLookup RoleLookup
	oidcLookup OIDCLookup
	sessions   *store.Table[SessionInfo]
	registry   *store.Registry
	mu         *lockmetrics.RWMutex
	accountID  string

	// Operation call counters — incremented atomically.
	cntAssumeRoleWithWebIdentity atomic.Int64
	cntAssumeRole                atomic.Int64
	cntAssumeRoleWithSAML        atomic.Int64
	cntAssumeRoot                atomic.Int64
	cntGetCallerIdentity         atomic.Int64
	cntGetDelegatedAccessToken   atomic.Int64
	cntGetFederationToken        atomic.Int64
	cntGetSessionToken           atomic.Int64
	cntGetWebIdentityToken       atomic.Int64
	cntGetAccessKeyInfo          atomic.Int64
	cntDecodeAuthorizationMsg    atomic.Int64

	// totalSessionsCreated is the lifetime count of sessions issued.
	totalSessionsCreated atomic.Int64

	// authMsgSigningKey is a random key used to HMAC-sign encoded authorization messages.
	// Only messages signed with this key are accepted by DecodeAuthorizationMessage,
	// matching AWS behaviour where only STS-issued encoded messages can be decoded.
	authMsgSigningKey [authMsgHMACSize]byte
}

// NewInMemoryBackend creates a new InMemoryBackend with the default account ID.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID.
func NewInMemoryBackendWithConfig(accountID string) *InMemoryBackend {
	var key [authMsgHMACSize]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic("sts: failed to generate authorization message signing key: " + err.Error())
	}

	b := &InMemoryBackend{
		accountID:         accountID,
		registry:          store.NewRegistry(),
		authMsgSigningKey: key,
		mu:                lockmetrics.New("sts"),
	}
	b.sessions = store.Register(b.registry, "sessions", store.New(sessionTableKey))

	return b
}

// sessionTableKey is the [store.Table] key function for the sessions table:
// sessions are looked up by the AccessKeyID of the credentials that were issued.
func sessionTableKey(s *SessionInfo) string {
	return s.AccessKeyID
}

// SetRoleLookup wires an optional role-lookup implementation (e.g. the IAM backend)
// so that AssumeRole can validate ExternalId and enforce MaxSessionDuration.
func (b *InMemoryBackend) SetRoleLookup(rl RoleLookup) {
	b.mu.Lock("SetRoleLookup")
	defer b.mu.Unlock()

	b.roleLookup = rl
}

// SetOIDCLookup wires an optional OIDC-lookup implementation (e.g. the IAM backend)
// so that AssumeRoleWithWebIdentity can validate that the OIDC provider exists.
func (b *InMemoryBackend) SetOIDCLookup(ol OIDCLookup) {
	b.mu.Lock("SetOIDCLookup")
	defer b.mu.Unlock()

	b.oidcLookup = ol
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// Region returns the AWS region for this STS backend (STS is global, defaults to us-east-1).
func (b *InMemoryBackend) Region() string {
	return defaultSTSRegion
}

// getEffectiveMaxDuration returns the effective maximum session duration for a role.
// When no RoleLookup is configured, or the role is not found, MaxDurationSeconds is returned.
// This is used by AssumeRoleWithWebIdentity and AssumeRoleWithSAML which do not validate ExternalId.
func (b *InMemoryBackend) getEffectiveMaxDuration(roleArn string) int32 {
	effectiveMax := int32(MaxDurationSeconds)

	b.mu.RLock("GetEffectiveMaxDuration")
	rl := b.roleLookup
	b.mu.RUnlock()

	if rl == nil {
		return effectiveMax
	}

	meta, _ := rl.GetRoleByArn(roleArn)
	if meta != nil && meta.MaxSessionDuration > 0 {
		effectiveMax = meta.MaxSessionDuration
	}

	return effectiveMax
}

// lookupRoleMeta returns the RoleMeta for the given role ARN via the configured
// RoleLookup, or nil when no lookup is wired or the role is not found. It never
// returns an error: a missing role or lookup means the emulator falls back to
// permissive behaviour for trust evaluation.
func (b *InMemoryBackend) lookupRoleMeta(roleArn string) *RoleMeta {
	b.mu.RLock("LookupRoleMeta")
	rl := b.roleLookup
	b.mu.RUnlock()

	if rl == nil {
		return nil
	}

	meta, _ := rl.GetRoleByArn(roleArn)

	return meta
}

// isSessionExpired reports whether s has a non-zero expiry time that has already passed.
func isSessionExpired(s *SessionInfo) bool {
	return !s.Expiration.IsZero() && !time.Now().UTC().Before(s.Expiration)
}

// sessionEvictThreshold is the session count above which inserting a new session
// triggers an opportunistic sweep of expired sessions. This bounds the sessions
// map even when the background janitor is disabled or runs at a long interval,
// while keeping the common (small) case allocation-free. The threshold is high
// enough that the O(n) sweep amortizes cheaply.
const sessionEvictThreshold = 256

// evictExpiredSessionsLocked removes all expired sessions from the table.
// The caller must hold b.mu.
func (b *InMemoryBackend) evictExpiredSessionsLocked() {
	var expired []string

	b.sessions.Range(func(session *SessionInfo) bool {
		if isSessionExpired(session) {
			expired = append(expired, session.AccessKeyID)
		}

		return true
	})

	for _, id := range expired {
		b.sessions.Delete(id)
	}
}

// maybeEvictExpiredSessions acquires its own lock and sweeps expired sessions when
// the session count is at or above sessionEvictThreshold. It runs in a separate
// critical section from storeSession so that session creation (O(1) map insert)
// is never blocked by an O(n) sweep.
func (b *InMemoryBackend) maybeEvictExpiredSessions() {
	b.mu.Lock("EvictExpiredSessions")
	defer b.mu.Unlock()

	if b.sessions.Len() >= sessionEvictThreshold {
		b.evictExpiredSessionsLocked()
	}
}

// storeSession registers a new session under its access key ID and increments
// the lifetime counter. The store is a fast O(1) operation; opportunistic
// eviction of expired sessions is deferred to a separate lock acquisition so
// that the 11 credential-issuing operations do not serialize on O(n) sweeps.
func (b *InMemoryBackend) storeSession(session *SessionInfo) {
	func() {
		b.mu.Lock("StoreSession")
		defer b.mu.Unlock()

		b.sessions.Put(session)
		b.totalSessionsCreated.Add(1)
	}()

	b.maybeEvictExpiredSessions()
}

// LookupSession returns the active SessionInfo for the given access key and optional
// session token, or nil if no matching non-expired session exists or the token mismatches.
func (b *InMemoryBackend) LookupSession(accessKeyID, sessionToken string) *SessionInfo {
	if accessKeyID == "" {
		return nil
	}

	var session *SessionInfo
	var ok bool

	func() {
		b.mu.Lock("LookupSession")
		defer b.mu.Unlock()

		session, ok = b.sessions.Get(accessKeyID)
		if ok && isSessionExpired(session) {
			b.sessions.Delete(accessKeyID)
			ok = false
		}
	}()

	if !ok {
		return nil
	}
	if sessionToken != "" && session.SessionToken != "" && sessionToken != session.SessionToken {
		return nil
	}

	return session
}

// ValidateSessionCredential looks up a session by (accessKeyID, sessionToken).
// Returns ErrSessionNotFound when the key is unknown, ErrAccessDenied on token mismatch,
// and ErrSessionExpired when the session has passed its expiry.
func (b *InMemoryBackend) ValidateSessionCredential(
	accessKeyID, sessionToken string,
) (*SessionInfo, error) {
	var session *SessionInfo
	var ok bool

	func() {
		b.mu.Lock("ValidateSessionCredential")
		defer b.mu.Unlock()

		session, ok = b.sessions.Get(accessKeyID)

		if ok && isSessionExpired(session) {
			b.sessions.Delete(accessKeyID)
			ok = false
		}
	}()

	if !ok {
		return nil, ErrSessionNotFound
	}

	if session.SessionToken != "" && sessionToken != session.SessionToken {
		return nil, fmt.Errorf("%w: session token mismatch", ErrAccessDenied)
	}

	return session, nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
// Operation counters and totalSessionsCreated are also reset to zero.
func (b *InMemoryBackend) Reset() {
	func() {
		b.mu.Lock("Reset")
		defer b.mu.Unlock()

		b.registry.ResetAll()
	}()

	b.cntAssumeRole.Store(0)
	b.cntAssumeRoleWithSAML.Store(0)
	b.cntAssumeRoleWithWebIdentity.Store(0)
	b.cntAssumeRoot.Store(0)
	b.cntGetCallerIdentity.Store(0)
	b.cntGetDelegatedAccessToken.Store(0)
	b.cntGetFederationToken.Store(0)
	b.cntGetSessionToken.Store(0)
	b.cntGetWebIdentityToken.Store(0)
	b.cntGetAccessKeyInfo.Store(0)
	b.cntDecodeAuthorizationMsg.Store(0)
	b.totalSessionsCreated.Store(0)
}

// SessionCounts returns active and expired session counts at the time of invocation.
func (b *InMemoryBackend) SessionCounts() (int, int) {
	b.mu.RLock("SessionCounts")
	defer b.mu.RUnlock()

	now := time.Now().UTC()
	active := 0
	expired := 0

	b.sessions.Range(func(session *SessionInfo) bool {
		// A zero expiration is treated as non-expiring in-memory session state.
		if !session.Expiration.IsZero() && !now.Before(session.Expiration) {
			expired++
		} else {
			active++
		}

		return true
	})

	return active, expired
}
