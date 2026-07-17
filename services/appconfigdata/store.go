package appconfigdata

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// InMemoryBackend implements StorageBackend for AppConfigData.
type InMemoryBackend struct {
	profiles    *store.Table[ConfigurationProfile]
	sessions    *store.Table[Session]
	graceTokens *store.Table[graceEntry] // keyed by rotated token → cached response
	registry    *store.Registry
	mu          *lockmetrics.RWMutex
	signingKey  []byte       // HMAC-SHA256 key for token integrity verification
	lastSweepAt atomic.Int64 // unix nanoseconds; accessed without the lock

	totalPolls    atomic.Int64
	totalFailures atomic.Int64
	totalChanges  atomic.Int64
}

// NewInMemoryBackend creates a new InMemoryBackend with a freshly generated signing key.
func NewInMemoryBackend() *InMemoryBackend {
	key := make([]byte, signingKeySize)
	if _, err := rand.Read(key); err != nil {
		// Fallback: derive from a fixed phrase — only happens if the OS entropy pool is
		// completely exhausted, which should never occur in practice.
		sum := sha256.Sum256([]byte("appconfigdata-fallback-key"))
		key = sum[:]
	}

	b := &InMemoryBackend{
		registry:   store.NewRegistry(),
		mu:         lockmetrics.New("appconfigdata"),
		signingKey: key,
	}
	registerAllTables(b)

	return b
}

func profileKey(app, env, profile string) string {
	return fmt.Sprintf("%s|%s|%s", app, env, profile)
}

// normalizedContentHash computes a SHA-256 hash of content.
// For JSON content types the input is normalised (parsed then re-serialised with sorted keys)
// so that semantically equivalent JSON payloads produce the same hash regardless of whitespace
// or key ordering.
func normalizedContentHash(content, contentType string) string {
	if isJSONContentType(contentType) {
		var v any
		if err := json.Unmarshal([]byte(content), &v); err == nil {
			if normalized, marshalErr := json.Marshal(v); marshalErr == nil {
				sum := sha256.Sum256(normalized)

				return hex.EncodeToString(sum[:])
			}
		}
	}

	sum := sha256.Sum256([]byte(content))

	return hex.EncodeToString(sum[:])
}

// isJSONContentType returns true when contentType signals a JSON payload.
func isJSONContentType(contentType string) bool {
	ct := strings.ToLower(contentType)

	return strings.Contains(ct, "application/json") || strings.HasSuffix(ct, "+json")
}

// tokenMAC returns an 8-byte HMAC-SHA256 tag over (rawToken + familyID) using the backend's
// signing key. Embedding the family ID in the MAC means a token cannot be transplanted across
// session families.
func (b *InMemoryBackend) tokenMAC(rawToken, familyID string) string {
	h := hmac.New(sha256.New, b.signingKey)
	h.Write([]byte(rawToken))
	h.Write([]byte("|"))
	h.Write([]byte(familyID))

	return hex.EncodeToString(h.Sum(nil)[:8])
}

// generateToken mints a new signed token for the given family.
// Format: <64-hex-random>.<16-hex-mac>.
func (b *InMemoryBackend) generateToken(familyID string) (string, error) {
	raw := make([]byte, tokenByteSize)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating token entropy: %w", err)
	}

	rawHex := hex.EncodeToString(raw)
	mac := b.tokenMAC(rawHex, familyID)

	return rawHex + "." + mac, nil
}

// verifyTokenMAC returns true when the token's embedded MAC is valid for the given family.
// An invalid MAC indicates tampering or cross-family reuse.
func (b *InMemoryBackend) verifyTokenMAC(token, familyID string) bool {
	parts := strings.SplitN(token, ".", tokenParts)
	if len(parts) != tokenParts {
		return false
	}

	expected := b.tokenMAC(parts[0], familyID)

	return hmac.Equal([]byte(parts[1]), []byte(expected))
}

// truncateToken returns a display-safe version of the token: first8…last4.
func truncateToken(token string) string {
	const prefixLen = 8
	const suffixLen = 4

	if len(token) <= prefixLen+suffixLen {
		return token
	}

	return token[:prefixLen] + "…" + token[len(token)-suffixLen:]
}

// ListProfiles returns all stored configuration profiles.
func (b *InMemoryBackend) ListProfiles() []ConfigurationProfile {
	b.mu.RLock("ListProfiles")
	defer b.mu.RUnlock()

	profiles := b.profiles.All()
	out := make([]ConfigurationProfile, 0, len(profiles))
	for _, p := range profiles {
		out = append(out, *p)
	}

	return out
}

// LookupSession returns the session for the given token, or nil if not found.
// This is a read-only lookup; it does not rotate the token.
func (b *InMemoryBackend) LookupSession(token string) *Session {
	b.mu.RLock("LookupSession")
	defer b.mu.RUnlock()

	sess, ok := b.sessions.Get(token)
	if !ok {
		return nil
	}

	snap := *sess

	return &snap
}

// ListSessions returns all active sessions.
func (b *InMemoryBackend) ListSessions() []Session {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	sessions := b.sessions.All()
	out := make([]Session, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, *s)
	}

	return out
}

// ListSessionsSafe returns all active sessions with tokens truncated for safe display.
// Use this for admin list endpoints; never return full tokens in list responses.
func (b *InMemoryBackend) ListSessionsSafe() []SafeSession {
	b.mu.RLock("ListSessionsSafe")
	defer b.mu.RUnlock()

	sessions := b.sessions.All()
	out := make([]SafeSession, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, SafeSession{
			CreatedAt:                      s.CreatedAt,
			LastAccessedAt:                 s.LastAccessedAt,
			LastPollAt:                     s.LastPollAt,
			ExpiresAt:                      s.ExpiresAt,
			TokenPrefix:                    truncateToken(s.Token),
			TokenFamilyID:                  s.TokenFamilyID,
			ApplicationIdentifier:          s.ApplicationIdentifier,
			EnvironmentIdentifier:          s.EnvironmentIdentifier,
			ConfigurationProfileIdentifier: s.ConfigurationProfileIdentifier,
			PollIntervalInSeconds:          s.PollIntervalInSeconds,
			PollCount:                      s.PollCount,
		})
	}

	return out
}

// EndSession terminates the session with the given token. Returns false if not found.
func (b *InMemoryBackend) EndSession(token string) bool {
	b.mu.Lock("EndSession")
	defer b.mu.Unlock()

	return b.sessions.Delete(token)
}

// DeleteProfile removes a configuration profile and its associated sessions.
func (b *InMemoryBackend) DeleteProfile(app, env, profile string) bool {
	b.mu.Lock("DeleteProfile")
	defer b.mu.Unlock()

	key := profileKey(app, env, profile)
	if !b.profiles.Delete(key) {
		return false
	}

	// Remove sessions linked to this profile.
	for _, s := range b.sessions.All() {
		if s.ApplicationIdentifier == app &&
			s.EnvironmentIdentifier == env &&
			s.ConfigurationProfileIdentifier == profile {
			b.sessions.Delete(s.Token)
		}
	}

	return true
}

// GetStats returns aggregate service statistics.
func (b *InMemoryBackend) GetStats() ServiceStats {
	b.mu.RLock("GetStats")
	sc := b.sessions.Len()
	pc := b.profiles.Len()
	b.mu.RUnlock()

	ns := b.lastSweepAt.Load()
	var lastSweep time.Time
	if ns != 0 {
		lastSweep = time.Unix(0, ns).UTC()
	}

	return ServiceStats{
		SessionCount:             sc,
		ProfileCount:             pc,
		LastSweepAt:              lastSweep,
		TotalPollCount:           b.totalPolls.Load(),
		TotalPollFailures:        b.totalFailures.Load(),
		ConfigurationChangeCount: b.totalChanges.Load(),
	}
}

// SweepExpiredSessions removes sessions that have been idle longer than ttl OR that have
// exceeded the absolute session lifetime (sessionAbsoluteMaxTTL from CreatedAt).
// Expired grace tokens are also purged in the same pass.
func (b *InMemoryBackend) SweepExpiredSessions(ctx context.Context, ttl time.Duration) {
	now := time.Now().UTC()

	b.mu.Lock("SweepExpiredSessions")
	beforeCount := b.sessions.Len()

	var expiredSessions []string
	for _, s := range b.sessions.All() {
		idleExpired := now.Sub(s.LastAccessedAt) > ttl
		absoluteExpired := now.After(s.ExpiresAt)

		if idleExpired || absoluteExpired {
			expiredSessions = append(expiredSessions, s.Token)
		}
	}

	for _, tok := range expiredSessions {
		b.sessions.Delete(tok)
	}

	// Purge grace tokens whose window has closed.
	var expiredGrace []string
	for _, g := range b.graceTokens.All() {
		if now.After(g.ExpiresAt) {
			expiredGrace = append(expiredGrace, g.Token)
		}
	}

	for _, tok := range expiredGrace {
		b.graceTokens.Delete(tok)
	}

	afterCount := b.sessions.Len()
	b.mu.Unlock()

	b.lastSweepAt.Store(now.UnixNano())

	diff := beforeCount - afterCount
	if diff > 0 {
		telemetry.RecordWorkerItems("appconfigdata", "SessionSweeper", diff)
		logger.Load(ctx).
			InfoContext(ctx, "AppConfig Data janitor: expired sessions purged", "count", diff)
	}

	telemetry.RecordWorkerTask("appconfigdata", "SessionSweeper", "success")
}

// generateFamilyID mints an opaque identifier shared across all tokens in a session family.
func generateFamilyID() (string, error) {
	b := make([]byte, familyIDSize)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
