package appconfigdata

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// graceEntry holds the response cached for a rotated token during the grace period.
// A client that retries with an old token receives the same response it would have
// gotten on the first successful poll, preventing duplicate-delivery confusion.
type graceEntry struct {
	nextToken   string
	content     []byte
	contentType string
	contentHash string
	versionLabel string
	expiresAt   time.Time
}

// InMemoryBackend implements StorageBackend for AppConfigData.
type InMemoryBackend struct {
	profiles    map[string]*ConfigurationProfile
	sessions    map[string]*Session
	graceTokens map[string]*graceEntry // rotated token → cached response
	mu          *lockmetrics.RWMutex
	signingKey  []byte        // HMAC-SHA256 key for token integrity verification
	lastSweepAt atomic.Int64  // unix nanoseconds; accessed without the lock

	totalPolls   atomic.Int64
	totalFailures atomic.Int64
	totalChanges  atomic.Int64
}

// NewInMemoryBackend creates a new InMemoryBackend with a freshly generated signing key.
func NewInMemoryBackend() *InMemoryBackend {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		// Fallback: derive from a fixed phrase — only happens if the OS entropy pool is
		// completely exhausted, which should never occur in practice.
		sum := sha256.Sum256([]byte("appconfigdata-fallback-key"))
		key = sum[:]
	}

	return &InMemoryBackend{
		profiles:    make(map[string]*ConfigurationProfile),
		sessions:    make(map[string]*Session),
		graceTokens: make(map[string]*graceEntry),
		mu:          lockmetrics.New("appconfigdata"),
		signingKey:  key,
	}
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
			if normalized, err := json.Marshal(v); err == nil {
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
// Format: <64-hex-random>.<16-hex-mac>
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
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
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

// SetConfiguration stores or updates configuration content for a profile.
// Returns ErrContentTooLarge if content exceeds maxContentBytes.
// Returns ErrContentTypeMismatch if contentType indicates JSON but content is not valid JSON.
func (b *InMemoryBackend) SetConfiguration(app, env, profile, content, contentType string) error {
	if len(content) > maxContentBytes {
		return ErrContentTooLarge
	}

	if isJSONContentType(contentType) {
		var v any
		if err := json.Unmarshal([]byte(content), &v); err != nil {
			return ErrContentTypeMismatch
		}
	}

	b.mu.Lock("SetConfiguration")
	defer b.mu.Unlock()

	key := profileKey(app, env, profile)
	now := time.Now().UTC()
	hash := normalizedContentHash(content, contentType)

	existing := b.profiles[key]
	var history []ConfigVersion
	var nextVersion int
	var changed bool

	if existing != nil && !existing.UpdatedAt.IsZero() {
		nextVersion = existing.VersionNumber + 1
		changed = existing.ContentHash != hash
		entry := ConfigVersion{
			Content:       existing.Content,
			ContentType:   existing.ContentType,
			ContentHash:   existing.ContentHash,
			UpdatedAt:     existing.UpdatedAt,
			VersionLabel:  existing.VersionLabel,
			VersionNumber: existing.VersionNumber,
			DeploymentID:  existing.DeploymentID,
		}
		history = append(history, entry)
		history = append(history, existing.History...)
		if len(history) > maxHistoryEntries {
			history = history[:maxHistoryEntries]
		}
	} else {
		nextVersion = 1
		changed = true
	}

	versionLabel := fmt.Sprintf("v%d", nextVersion)

	b.profiles[key] = &ConfigurationProfile{
		ApplicationIdentifier:          app,
		EnvironmentIdentifier:          env,
		ConfigurationProfileIdentifier: profile,
		Content:                        content,
		ContentType:                    contentType,
		ContentHash:                    hash,
		VersionLabel:                   versionLabel,
		VersionNumber:                  nextVersion,
		UpdatedAt:                      now,
		History:                        history,
	}

	if changed {
		b.totalChanges.Add(1)
	}

	return nil
}

// StartSession creates a new retrieval session and returns the initial token.
// pollIntervalInSeconds must be 0 (use default) or >= minPollIntervalSeconds.
// Returns ErrNoActiveDeployment when no configuration has been published for the profile.
func (b *InMemoryBackend) StartSession(
	app, env, profile string,
	pollIntervalInSeconds int,
) (string, error) {
	if pollIntervalInSeconds != 0 && pollIntervalInSeconds < minPollIntervalSeconds {
		return "", ErrInvalidPollInterval
	}

	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	// Require an active deployment — no configuration published yet means 404 on AWS.
	key := profileKey(app, env, profile)
	if _, exists := b.profiles[key]; !exists {
		return "", ErrNoActiveDeployment
	}

	familyID, err := generateFamilyID()
	if err != nil {
		return "", fmt.Errorf("generating family ID: %w", err)
	}

	token, err := b.generateToken(familyID)
	if err != nil {
		return "", fmt.Errorf("generating session token: %w", err)
	}

	now := time.Now().UTC()
	b.sessions[token] = &Session{
		Token:                          token,
		TokenFamilyID:                  familyID,
		ApplicationIdentifier:          app,
		EnvironmentIdentifier:          env,
		ConfigurationProfileIdentifier: profile,
		CreatedAt:                      now,
		LastAccessedAt:                 now,
		ExpiresAt:                      now.Add(sessionAbsoluteMaxTTL),
		PollIntervalInSeconds:          pollIntervalInSeconds,
	}

	telemetry.RecordWorkerItems("appconfigdata", "Sessions", 1)

	return token, nil
}

// GetLatestConfiguration retrieves configuration data for the given token and returns a new token.
// The token is rotated on every successful call; the old token enters a short grace window so
// that clients can safely retry after a transient failure without losing their session.
//
// Returned values: content, contentType, nextToken, contentHash, versionLabel, error.
func (b *InMemoryBackend) GetLatestConfiguration(
	token string,
) ([]byte, string, string, string, string, error) {
	b.mu.Lock("GetLatestConfiguration")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	// Fast path: check grace tokens first to serve idempotent retries.
	if grace, ok := b.graceTokens[token]; ok {
		if now.Before(grace.expiresAt) {
			b.totalPolls.Add(1)

			return grace.content, grace.contentType, grace.nextToken, grace.contentHash, grace.versionLabel, nil
		}
		// Grace period expired; fall through to the normal "not found" path.
		delete(b.graceTokens, token)
	}

	sess, ok := b.sessions[token]
	if !ok {
		b.totalFailures.Add(1)

		return nil, "", "", "", "", ErrSessionNotFound
	}

	// Reject expired sessions (absolute token TTL).
	if now.After(sess.ExpiresAt) {
		delete(b.sessions, token)
		b.totalFailures.Add(1)

		return nil, "", "", "", "", ErrTokenExpired
	}

	// Enforce minimum poll interval BEFORE any further processing (item 8).
	if !sess.LastPollAt.IsZero() && sess.PollIntervalInSeconds > 0 {
		elapsed := now.Sub(sess.LastPollAt)
		minInterval := time.Duration(sess.PollIntervalInSeconds) * time.Second

		if elapsed < minInterval {
			b.totalFailures.Add(1)

			return nil, "", "", "", "", ErrPollTooFrequent
		}
	}

	// Verify token MAC to detect tampering / cross-family reuse.
	if !b.verifyTokenMAC(token, sess.TokenFamilyID) {
		delete(b.sessions, token)
		b.totalFailures.Add(1)

		return nil, "", "", "", "", ErrSessionNotFound
	}

	// Re-validate that the profile still exists (it may have been deleted mid-session).
	key := profileKey(
		sess.ApplicationIdentifier,
		sess.EnvironmentIdentifier,
		sess.ConfigurationProfileIdentifier,
	)
	profile := b.profiles[key]
	if profile == nil {
		delete(b.sessions, token)
		b.totalFailures.Add(1)

		return nil, "", "", "", "", ErrResourceRemoved
	}

	// Change detection: return empty content when the profile hash matches the previous poll.
	var content []byte
	contentType := "application/octet-stream"
	hash := profile.ContentHash
	versionLabel := profile.VersionLabel

	if hash != sess.PreviousContentHash {
		content = []byte(profile.Content)
		if profile.ContentType != "" {
			contentType = profile.ContentType
		}
	}

	// Rotate token: the next token belongs to the same family.
	nextToken, err := b.generateToken(sess.TokenFamilyID)
	if err != nil {
		return nil, "", "", "", "", fmt.Errorf("generating next token: %w", err)
	}

	newSess := *sess
	newSess.Token = nextToken
	newSess.LastAccessedAt = now
	newSess.LastPollAt = now
	newSess.PollCount = sess.PollCount + 1
	newSess.PreviousContentHash = hash

	delete(b.sessions, token)
	b.sessions[nextToken] = &newSess

	// Keep the old token alive for a short grace period (retry idempotency).
	b.graceTokens[token] = &graceEntry{
		nextToken:    nextToken,
		content:      content,
		contentType:  contentType,
		contentHash:  hash,
		versionLabel: versionLabel,
		expiresAt:    now.Add(tokenGracePeriod),
	}

	b.totalPolls.Add(1)

	return content, contentType, nextToken, hash, versionLabel, nil
}

// ListProfiles returns all stored configuration profiles.
func (b *InMemoryBackend) ListProfiles() []ConfigurationProfile {
	b.mu.RLock("ListProfiles")
	defer b.mu.RUnlock()

	out := make([]ConfigurationProfile, 0, len(b.profiles))
	for _, p := range b.profiles {
		out = append(out, *p)
	}

	return out
}

// LookupSession returns the session for the given token, or nil if not found.
// This is a read-only lookup; it does not rotate the token.
func (b *InMemoryBackend) LookupSession(token string) *Session {
	b.mu.RLock("LookupSession")
	defer b.mu.RUnlock()

	sess := b.sessions[token]
	if sess == nil {
		return nil
	}

	snap := *sess

	return &snap
}

// ListSessions returns all active sessions.
func (b *InMemoryBackend) ListSessions() []Session {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	out := make([]Session, 0, len(b.sessions))
	for _, s := range b.sessions {
		out = append(out, *s)
	}

	return out
}

// ListSessionsSafe returns all active sessions with tokens truncated for safe display.
// Use this for admin list endpoints; never return full tokens in list responses.
func (b *InMemoryBackend) ListSessionsSafe() []SafeSession {
	b.mu.RLock("ListSessionsSafe")
	defer b.mu.RUnlock()

	out := make([]SafeSession, 0, len(b.sessions))
	for _, s := range b.sessions {
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

	if _, ok := b.sessions[token]; !ok {
		return false
	}

	delete(b.sessions, token)

	return true
}

// DeleteProfile removes a configuration profile and its associated sessions.
func (b *InMemoryBackend) DeleteProfile(app, env, profile string) bool {
	b.mu.Lock("DeleteProfile")
	defer b.mu.Unlock()

	key := profileKey(app, env, profile)
	if _, ok := b.profiles[key]; !ok {
		return false
	}

	delete(b.profiles, key)

	// Remove sessions linked to this profile.
	maps.DeleteFunc(b.sessions, func(_ string, s *Session) bool {
		return s.ApplicationIdentifier == app &&
			s.EnvironmentIdentifier == env &&
			s.ConfigurationProfileIdentifier == profile
	})

	return true
}

// GetStats returns aggregate service statistics.
func (b *InMemoryBackend) GetStats() ServiceStats {
	b.mu.RLock("GetStats")
	sc := len(b.sessions)
	pc := len(b.profiles)
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
	beforeCount := len(b.sessions)
	maps.DeleteFunc(b.sessions, func(_ string, s *Session) bool {
		idleExpired := now.Sub(s.LastAccessedAt) > ttl
		absoluteExpired := now.After(s.ExpiresAt)

		return idleExpired || absoluteExpired
	})
	// Purge grace tokens whose window has closed.
	maps.DeleteFunc(b.graceTokens, func(_ string, g *graceEntry) bool {
		return now.After(g.expiresAt)
	})
	afterCount := len(b.sessions)
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
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
