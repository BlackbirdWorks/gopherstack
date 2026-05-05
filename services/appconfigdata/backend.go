package appconfigdata

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// InMemoryBackend implements StorageBackend for AppConfigData.
type InMemoryBackend struct {
	profiles   map[string]*ConfigurationProfile
	sessions   map[string]*Session
	mu         *lockmetrics.RWMutex
	lastSweepAt atomic.Int64 // unix nanoseconds; accessed without the lock
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		profiles: make(map[string]*ConfigurationProfile),
		sessions: make(map[string]*Session),
		mu:       lockmetrics.New("appconfigdata"),
	}
}

func profileKey(app, env, profile string) string {
	return fmt.Sprintf("%s|%s|%s", app, env, profile)
}

func contentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// SetConfiguration stores or updates configuration content for a profile.
// Returns ErrContentTooLarge if content exceeds maxContentBytes.
func (b *InMemoryBackend) SetConfiguration(app, env, profile, content, contentType string) error {
	if len(content) > maxContentBytes {
		return ErrContentTooLarge
	}

	b.mu.Lock("SetConfiguration")
	defer b.mu.Unlock()

	key := profileKey(app, env, profile)
	now := time.Now().UTC()
	hash := contentHash(content)

	existing := b.profiles[key]
	var history []ConfigVersion
	if existing != nil && !existing.UpdatedAt.IsZero() {
		// Prepend so history[0] is the most recent previous version.
		entry := ConfigVersion{
			Content:     existing.Content,
			ContentType: existing.ContentType,
			ContentHash: existing.ContentHash,
			UpdatedAt:   existing.UpdatedAt,
		}
		history = append(history, entry)
		history = append(history, existing.History...)
		if len(history) > maxHistoryEntries {
			history = history[:maxHistoryEntries]
		}
	}

	b.profiles[key] = &ConfigurationProfile{
		ApplicationIdentifier:          app,
		EnvironmentIdentifier:          env,
		ConfigurationProfileIdentifier: profile,
		Content:                        content,
		ContentType:                    contentType,
		ContentHash:                    hash,
		UpdatedAt:                      now,
		History:                        history,
	}

	return nil
}

// StartSession creates a new retrieval session and returns the initial token.
// pollIntervalInSeconds must be 0 (use default) or >= minPollIntervalSeconds.
func (b *InMemoryBackend) StartSession(app, env, profile string, pollIntervalInSeconds int) (string, error) {
	if pollIntervalInSeconds != 0 && pollIntervalInSeconds < minPollIntervalSeconds {
		return "", ErrInvalidPollInterval
	}

	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("generating session token: %w", err)
	}

	now := time.Now().UTC()
	b.sessions[token] = &Session{
		Token:                          token,
		ApplicationIdentifier:          app,
		EnvironmentIdentifier:          env,
		ConfigurationProfileIdentifier: profile,
		CreatedAt:                      now,
		LastAccessedAt:                 now,
		PollIntervalInSeconds:          pollIntervalInSeconds,
	}

	telemetry.RecordWorkerItems("appconfigdata", "Sessions", 1)

	return token, nil
}

// GetLatestConfiguration retrieves configuration data for the given token and returns a new token.
func (b *InMemoryBackend) GetLatestConfiguration(token string) ([]byte, string, string, string, error) {
	b.mu.Lock("GetLatestConfiguration")
	defer b.mu.Unlock()

	sess, ok := b.sessions[token]
	if !ok {
		return nil, "", "", "", ErrSessionNotFound
	}

	key := profileKey(sess.ApplicationIdentifier, sess.EnvironmentIdentifier, sess.ConfigurationProfileIdentifier)
	profile := b.profiles[key]

	var content []byte
	contentType := "application/octet-stream"
	hash := ""

	if profile != nil {
		content = []byte(profile.Content)
		hash = profile.ContentHash
		if profile.ContentType != "" {
			contentType = profile.ContentType
		}
	}

	// Generate a new token for the next poll and rotate the session atomically.
	nextToken, err := generateToken()
	if err != nil {
		return nil, "", "", "", fmt.Errorf("generating next token: %w", err)
	}

	now := time.Now().UTC()
	newSess := *sess
	newSess.Token = nextToken
	newSess.LastAccessedAt = now
	newSess.PollCount = sess.PollCount + 1

	delete(b.sessions, token)
	b.sessions[nextToken] = &newSess

	return content, contentType, nextToken, hash, nil
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
		SessionCount: sc,
		ProfileCount: pc,
		LastSweepAt:  lastSweep,
	}
}

// SweepExpiredSessions removes sessions that have been idle longer than ttl.
func (b *InMemoryBackend) SweepExpiredSessions(ctx context.Context, ttl time.Duration) {
	now := time.Now().UTC()

	b.mu.Lock("SweepExpiredSessions")
	beforeCount := len(b.sessions)
	maps.DeleteFunc(b.sessions, func(_ string, s *Session) bool {
		return now.Sub(s.LastAccessedAt) > ttl
	})
	afterCount := len(b.sessions)
	b.mu.Unlock()

	b.lastSweepAt.Store(now.UnixNano())

	diff := beforeCount - afterCount
	if diff > 0 {
		telemetry.RecordWorkerItems("appconfigdata", "SessionSweeper", diff)
		logger.Load(ctx).InfoContext(ctx, "AppConfig Data janitor: expired sessions purged", "count", diff)
	}

	telemetry.RecordWorkerTask("appconfigdata", "SessionSweeper", "success")
}

const tokenByteSize = 16

func generateToken() (string, error) {
	b := make([]byte, tokenByteSize)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
