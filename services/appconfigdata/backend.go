package appconfigdata

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// InMemoryBackend implements StorageBackend for AppConfigData.
type InMemoryBackend struct {
	profiles map[string]*ConfigurationProfile
	sessions map[string]*Session
	mu       *lockmetrics.RWMutex
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

// SetConfiguration stores or updates configuration content for a profile.
func (b *InMemoryBackend) SetConfiguration(app, env, profile, content, contentType string) {
	b.mu.Lock("SetConfiguration")
	defer b.mu.Unlock()

	key := profileKey(app, env, profile)
	b.profiles[key] = &ConfigurationProfile{
		ApplicationIdentifier:          app,
		EnvironmentIdentifier:          env,
		ConfigurationProfileIdentifier: profile,
		Content:                        content,
		ContentType:                    contentType,
	}
}

// StartSession creates a new retrieval session and returns the initial token.
func (b *InMemoryBackend) StartSession(app, env, profile string) (string, error) {
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
	}

	return token, nil
}

// GetLatestConfiguration retrieves configuration data for the given token and returns a new token.
func (b *InMemoryBackend) GetLatestConfiguration(token string) ([]byte, string, string, error) {
	b.mu.Lock("GetLatestConfiguration")
	defer b.mu.Unlock()

	sess, ok := b.sessions[token]
	if !ok {
		return nil, "", "", ErrSessionNotFound
	}

	key := profileKey(sess.ApplicationIdentifier, sess.EnvironmentIdentifier, sess.ConfigurationProfileIdentifier)
	profile := b.profiles[key]

	var content []byte
	contentType := "application/octet-stream"

	if profile != nil {
		content = []byte(profile.Content)
		if profile.ContentType != "" {
			contentType = profile.ContentType
		}
	}

	// Generate a new token for the next poll and rotate the session.
	nextToken, err := generateToken()
	if err != nil {
		return nil, "", "", fmt.Errorf("generating next token: %w", err)
	}

	delete(b.sessions, token)

	now := time.Now().UTC()
	newSess := *sess
	newSess.Token = nextToken
	newSess.LastAccessedAt = now
	b.sessions[nextToken] = &newSess

	return content, contentType, nextToken, nil
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

// SweepExpiredSessions removes sessions that have not been accessed for more than 24 hours.
func (b *InMemoryBackend) SweepExpiredSessions(ctx context.Context) {
	const sessionExpiry = 24 * time.Hour
	now := time.Now().UTC()

	b.mu.Lock("SweepExpiredSessions")
	beforeCount := len(b.sessions)
	maps.DeleteFunc(b.sessions, func(_ string, s *Session) bool {
		return now.Sub(s.LastAccessedAt) > sessionExpiry
	})
	afterCount := len(b.sessions)
	b.mu.Unlock()

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
