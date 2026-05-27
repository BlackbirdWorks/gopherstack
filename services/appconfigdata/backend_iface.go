package appconfigdata

import (
	"context"
	"time"
)

// StorageBackend defines the operations supported by the AppConfigData in-memory backend.
type StorageBackend interface {
	// SetConfiguration stores or updates configuration content for a profile.
	SetConfiguration(app, env, profile, content, contentType string) error
	// StartSession creates a new retrieval session and returns the initial token.
	StartSession(app, env, profile string, pollIntervalInSeconds int) (string, error)
	// GetLatestConfiguration retrieves configuration for the token.
	// Returns content, contentType, nextToken, contentHash, versionLabel.
	GetLatestConfiguration(
		token string,
	) (content []byte, contentType string, nextToken string, contentHash string, versionLabel string, err error)
	// LookupSession returns the session for the given token, or nil if not found.
	LookupSession(token string) *Session
	// ListProfiles returns all stored configuration profiles.
	ListProfiles() []ConfigurationProfile
	// ListSessions returns all active sessions (includes full tokens — use only internally).
	ListSessions() []Session
	// ListSessionsSafe returns all active sessions with tokens truncated for safe display.
	ListSessionsSafe() []SafeSession
	// EndSession terminates the session with the given token. Returns false if not found.
	EndSession(token string) bool
	// DeleteProfile removes a configuration profile and its associated sessions.
	DeleteProfile(app, env, profile string) bool
	// GetStats returns aggregate service statistics.
	GetStats() ServiceStats
	// SweepExpiredSessions removes sessions idle longer than ttl or past absolute expiry.
	SweepExpiredSessions(ctx context.Context, ttl time.Duration)
}
