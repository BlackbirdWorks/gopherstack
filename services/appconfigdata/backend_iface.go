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
	// GetLatestConfiguration retrieves configuration data for the given token and returns content, contentType, nextToken, contentHash.
	GetLatestConfiguration(token string) (content []byte, contentType string, nextToken string, contentHash string, err error)
	// LookupSession returns the session for the given token, or nil if not found.
	LookupSession(token string) *Session
	// ListProfiles returns all stored configuration profiles.
	ListProfiles() []ConfigurationProfile
	// ListSessions returns all active sessions.
	ListSessions() []Session
	// EndSession terminates the session with the given token. Returns false if not found.
	EndSession(token string) bool
	// DeleteProfile removes a configuration profile and its associated sessions.
	DeleteProfile(app, env, profile string) bool
	// GetStats returns aggregate service statistics.
	GetStats() ServiceStats
	// SweepExpiredSessions removes sessions idle longer than ttl.
	SweepExpiredSessions(ctx context.Context, ttl time.Duration)
}
