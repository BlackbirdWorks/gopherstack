package appconfigdata

// StorageBackend defines the operations supported by the AppConfigData in-memory backend.
type StorageBackend interface {
	// SetConfiguration stores or updates configuration content for a profile.
	SetConfiguration(app, env, profile, content, contentType string)
	// StartSession creates a new retrieval session and returns the initial token.
	StartSession(app, env, profile string) (string, error)
	// GetLatestConfiguration retrieves configuration data for the given token and returns a new token.
	GetLatestConfiguration(token string) (content []byte, contentType string, nextToken string, err error)
	// LookupSession returns the session for the given token, or nil if not found.
	LookupSession(token string) *Session
	// ListProfiles returns all stored configuration profiles.
	ListProfiles() []ConfigurationProfile
	// ListSessions returns all active sessions.
	ListSessions() []Session
	// DeleteProfile removes a configuration profile and its associated sessions.
	DeleteProfile(app, env, profile string) bool
}
