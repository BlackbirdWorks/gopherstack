// Package appconfigdata provides an in-memory stub for the
// AWS AppConfigData service, which is used to retrieve deployed
// configuration data for applications at runtime.
package appconfigdata

import (
	"errors"
	"time"
)

const (
	// maxHistoryEntries is the maximum number of historical versions retained per profile.
	maxHistoryEntries = 50
	// maxContentBytes is the maximum size of configuration content (1 MiB, matching AWS).
	maxContentBytes = 1 * 1024 * 1024
	// minPollIntervalSeconds is the AWS-enforced minimum for RequiredMinimumPollIntervalInSeconds.
	minPollIntervalSeconds = 15
	// DefaultSessionTTL is how long a session may be idle before the janitor evicts it.
	// AWS tokens expire after ~1 h; we match that behaviour.
	DefaultSessionTTL = 1 * time.Hour
	// sessionAbsoluteMaxTTL is the maximum session lifetime from creation, regardless of activity.
	// Mirrors AWS token expiry semantics: no session lives longer than 1 h.
	sessionAbsoluteMaxTTL = 1 * time.Hour
	// tokenGracePeriod is how long a rotated (old) token remains valid for retry idempotency.
	tokenGracePeriod = 5 * time.Minute
	// tokenByteSize is the number of random bytes used per token (32 → 64 hex chars).
	// Increased from 16 to 32 for stronger entropy, matching AWS token length expectations.
	tokenByteSize = 32
	// signingKeySize is the number of bytes used for the HMAC-SHA256 signing key.
	signingKeySize = 32
	// tokenParts is the expected number of parts when splitting a token by its separator.
	tokenParts = 2
	// familyIDSize is the number of random bytes used for a token family identifier.
	familyIDSize = 8
)

var (
	// ErrSessionNotFound is returned when the requested session token does not exist.
	ErrSessionNotFound = errors.New("bad request: invalid configuration token")
	// ErrTokenExpired is returned when the session token has passed its expiry time.
	ErrTokenExpired = errors.New("unauthorized: configuration token has expired")
	// ErrProfileNotFound is returned when no configuration has been stored for a profile.
	ErrProfileNotFound = errors.New("resource not found: configuration profile not found")
	// ErrResourceRemoved is returned when a session's app/env/profile was deleted after the session started.
	ErrResourceRemoved = errors.New("resource not found: application, environment, or profile no longer exists")
	// ErrContentTooLarge is returned when configuration content exceeds the size limit.
	ErrContentTooLarge = errors.New("bad request: content exceeds maximum size of 1 MiB")
	// ErrInvalidPollInterval is returned when RequiredMinimumPollIntervalInSeconds is out of range.
	ErrInvalidPollInterval = errors.New(
		"bad request: RequiredMinimumPollIntervalInSeconds must be 0 or >= 15",
	)
	// ErrPollTooFrequent is returned when a client polls faster than its declared minimum interval.
	ErrPollTooFrequent = errors.New(
		"bad request: polling too frequently — wait for the interval in Next-Poll-Interval-In-Seconds",
	)
	// ErrContentTypeMismatch is returned when content is declared as JSON but is not valid JSON.
	ErrContentTypeMismatch = errors.New("bad request: content is not valid for the declared content type")
	// ErrNoActiveDeployment is returned when StartConfigurationSession is called for a profile
	// that has no active deployment (no configuration has been published yet).
	ErrNoActiveDeployment = errors.New(
		"resource not found: no active deployment found for the given application, environment, and configuration profile",
	)
)

// ConfigVersion records a historical snapshot of configuration content.
type ConfigVersion struct {
	UpdatedAt     time.Time `json:"updatedAt"`
	Content       string    `json:"content"`
	ContentType   string    `json:"contentType"`
	ContentHash   string    `json:"contentHash"`
	VersionLabel  string    `json:"versionLabel"`
	DeploymentID  string    `json:"deploymentId"`
	VersionNumber int       `json:"versionNumber"`
}

// ConfigurationProfile stores configuration content for an application/environment/profile combination.
type ConfigurationProfile struct {
	UpdatedAt                      time.Time       `json:"updatedAt"`
	ApplicationIdentifier          string          `json:"applicationIdentifier"`
	EnvironmentIdentifier          string          `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string          `json:"configurationProfileIdentifier"`
	Content                        string          `json:"content"`
	ContentType                    string          `json:"contentType"`
	ContentHash                    string          `json:"contentHash"`
	VersionLabel                   string          `json:"versionLabel"`
	DeploymentID                   string          `json:"deploymentId"`
	History                        []ConfigVersion `json:"history"`
	VersionNumber                  int             `json:"versionNumber"`
}

// Session represents an active configuration retrieval session.
type Session struct {
	CreatedAt                      time.Time `json:"createdAt"`
	LastAccessedAt                 time.Time `json:"lastAccessedAt"`
	LastPollAt                     time.Time `json:"lastPollAt"`
	ExpiresAt                      time.Time `json:"expiresAt"`
	Token                          string    `json:"token"`
	TokenFamilyID                  string    `json:"tokenFamilyId"`
	PreviousContentHash            string    `json:"previousContentHash"`
	ApplicationIdentifier          string    `json:"applicationIdentifier"`
	EnvironmentIdentifier          string    `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string    `json:"configurationProfileIdentifier"`
	PollIntervalInSeconds          int       `json:"pollIntervalInSeconds"`
	PollCount                      int       `json:"pollCount"`
}

// SafeSession mirrors Session but truncates the token for safe display in admin UIs.
// The full token is never returned in list responses; only via authenticated audit endpoints.
type SafeSession struct {
	CreatedAt                      time.Time `json:"createdAt"`
	LastAccessedAt                 time.Time `json:"lastAccessedAt"`
	LastPollAt                     time.Time `json:"lastPollAt"`
	ExpiresAt                      time.Time `json:"expiresAt"`
	TokenPrefix                    string    `json:"tokenPrefix"` // first8…last4, e.g. "abcd1234...ef12"
	TokenFamilyID                  string    `json:"tokenFamilyId"`
	ApplicationIdentifier          string    `json:"applicationIdentifier"`
	EnvironmentIdentifier          string    `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string    `json:"configurationProfileIdentifier"`
	PollIntervalInSeconds          int       `json:"pollIntervalInSeconds"`
	PollCount                      int       `json:"pollCount"`
}

// ServiceStats holds aggregate metrics for the AppConfigData service.
type ServiceStats struct {
	LastSweepAt              time.Time `json:"lastSweepAt"`
	SessionTTL               string    `json:"sessionTtl"`
	JanitorPeriod            string    `json:"janitorPeriod"`
	SessionCount             int       `json:"sessionCount"`
	ProfileCount             int       `json:"profileCount"`
	TotalPollCount           int64     `json:"totalPollCount"`
	TotalPollFailures        int64     `json:"totalPollFailures"`
	ConfigurationChangeCount int64     `json:"configurationChangeCount"`
}

// startSessionRequest is the JSON body for StartConfigurationSession.
type startSessionRequest struct {
	ApplicationIdentifier                string `json:"ApplicationIdentifier"`
	EnvironmentIdentifier                string `json:"EnvironmentIdentifier"`
	ConfigurationProfileIdentifier       string `json:"ConfigurationProfileIdentifier"`
	RequiredMinimumPollIntervalInSeconds int    `json:"RequiredMinimumPollIntervalInSeconds"`
}

// startSessionResponse is the JSON response for StartConfigurationSession.
type startSessionResponse struct {
	InitialConfigurationToken string `json:"InitialConfigurationToken"`
}
