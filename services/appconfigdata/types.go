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
	DefaultSessionTTL = 24 * time.Hour
)

var (
	// ErrSessionNotFound is returned when the requested session token does not exist.
	ErrSessionNotFound = errors.New("bad request: invalid configuration token")
	// ErrProfileNotFound is returned when no configuration has been stored for a profile.
	ErrProfileNotFound = errors.New("resource not found: configuration profile not found")
	// ErrContentTooLarge is returned when configuration content exceeds the size limit.
	ErrContentTooLarge = errors.New("bad request: content exceeds maximum size of 1 MiB")
	// ErrInvalidPollInterval is returned when RequiredMinimumPollIntervalInSeconds is out of range.
	ErrInvalidPollInterval = errors.New("bad request: RequiredMinimumPollIntervalInSeconds must be 0 or >= 15")
)

// ConfigVersion records a historical snapshot of configuration content.
type ConfigVersion struct {
	UpdatedAt   time.Time `json:"updatedAt"`
	Content     string    `json:"content"`
	ContentType string    `json:"contentType"`
	ContentHash string    `json:"contentHash"`
}

// ConfigurationProfile stores configuration content for an application/environment/profile combination.
type ConfigurationProfile struct {
	ApplicationIdentifier          string          `json:"applicationIdentifier"`
	EnvironmentIdentifier          string          `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string          `json:"configurationProfileIdentifier"`
	Content                        string          `json:"content"`
	ContentType                    string          `json:"contentType"`
	ContentHash                    string          `json:"contentHash"`
	UpdatedAt                      time.Time       `json:"updatedAt"`
	History                        []ConfigVersion `json:"history"`
}

// Session represents an active configuration retrieval session.
type Session struct {
	CreatedAt                      time.Time `json:"createdAt"`
	LastAccessedAt                 time.Time `json:"lastAccessedAt"`
	Token                          string    `json:"token"`
	ApplicationIdentifier          string    `json:"applicationIdentifier"`
	EnvironmentIdentifier          string    `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string    `json:"configurationProfileIdentifier"`
	PollIntervalInSeconds          int       `json:"pollIntervalInSeconds"`
	PollCount                      int       `json:"pollCount"`
}

// ServiceStats holds aggregate metrics for the AppConfigData service.
type ServiceStats struct {
	SessionCount  int       `json:"sessionCount"`
	ProfileCount  int       `json:"profileCount"`
	LastSweepAt   time.Time `json:"lastSweepAt"`
	SessionTTL    string    `json:"sessionTtl"`
	JanitorPeriod string    `json:"janitorPeriod"`
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
