// Package appconfigdata provides an in-memory stub for the
// AWS AppConfigData service, which is used to retrieve deployed
// configuration data for applications at runtime.
package appconfigdata

import (
	"errors"
	"time"
)

var (
	// ErrSessionNotFound is returned when the requested session token does not exist.
	ErrSessionNotFound = errors.New("bad request: invalid configuration token")
	// ErrProfileNotFound is returned when no configuration has been stored for a profile.
	ErrProfileNotFound = errors.New("resource not found: configuration profile not found")
)

// ConfigVersion records a historical snapshot of configuration content.
type ConfigVersion struct {
	Content     string    `json:"content"`
	ContentType string    `json:"contentType"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// ConfigurationProfile stores configuration content for an application/environment/profile combination.
type ConfigurationProfile struct {
	ApplicationIdentifier          string          `json:"applicationIdentifier"`
	EnvironmentIdentifier          string          `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string          `json:"configurationProfileIdentifier"`
	Content                        string          `json:"content"`
	ContentType                    string          `json:"contentType"`
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
