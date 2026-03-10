// Package appconfig provides an in-memory stub for the AWS AppConfig service,
// which manages feature flags and application configuration.
package appconfig

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrApplicationNotFound is returned when the requested application does not exist.
	ErrApplicationNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrEnvironmentNotFound is returned when the requested environment does not exist.
	ErrEnvironmentNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrConfigurationProfileNotFound is returned when the requested configuration profile does not exist.
	ErrConfigurationProfileNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrHostedConfigVersionNotFound is returned when the requested hosted configuration version does not exist.
	ErrHostedConfigVersionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrDeploymentStrategyNotFound is returned when the requested deployment strategy does not exist.
	ErrDeploymentStrategyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrDeploymentNotFound is returned when the requested deployment does not exist.
	ErrDeploymentNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)

// Application represents an AppConfig application.
// JSON field names match the AWS AppConfig REST API (PascalCase).
type Application struct {
	CreatedAt   time.Time `json:"CreatedAt,omitzero"`
	UpdatedAt   time.Time `json:"UpdatedAt,omitzero"`
	ID          string    `json:"Id"`
	Name        string    `json:"Name"`
	Description string    `json:"Description,omitempty"`
}

// Environment represents an AppConfig environment.
type Environment struct {
	CreatedAt     time.Time `json:"CreatedAt,omitzero"`
	UpdatedAt     time.Time `json:"UpdatedAt,omitzero"`
	ApplicationID string    `json:"ApplicationId"`
	ID            string    `json:"Id"`
	Name          string    `json:"Name"`
	Description   string    `json:"Description,omitempty"`
	State         string    `json:"State"`
}

// ConfigurationProfile represents an AppConfig configuration profile.
type ConfigurationProfile struct {
	ApplicationID string `json:"ApplicationId"`
	ID            string `json:"Id"`
	Name          string `json:"Name"`
	Description   string `json:"Description,omitempty"`
	LocationURI   string `json:"LocationUri"`
	Type          string `json:"Type,omitempty"`
}

// HostedConfigurationVersion represents a hosted configuration version.
type HostedConfigurationVersion struct {
	CreatedAt              time.Time `json:"CreatedAt,omitzero"`
	ApplicationID          string    `json:"ApplicationId"`
	ConfigurationProfileID string    `json:"ConfigurationProfileId"`
	ContentType            string    `json:"ContentType"`
	Content                []byte    `json:"-"`
	VersionNumber          int32     `json:"VersionNumber"`
}

// DeploymentStrategy represents an AppConfig deployment strategy.
type DeploymentStrategy struct {
	CreatedAt                   time.Time `json:"CreatedAt,omitzero"`
	UpdatedAt                   time.Time `json:"UpdatedAt,omitzero"`
	ID                          string    `json:"Id"`
	Name                        string    `json:"Name"`
	Description                 string    `json:"Description,omitempty"`
	GrowthType                  string    `json:"GrowthType"`
	ReplicateTo                 string    `json:"ReplicateTo"`
	DeploymentDurationInMinutes int32     `json:"DeploymentDurationInMinutes"`
	GrowthFactor                float32   `json:"GrowthFactor"`
	FinalBakeTimeInMinutes      int32     `json:"FinalBakeTimeInMinutes"`
}

// Deployment represents an AppConfig deployment.
type Deployment struct {
	StartedAt              time.Time `json:"StartedAt,omitzero"`
	CompletedAt            time.Time `json:"CompletedAt,omitzero"`
	ApplicationID          string    `json:"ApplicationId"`
	EnvironmentID          string    `json:"EnvironmentId"`
	ConfigurationProfileID string    `json:"ConfigurationProfileId"`
	DeploymentStrategyID   string    `json:"DeploymentStrategyId"`
	ConfigurationVersion   string    `json:"ConfigurationVersion"`
	State                  string    `json:"State"`
	DeploymentNumber       int32     `json:"DeploymentNumber"`
}
