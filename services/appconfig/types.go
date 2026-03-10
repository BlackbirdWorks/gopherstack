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
type Application struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
}

// Environment represents an AppConfig environment.
type Environment struct {
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	ApplicationID string    `json:"applicationId"`
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Description   string    `json:"description,omitempty"`
	State         string    `json:"state"`
}

// ConfigurationProfile represents an AppConfig configuration profile.
type ConfigurationProfile struct {
	ApplicationID string `json:"applicationId"`
	ID            string `json:"id"`
	Name          string `json:"name"`
	Description   string `json:"description,omitempty"`
	LocationURI   string `json:"locationUri"`
	Type          string `json:"type"`
}

// HostedConfigurationVersion represents a hosted configuration version.
type HostedConfigurationVersion struct {
	CreatedAt              time.Time `json:"createdAt"`
	ApplicationID          string    `json:"applicationId"`
	ConfigurationProfileID string    `json:"configurationProfileId"`
	ContentType            string    `json:"contentType"`
	Content                []byte    `json:"-"`
	VersionNumber          int32     `json:"versionNumber"`
}

// DeploymentStrategy represents an AppConfig deployment strategy.
type DeploymentStrategy struct {
	CreatedAt                   time.Time `json:"createdAt"`
	UpdatedAt                   time.Time `json:"updatedAt"`
	ID                          string    `json:"id"`
	Name                        string    `json:"name"`
	Description                 string    `json:"description,omitempty"`
	GrowthType                  string    `json:"growthType"`
	ReplicateTo                 string    `json:"replicateTo"`
	DeploymentDurationInMinutes int32     `json:"deploymentDurationInMinutes"`
	GrowthFactor                float32   `json:"growthFactor"`
	FinalBakeTimeInMinutes      int32     `json:"finalBakeTimeInMinutes"`
}

// Deployment represents an AppConfig deployment.
type Deployment struct {
	StartedAt              time.Time `json:"startedAt"`
	CompletedAt            time.Time `json:"completedAt"`
	ApplicationID          string    `json:"applicationId"`
	EnvironmentID          string    `json:"environmentId"`
	ConfigurationProfileID string    `json:"configurationProfileId"`
	DeploymentStrategyID   string    `json:"deploymentStrategyId"`
	ConfigurationVersion   string    `json:"configurationVersion"`
	State                  string    `json:"state"`
	DeploymentNumber       int32     `json:"deploymentNumber"`
}
