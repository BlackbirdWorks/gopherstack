package appconfig

import (
	"time"
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

// Monitor represents an Amazon CloudWatch alarm used to monitor an AppConfig environment.
type Monitor struct {
	AlarmArn     string `json:"AlarmArn"`
	AlarmRoleArn string `json:"AlarmRoleArn,omitempty"`
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
	Monitors      []Monitor `json:"Monitors,omitempty"`
}

// Validator represents a validator for a configuration profile.
type Validator struct {
	Type    string `json:"Type"`    // JSON_SCHEMA or LAMBDA
	Content string `json:"Content"` // JSON schema doc or Lambda ARN
}

// ConfigurationProfile represents an AppConfig configuration profile.
type ConfigurationProfile struct {
	ApplicationID    string      `json:"ApplicationId"`
	ID               string      `json:"Id"`
	Name             string      `json:"Name"`
	Description      string      `json:"Description,omitempty"`
	LocationURI      string      `json:"LocationUri"`
	Type             string      `json:"Type,omitempty"`
	RetrievalRoleArn string      `json:"RetrievalRoleArn,omitempty"`
	Validators       []Validator `json:"Validators,omitempty"`
}

// HostedConfigurationVersion represents a hosted configuration version.
type HostedConfigurationVersion struct {
	CreatedAt              time.Time `json:"CreatedAt,omitzero"`
	ApplicationID          string    `json:"ApplicationId"`
	ConfigurationProfileID string    `json:"ConfigurationProfileId"`
	ContentType            string    `json:"ContentType"`
	Description            string    `json:"Description,omitempty"`
	VersionLabel           string    `json:"VersionLabel,omitempty"`
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

// DeploymentEvent represents a single event in a deployment's history.
// ActionInvocations is intentionally unmodeled: this backend does not
// simulate real extension-action execution (Lambda invocation, SSM
// documents, ...), so a real SDK client's ActionInvocations would always
// come back empty here regardless -- see AppliedExtensions on Deployment
// for the same rationale. That matches AWS's own shape (the field is
// optional) rather than fabricating invocation data.
type DeploymentEvent struct {
	OccurredAt  time.Time `json:"OccurredAt,omitzero"`
	EventType   string    `json:"EventType"`
	Description string    `json:"Description,omitempty"`
	TriggeredBy string    `json:"TriggeredBy,omitempty"`
}

// AppliedExtension identifies an extension association that was in effect
// for an application, environment, or configuration profile when a
// deployment started.
type AppliedExtension struct {
	Parameters             map[string]string `json:"Parameters,omitempty"`
	ExtensionAssociationID string            `json:"ExtensionAssociationId,omitempty"`
	ExtensionID            string            `json:"ExtensionId,omitempty"`
	VersionNumber          int32             `json:"VersionNumber,omitempty"`
}

// Deployment represents an AppConfig deployment.
type Deployment struct {
	StartedAt                   time.Time          `json:"StartedAt,omitzero"`
	CompletedAt                 time.Time          `json:"CompletedAt,omitzero"`
	ApplicationID               string             `json:"ApplicationId"`
	EnvironmentID               string             `json:"EnvironmentId"`
	ConfigurationProfileID      string             `json:"ConfigurationProfileId"`
	DeploymentStrategyID        string             `json:"DeploymentStrategyId"`
	ConfigurationVersion        string             `json:"ConfigurationVersion"`
	State                       string             `json:"State"`
	TriggeredBy                 string             `json:"TriggeredBy,omitempty"`
	Description                 string             `json:"Description,omitempty"`
	ConfigurationName           string             `json:"ConfigurationName,omitempty"`
	ConfigurationLocationURI    string             `json:"ConfigurationLocationUri,omitempty"`
	GrowthType                  string             `json:"GrowthType,omitempty"`
	VersionLabel                string             `json:"VersionLabel,omitempty"`
	EventLog                    []DeploymentEvent  `json:"EventLog,omitempty"`
	AppliedExtensions           []AppliedExtension `json:"AppliedExtensions,omitempty"`
	PercentageComplete          float32            `json:"PercentageComplete,omitempty"`
	GrowthFactor                float32            `json:"GrowthFactor,omitempty"`
	DeploymentNumber            int32              `json:"DeploymentNumber"`
	DeploymentDurationInMinutes int32              `json:"DeploymentDurationInMinutes,omitempty"`
	FinalBakeTimeInMinutes      int32              `json:"FinalBakeTimeInMinutes,omitempty"`
}

// ExtensionAction represents a single action in an AppConfig extension.
type ExtensionAction struct {
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	RoleArn     string `json:"RoleArn,omitempty"`
	URI         string `json:"Uri,omitempty"`
}

// ExtensionParameter describes a parameter accepted by an extension.
type ExtensionParameter struct {
	Description string `json:"Description,omitempty"`
	Required    bool   `json:"Required,omitempty"`
}

// Extension represents an AppConfig extension.
type Extension struct {
	Actions       map[string][]ExtensionAction  `json:"Actions,omitempty"`
	Parameters    map[string]ExtensionParameter `json:"Parameters,omitempty"`
	Arn           string                        `json:"Arn"`
	Description   string                        `json:"Description,omitempty"`
	ID            string                        `json:"Id"`
	Name          string                        `json:"Name"`
	VersionNumber int32                         `json:"VersionNumber"`
}

// ExtensionAssociation represents an association between an extension and an AppConfig resource.
type ExtensionAssociation struct {
	Parameters             map[string]string `json:"Parameters,omitempty"`
	Arn                    string            `json:"Arn"`
	ExtensionArn           string            `json:"ExtensionArn"`
	ID                     string            `json:"Id"`
	ResourceArn            string            `json:"ResourceArn"`
	ExtensionVersionNumber int32             `json:"ExtensionVersionNumber"`
}

// DeletionProtectionSettings represents the deletion protection configuration for an account.
type DeletionProtectionSettings struct {
	Enabled                   *bool  `json:"Enabled,omitempty"`
	ProtectionPeriodInMinutes *int32 `json:"ProtectionPeriodInMinutes,omitempty"`
}

// AccountSettings holds account-level AppConfig settings.
type AccountSettings struct {
	DeletionProtection *DeletionProtectionSettings `json:"DeletionProtection,omitempty"`
}
