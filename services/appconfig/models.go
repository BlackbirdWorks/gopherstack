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

// AttributeValue is a single attribute value attached to a Treatment's
// FlagValue.AttributeValues map. Real AWS AppConfig models this as a tagged
// union (aws-sdk-go-v2/service/appconfig/types.AttributeValue:
// BooleanValue | NumberValue | StringValue | NumberArray | StringArray,
// selected by which JSON key is present on the wire -- see
// awsRestjson1_serializeDocumentAttributeValue in the SDK's serializers.go).
// This backend does not evaluate flag values (no flag-evaluation engine
// exists here, matching the family's other "storage only" fields), so
// rather than reimplement a Go union type it stores whichever member(s) the
// client sent and echoes them back unchanged on the same wire keys -- a
// faithful round-trip without fabricating semantics this backend can't
// give meaning to.
type AttributeValue struct {
	BooleanValue *bool     `json:"BooleanValue,omitempty"`
	NumberValue  *float64  `json:"NumberValue,omitempty"`
	StringValue  *string   `json:"StringValue,omitempty"`
	NumberArray  []float64 `json:"NumberArray,omitempty"`
	StringArray  []string  `json:"StringArray,omitempty"`
}

// FlagValue is the feature flag value served to users assigned to a Treatment.
type FlagValue struct {
	AttributeValues map[string]AttributeValue `json:"AttributeValues,omitempty"`
	Enabled         bool                      `json:"Enabled"`
}

// Treatment represents one variation (or the control) evaluated during an
// experiment. Key is server-generated: real CreateExperimentDefinition's
// TreatmentInput/UpdateExperimentDefinitionInput carry no client-supplied
// key at all (only Description/FlagValue/Weight), so AWS itself must assign
// it. This backend assigns "Control" to the control treatment and
// "Treatment1".."TreatmentN" (1-indexed, in the order supplied) to the
// rest -- see CreateExperimentDefinition's doc comment in
// experiment_definitions.go for why this specific, deterministic scheme was
// chosen over a random one.
type Treatment struct {
	FlagValue   *FlagValue `json:"FlagValue,omitempty"`
	Description string     `json:"Description,omitempty"`
	Key         string     `json:"Key,omitempty"`
	Weight      float32    `json:"Weight"`
}

// TreatmentOverrides assigns specific entity IDs directly to treatment
// keys, bypassing random assignment. Real AWS models this as a tagged union
// (types.TreatmentOverrides) with exactly one known member, "Inline" (a map
// of entity ID -> treatment key, see
// awsRestjson1_serializeDocumentTreatmentOverrides in the SDK); this
// backend models the union directly as that one member since it is the
// only variant the SDK ships.
type TreatmentOverrides struct {
	Inline map[string]string `json:"Inline,omitempty"`
}

// DeploymentParameters carries the optional KMS/extension-parameter
// configuration a StartExperimentRun/StopExperimentRun/UpdateExperimentRun
// request can attach to the underlying deployment real AWS creates to
// expose treatments. This backend has no such underlying deployment to
// configure (see the ExperimentRun doc comment in experiment_runs.go) and
// real GetExperimentRun/StartExperimentRun/etc. output shapes never echo
// DeploymentParameters back to the caller either -- so it is accepted on
// input and intentionally discarded rather than stored, matching what a
// real client would actually observe (nothing).
type DeploymentParameters struct {
	DynamicExtensionParameters map[string]string `json:"DynamicExtensionParameters,omitempty"`
	Tags                       map[string]string `json:"Tags,omitempty"`
}

// ExperimentRunResult captures the free-text, client-supplied narrative
// outcome of an experiment run (an executive summary and launch/no-launch
// rationale). This backend has no analytics engine to compute these itself
// -- see the results_verdict discussion in PARITY.md -- so it only stores
// and echoes back whatever a caller (typically StopExperimentRun) supplies.
type ExperimentRunResult struct {
	ExecutiveSummary   string `json:"ExecutiveSummary,omitempty"`
	ReasonsNotToLaunch string `json:"ReasonsNotToLaunch,omitempty"`
	ReasonsToLaunch    string `json:"ReasonsToLaunch,omitempty"`
}

// ExperimentDefinitionSnapshot captures an ExperimentDefinition's fields at
// the moment StartExperimentRun was called, matching real AWS's "a snapshot
// of the experiment definition at the time the run was started" semantics:
// a later UpdateExperimentDefinition must not retroactively change what an
// already-started run reports it ran against.
type ExperimentDefinitionSnapshot struct {
	ApplicationID          string      `json:"ApplicationId"`
	AudienceDescription    string      `json:"AudienceDescription,omitempty"`
	AudienceRule           string      `json:"AudienceRule"`
	ConfigurationProfileID string      `json:"ConfigurationProfileId"`
	Control                Treatment   `json:"Control"`
	EnvironmentID          string      `json:"EnvironmentId"`
	FlagKey                string      `json:"FlagKey"`
	Hypothesis             string      `json:"Hypothesis,omitempty"`
	ID                     string      `json:"Id"`
	LaunchCriteria         string      `json:"LaunchCriteria,omitempty"`
	Name                   string      `json:"Name"`
	Treatments             []Treatment `json:"Treatments,omitempty"`
}

// ExperimentDefinition represents an AppConfig experiment definition: the
// purpose, scope, and treatment configuration of an A/B test attached to a
// feature-flag configuration profile.
type ExperimentDefinition struct {
	CreatedAt              time.Time   `json:"CreatedAt,omitzero"`
	UpdatedAt              time.Time   `json:"UpdatedAt,omitzero"`
	ApplicationID          string      `json:"ApplicationId"`
	ID                     string      `json:"Id"`
	Name                   string      `json:"Name"`
	ConfigurationProfileID string      `json:"ConfigurationProfileId"`
	EnvironmentID          string      `json:"EnvironmentId"`
	FlagKey                string      `json:"FlagKey"`
	AudienceDescription    string      `json:"AudienceDescription,omitempty"`
	AudienceRule           string      `json:"AudienceRule"`
	Hypothesis             string      `json:"Hypothesis,omitempty"`
	LaunchCriteria         string      `json:"LaunchCriteria,omitempty"`
	KmsKeyIdentifier       string      `json:"KmsKeyIdentifier,omitempty"`
	Status                 string      `json:"Status"`
	Control                Treatment   `json:"Control"`
	Treatments             []Treatment `json:"Treatments,omitempty"`
}

// ExperimentRunEvent records a single lifecycle event -- run start, an
// exposure-percentage change, a treatment-override change, or a run stop --
// observed during an experiment run. Events are recorded by this backend as
// they actually occur (see experiment_runs.go), never fabricated after the
// fact or synthesized as a plausible-looking timeline.
type ExperimentRunEvent struct {
	OccurredAt           time.Time          `json:"OccurredAt,omitzero"`
	TreatmentOverrides   TreatmentOverrides `json:"TreatmentOverrides,omitzero"`
	AssociatedDeployment string             `json:"AssociatedDeployment,omitempty"`
	Description          string             `json:"Description,omitempty"`
	EventType            string             `json:"EventType"`
	TriggeredBy          string             `json:"TriggeredBy,omitempty"`
	ExposurePercentage   float32            `json:"ExposurePercentage,omitempty"`
}

// ExperimentRun represents one execution of an ExperimentDefinition:
// audience exposure, treatment overrides, and status over its lifetime.
// Real GetExperimentRun/StartExperimentRun/etc. output shapes have no
// DeploymentParameters member and no embedded event list -- events are
// retrieved separately via ListExperimentRunEvents -- so Events is
// intentionally unexported from JSON (internal bookkeeping only).
type ExperimentRun struct {
	StartedAt                    time.Time                     `json:"StartedAt,omitzero"`
	EndedAt                      time.Time                     `json:"EndedAt,omitzero"`
	UpdatedAt                    time.Time                     `json:"UpdatedAt,omitzero"`
	ApplicationID                string                        `json:"ApplicationId"`
	ExperimentDefinitionID       string                        `json:"ExperimentDefinitionId"`
	Description                  string                        `json:"Description,omitempty"`
	Status                       string                        `json:"Status"`
	Result                       *ExperimentRunResult          `json:"Result,omitempty"`
	ExperimentDefinitionSnapshot *ExperimentDefinitionSnapshot `json:"ExperimentDefinitionSnapshot,omitempty"`
	TreatmentOverrides           TreatmentOverrides            `json:"TreatmentOverrides,omitzero"`
	Events                       []ExperimentRunEvent          `json:"-"`
	ExposurePercentage           float32                       `json:"ExposurePercentage,omitempty"`
	Run                          int32                         `json:"Run"`
}
