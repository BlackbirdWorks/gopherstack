package codepipeline

import "time"

// EncryptionKey represents a KMS or custom encryption key for an artifact store.
type EncryptionKey struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// ArtifactStore represents the artifact store for a pipeline stage.
type ArtifactStore struct {
	EncryptionKey *EncryptionKey `json:"encryptionKey,omitempty"`
	Type          string         `json:"type"`
	Location      string         `json:"location"`
}

// ActionTypeID represents the identifier for an action type.
type ActionTypeID struct {
	Category string `json:"category"`
	Owner    string `json:"owner"`
	Provider string `json:"provider"`
	Version  string `json:"version"`
}

// ArtifactDetails represents min/max artifact counts for a custom action type.
type ArtifactDetails struct {
	MinimumCount int `json:"minimumCount"`
	MaximumCount int `json:"maximumCount"`
}

// ActionTypeSettings represents the URLs for a custom action type.
type ActionTypeSettings struct {
	EntityURLTemplate          string `json:"entityUrlTemplate,omitempty"`
	ExecutionURLTemplate       string `json:"executionUrlTemplate,omitempty"`
	RevisionURLTemplate        string `json:"revisionUrlTemplate,omitempty"`
	ThirdPartyConfigurationURL string `json:"thirdPartyConfigurationUrl,omitempty"`
}

// ActionConfigurationProperty represents a property in a custom action type's configuration.
type ActionConfigurationProperty struct {
	Description string `json:"description,omitempty"`
	Name        string `json:"name"`
	Type        string `json:"type,omitempty"`
	Key         bool   `json:"key"`
	Queryable   bool   `json:"queryable,omitempty"`
	Required    bool   `json:"required"`
	Secret      bool   `json:"secret"`
}

// CustomActionType represents an in-memory custom action type.
type CustomActionType struct {
	Settings *ActionTypeSettings `json:"settings,omitempty"`
	Tags     map[string]string   `json:"-"`
	// region is the AWS region this custom action type belongs to. It is the
	// outer half of the composite key ("region|category/provider/version")
	// used by the backend's flat store.Table[CustomActionType] (see
	// customActionTypeKeyFn in store_setup.go), which replaces the old
	// map[string]map[customActionTypeKey]*CustomActionType nesting (outer
	// key = region). Unexported so it never appears in wire responses (those
	// are built by marshaling CustomActionType directly, and this field
	// carries no json tag so encoding/json skips it regardless), but
	// persistence.go must carry it through a DTO explicitly since
	// json.Marshal never sees unexported fields.
	region                  string
	Category                string                        `json:"category"`
	Owner                   string                        `json:"owner"`
	Provider                string                        `json:"provider"`
	Version                 string                        `json:"version"`
	ConfigurationProperties []ActionConfigurationProperty `json:"configurationProperties,omitempty"`
	InputArtifactDetails    ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails   ArtifactDetails               `json:"outputArtifactDetails"`
}

// customActionTypeKey is the composite key for a custom action type.
type customActionTypeKey struct {
	Category string
	Provider string
	Version  string
}

// Job represents a CodePipeline job queued for a custom action.
type Job struct {
	ActionTypeID ActionTypeID `json:"actionTypeId,omitzero"`
	// region is the AWS region this job belongs to; the outer half of the
	// composite "region|id" key used by the backend's flat store.Table[Job]
	// (see regionKey in store.go). Unexported so it never appears in wire
	// responses; persistence.go carries it through a DTO explicitly since
	// json.Marshal never sees unexported fields.
	region       string
	ID           string `json:"id"`
	PipelineName string `json:"pipelineName,omitempty"`
	Nonce        string `json:"nonce"`
	Status       string `json:"status"`
}

// WebhookFilter represents a filter applied to incoming webhook payloads.
type WebhookFilter struct {
	JSONPath    string `json:"jsonPath"`
	MatchEquals string `json:"matchEquals,omitempty"`
}

// WebhookAuthConfig holds the authentication configuration for a webhook.
type WebhookAuthConfig struct {
	SecretToken    string `json:"secretToken,omitempty"`
	AllowedIPRange string `json:"allowedIPRange,omitempty"`
}

// Webhook represents a CodePipeline webhook with full AWS-parity fields.
type Webhook struct {
	Tags map[string]string `json:"-"`
	// region is the AWS region this webhook belongs to; the outer half of the
	// composite "region|id" key used by the backend's flat store.Table[Webhook]
	// (see regionKey in store.go). Unexported so it never appears in wire
	// responses; persistence.go carries it through a DTO explicitly since
	// json.Marshal never sees unexported fields.
	region                      string
	AuthenticationConfiguration WebhookAuthConfig `json:"authenticationConfiguration,omitzero"`
	Name                        string            `json:"name"`
	TargetPipeline              string            `json:"targetPipeline"`
	TargetAction                string            `json:"targetAction"`
	Authentication              string            `json:"authentication,omitempty"`
	URL                         string            `json:"url,omitempty"`
	ARN                         string            `json:"arn,omitempty"`
	LastTriggered               string            `json:"lastTriggered,omitempty"`
	Filters                     []WebhookFilter   `json:"filters,omitempty"`
	RegisteredWithThirdParty    bool              `json:"registeredWithThirdParty"`
}

// StageTransitionState holds the disabled state and reason for a pipeline stage transition.
type StageTransitionState struct {
	// region is the AWS region this stage transition belongs to; the outer
	// half of the composite key used by the backend's flat
	// store.Table[StageTransitionState] (see stageTransitionKeyFn in
	// store_setup.go). Unexported so it never appears in wire responses;
	// persistence.go carries it through a DTO explicitly since json.Marshal
	// never sees unexported fields.
	region         string
	PipelineName   string `json:"pipelineName"`
	StageName      string `json:"stageName"`
	TransitionType string `json:"transitionType"`
	Reason         string `json:"reason"`
	Disabled       bool   `json:"disabled"`
}

// stageTransitionKey is the composite key for a stage transition.
type stageTransitionKey struct {
	PipelineName   string
	StageName      string
	TransitionType string
}

// String returns a unique string for k, used to build the composite
// store.Table key for the stageTransitions table (see stageTransitionKeyFn in
// store_setup.go).
func (k stageTransitionKey) String() string {
	return k.PipelineName + "/" + k.StageName + "/" + k.TransitionType
}

// Rule represents a condition rule within a stage condition.
type Rule struct {
	Configuration  map[string]string `json:"configuration,omitempty"`
	RuleTypeID     ActionTypeID      `json:"ruleTypeId"`
	Name           string            `json:"name"`
	RoleArn        string            `json:"roleArn,omitempty"`
	Region         string            `json:"region,omitempty"`
	InputArtifacts []ArtifactRef     `json:"inputArtifacts,omitempty"`
}

// Condition represents a set of rules that control stage entry or exit.
type Condition struct {
	Result string `json:"result,omitempty"`
	Rules  []Rule `json:"rules,omitempty"`
}

// Action represents a single action within a pipeline stage.
type Action struct {
	Configuration    map[string]string `json:"configuration,omitempty"`
	ActionTypeID     ActionTypeID      `json:"actionTypeId"`
	Name             string            `json:"name"`
	RoleArn          string            `json:"roleArn,omitempty"`
	Region           string            `json:"region,omitempty"`
	Namespace        string            `json:"namespace,omitempty"`
	InputArtifacts   []ArtifactRef     `json:"inputArtifacts,omitempty"`
	OutputArtifacts  []ArtifactRef     `json:"outputArtifacts,omitempty"`
	RunOrder         int               `json:"runOrder,omitempty"`
	TimeoutInMinutes int               `json:"timeoutInMinutes,omitempty"`
}

// ArtifactRef represents a reference to an artifact.
type ArtifactRef struct {
	Name string `json:"name"`
}

// Stage represents a pipeline stage.
type Stage struct {
	BeforeEntry *Condition `json:"beforeEntry,omitempty"`
	OnFailure   *Condition `json:"onFailure,omitempty"`
	OnSuccess   *Condition `json:"onSuccess,omitempty"`
	Name        string     `json:"name"`
	Type        string     `json:"type,omitempty"`
	Actions     []Action   `json:"actions"`
}

// GitBranchFilterCriteria is the include/exclude filter for branch names.
type GitBranchFilterCriteria struct {
	Includes []string `json:"includes,omitempty"`
	Excludes []string `json:"excludes,omitempty"`
}

// GitTagFilterCriteria is the include/exclude filter for git tags.
type GitTagFilterCriteria struct {
	Includes []string `json:"includes,omitempty"`
	Excludes []string `json:"excludes,omitempty"`
}

// GitFilePathsFilterCriteria is the include/exclude filter for file paths.
type GitFilePathsFilterCriteria struct {
	Includes []string `json:"includes,omitempty"`
	Excludes []string `json:"excludes,omitempty"`
}

// GitPushFilter describes what git push events trigger the pipeline.
type GitPushFilter struct {
	Branches  *GitBranchFilterCriteria    `json:"branches,omitempty"`
	Tags      *GitTagFilterCriteria       `json:"tags,omitempty"`
	FilePaths *GitFilePathsFilterCriteria `json:"filePaths,omitempty"`
}

// GitPullRequestFilter describes what pull request events trigger the pipeline.
type GitPullRequestFilter struct {
	Branches  *GitBranchFilterCriteria    `json:"branches,omitempty"`
	FilePaths *GitFilePathsFilterCriteria `json:"filePaths,omitempty"`
	Events    []string                    `json:"events,omitempty"`
}

// GitConfiguration holds the source action name and push/PR trigger filters.
type GitConfiguration struct {
	SourceActionName string                 `json:"sourceActionName"`
	Push             []GitPushFilter        `json:"push,omitempty"`
	PullRequest      []GitPullRequestFilter `json:"pullRequest,omitempty"`
}

// Trigger represents a pipeline trigger definition.
type Trigger struct {
	GitConfiguration *GitConfiguration `json:"gitConfiguration,omitempty"`
	ProviderType     string            `json:"providerType"`
}

// PipelineVariable represents a pipeline-level variable declaration.
type PipelineVariable struct {
	Name         string `json:"name"`
	DefaultValue string `json:"defaultValue,omitempty"`
	Description  string `json:"description,omitempty"`
}

// PipelineDeclaration represents the full pipeline structure.
type PipelineDeclaration struct {
	ArtifactStores map[string]ArtifactStore `json:"artifactStores,omitempty"`
	ArtifactStore  ArtifactStore            `json:"artifactStore"`
	Name           string                   `json:"name"`
	RoleArn        string                   `json:"roleArn"`
	PipelineType   string                   `json:"pipelineType,omitempty"`
	ExecutionMode  string                   `json:"executionMode,omitempty"`
	Stages         []Stage                  `json:"stages"`
	Variables      []PipelineVariable       `json:"variables,omitempty"`
	Triggers       []Trigger                `json:"triggers,omitempty"`
	Version        int                      `json:"version"`
}

// PipelineMetadata holds pipeline metadata.
type PipelineMetadata struct {
	PipelineArn string  `json:"pipelineArn"`
	Created     float64 `json:"created"`
	Updated     float64 `json:"updated"`
}

// Pipeline wraps the declaration and metadata.
type Pipeline struct {
	// region is the AWS region this pipeline belongs to; the outer half of
	// the composite "region|name" key used by the backend's flat
	// store.Table[Pipeline] (see regionKey in store.go and pipelineKeyFn in
	// store_setup.go), which replaces the old
	// map[string]map[string]*Pipeline nesting (outer key = region).
	// Unexported so it never appears in wire responses; persistence.go
	// carries it through a DTO explicitly since json.Marshal never sees
	// unexported fields.
	region      string
	Tags        map[string]string   `json:"tags"`
	Declaration PipelineDeclaration `json:"declaration"`
	Metadata    PipelineMetadata    `json:"metadata"`
}

// PipelineSummary is a condensed view of a pipeline for listing.
type PipelineSummary struct {
	PipelineArn   string  `json:"pipelineArn,omitempty"`
	Name          string  `json:"name"`
	PipelineType  string  `json:"pipelineType,omitempty"`
	ExecutionMode string  `json:"executionMode,omitempty"`
	Version       int     `json:"version"`
	Created       float64 `json:"created"`
	Updated       float64 `json:"updated"`
}

// Tag represents a key-value tag.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// PipelineExecution represents a stored pipeline execution.
type PipelineExecution struct {
	PipelineName        string `json:"pipelineName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	Status              string `json:"status"`
	Trigger             string `json:"trigger,omitempty"`
	PipelineVersion     int    `json:"pipelineVersion"`
}

// StageState represents the state of a pipeline stage.
type StageState struct {
	InboundTransitionState  *StageTransitionState
	OutboundTransitionState *StageTransitionState
	StageName               string
	ActionStates            []map[string]any
}

// ActionExecution records a single action's execution within a pipeline run.
type ActionExecution struct {
	StartTime           time.Time `json:"startTime"`
	LastUpdateTime      time.Time `json:"lastUpdateTime"`
	PipelineExecutionID string    `json:"pipelineExecutionId"`
	ActionExecutionID   string    `json:"actionExecutionId"`
	StageName           string    `json:"stageName"`
	ActionName          string    `json:"actionName"`
	Status              string    `json:"status"`
}
