package bedrockagent

import "time"

// defaultIdleSessionTTLSeconds is the default agent idle session TTL (10 minutes),
// matching the AWS Bedrock Agent default.
const defaultIdleSessionTTLSeconds = 600

// ---------------------------------------------------------------------------
// Status constants
// ---------------------------------------------------------------------------

const (
	agentStatusNotPrepared = "NOT_PREPARED"
	agentStatusPreparing   = "PREPARING"
	agentStatusPrepared    = "PREPARED"
	kbStatusActive         = "ACTIVE"
	dsStatusAvailable      = "AVAILABLE"
	aliasStatusPrepared    = "PREPARED"
	// FlowStatus is one of the few bedrockagent enums that is NOT
	// SCREAMING_SNAKE_CASE on the wire: the real aws-sdk-go-v2 enum values
	// are "Prepared"/"Preparing"/"NotPrepared"/"Failed" (see
	// types.FlowStatus in aws-sdk-go-v2/service/bedrockagent). Every other
	// status enum in this file (AgentStatus, DataSourceStatus,
	// KnowledgeBaseStatus, ...) IS upper-snake-case -- don't "fix" these to
	// match that pattern.
	flowStatusPrepared    = "Prepared"
	flowStatusNotPrepared = "NotPrepared"
	ingestionJobRunning   = "IN_PROGRESS"
	ingestionJobComplete  = "COMPLETE"
	actionGroupEnabled    = "ENABLED"
	collabEnabled         = "ENABLED"
	docStatusIndexed      = "INDEXED"
	defaultAgentVersion   = "DRAFT"

	bedrockAgentService = "bedrock"
)

// ---------------------------------------------------------------------------
// Config structs
// ---------------------------------------------------------------------------

// AgentConfig holds fields for creating or updating an Agent.
type AgentConfig struct {
	Tags                    map[string]string
	Guardrail               map[string]any
	Memory                  map[string]any
	AgentName               string
	Collaboration           string
	Description             string
	FoundationModel         string
	Instruction             string
	RoleARN                 string
	IdleSessionTTLInSeconds int
}

// ActionGroupConfig holds fields for creating or updating an AgentActionGroup.
type ActionGroupConfig struct {
	ActionGroupExecutor map[string]any
	APISchema           map[string]any
	FunctionSchema      map[string]any
	ActionGroupName     string
	Description         string
	ActionGroupState    string
}

// AliasConfig holds fields for creating or updating an AgentAlias.
type AliasConfig struct {
	Tags                 map[string]string
	AliasName            string
	Description          string
	RoutingConfiguration []AliasRouting
}

// CollaboratorConfig holds fields for an AgentCollaborator.
type CollaboratorConfig struct {
	AgentDescriptor          map[string]any
	CollaboratorName         string
	CollaborationInstruction string
	RelayConversationHistory string
}

// KnowledgeBaseConfig holds fields for creating or updating a KnowledgeBase.
type KnowledgeBaseConfig struct {
	Tags                 map[string]string
	KBConfiguration      map[string]any
	StorageConfiguration map[string]any
	Name                 string
	Description          string
	RoleARN              string
}

// DataSourceConfig holds fields for creating or updating a DataSource.
type DataSourceConfig struct {
	DataSourceConfiguration map[string]any
	VectorIngestionConfig   map[string]any
	Name                    string
	Description             string
	DataDeletionPolicy      string
}

// FlowConfig holds fields for creating or updating a Flow.
type FlowConfig struct {
	Tags        map[string]string
	Definition  map[string]any
	Name        string
	Description string
	RoleARN     string
}

// FlowAliasConfig holds fields for creating or updating a FlowAlias.
type FlowAliasConfig struct {
	Tags                 map[string]string
	Name                 string
	Description          string
	RoutingConfiguration []FlowAliasRouting
}

// PromptConfig holds fields for creating or updating a Prompt.
type PromptConfig struct {
	Tags           map[string]string
	Name           string
	Description    string
	DefaultVariant string
	Variants       []map[string]any
}

// KBDocument is a knowledge base document for ingestion.
type KBDocument struct {
	Metadata map[string]any
	Content  map[string]any
	DocID    string
}

// ---------------------------------------------------------------------------
// Model types
// ---------------------------------------------------------------------------

// Agent represents a Bedrock Agent.
type Agent struct {
	CreatedAt                   time.Time         `json:"createdAt"`
	UpdatedAt                   time.Time         `json:"updatedAt"`
	PreparedAt                  *time.Time        `json:"preparedAt,omitempty"`
	Tags                        map[string]string `json:"tags,omitempty"`
	Guardrail                   map[string]any    `json:"guardrailConfiguration,omitempty"`
	Memory                      map[string]any    `json:"memoryConfiguration,omitempty"`
	PromptOverrideConfiguration map[string]any    `json:"promptOverrideConfiguration"`
	AgentID                     string            `json:"agentId"`
	AgentARN                    string            `json:"agentArn"`
	AgentName                   string            `json:"agentName"`
	AgentVersion                string            `json:"agentVersion"`
	AgentStatus                 string            `json:"agentStatus"`
	Collaboration               string            `json:"agentCollaboration"`
	Description                 string            `json:"description,omitempty"`
	FoundationModel             string            `json:"foundationModel,omitempty"`
	Instruction                 string            `json:"instruction,omitempty"`
	RoleARN                     string            `json:"agentResourceRoleArn,omitempty"`
	IdleSessionTTLInSeconds     int               `json:"idleSessionTTLInSeconds"`
}

// AgentSummary is the condensed agent representation used in list responses.
type AgentSummary struct {
	UpdatedAt   time.Time `json:"updatedAt"`
	AgentID     string    `json:"agentId"`
	AgentName   string    `json:"agentName"`
	AgentStatus string    `json:"agentStatus"`
	Description string    `json:"description,omitempty"`
}

// AgentVersion holds a snapshot version of an agent.
type AgentVersion struct {
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	AgentID         string    `json:"agentId"`
	AgentARN        string    `json:"agentArn"`
	AgentName       string    `json:"agentName"`
	AgentStatus     string    `json:"agentStatus"`
	AgentVersion    string    `json:"agentVersion"`
	Description     string    `json:"description,omitempty"`
	FoundationModel string    `json:"foundationModel,omitempty"`
	Instruction     string    `json:"instruction,omitempty"`
	RoleARN         string    `json:"agentResourceRoleArn,omitempty"`
}

// AgentVersionSummary is used in list-agent-versions responses.
type AgentVersionSummary struct {
	UpdatedAt    time.Time `json:"updatedAt"`
	AgentName    string    `json:"agentName"`
	AgentStatus  string    `json:"agentStatus"`
	AgentVersion string    `json:"agentVersion"`
	Description  string    `json:"description,omitempty"`
}

// AgentActionGroup is an action group attached to an agent version.
type AgentActionGroup struct {
	CreatedAt           time.Time      `json:"createdAt"`
	UpdatedAt           time.Time      `json:"updatedAt"`
	ActionGroupExecutor map[string]any `json:"actionGroupExecutor,omitempty"`
	APISchema           map[string]any `json:"apiSchema,omitempty"`
	FunctionSchema      map[string]any `json:"functionSchema,omitempty"`
	ActionGroupID       string         `json:"actionGroupId"`
	ActionGroupName     string         `json:"actionGroupName"`
	AgentID             string         `json:"agentId"`
	AgentVersion        string         `json:"agentVersion"`
	ActionGroupState    string         `json:"actionGroupState"`
	Description         string         `json:"description,omitempty"`
}

// ActionGroupSummary is used in list responses.
type ActionGroupSummary struct {
	ActionGroupID    string `json:"actionGroupId"`
	ActionGroupName  string `json:"actionGroupName"`
	ActionGroupState string `json:"actionGroupState"`
	Description      string `json:"description,omitempty"`
}

// AliasRouting maps an alias to an agent version.
type AliasRouting struct {
	AgentVersion string `json:"agentVersion"`
}

// AgentAlias routes traffic to a specific agent version.
type AgentAlias struct {
	CreatedAt            time.Time         `json:"createdAt"`
	UpdatedAt            time.Time         `json:"updatedAt"`
	Tags                 map[string]string `json:"tags,omitempty"`
	AgentAliasID         string            `json:"agentAliasId"`
	AgentAliasARN        string            `json:"agentAliasArn"`
	AgentAliasName       string            `json:"agentAliasName"`
	AgentAliasStatus     string            `json:"agentAliasStatus"`
	AgentID              string            `json:"agentId"`
	Description          string            `json:"description,omitempty"`
	RoutingConfiguration []AliasRouting    `json:"routingConfiguration"`
}

// AgentAliasSummary is used in list responses.
type AgentAliasSummary struct {
	AgentAliasID     string `json:"agentAliasId"`
	AgentAliasName   string `json:"agentAliasName"`
	AgentAliasStatus string `json:"agentAliasStatus"`
	Description      string `json:"description,omitempty"`
}

// AgentCollaborator links two agents for multi-agent collaboration.
type AgentCollaborator struct {
	CreatedAt                time.Time      `json:"createdAt"`
	UpdatedAt                time.Time      `json:"updatedAt"`
	AgentDescriptor          map[string]any `json:"agentDescriptor,omitempty"`
	AgentID                  string         `json:"agentId"`
	AgentVersion             string         `json:"agentVersion"`
	CollaboratorID           string         `json:"collaboratorId"`
	CollaboratorName         string         `json:"collaboratorName"`
	CollaborationInstruction string         `json:"collaborationInstruction,omitempty"`
	RelayConversationHistory string         `json:"relayConversationHistory,omitempty"`
	CollaboratorStatus       string         `json:"collaboratorStatus"`
}

// KnowledgeBase is a Bedrock Knowledge Base.
type KnowledgeBase struct {
	CreatedAt            time.Time         `json:"createdAt"`
	UpdatedAt            time.Time         `json:"updatedAt"`
	Tags                 map[string]string `json:"tags,omitempty"`
	KBConfiguration      map[string]any    `json:"knowledgeBaseConfiguration,omitempty"`
	StorageConfiguration map[string]any    `json:"storageConfiguration,omitempty"`
	KnowledgeBaseID      string            `json:"knowledgeBaseId"`
	KnowledgeBaseARN     string            `json:"knowledgeBaseArn"`
	Name                 string            `json:"name"`
	Status               string            `json:"status"`
	Description          string            `json:"description,omitempty"`
	RoleARN              string            `json:"roleArn,omitempty"`
}

// KnowledgeBaseSummary is used in list responses.
type KnowledgeBaseSummary struct {
	UpdatedAt       time.Time `json:"updatedAt"`
	KnowledgeBaseID string    `json:"knowledgeBaseId"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	Description     string    `json:"description,omitempty"`
}

// AgentKnowledgeBase is the association between an agent and a knowledge base.
type AgentKnowledgeBase struct {
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	AgentID         string    `json:"agentId"`
	AgentVersion    string    `json:"agentVersion"`
	KnowledgeBaseID string    `json:"knowledgeBaseId"`
	KBState         string    `json:"knowledgeBaseState"`
	Description     string    `json:"description,omitempty"`
}

// DataSource is a knowledge base data source.
type DataSource struct {
	CreatedAt               time.Time      `json:"createdAt"`
	UpdatedAt               time.Time      `json:"updatedAt"`
	DataSourceConfiguration map[string]any `json:"dataSourceConfiguration,omitempty"`
	VectorIngestionConfig   map[string]any `json:"vectorIngestionConfiguration,omitempty"`
	DataSourceID            string         `json:"dataSourceId"`
	KnowledgeBaseID         string         `json:"knowledgeBaseId"`
	Name                    string         `json:"name"`
	DataSourceStatus        string         `json:"dataSourceStatus"`
	Description             string         `json:"description,omitempty"`
	DataDeletionPolicy      string         `json:"dataDeletionPolicy,omitempty"`
}

// DataSourceSummary is used in list responses.
type DataSourceSummary struct {
	UpdatedAt        time.Time `json:"updatedAt"`
	DataSourceID     string    `json:"dataSourceId"`
	KnowledgeBaseID  string    `json:"knowledgeBaseId"`
	Name             string    `json:"name"`
	DataSourceStatus string    `json:"dataSourceStatus"`
	Description      string    `json:"description,omitempty"`
}

// IngestionJob is a knowledge base data ingestion job.
type IngestionJob struct {
	StartedAt       time.Time `json:"startedAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	IngestionJobID  string    `json:"ingestionJobId"`
	KnowledgeBaseID string    `json:"knowledgeBaseId"`
	DataSourceID    string    `json:"dataSourceId"`
	Status          string    `json:"status"`
	Description     string    `json:"description,omitempty"`
}

// Flow is a Bedrock prompt flow.
type Flow struct {
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	Definition  map[string]any    `json:"definition,omitempty"`
	FlowID      string            `json:"id"`
	FlowARN     string            `json:"arn"`
	Name        string            `json:"name"`
	Status      string            `json:"status"`
	Description string            `json:"description,omitempty"`
	RoleARN     string            `json:"executionRoleArn,omitempty"`
	Version     string            `json:"version"`
}

// FlowSummary is used in list responses.
type FlowSummary struct {
	UpdatedAt   time.Time `json:"updatedAt"`
	FlowID      string    `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Description string    `json:"description,omitempty"`
	Version     string    `json:"version"`
}

// FlowVersion is a snapshot of a flow.
type FlowVersion struct {
	CreatedAt   time.Time      `json:"createdAt"`
	Definition  map[string]any `json:"definition,omitempty"`
	FlowARN     string         `json:"arn"`
	FlowID      string         `json:"id"`
	Name        string         `json:"name"`
	Status      string         `json:"status"`
	Version     string         `json:"version"`
	Description string         `json:"description,omitempty"`
}

// FlowVersionSummary is used in list responses.
type FlowVersionSummary struct {
	CreatedAt   time.Time `json:"createdAt"`
	Arn         string    `json:"arn"`
	FlowID      string    `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Version     string    `json:"version"`
	Description string    `json:"description,omitempty"`
}

// FlowAliasRouting maps a flow alias to a specific flow version.
type FlowAliasRouting struct {
	FlowVersion string `json:"flowVersion"`
}

// FlowAlias routes traffic to a specific flow version.
type FlowAlias struct {
	CreatedAt            time.Time          `json:"createdAt"`
	UpdatedAt            time.Time          `json:"updatedAt"`
	Tags                 map[string]string  `json:"tags,omitempty"`
	AliasID              string             `json:"id"`
	AliasARN             string             `json:"arn"`
	FlowID               string             `json:"flowId"`
	Name                 string             `json:"name"`
	Description          string             `json:"description,omitempty"`
	RoutingConfiguration []FlowAliasRouting `json:"routingConfiguration,omitempty"`
}

// FlowAliasSummary is used in list responses.
type FlowAliasSummary struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	AliasID     string    `json:"id"`
	AliasARN    string    `json:"arn"`
	FlowID      string    `json:"flowId"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
}

// FlowValidationError is a flow definition validation error.
type FlowValidationError struct {
	Message  string `json:"message"`
	Severity string `json:"severity"`
}

// Prompt is a Bedrock Prompt resource.
type Prompt struct {
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedAt      time.Time         `json:"updatedAt"`
	Tags           map[string]string `json:"tags,omitempty"`
	PromptID       string            `json:"id"`
	PromptARN      string            `json:"arn"`
	Name           string            `json:"name"`
	Description    string            `json:"description,omitempty"`
	DefaultVariant string            `json:"defaultVariant,omitempty"`
	Version        string            `json:"version"`
	Variants       []map[string]any  `json:"variants,omitempty"`
}

// PromptSummary is used in list responses.
type PromptSummary struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	PromptID    string    `json:"id"`
	PromptARN   string    `json:"arn"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Version     string    `json:"version"`
}

// PromptVersion is an immutable snapshot of a prompt.
type PromptVersion struct {
	CreatedAt   time.Time        `json:"createdAt"`
	PromptARN   string           `json:"arn"`
	PromptID    string           `json:"id"`
	Name        string           `json:"name"`
	Version     string           `json:"version"`
	Description string           `json:"description,omitempty"`
	Variants    []map[string]any `json:"variants,omitempty"`
}

// KBDocumentDetail is the status of a knowledge base document operation.
type KBDocumentDetail struct {
	DocumentID      string `json:"documentId"`
	KnowledgeBaseID string `json:"knowledgeBaseId"`
	DataSourceID    string `json:"dataSourceId"`
	Status          string `json:"status"`
}
