package bedrockagent

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// defaultIdleSessionTTLSeconds is the default agent idle session TTL (10 minutes),
// matching the AWS Bedrock Agent default.
const defaultIdleSessionTTLSeconds = 600

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the given name already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// ---------------------------------------------------------------------------
// Context key
// ---------------------------------------------------------------------------

type regionKey struct{}

func ctxRegion(ctx context.Context, dflt string) string {
	if r, ok := ctx.Value(regionKey{}).(string); ok && r != "" {
		return r
	}

	return dflt
}

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
	flowStatusPrepared     = "PREPARED"
	flowStatusNotPrepared  = "NOT_PREPARED"
	ingestionJobRunning    = "IN_PROGRESS"
	ingestionJobComplete   = "COMPLETE"
	actionGroupEnabled     = "ENABLED"
	collabEnabled          = "ENABLED"
	docStatusIndexed       = "INDEXED"
	defaultAgentVersion    = "DRAFT"

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

// ---------------------------------------------------------------------------
// InMemoryBackend
// ---------------------------------------------------------------------------

// InMemoryBackend implements StorageBackend with in-memory maps, isolated by region.
//
// Phase 3.3 datalayer conversion: every map[string]*T resource collection is
// a *store.Table[T] registered on b.registry, with a *store.Index[T] added
// where a parent-scoped List/bulk-delete previously did a linear key-prefix
// scan (see store_setup.go's registerAllTables). Resources with a real,
// globally-unique identity field (agents, knowledgeBases, flows, prompts)
// are registered directly by that ID; resources whose ID is only unique
// within a parent (agent/flow/knowledge-base versions, action groups,
// aliases, collaborators, associations, data sources, ingestion jobs, KB
// documents) keep the same "parent/child[/grandchild]" composite key the
// original map already used, matching the composite-key rule in
// .claude/memories/parity-principles.md.
//
// agentsByName, kbsByName, flowsByName, and promptsByName are left as plain
// map[string]string reverse-lookup caches (name -> ID): their values are
// bare strings, not *T, so there is nothing for store.Table to key on.
// agentVersionCtrs, flowVersionCtrs, and promptVersionCtrs (map[string]int)
// are likewise left raw for the same reason. tags (map[string]map[string]string)
// is the one remaining grouping map with a non-*T value.
type InMemoryBackend struct {
	kbDocuments             *store.Table[KBDocumentDetail]
	kbDocumentsByDataSource *store.Index[KBDocumentDetail]

	agentsByName map[string]string

	agents *store.Table[Agent]

	agentVersions        *store.Table[AgentVersion]
	agentVersionsByAgent *store.Index[AgentVersion]

	actionGroups               *store.Table[AgentActionGroup]
	actionGroupsByAgentVersion *store.Index[AgentActionGroup]

	agentAliases        *store.Table[AgentAlias]
	agentAliasesByAgent *store.Index[AgentAlias]

	agentCollaborators               *store.Table[AgentCollaborator]
	agentCollaboratorsByAgentVersion *store.Index[AgentCollaborator]

	knowledgeBases *store.Table[KnowledgeBase]
	kbsByName      map[string]string

	agentKBAssocs               *store.Table[AgentKnowledgeBase]
	agentKBAssocsByAgentVersion *store.Index[AgentKnowledgeBase]

	dataSources     *store.Table[DataSource]
	dataSourcesByKB *store.Index[DataSource]

	ingestionJobs             *store.Table[IngestionJob]
	ingestionJobsByDataSource *store.Index[IngestionJob]

	flows       *store.Table[Flow]
	flowsByName map[string]string

	flowVersions       *store.Table[FlowVersion]
	flowVersionsByFlow *store.Index[FlowVersion]

	flowAliases       *store.Table[FlowAlias]
	flowAliasesByFlow *store.Index[FlowAlias]

	prompts *store.Table[Prompt]

	promptVersions         *store.Table[PromptVersion]
	promptVersionsByPrompt *store.Index[PromptVersion]
	promptsByName          map[string]string
	promptVersionCtrs      map[string]int

	tags             map[string]map[string]string
	flowVersionCtrs  map[string]int
	agentVersionCtrs map[string]int

	registry *store.Registry

	accountID          string
	defaultRegion      string
	dsCounter          int
	collabCounter      int
	kbCounter          int
	flowCounter        int
	aliasCounter       int
	agentCounter       int
	actionGroupCounter int
	flowAliasCounter   int
	promptCounter      int
	jobCounter         int
	mu                 sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates and initialises an InMemoryBackend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:          store.NewRegistry(),
		agentsByName:      make(map[string]string),
		kbsByName:         make(map[string]string),
		flowsByName:       make(map[string]string),
		promptsByName:     make(map[string]string),
		tags:              make(map[string]map[string]string),
		agentVersionCtrs:  make(map[string]int),
		flowVersionCtrs:   make(map[string]int),
		promptVersionCtrs: make(map[string]int),
		defaultRegion:     region,
		accountID:         accountID,
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state (used in tests).
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.agentsByName = make(map[string]string)
	b.kbsByName = make(map[string]string)
	b.flowsByName = make(map[string]string)
	b.promptsByName = make(map[string]string)
	b.tags = make(map[string]map[string]string)
	b.agentVersionCtrs = make(map[string]int)
	b.flowVersionCtrs = make(map[string]int)
	b.promptVersionCtrs = make(map[string]int)
	b.agentCounter = 0
	b.actionGroupCounter = 0
	b.aliasCounter = 0
	b.collabCounter = 0
	b.kbCounter = 0
	b.dsCounter = 0
	b.jobCounter = 0
	b.flowCounter = 0
	b.flowAliasCounter = 0
	b.promptCounter = 0
}

// ---------------------------------------------------------------------------
// ID/ARN helpers
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) nextID(prefix string, counter *int) string {
	*counter++

	return fmt.Sprintf("%s-%08d", prefix, *counter)
}

func (b *InMemoryBackend) buildAgentARN(region, agentID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "agent/"+agentID)
}

func (b *InMemoryBackend) buildKBARN(region, kbID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "knowledge-base/"+kbID)
}

func (b *InMemoryBackend) buildFlowARN(region, flowID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "flow/"+flowID)
}

func (b *InMemoryBackend) buildPromptARN(region, promptID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "prompt/"+promptID)
}

func (b *InMemoryBackend) buildAliasARN(region, agentID, aliasID string) string {
	return arn.Build(
		bedrockAgentService,
		region,
		b.accountID,
		fmt.Sprintf("agent-alias/%s/%s", agentID, aliasID),
	)
}

func (b *InMemoryBackend) buildFlowAliasARN(region, flowID, aliasID string) string {
	return arn.Build(
		bedrockAgentService,
		region,
		b.accountID,
		fmt.Sprintf("flow-alias/%s/%s", flowID, aliasID),
	)
}

// ---------------------------------------------------------------------------
// store.Table composite-key helpers
// ---------------------------------------------------------------------------

// agentVersionScope is the composite key shared by every table nested two
// levels under an agent (actionGroups, agentCollaborators, agentKBAssocs):
// agentID + "/" + agentVersion. It doubles as the byAgentVersion secondary
// index key for each of those tables.
func agentVersionScope(agentID, agentVersion string) string {
	return agentID + "/" + agentVersion
}

func agentVersionKey(agentID, version string) string { return agentID + "/" + version }

func agentCollabKey(agentID, agentVersion, collaboratorID string) string {
	return agentVersionScope(agentID, agentVersion) + "/" + collaboratorID
}

func flowVersionKey(flowID, version string) string { return flowID + "/" + version }

func promptVersionKey(promptID, version string) string { return promptID + "/" + version }

// ---------------------------------------------------------------------------
// Agent CRUD
// ---------------------------------------------------------------------------

// CreateAgent creates a new agent.
func (b *InMemoryBackend) CreateAgent(ctx context.Context, cfg AgentConfig) (*Agent, error) {
	if cfg.AgentName == "" {
		return nil, fmt.Errorf("%w: agentName is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.agentsByName[cfg.AgentName]; exists {
		return nil, fmt.Errorf("%w: agent %q already exists", ErrAlreadyExists, cfg.AgentName)
	}

	id := b.nextID("agent", &b.agentCounter)
	now := time.Now().UTC()

	collab := cfg.Collaboration
	if collab == "" {
		collab = "DISABLED"
	}

	a := &Agent{
		AgentID:         id,
		AgentARN:        b.buildAgentARN(region, id),
		AgentName:       cfg.AgentName,
		AgentVersion:    defaultAgentVersion,
		AgentStatus:     agentStatusNotPrepared,
		Collaboration:   collab,
		Description:     cfg.Description,
		FoundationModel: cfg.FoundationModel,
		Instruction:     cfg.Instruction,
		RoleARN:         cfg.RoleARN,
		Tags:            maps.Clone(cfg.Tags),
		Guardrail:       cfg.Guardrail,
		Memory:          cfg.Memory,
		PromptOverrideConfiguration: map[string]any{
			"promptConfigurations": []any{},
		},
		IdleSessionTTLInSeconds: ttlOrDefault(cfg.IdleSessionTTLInSeconds),
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	b.agents.Put(a)
	b.agentsByName[cfg.AgentName] = id
	b.tags[a.AgentARN] = maps.Clone(cfg.Tags)

	return agentCopy(a), nil
}

// GetAgent returns an agent by ID.
func (b *InMemoryBackend) GetAgent(_ context.Context, agentID string) (*Agent, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	return agentCopy(a), nil
}

// UpdateAgent updates an existing agent.
func (b *InMemoryBackend) UpdateAgent(_ context.Context, agentID string, cfg AgentConfig) (*Agent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if cfg.AgentName != "" && cfg.AgentName != a.AgentName {
		if _, exists := b.agentsByName[cfg.AgentName]; exists {
			return nil, fmt.Errorf("%w: agent name %q already in use", ErrAlreadyExists, cfg.AgentName)
		}

		delete(b.agentsByName, a.AgentName)
		b.agentsByName[cfg.AgentName] = agentID
		a.AgentName = cfg.AgentName
	}

	applyAgentConfig(a, cfg)
	a.UpdatedAt = time.Now().UTC()

	return agentCopy(a), nil
}

func ttlOrDefault(ttl int) int {
	if ttl > 0 {
		return ttl
	}

	return defaultIdleSessionTTLSeconds
}

func applyAgentConfig(a *Agent, cfg AgentConfig) {
	if cfg.Collaboration != "" {
		a.Collaboration = cfg.Collaboration
	}

	if cfg.Description != "" {
		a.Description = cfg.Description
	}

	if cfg.FoundationModel != "" {
		a.FoundationModel = cfg.FoundationModel
	}

	if cfg.Instruction != "" {
		a.Instruction = cfg.Instruction
	}

	if cfg.RoleARN != "" {
		a.RoleARN = cfg.RoleARN
	}

	if cfg.Guardrail != nil {
		a.Guardrail = cfg.Guardrail
	}

	if cfg.Memory != nil {
		a.Memory = cfg.Memory
	}

	if cfg.IdleSessionTTLInSeconds > 0 {
		a.IdleSessionTTLInSeconds = cfg.IdleSessionTTLInSeconds
	}
}

// DeleteAgent deletes an agent.
//
// Note: this only removes the agent itself, its name-lookup entry, its
// versions (agentVersions), and its version counter -- it does NOT clean up
// actionGroups, agentAliases, or agentCollaborators for the deleted agent
// (the pre-Phase-3.3 map version keyed agentCollaborators by "agentID/"+
// agentVersion and called delete(b.agentCollaborators, agentID) here, which
// never matched any real key -- a no-op cleanup bug preserved as-is by this
// conversion; see the DeleteAgent doc in .claude/memories for the
// byte-for-byte preservation rule).
func (b *InMemoryBackend) DeleteAgent(_ context.Context, agentID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	delete(b.agentsByName, a.AgentName)
	b.agents.Delete(agentID)

	for _, av := range slices.Clone(b.agentVersionsByAgent.Get(agentID)) {
		b.agentVersions.Delete(agentVersionKey(av.AgentID, av.AgentVersion))
	}

	delete(b.agentVersionCtrs, agentID)

	return nil
}

// ListAgents returns a paginated list of agent summaries.
func (b *InMemoryBackend) ListAgents(
	_ context.Context, maxResults int, nextToken string,
) ([]*AgentSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := tableIDs(b.agents.Snapshot(), func(a *Agent) string { return a.AgentID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentSummary, 0, len(ids))

	for _, id := range ids {
		a, _ := b.agents.Get(id)
		out = append(out, &AgentSummary{
			AgentID:     a.AgentID,
			AgentName:   a.AgentName,
			AgentStatus: a.AgentStatus,
			Description: a.Description,
			UpdatedAt:   a.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// PrepareAgent transitions agent to PREPARED status.
func (b *InMemoryBackend) PrepareAgent(_ context.Context, agentID string) (*Agent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	now := time.Now().UTC()
	a.AgentStatus = agentStatusPrepared
	a.UpdatedAt = now
	a.PreparedAt = &now

	return agentCopy(a), nil
}

// ---------------------------------------------------------------------------
// Agent version CRUD
// ---------------------------------------------------------------------------

// CreateAgentVersion creates a numbered snapshot of an agent.
func (b *InMemoryBackend) CreateAgentVersion(
	_ context.Context, agentID, description string,
) (*AgentVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.agents.Get(agentID)
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.agentVersionCtrs[agentID]++
	versionNum := b.agentVersionCtrs[agentID]
	version := strconv.Itoa(versionNum)

	now := time.Now().UTC()
	av := &AgentVersion{
		AgentID:         agentID,
		AgentARN:        a.AgentARN,
		AgentName:       a.AgentName,
		AgentVersion:    version,
		AgentStatus:     agentStatusPrepared,
		FoundationModel: a.FoundationModel,
		Instruction:     a.Instruction,
		RoleARN:         a.RoleARN,
		Description:     description,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	b.agentVersions.Put(av)

	return agentVersionCopy(av), nil
}

// GetAgentVersion returns a specific agent version.
//
// Not-found precedence note: the pre-Phase-3.3 map checked for the presence
// of the per-agent inner map (created lazily by the first
// CreateAgentVersion call and never removed except by DeleteAgent) to decide
// between "agent not found" and "agent version not found". That inner-map
// presence check is not reproducible with a flat store.Table + secondary
// Index (an Index prunes a group the moment its last member is deleted, see
// pkgs/store's Index.remove), so this checks b.agents.Has(agentID) instead --
// identical result in every case except the one where every version of a
// still-existing agent has been deleted via DeleteAgentVersion, where the
// pre-conversion code returned "agent not found" (arguably itself a
// mislabeled error) and this returns "agent version not found".
func (b *InMemoryBackend) GetAgentVersion(
	_ context.Context, agentID, agentVersion string,
) (*AgentVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	av, ok := b.agentVersions.Get(agentVersionKey(agentID, agentVersion))
	if !ok {
		return nil, fmt.Errorf("%w: agent version %q not found", ErrNotFound, agentVersion)
	}

	return agentVersionCopy(av), nil
}

// DeleteAgentVersion deletes an agent version. See the not-found precedence
// note on GetAgentVersion.
func (b *InMemoryBackend) DeleteAgentVersion(
	_ context.Context, agentID, agentVersion string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	key := agentVersionKey(agentID, agentVersion)
	if !b.agentVersions.Has(key) {
		return fmt.Errorf("%w: agent version %q not found", ErrNotFound, agentVersion)
	}

	b.agentVersions.Delete(key)

	return nil
}

// ListAgentVersions returns paginated agent version summaries.
func (b *InMemoryBackend) ListAgentVersions(
	_ context.Context, agentID string, maxResults int, nextToken string,
) ([]*AgentVersionSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.agents.Has(agentID) {
		return nil, "", fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	group := b.agentVersionsByAgent.Get(agentID)
	keys := tableIDs(group, func(av *AgentVersion) string { return av.AgentVersion })
	keys, outToken := paginate(keys, nextToken, maxResults)

	out := make([]*AgentVersionSummary, 0, len(keys))

	for _, k := range keys {
		av, _ := b.agentVersions.Get(agentVersionKey(agentID, k))
		out = append(out, &AgentVersionSummary{
			AgentName:    av.AgentName,
			AgentVersion: av.AgentVersion,
			AgentStatus:  av.AgentStatus,
			Description:  av.Description,
			UpdatedAt:    av.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Agent action group CRUD
// ---------------------------------------------------------------------------

func agActionGroupKey(agentID, agentVersion, actionGroupID string) string {
	return agentID + "/" + agentVersion + "/" + actionGroupID
}

// CreateAgentActionGroup creates an action group for an agent version.
func (b *InMemoryBackend) CreateAgentActionGroup(
	_ context.Context, agentID string, cfg ActionGroupConfig,
) (*AgentActionGroup, error) {
	if cfg.ActionGroupName == "" {
		return nil, fmt.Errorf("%w: actionGroupName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	id := b.nextID("ag", &b.actionGroupCounter)
	agentVersion := defaultAgentVersion
	now := time.Now().UTC()

	ag := &AgentActionGroup{
		ActionGroupID:       id,
		ActionGroupName:     cfg.ActionGroupName,
		AgentID:             agentID,
		AgentVersion:        agentVersion,
		ActionGroupState:    actionGroupEnabled,
		Description:         cfg.Description,
		ActionGroupExecutor: cfg.ActionGroupExecutor,
		APISchema:           cfg.APISchema,
		FunctionSchema:      cfg.FunctionSchema,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if cfg.ActionGroupState != "" {
		ag.ActionGroupState = cfg.ActionGroupState
	}

	b.actionGroups.Put(ag)

	return actionGroupCopy(ag), nil
}

// GetAgentActionGroup returns an action group.
func (b *InMemoryBackend) GetAgentActionGroup(
	_ context.Context, agentID, agentVersion, actionGroupID string,
) (*AgentActionGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ag, ok := b.actionGroups.Get(agActionGroupKey(agentID, agentVersion, actionGroupID))
	if !ok {
		return nil, fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	return actionGroupCopy(ag), nil
}

// UpdateAgentActionGroup updates an action group.
func (b *InMemoryBackend) UpdateAgentActionGroup(
	_ context.Context, agentID, agentVersion, actionGroupID string, cfg ActionGroupConfig,
) (*AgentActionGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := agActionGroupKey(agentID, agentVersion, actionGroupID)

	ag, ok := b.actionGroups.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	applyActionGroupConfig(ag, cfg)
	ag.UpdatedAt = time.Now().UTC()

	return actionGroupCopy(ag), nil
}

func applyActionGroupConfig(ag *AgentActionGroup, cfg ActionGroupConfig) {
	if cfg.ActionGroupName != "" {
		ag.ActionGroupName = cfg.ActionGroupName
	}

	if cfg.Description != "" {
		ag.Description = cfg.Description
	}

	if cfg.ActionGroupState != "" {
		ag.ActionGroupState = cfg.ActionGroupState
	}

	if cfg.ActionGroupExecutor != nil {
		ag.ActionGroupExecutor = cfg.ActionGroupExecutor
	}

	if cfg.APISchema != nil {
		ag.APISchema = cfg.APISchema
	}

	if cfg.FunctionSchema != nil {
		ag.FunctionSchema = cfg.FunctionSchema
	}
}

// DeleteAgentActionGroup deletes an action group.
func (b *InMemoryBackend) DeleteAgentActionGroup(
	_ context.Context, agentID, agentVersion, actionGroupID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := agActionGroupKey(agentID, agentVersion, actionGroupID)

	if !b.actionGroups.Has(key) {
		return fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	b.actionGroups.Delete(key)

	return nil
}

// ListAgentActionGroups returns all action groups for an agent version.
func (b *InMemoryBackend) ListAgentActionGroups(
	_ context.Context, agentID, agentVersion string, maxResults int, nextToken string,
) ([]*ActionGroupSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.actionGroupsByAgentVersion.Get(agentVersionScope(agentID, agentVersion))
	ids := tableIDs(group, func(ag *AgentActionGroup) string { return ag.ActionGroupID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*ActionGroupSummary, 0, len(ids))

	for _, id := range ids {
		ag, _ := b.actionGroups.Get(agActionGroupKey(agentID, agentVersion, id))
		out = append(out, &ActionGroupSummary{
			ActionGroupID:    ag.ActionGroupID,
			ActionGroupName:  ag.ActionGroupName,
			ActionGroupState: ag.ActionGroupState,
			Description:      ag.Description,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Agent alias CRUD
// ---------------------------------------------------------------------------

func aliasKey(agentID, aliasID string) string { return agentID + "/" + aliasID }

// CreateAgentAlias creates an alias for an agent.
func (b *InMemoryBackend) CreateAgentAlias(
	ctx context.Context, agentID string, cfg AliasConfig,
) (*AgentAlias, error) {
	if cfg.AliasName == "" {
		return nil, fmt.Errorf("%w: agentAliasName is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	id := b.nextID("alias", &b.aliasCounter)
	now := time.Now().UTC()

	al := &AgentAlias{
		AgentAliasID:         id,
		AgentAliasARN:        b.buildAliasARN(region, agentID, id),
		AgentAliasName:       cfg.AliasName,
		AgentAliasStatus:     aliasStatusPrepared,
		AgentID:              agentID,
		Description:          cfg.Description,
		Tags:                 maps.Clone(cfg.Tags),
		RoutingConfiguration: cfg.RoutingConfiguration,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	b.agentAliases.Put(al)

	return aliasCopy(al), nil
}

// GetAgentAlias returns an agent alias.
func (b *InMemoryBackend) GetAgentAlias(_ context.Context, agentID, aliasID string) (*AgentAlias, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	al, ok := b.agentAliases.Get(aliasKey(agentID, aliasID))
	if !ok {
		return nil, fmt.Errorf("%w: alias %q not found", ErrNotFound, aliasID)
	}

	return aliasCopy(al), nil
}

// UpdateAgentAlias updates an agent alias.
func (b *InMemoryBackend) UpdateAgentAlias(
	_ context.Context, agentID, aliasID string, cfg AliasConfig,
) (*AgentAlias, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	al, ok := b.agentAliases.Get(aliasKey(agentID, aliasID))
	if !ok {
		return nil, fmt.Errorf("%w: alias %q not found", ErrNotFound, aliasID)
	}

	if cfg.AliasName != "" {
		al.AgentAliasName = cfg.AliasName
	}

	if cfg.Description != "" {
		al.Description = cfg.Description
	}

	if cfg.RoutingConfiguration != nil {
		al.RoutingConfiguration = cfg.RoutingConfiguration
	}

	if cfg.Tags != nil {
		al.Tags = maps.Clone(cfg.Tags)
	}

	al.UpdatedAt = time.Now().UTC()

	return aliasCopy(al), nil
}

// DeleteAgentAlias deletes an agent alias.
func (b *InMemoryBackend) DeleteAgentAlias(_ context.Context, agentID, aliasID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := aliasKey(agentID, aliasID)
	if !b.agentAliases.Has(key) {
		return fmt.Errorf("%w: alias %q not found", ErrNotFound, aliasID)
	}

	b.agentAliases.Delete(key)

	return nil
}

// ListAgentAliases returns paginated alias summaries for an agent.
func (b *InMemoryBackend) ListAgentAliases(
	_ context.Context, agentID string, maxResults int, nextToken string,
) ([]*AgentAliasSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.agentAliasesByAgent.Get(agentID)
	ids := tableIDs(group, func(al *AgentAlias) string { return al.AgentAliasID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentAliasSummary, 0, len(ids))

	for _, id := range ids {
		al, _ := b.agentAliases.Get(aliasKey(agentID, id))
		out = append(out, &AgentAliasSummary{
			AgentAliasID:     al.AgentAliasID,
			AgentAliasName:   al.AgentAliasName,
			AgentAliasStatus: al.AgentAliasStatus,
			Description:      al.Description,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Agent collaborator CRUD
// ---------------------------------------------------------------------------

// AssociateAgentCollaborator creates a collaborator association.
func (b *InMemoryBackend) AssociateAgentCollaborator(
	_ context.Context, agentID, agentVersion string, cfg CollaboratorConfig,
) (*AgentCollaborator, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	id := b.nextID("collab", &b.collabCounter)

	now := time.Now().UTC()
	c := &AgentCollaborator{
		AgentID:                  agentID,
		AgentVersion:             agentVersion,
		CollaboratorID:           id,
		CollaboratorName:         cfg.CollaboratorName,
		CollaborationInstruction: cfg.CollaborationInstruction,
		RelayConversationHistory: cfg.RelayConversationHistory,
		AgentDescriptor:          cfg.AgentDescriptor,
		CollaboratorStatus:       collabEnabled,
		CreatedAt:                now,
		UpdatedAt:                now,
	}

	b.agentCollaborators.Put(c)

	return collabCopy(c), nil
}

// GetAgentCollaborator returns a collaborator by ID.
func (b *InMemoryBackend) GetAgentCollaborator(
	_ context.Context, agentID, agentVersion, collaboratorID string,
) (*AgentCollaborator, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	c, ok := b.agentCollaborators.Get(agentCollabKey(agentID, agentVersion, collaboratorID))
	if !ok {
		return nil, fmt.Errorf("%w: collaborator %q not found", ErrNotFound, collaboratorID)
	}

	return collabCopy(c), nil
}

// UpdateAgentCollaborator updates a collaborator.
func (b *InMemoryBackend) UpdateAgentCollaborator(
	_ context.Context, agentID, agentVersion, collaboratorID string, cfg CollaboratorConfig,
) (*AgentCollaborator, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.agentCollaborators.Get(agentCollabKey(agentID, agentVersion, collaboratorID))
	if !ok {
		return nil, fmt.Errorf("%w: collaborator %q not found", ErrNotFound, collaboratorID)
	}

	if cfg.CollaboratorName != "" {
		c.CollaboratorName = cfg.CollaboratorName
	}

	if cfg.CollaborationInstruction != "" {
		c.CollaborationInstruction = cfg.CollaborationInstruction
	}

	if cfg.RelayConversationHistory != "" {
		c.RelayConversationHistory = cfg.RelayConversationHistory
	}

	if cfg.AgentDescriptor != nil {
		c.AgentDescriptor = cfg.AgentDescriptor
	}

	c.UpdatedAt = time.Now().UTC()

	return collabCopy(c), nil
}

// DisassociateAgentCollaborator removes a collaborator.
func (b *InMemoryBackend) DisassociateAgentCollaborator(
	_ context.Context, agentID, agentVersion, collaboratorID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := agentCollabKey(agentID, agentVersion, collaboratorID)
	if !b.agentCollaborators.Has(key) {
		return fmt.Errorf("%w: collaborator %q not found", ErrNotFound, collaboratorID)
	}

	b.agentCollaborators.Delete(key)

	return nil
}

// ListAgentCollaborators returns paginated collaborators.
func (b *InMemoryBackend) ListAgentCollaborators(
	_ context.Context, agentID, agentVersion string, maxResults int, nextToken string,
) ([]*AgentCollaborator, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.agentCollaboratorsByAgentVersion.Get(agentVersionScope(agentID, agentVersion))
	ids := tableIDs(group, func(c *AgentCollaborator) string { return c.CollaboratorID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentCollaborator, 0, len(ids))

	for _, id := range ids {
		c, _ := b.agentCollaborators.Get(agentCollabKey(agentID, agentVersion, id))
		out = append(out, collabCopy(c))
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Knowledge base CRUD
// ---------------------------------------------------------------------------

// CreateKnowledgeBase creates a new knowledge base.
func (b *InMemoryBackend) CreateKnowledgeBase(
	ctx context.Context, cfg KnowledgeBaseConfig,
) (*KnowledgeBase, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.kbsByName[cfg.Name]; exists {
		return nil, fmt.Errorf("%w: knowledge base %q already exists", ErrAlreadyExists, cfg.Name)
	}

	id := b.nextID("kb", &b.kbCounter)
	now := time.Now().UTC()

	kb := &KnowledgeBase{
		KnowledgeBaseID:      id,
		KnowledgeBaseARN:     b.buildKBARN(region, id),
		Name:                 cfg.Name,
		Status:               kbStatusActive,
		Description:          cfg.Description,
		RoleARN:              cfg.RoleARN,
		KBConfiguration:      cfg.KBConfiguration,
		StorageConfiguration: cfg.StorageConfiguration,
		Tags:                 maps.Clone(cfg.Tags),
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	b.knowledgeBases.Put(kb)
	b.kbsByName[cfg.Name] = id
	b.tags[kb.KnowledgeBaseARN] = maps.Clone(cfg.Tags)

	return kbCopy(kb), nil
}

// GetKnowledgeBase returns a knowledge base.
func (b *InMemoryBackend) GetKnowledgeBase(_ context.Context, kbID string) (*KnowledgeBase, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	kb, ok := b.knowledgeBases.Get(kbID)
	if !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	return kbCopy(kb), nil
}

// UpdateKnowledgeBase updates a knowledge base.
func (b *InMemoryBackend) UpdateKnowledgeBase(
	_ context.Context, kbID string, cfg KnowledgeBaseConfig,
) (*KnowledgeBase, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	kb, ok := b.knowledgeBases.Get(kbID)
	if !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if cfg.Name != "" {
		kb.Name = cfg.Name
	}

	if cfg.Description != "" {
		kb.Description = cfg.Description
	}

	if cfg.RoleARN != "" {
		kb.RoleARN = cfg.RoleARN
	}

	if cfg.KBConfiguration != nil {
		kb.KBConfiguration = cfg.KBConfiguration
	}

	if cfg.StorageConfiguration != nil {
		kb.StorageConfiguration = cfg.StorageConfiguration
	}

	kb.UpdatedAt = time.Now().UTC()

	return kbCopy(kb), nil
}

// DeleteKnowledgeBase deletes a knowledge base.
func (b *InMemoryBackend) DeleteKnowledgeBase(_ context.Context, kbID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	kb, ok := b.knowledgeBases.Get(kbID)
	if !ok {
		return fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	delete(b.kbsByName, kb.Name)
	b.knowledgeBases.Delete(kbID)

	return nil
}

// ListKnowledgeBases returns paginated knowledge base summaries.
func (b *InMemoryBackend) ListKnowledgeBases(
	_ context.Context, maxResults int, nextToken string,
) ([]*KnowledgeBaseSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := tableIDs(b.knowledgeBases.Snapshot(), func(kb *KnowledgeBase) string { return kb.KnowledgeBaseID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*KnowledgeBaseSummary, 0, len(ids))

	for _, id := range ids {
		kb, _ := b.knowledgeBases.Get(id)
		out = append(out, &KnowledgeBaseSummary{
			KnowledgeBaseID: kb.KnowledgeBaseID,
			Name:            kb.Name,
			Status:          kb.Status,
			Description:     kb.Description,
			UpdatedAt:       kb.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Agent–knowledge base association CRUD
// ---------------------------------------------------------------------------

func agKBKey(agentID, agentVersion, kbID string) string {
	return agentID + "/" + agentVersion + "/" + kbID
}

// AssociateAgentKnowledgeBase creates an agent–KB association.
func (b *InMemoryBackend) AssociateAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID, description, kbState string,
) (*AgentKnowledgeBase, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.agents.Has(agentID) {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	now := time.Now().UTC()
	state := "ENABLED"

	if kbState != "" {
		state = kbState
	}

	assoc := &AgentKnowledgeBase{
		AgentID:         agentID,
		AgentVersion:    agentVersion,
		KnowledgeBaseID: kbID,
		KBState:         state,
		Description:     description,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	b.agentKBAssocs.Put(assoc)

	return agKBCopy(assoc), nil
}

// GetAgentKnowledgeBase returns an agent–KB association.
func (b *InMemoryBackend) GetAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID string,
) (*AgentKnowledgeBase, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	assoc, ok := b.agentKBAssocs.Get(agKBKey(agentID, agentVersion, kbID))
	if !ok {
		return nil, fmt.Errorf("%w: association for kb %q not found", ErrNotFound, kbID)
	}

	return agKBCopy(assoc), nil
}

// UpdateAgentKnowledgeBase updates an agent–KB association.
func (b *InMemoryBackend) UpdateAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID, description, kbState string,
) (*AgentKnowledgeBase, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := agKBKey(agentID, agentVersion, kbID)

	assoc, ok := b.agentKBAssocs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: association for kb %q not found", ErrNotFound, kbID)
	}

	if description != "" {
		assoc.Description = description
	}

	if kbState != "" {
		assoc.KBState = kbState
	}

	assoc.UpdatedAt = time.Now().UTC()

	return agKBCopy(assoc), nil
}

// DisassociateAgentKnowledgeBase removes an agent–KB association.
func (b *InMemoryBackend) DisassociateAgentKnowledgeBase(
	_ context.Context, agentID, agentVersion, kbID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := agKBKey(agentID, agentVersion, kbID)

	if !b.agentKBAssocs.Has(key) {
		return fmt.Errorf("%w: association for kb %q not found", ErrNotFound, kbID)
	}

	b.agentKBAssocs.Delete(key)

	return nil
}

// ListAgentKnowledgeBases returns paginated agent–KB associations.
func (b *InMemoryBackend) ListAgentKnowledgeBases(
	_ context.Context, agentID, agentVersion string, maxResults int, nextToken string,
) ([]*AgentKnowledgeBase, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.agentKBAssocsByAgentVersion.Get(agentVersionScope(agentID, agentVersion))
	ids := tableIDs(group, func(a *AgentKnowledgeBase) string { return a.KnowledgeBaseID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*AgentKnowledgeBase, 0, len(ids))

	for _, id := range ids {
		assoc, _ := b.agentKBAssocs.Get(agKBKey(agentID, agentVersion, id))
		out = append(out, agKBCopy(assoc))
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Data source CRUD
// ---------------------------------------------------------------------------

func dsKey(kbID, dsID string) string { return kbID + "/" + dsID }

// CreateDataSource creates a data source in a knowledge base.
func (b *InMemoryBackend) CreateDataSource(
	_ context.Context, kbID string, cfg DataSourceConfig,
) (*DataSource, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	id := b.nextID("ds", &b.dsCounter)
	now := time.Now().UTC()

	ds := &DataSource{
		DataSourceID:            id,
		KnowledgeBaseID:         kbID,
		Name:                    cfg.Name,
		DataSourceStatus:        dsStatusAvailable,
		Description:             cfg.Description,
		DataDeletionPolicy:      cfg.DataDeletionPolicy,
		DataSourceConfiguration: cfg.DataSourceConfiguration,
		VectorIngestionConfig:   cfg.VectorIngestionConfig,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	b.dataSources.Put(ds)

	return dsCopy(ds), nil
}

// GetDataSource returns a data source.
func (b *InMemoryBackend) GetDataSource(_ context.Context, kbID, dsID string) (*DataSource, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ds, ok := b.dataSources.Get(dsKey(kbID, dsID))
	if !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	return dsCopy(ds), nil
}

// UpdateDataSource updates a data source.
func (b *InMemoryBackend) UpdateDataSource(
	_ context.Context, kbID, dsID string, cfg DataSourceConfig,
) (*DataSource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ds, ok := b.dataSources.Get(dsKey(kbID, dsID))
	if !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	if cfg.Name != "" {
		ds.Name = cfg.Name
	}

	if cfg.Description != "" {
		ds.Description = cfg.Description
	}

	if cfg.DataDeletionPolicy != "" {
		ds.DataDeletionPolicy = cfg.DataDeletionPolicy
	}

	if cfg.DataSourceConfiguration != nil {
		ds.DataSourceConfiguration = cfg.DataSourceConfiguration
	}

	if cfg.VectorIngestionConfig != nil {
		ds.VectorIngestionConfig = cfg.VectorIngestionConfig
	}

	ds.UpdatedAt = time.Now().UTC()

	return dsCopy(ds), nil
}

// DeleteDataSource deletes a data source.
func (b *InMemoryBackend) DeleteDataSource(_ context.Context, kbID, dsID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := dsKey(kbID, dsID)
	if !b.dataSources.Has(key) {
		return fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	b.dataSources.Delete(key)

	return nil
}

// ListDataSources returns paginated data source summaries.
func (b *InMemoryBackend) ListDataSources(
	_ context.Context, kbID string, maxResults int, nextToken string,
) ([]*DataSourceSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.dataSourcesByKB.Get(kbID)
	ids := tableIDs(group, func(ds *DataSource) string { return ds.DataSourceID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*DataSourceSummary, 0, len(ids))

	for _, id := range ids {
		ds, _ := b.dataSources.Get(dsKey(kbID, id))
		out = append(out, &DataSourceSummary{
			DataSourceID:     ds.DataSourceID,
			KnowledgeBaseID:  ds.KnowledgeBaseID,
			Name:             ds.Name,
			DataSourceStatus: ds.DataSourceStatus,
			Description:      ds.Description,
			UpdatedAt:        ds.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Ingestion job CRUD
// ---------------------------------------------------------------------------

func jobKey(kbID, dsID, jobID string) string { return kbID + "/" + dsID + "/" + jobID }

// StartIngestionJob creates and starts a new ingestion job.
func (b *InMemoryBackend) StartIngestionJob(
	_ context.Context, kbID, dsID, description string,
) (*IngestionJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.dataSources.Has(dsKey(kbID, dsID)) {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	id := b.nextID("job", &b.jobCounter)
	now := time.Now().UTC()

	job := &IngestionJob{
		IngestionJobID:  id,
		KnowledgeBaseID: kbID,
		DataSourceID:    dsID,
		Status:          ingestionJobComplete,
		Description:     description,
		StartedAt:       now,
		UpdatedAt:       now,
	}

	b.ingestionJobs.Put(job)

	return jobCopy(job), nil
}

// GetIngestionJob returns an ingestion job.
func (b *InMemoryBackend) GetIngestionJob(
	_ context.Context, kbID, dsID, jobID string,
) (*IngestionJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	job, ok := b.ingestionJobs.Get(jobKey(kbID, dsID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	return jobCopy(job), nil
}

// StopIngestionJob stops an ingestion job.
func (b *InMemoryBackend) StopIngestionJob(
	_ context.Context, kbID, dsID, jobID string,
) (*IngestionJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.ingestionJobs.Get(jobKey(kbID, dsID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	job.Status = "STOPPED"
	job.UpdatedAt = time.Now().UTC()

	return jobCopy(job), nil
}

// ListIngestionJobs returns paginated ingestion job summaries.
func (b *InMemoryBackend) ListIngestionJobs(
	_ context.Context, kbID, dsID string, maxResults int, nextToken string,
) ([]*IngestionJob, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.ingestionJobsByDataSource.Get(dsKey(kbID, dsID))
	ids := tableIDs(group, func(j *IngestionJob) string { return j.IngestionJobID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*IngestionJob, 0, len(ids))

	for _, id := range ids {
		job, _ := b.ingestionJobs.Get(jobKey(kbID, dsID, id))
		out = append(out, jobCopy(job))
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Flow CRUD
// ---------------------------------------------------------------------------

// CreateFlow creates a new flow.
func (b *InMemoryBackend) CreateFlow(ctx context.Context, cfg FlowConfig) (*Flow, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.flowsByName[cfg.Name]; exists {
		return nil, fmt.Errorf("%w: flow %q already exists", ErrAlreadyExists, cfg.Name)
	}

	id := b.nextID("flow", &b.flowCounter)
	now := time.Now().UTC()

	f := &Flow{
		FlowID:      id,
		FlowARN:     b.buildFlowARN(region, id),
		Name:        cfg.Name,
		Status:      flowStatusNotPrepared,
		Description: cfg.Description,
		RoleARN:     cfg.RoleARN,
		Definition:  cfg.Definition,
		Tags:        maps.Clone(cfg.Tags),
		Version:     "DRAFT",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	b.flows.Put(f)
	b.flowsByName[cfg.Name] = id
	b.tags[f.FlowARN] = maps.Clone(cfg.Tags)

	return flowCopy(f), nil
}

// GetFlow returns a flow.
func (b *InMemoryBackend) GetFlow(_ context.Context, flowID string) (*Flow, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	f, ok := b.flows.Get(flowID)
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	return flowCopy(f), nil
}

// UpdateFlow updates a flow.
func (b *InMemoryBackend) UpdateFlow(_ context.Context, flowID string, cfg FlowConfig) (*Flow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, ok := b.flows.Get(flowID)
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	applyFlowConfig(f, cfg)
	f.UpdatedAt = time.Now().UTC()

	return flowCopy(f), nil
}

func applyFlowConfig(f *Flow, cfg FlowConfig) {
	if cfg.Name != "" {
		f.Name = cfg.Name
	}

	if cfg.Description != "" {
		f.Description = cfg.Description
	}

	if cfg.RoleARN != "" {
		f.RoleARN = cfg.RoleARN
	}

	if cfg.Definition != nil {
		f.Definition = cfg.Definition
	}

	if cfg.Tags != nil {
		f.Tags = maps.Clone(cfg.Tags)
	}
}

// DeleteFlow deletes a flow.
func (b *InMemoryBackend) DeleteFlow(_ context.Context, flowID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, ok := b.flows.Get(flowID)
	if !ok {
		return fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	delete(b.flowsByName, f.Name)
	b.flows.Delete(flowID)

	for _, fv := range slices.Clone(b.flowVersionsByFlow.Get(flowID)) {
		b.flowVersions.Delete(flowVersionKey(fv.FlowID, fv.Version))
	}

	delete(b.flowVersionCtrs, flowID)

	return nil
}

// ListFlows returns paginated flow summaries.
func (b *InMemoryBackend) ListFlows(
	_ context.Context, maxResults int, nextToken string,
) ([]*FlowSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := tableIDs(b.flows.Snapshot(), func(f *Flow) string { return f.FlowID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*FlowSummary, 0, len(ids))

	for _, id := range ids {
		f, _ := b.flows.Get(id)
		out = append(out, &FlowSummary{
			FlowID:      f.FlowID,
			Name:        f.Name,
			Status:      f.Status,
			Description: f.Description,
			Version:     f.Version,
			UpdatedAt:   f.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// PrepareFlow transitions a flow to prepared status.
func (b *InMemoryBackend) PrepareFlow(_ context.Context, flowID string) (*Flow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, ok := b.flows.Get(flowID)
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	f.Status = flowStatusPrepared
	f.UpdatedAt = time.Now().UTC()

	return flowCopy(f), nil
}

// ValidateFlowDefinition validates a flow definition (stub - always passes).
func (b *InMemoryBackend) ValidateFlowDefinition(
	_ context.Context, _ map[string]any,
) ([]FlowValidationError, error) {
	return []FlowValidationError{}, nil
}

// ---------------------------------------------------------------------------
// Flow version CRUD
// ---------------------------------------------------------------------------

// CreateFlowVersion creates a numbered snapshot of a flow.
func (b *InMemoryBackend) CreateFlowVersion(
	_ context.Context, flowID, description string,
) (*FlowVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, ok := b.flows.Get(flowID)
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	b.flowVersionCtrs[flowID]++
	vNum := b.flowVersionCtrs[flowID]
	version := strconv.Itoa(vNum)

	fv := &FlowVersion{
		FlowID:      flowID,
		FlowARN:     f.FlowARN,
		Name:        f.Name,
		Version:     version,
		Status:      flowStatusPrepared,
		Definition:  f.Definition,
		Description: description,
		CreatedAt:   time.Now().UTC(),
	}

	b.flowVersions.Put(fv)

	return flowVersionCopy(fv), nil
}

// GetFlowVersion returns a flow version. See the not-found precedence note
// on GetAgentVersion in the Agent version CRUD section above -- the same
// b.flows.Has(flowID)-instead-of-inner-map-presence reasoning applies here.
func (b *InMemoryBackend) GetFlowVersion(
	_ context.Context, flowID, flowVersion string,
) (*FlowVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.flows.Has(flowID) {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	fv, ok := b.flowVersions.Get(flowVersionKey(flowID, flowVersion))
	if !ok {
		return nil, fmt.Errorf("%w: flow version %q not found", ErrNotFound, flowVersion)
	}

	return flowVersionCopy(fv), nil
}

// DeleteFlowVersion deletes a flow version.
func (b *InMemoryBackend) DeleteFlowVersion(_ context.Context, flowID, flowVersion string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.flows.Has(flowID) {
		return fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	key := flowVersionKey(flowID, flowVersion)
	if !b.flowVersions.Has(key) {
		return fmt.Errorf("%w: flow version %q not found", ErrNotFound, flowVersion)
	}

	b.flowVersions.Delete(key)

	return nil
}

// ListFlowVersions returns paginated flow version summaries.
func (b *InMemoryBackend) ListFlowVersions(
	_ context.Context, flowID string, maxResults int, nextToken string,
) ([]*FlowVersionSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.flows.Has(flowID) {
		return nil, "", fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	group := b.flowVersionsByFlow.Get(flowID)
	keys := tableIDs(group, func(fv *FlowVersion) string { return fv.Version })
	keys, outToken := paginate(keys, nextToken, maxResults)

	out := make([]*FlowVersionSummary, 0, len(keys))

	for _, k := range keys {
		fv, _ := b.flowVersions.Get(flowVersionKey(flowID, k))
		out = append(out, &FlowVersionSummary{
			FlowID:      fv.FlowID,
			Arn:         fv.FlowARN,
			Name:        fv.Name,
			Version:     fv.Version,
			Status:      fv.Status,
			Description: fv.Description,
			CreatedAt:   fv.CreatedAt,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Flow alias CRUD
// ---------------------------------------------------------------------------

func flowAliasKey(flowID, aliasID string) string { return flowID + "/" + aliasID }

// CreateFlowAlias creates a flow alias.
func (b *InMemoryBackend) CreateFlowAlias(
	ctx context.Context, flowID string, cfg FlowAliasConfig,
) (*FlowAlias, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.flows.Has(flowID) {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	id := b.nextID("falias", &b.flowAliasCounter)
	now := time.Now().UTC()

	al := &FlowAlias{
		AliasID:              id,
		AliasARN:             b.buildFlowAliasARN(region, flowID, id),
		FlowID:               flowID,
		Name:                 cfg.Name,
		Description:          cfg.Description,
		RoutingConfiguration: cfg.RoutingConfiguration,
		Tags:                 maps.Clone(cfg.Tags),
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	b.flowAliases.Put(al)

	return flowAliasCopy(al), nil
}

// GetFlowAlias returns a flow alias.
func (b *InMemoryBackend) GetFlowAlias(_ context.Context, flowID, aliasID string) (*FlowAlias, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	al, ok := b.flowAliases.Get(flowAliasKey(flowID, aliasID))
	if !ok {
		return nil, fmt.Errorf("%w: flow alias %q not found", ErrNotFound, aliasID)
	}

	return flowAliasCopy(al), nil
}

// UpdateFlowAlias updates a flow alias.
func (b *InMemoryBackend) UpdateFlowAlias(
	_ context.Context, flowID, aliasID string, cfg FlowAliasConfig,
) (*FlowAlias, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	al, ok := b.flowAliases.Get(flowAliasKey(flowID, aliasID))
	if !ok {
		return nil, fmt.Errorf("%w: flow alias %q not found", ErrNotFound, aliasID)
	}

	if cfg.Name != "" {
		al.Name = cfg.Name
	}

	if cfg.Description != "" {
		al.Description = cfg.Description
	}

	if cfg.RoutingConfiguration != nil {
		al.RoutingConfiguration = cfg.RoutingConfiguration
	}

	if cfg.Tags != nil {
		al.Tags = maps.Clone(cfg.Tags)
	}

	al.UpdatedAt = time.Now().UTC()

	return flowAliasCopy(al), nil
}

// DeleteFlowAlias deletes a flow alias.
func (b *InMemoryBackend) DeleteFlowAlias(_ context.Context, flowID, aliasID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := flowAliasKey(flowID, aliasID)
	if !b.flowAliases.Has(key) {
		return fmt.Errorf("%w: flow alias %q not found", ErrNotFound, aliasID)
	}

	b.flowAliases.Delete(key)

	return nil
}

// ListFlowAliases returns paginated flow alias summaries.
func (b *InMemoryBackend) ListFlowAliases(
	_ context.Context, flowID string, maxResults int, nextToken string,
) ([]*FlowAliasSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.flowAliasesByFlow.Get(flowID)
	ids := tableIDs(group, func(al *FlowAlias) string { return al.AliasID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*FlowAliasSummary, 0, len(ids))

	for _, id := range ids {
		al, _ := b.flowAliases.Get(flowAliasKey(flowID, id))
		out = append(out, &FlowAliasSummary{
			AliasID:     al.AliasID,
			AliasARN:    al.AliasARN,
			FlowID:      al.FlowID,
			Name:        al.Name,
			Description: al.Description,
			CreatedAt:   al.CreatedAt,
			UpdatedAt:   al.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Prompt CRUD
// ---------------------------------------------------------------------------

// CreatePrompt creates a new prompt.
func (b *InMemoryBackend) CreatePrompt(ctx context.Context, cfg PromptConfig) (*Prompt, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.promptsByName[cfg.Name]; exists {
		return nil, fmt.Errorf("%w: prompt %q already exists", ErrAlreadyExists, cfg.Name)
	}

	id := b.nextID("prompt", &b.promptCounter)
	now := time.Now().UTC()

	p := &Prompt{
		PromptID:       id,
		PromptARN:      b.buildPromptARN(region, id),
		Name:           cfg.Name,
		Description:    cfg.Description,
		DefaultVariant: cfg.DefaultVariant,
		Variants:       cfg.Variants,
		Tags:           maps.Clone(cfg.Tags),
		Version:        "DRAFT",
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.prompts.Put(p)
	b.promptsByName[cfg.Name] = id
	b.tags[p.PromptARN] = maps.Clone(cfg.Tags)

	return promptCopy(p), nil
}

// GetPrompt returns a prompt.
func (b *InMemoryBackend) GetPrompt(_ context.Context, promptID string) (*Prompt, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	return promptCopy(p), nil
}

// UpdatePrompt updates a prompt.
func (b *InMemoryBackend) UpdatePrompt(
	_ context.Context, promptID string, cfg PromptConfig,
) (*Prompt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	if cfg.Name != "" {
		p.Name = cfg.Name
	}

	if cfg.Description != "" {
		p.Description = cfg.Description
	}

	if cfg.DefaultVariant != "" {
		p.DefaultVariant = cfg.DefaultVariant
	}

	if cfg.Variants != nil {
		p.Variants = cfg.Variants
	}

	if cfg.Tags != nil {
		p.Tags = maps.Clone(cfg.Tags)
	}

	p.UpdatedAt = time.Now().UTC()

	return promptCopy(p), nil
}

// DeletePrompt deletes a prompt.
func (b *InMemoryBackend) DeletePrompt(_ context.Context, promptID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	delete(b.promptsByName, p.Name)
	b.prompts.Delete(promptID)

	for _, pv := range slices.Clone(b.promptVersionsByPrompt.Get(promptID)) {
		b.promptVersions.Delete(promptVersionKey(pv.PromptID, pv.Version))
	}

	delete(b.promptVersionCtrs, promptID)

	return nil
}

// ListPrompts returns paginated prompt summaries.
func (b *InMemoryBackend) ListPrompts(
	_ context.Context, maxResults int, nextToken string,
) ([]*PromptSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := tableIDs(b.prompts.Snapshot(), func(p *Prompt) string { return p.PromptID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*PromptSummary, 0, len(ids))

	for _, id := range ids {
		p, _ := b.prompts.Get(id)
		out = append(out, &PromptSummary{
			PromptID:    p.PromptID,
			PromptARN:   p.PromptARN,
			Name:        p.Name,
			Description: p.Description,
			Version:     p.Version,
			CreatedAt:   p.CreatedAt,
			UpdatedAt:   p.UpdatedAt,
		})
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Prompt version CRUD
// ---------------------------------------------------------------------------

// CreatePromptVersion creates a versioned snapshot of a prompt.
func (b *InMemoryBackend) CreatePromptVersion(
	_ context.Context, promptID, description string,
) (*PromptVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.prompts.Get(promptID)
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	b.promptVersionCtrs[promptID]++
	vNum := b.promptVersionCtrs[promptID]
	version := strconv.Itoa(vNum)

	pv := &PromptVersion{
		PromptID:    promptID,
		PromptARN:   p.PromptARN,
		Name:        p.Name,
		Version:     version,
		Variants:    p.Variants,
		Description: description,
		CreatedAt:   time.Now().UTC(),
	}

	b.promptVersions.Put(pv)

	return promptVersionCopy(pv), nil
}

// GetPromptVersion returns a specific prompt version. See the not-found
// precedence note on GetAgentVersion in the Agent version CRUD section above
// -- the same b.prompts.Has(promptID)-instead-of-inner-map-presence
// reasoning applies here.
func (b *InMemoryBackend) GetPromptVersion(
	_ context.Context, promptID, version string,
) (*PromptVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.prompts.Has(promptID) {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	pv, ok := b.promptVersions.Get(promptVersionKey(promptID, version))
	if !ok {
		return nil, fmt.Errorf("%w: prompt version %q not found", ErrNotFound, version)
	}

	return promptVersionCopy(pv), nil
}

// DeletePromptVersion deletes a prompt version.
func (b *InMemoryBackend) DeletePromptVersion(
	_ context.Context, promptID, version string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.prompts.Has(promptID) {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	key := promptVersionKey(promptID, version)
	if !b.promptVersions.Has(key) {
		return fmt.Errorf("%w: prompt version %q not found", ErrNotFound, version)
	}

	b.promptVersions.Delete(key)

	return nil
}

// ---------------------------------------------------------------------------
// Knowledge base document operations
// ---------------------------------------------------------------------------

func kbDocKey(kbID, dsID, docID string) string { return kbID + "/" + dsID + "/" + docID }

// IngestKnowledgeBaseDocuments ingests documents into a knowledge base data source.
func (b *InMemoryBackend) IngestKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, docs []KBDocument,
) ([]KBDocumentDetail, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.dataSources.Has(dsKey(kbID, dsID)) {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	out := make([]KBDocumentDetail, 0, len(docs))

	for _, doc := range docs {
		detail := &KBDocumentDetail{
			DocumentID:      doc.DocID,
			KnowledgeBaseID: kbID,
			DataSourceID:    dsID,
			Status:          docStatusIndexed,
		}
		b.kbDocuments.Put(detail)
		out = append(out, *detail)
	}

	return out, nil
}

// GetKnowledgeBaseDocuments retrieves document details.
func (b *InMemoryBackend) GetKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, docIDs []string,
) ([]KBDocumentDetail, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]KBDocumentDetail, 0, len(docIDs))

	for _, id := range docIDs {
		detail, ok := b.kbDocuments.Get(kbDocKey(kbID, dsID, id))
		if !ok {
			return nil, fmt.Errorf("%w: document %q not found", ErrNotFound, id)
		}

		out = append(out, *detail)
	}

	return out, nil
}

// DeleteKnowledgeBaseDocuments deletes documents from a knowledge base data source.
func (b *InMemoryBackend) DeleteKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, docIDs []string,
) ([]KBDocumentDetail, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]KBDocumentDetail, 0, len(docIDs))

	for _, id := range docIDs {
		key := kbDocKey(kbID, dsID, id)

		detail, ok := b.kbDocuments.Get(key)
		if !ok {
			out = append(out, KBDocumentDetail{
				DocumentID:      id,
				KnowledgeBaseID: kbID,
				DataSourceID:    dsID,
				Status:          "NOT_FOUND",
			})

			continue
		}

		b.kbDocuments.Delete(key)

		d := *detail
		d.Status = "DELETED"
		out = append(out, d)
	}

	return out, nil
}

// ListKnowledgeBaseDocuments returns paginated document details.
func (b *InMemoryBackend) ListKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, maxResults int, nextToken string,
) ([]KBDocumentDetail, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.kbDocumentsByDataSource.Get(dsKey(kbID, dsID))
	keys := tableIDs(group, func(d *KBDocumentDetail) string {
		return kbDocKey(d.KnowledgeBaseID, d.DataSourceID, d.DocumentID)
	})
	keys, outToken := paginate(keys, nextToken, maxResults)

	out := make([]KBDocumentDetail, 0, len(keys))

	for _, k := range keys {
		d, _ := b.kbDocuments.Get(k)
		out = append(out, *d)
	}

	return out, outToken, nil
}

// ---------------------------------------------------------------------------
// Tagging operations
// ---------------------------------------------------------------------------

// ListTagsForResource returns tags for a resource ARN.
func (b *InMemoryBackend) ListTagsForResource(
	_ context.Context, resourceARN string,
) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.tags[resourceARN]
	if !ok {
		return map[string]string{}, nil
	}

	return maps.Clone(t), nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(
	_ context.Context, resourceARN string, tags map[string]string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(
	_ context.Context, resourceARN string, tagKeys []string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t := b.tags[resourceARN]

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Pagination helper
// ---------------------------------------------------------------------------

const defaultPageSize = 100

func paginate(ids []string, nextToken string, maxResults int) ([]string, string) {
	start := 0

	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	size := defaultPageSize

	if maxResults > 0 && maxResults < defaultPageSize {
		size = maxResults
	}

	end := min(start+size, len(ids))

	page := ids[start:end]

	var outToken string

	if end < len(ids) {
		outToken = ids[end]
	}

	return page, outToken
}

// tableIDs extracts an ID from every item in items via idFn and returns the
// IDs sorted ascending -- the store.Table/store.Index equivalent of the
// pre-Phase-3.3 sortedKeys(map[string]V) helper (both ultimately do a plain
// lexicographic string sort, matching pkgs/collections.SortedKeys, which
// pkgs/store.Table.Snapshot also uses internally). items may come from
// [store.Table.Snapshot] (already key-sorted, so the sort below is a cheap
// no-op) or a [store.Index.Get] group (unordered, so the sort is required).
func tableIDs[V any](items []*V, idFn func(*V) string) []string {
	ids := make([]string, len(items))
	for i, v := range items {
		ids[i] = idFn(v)
	}

	sort.Strings(ids)

	return ids
}

// ---------------------------------------------------------------------------
// Deep-copy helpers
// ---------------------------------------------------------------------------

func agentCopy(a *Agent) *Agent {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

func agentVersionCopy(av *AgentVersion) *AgentVersion {
	cp := *av

	return &cp
}

func actionGroupCopy(ag *AgentActionGroup) *AgentActionGroup {
	cp := *ag

	return &cp
}

func aliasCopy(al *AgentAlias) *AgentAlias {
	cp := *al
	cp.Tags = maps.Clone(al.Tags)

	if al.RoutingConfiguration != nil {
		cp.RoutingConfiguration = append([]AliasRouting{}, al.RoutingConfiguration...)
	}

	return &cp
}

func collabCopy(c *AgentCollaborator) *AgentCollaborator {
	cp := *c

	return &cp
}

func kbCopy(kb *KnowledgeBase) *KnowledgeBase {
	cp := *kb
	cp.Tags = maps.Clone(kb.Tags)

	return &cp
}

func agKBCopy(a *AgentKnowledgeBase) *AgentKnowledgeBase {
	cp := *a

	return &cp
}

func dsCopy(ds *DataSource) *DataSource {
	cp := *ds

	return &cp
}

func jobCopy(j *IngestionJob) *IngestionJob {
	cp := *j

	return &cp
}

func flowCopy(f *Flow) *Flow {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	return &cp
}

func flowVersionCopy(fv *FlowVersion) *FlowVersion {
	cp := *fv

	return &cp
}

func flowAliasCopy(al *FlowAlias) *FlowAlias {
	cp := *al
	cp.Tags = maps.Clone(al.Tags)

	if al.RoutingConfiguration != nil {
		cp.RoutingConfiguration = append([]FlowAliasRouting{}, al.RoutingConfiguration...)
	}

	return &cp
}

func promptCopy(p *Prompt) *Prompt {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

func promptVersionCopy(pv *PromptVersion) *PromptVersion {
	cp := *pv

	return &cp
}
