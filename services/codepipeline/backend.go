// Package codepipeline provides an in-memory implementation of the AWS CodePipeline service.
package codepipeline

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// CodePipeline resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// nested store. Pipelines, action types, jobs, webhooks, executions, and stage
// transitions are all region-scoped in AWS, so cross-region references never occur
// and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	// statusInProgress is the status for an in-progress job or execution.
	statusInProgress = "InProgress"

	// PipelineTypeV1 and PipelineTypeV2 are the valid PipelineType values.
	PipelineTypeV1 = "V1"
	PipelineTypeV2 = "V2"

	// ExecutionModeQueued is the QUEUED execution mode.
	ExecutionModeQueued = "QUEUED"
	// ExecutionModeSuperseded is the SUPERSEDED execution mode.
	ExecutionModeSuperseded = "SUPERSEDED"
	// ExecutionModeParallel is the PARALLEL execution mode.
	ExecutionModeParallel = "PARALLEL"

	// WebhookAuthGitHubHMAC is the GITHUB_HMAC authentication type for webhooks.
	WebhookAuthGitHubHMAC = "GITHUB_HMAC"
	// WebhookAuthIP is the IP authentication type for webhooks.
	WebhookAuthIP = "IP"
	// WebhookAuthUnauthenticated is the UNAUTHENTICATED authentication type for webhooks.
	WebhookAuthUnauthenticated = "UNAUTHENTICATED"

	// kindPipeline is the resource kind string for pipelines.
	kindPipeline = "pipeline"
	// kindWebhook is the resource kind string for webhooks.
	kindWebhook = "webhook"

	// keyPipelineExecutionID and keyStatus are JSON keys shared across the
	// execution-detail response maps.
	keyPipelineExecutionID = "pipelineExecutionId"
	keyStatus              = "status"

	// statusSucceeded is the terminal success status for executions and actions.
	statusSucceeded = "Succeeded"

	// ruleOwnerAWS is the owner value for AWS-managed CodePipeline rule types.
	ruleOwnerAWS = "AWS"
)

var (
	// ErrNotFound is returned when a pipeline resource does not exist.
	ErrNotFound = awserr.New("PipelineNotFoundException", awserr.ErrNotFound)
	// ErrPipelineNameInUse is returned when a pipeline with the same name already exists.
	ErrPipelineNameInUse = awserr.New("PipelineNameInUseException", awserr.ErrAlreadyExists)
	// ErrAlreadyExists is returned when a non-pipeline resource with the same key already exists.
	ErrAlreadyExists = awserr.New("InvalidStructureException", awserr.ErrAlreadyExists)
	// ErrActionTypeNotFound is returned when a requested custom action type does not exist.
	ErrActionTypeNotFound = awserr.New("ActionTypeNotFoundException", awserr.ErrNotFound)
	// ErrJobNotFound is returned when a requested job does not exist.
	ErrJobNotFound = awserr.New("JobNotFoundException", awserr.ErrNotFound)
	// ErrWebhookNotFound is returned when a requested webhook does not exist.
	ErrWebhookNotFound = awserr.New("WebhookNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrConflict is returned on optimistic-concurrency version mismatch.
	ErrConflict = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrResourceInUse is returned when a resource is referenced by another resource.
	ErrResourceInUse = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrResourceNotFound is returned for non-pipeline ARNs (e.g. webhook ARNs).
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrStageNotFound is returned when a stage name does not exist in a pipeline.
	ErrStageNotFound = awserr.New("StageNotFoundException", awserr.ErrNotFound)
	// ErrInvalidStructure is returned for structural pipeline validation errors.
	ErrInvalidStructure = awserr.New("InvalidStructureException", awserr.ErrInvalidParameter)
)

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
	Settings                *ActionTypeSettings           `json:"settings,omitempty"`
	Tags                    map[string]string             `json:"-"`
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
	ID           string       `json:"id"`
	PipelineName string       `json:"pipelineName,omitempty"`
	Nonce        string       `json:"nonce"`
	Status       string       `json:"status"`
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
	Tags                        map[string]string `json:"-"`
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

// InMemoryBackend is a thread-safe in-memory store for CodePipeline resources.
//
// All resource maps are nested by region (outer key = region) so that same-named
// resources are isolated across regions. The per-region inner maps are created
// lazily via the *Store helpers. Callers must hold b.mu while accessing the inner
// maps.
type InMemoryBackend struct {
	pipelines         map[string]map[string]*Pipeline
	pipelineARNIndex  map[string]map[string]string // region → ARN → pipeline name
	customActionTypes map[string]map[customActionTypeKey]*CustomActionType
	jobs              map[string]map[string]*Job     // region → jobID → Job
	webhooks          map[string]map[string]*Webhook // region → name → Webhook
	webhookARNIndex   map[string]map[string]string   // region → ARN → webhook name
	stageTransitions  map[string]map[stageTransitionKey]*StageTransitionState
	executions        map[string]map[string][]*PipelineExecution // region → pipelineName → executions
	actionExecutions  map[string]map[string][]*ActionExecution   // region → pipelineName → action executions
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipelines:         make(map[string]map[string]*Pipeline),
		pipelineARNIndex:  make(map[string]map[string]string),
		customActionTypes: make(map[string]map[customActionTypeKey]*CustomActionType),
		jobs:              make(map[string]map[string]*Job),
		webhooks:          make(map[string]map[string]*Webhook),
		webhookARNIndex:   make(map[string]map[string]string),
		stageTransitions:  make(map[string]map[stageTransitionKey]*StageTransitionState),
		executions:        make(map[string]map[string][]*PipelineExecution),
		actionExecutions:  make(map[string]map[string][]*ActionExecution),
		accountID:         accountID,
		region:            region,
		mu:                lockmetrics.New("codepipeline-" + region),
	}
}

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) pipelinesStore(region string) map[string]*Pipeline {
	if b.pipelines[region] == nil {
		b.pipelines[region] = make(map[string]*Pipeline)
	}

	return b.pipelines[region]
}

func (b *InMemoryBackend) pipelineARNIndexStore(region string) map[string]string {
	if b.pipelineARNIndex[region] == nil {
		b.pipelineARNIndex[region] = make(map[string]string)
	}

	return b.pipelineARNIndex[region]
}

func (b *InMemoryBackend) customActionTypesStore(region string) map[customActionTypeKey]*CustomActionType {
	if b.customActionTypes[region] == nil {
		b.customActionTypes[region] = make(map[customActionTypeKey]*CustomActionType)
	}

	return b.customActionTypes[region]
}

func (b *InMemoryBackend) jobsStore(region string) map[string]*Job {
	if b.jobs[region] == nil {
		b.jobs[region] = make(map[string]*Job)
	}

	return b.jobs[region]
}

func (b *InMemoryBackend) webhooksStore(region string) map[string]*Webhook {
	if b.webhooks[region] == nil {
		b.webhooks[region] = make(map[string]*Webhook)
	}

	return b.webhooks[region]
}

func (b *InMemoryBackend) webhookARNIndexStore(region string) map[string]string {
	if b.webhookARNIndex[region] == nil {
		b.webhookARNIndex[region] = make(map[string]string)
	}

	return b.webhookARNIndex[region]
}

func (b *InMemoryBackend) stageTransitionsStore(region string) map[stageTransitionKey]*StageTransitionState {
	if b.stageTransitions[region] == nil {
		b.stageTransitions[region] = make(map[stageTransitionKey]*StageTransitionState)
	}

	return b.stageTransitions[region]
}

func (b *InMemoryBackend) executionsStore(region string) map[string][]*PipelineExecution {
	if b.executions[region] == nil {
		b.executions[region] = make(map[string][]*PipelineExecution)
	}

	return b.executions[region]
}

func (b *InMemoryBackend) actionExecutionsStore(region string) map[string][]*ActionExecution {
	if b.actionExecutions[region] == nil {
		b.actionExecutions[region] = make(map[string][]*ActionExecution)
	}

	return b.actionExecutions[region]
}

// Region returns the default region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state in the backend, resetting it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.pipelines = make(map[string]map[string]*Pipeline)
	b.pipelineARNIndex = make(map[string]map[string]string)
	b.customActionTypes = make(map[string]map[customActionTypeKey]*CustomActionType)
	b.jobs = make(map[string]map[string]*Job)
	b.webhooks = make(map[string]map[string]*Webhook)
	b.webhookARNIndex = make(map[string]map[string]string)
	b.stageTransitions = make(map[string]map[stageTransitionKey]*StageTransitionState)
	b.executions = make(map[string]map[string][]*PipelineExecution)
	b.actionExecutions = make(map[string]map[string][]*ActionExecution)
}

func (b *InMemoryBackend) buildPipelineARN(region, name string) string {
	return arn.Build("codepipeline", region, b.accountID, name)
}

func (b *InMemoryBackend) buildWebhookARN(region, name string) string {
	return arn.Build("codepipeline", region, b.accountID, "webhook:"+name)
}

// CreatePipeline creates a new CodePipeline pipeline.
func (b *InMemoryBackend) CreatePipeline(
	ctx context.Context,
	decl PipelineDeclaration,
	tags map[string]string,
) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.pipelinesStore(region)
	arnIndex := b.pipelineARNIndexStore(region)

	if _, exists := store[decl.Name]; exists {
		return nil, fmt.Errorf("%w: pipeline %q already exists", ErrPipelineNameInUse, decl.Name)
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	if decl.Version == 0 {
		decl.Version = 1
	}

	if decl.PipelineType == "" {
		decl.PipelineType = PipelineTypeV1
	}

	if decl.ExecutionMode == "" {
		decl.ExecutionMode = ExecutionModeSuperseded
	}

	p := &Pipeline{
		Declaration: decl,
		Metadata: PipelineMetadata{
			PipelineArn: b.buildPipelineARN(region, decl.Name),
			Created:     now,
			Updated:     now,
		},
		Tags: tagsCopy,
	}
	store[decl.Name] = p
	arnIndex[p.Metadata.PipelineArn] = decl.Name

	return copyPipeline(p), nil
}

// GetPipeline returns the pipeline with the given name.
func (b *InMemoryBackend) GetPipeline(ctx context.Context, name string) (*Pipeline, error) {
	b.mu.RLock("GetPipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelinesStore(getRegion(ctx, b.region))[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	return copyPipeline(p), nil
}

// UpdatePipeline replaces the pipeline declaration.
// If decl.Version is non-zero it must match the current version (optimistic concurrency).
func (b *InMemoryBackend) UpdatePipeline(ctx context.Context, decl PipelineDeclaration) (*Pipeline, error) {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelinesStore(getRegion(ctx, b.region))[decl.Name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, decl.Name)
	}

	if decl.Version != 0 && decl.Version != p.Declaration.Version {
		return nil, fmt.Errorf("%w: pipeline %q version mismatch: got %d, current %d",
			ErrConflict, decl.Name, decl.Version, p.Declaration.Version)
	}

	currentVersion := p.Declaration.Version
	p.Declaration = decl
	p.Declaration.Version = currentVersion + 1
	p.Metadata.Updated = float64(time.Now().Unix())

	return copyPipeline(p), nil
}

// DeletePipeline removes the pipeline with the given name and cleans up associated state.
func (b *InMemoryBackend) DeletePipeline(ctx context.Context, name string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.pipelinesStore(region)

	p, ok := store[name]
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	delete(b.pipelineARNIndexStore(region), p.Metadata.PipelineArn)
	delete(store, name)
	delete(b.executionsStore(region), name)

	// Cascade: remove disabled stage transitions for this pipeline.
	transitions := b.stageTransitionsStore(region)
	for key := range transitions {
		if key.PipelineName == name {
			delete(transitions, key)
		}
	}

	return nil
}

// ListPipelines returns a sorted summary of all pipelines in the request region.
func (b *InMemoryBackend) ListPipelines(ctx context.Context) []PipelineSummary {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	store := b.pipelinesStore(getRegion(ctx, b.region))

	names := collections.SortedKeys(store)

	summaries := make([]PipelineSummary, 0, len(store))
	for _, name := range names {
		p := store[name]
		summaries = append(summaries, PipelineSummary{
			Name:          p.Declaration.Name,
			Version:       p.Declaration.Version,
			PipelineType:  p.Declaration.PipelineType,
			ExecutionMode: p.Declaration.ExecutionMode,
			Created:       p.Metadata.Created,
			Updated:       p.Metadata.Updated,
			PipelineArn:   p.Metadata.PipelineArn,
		})
	}

	return summaries
}

// resolveResourceARN looks up a resource by ARN within the given region, returning
// its type ("pipeline" or "webhook") and name. The ARN's region segment is used
// when present so callers cannot resolve resources outside their region. Returns
// ErrNotFound if unknown. Callers must hold b.mu.
func (b *InMemoryBackend) resolveResourceARN(region, resourceARN string) (string, string, error) {
	if n, ok := b.pipelineARNIndexStore(region)[resourceARN]; ok {
		return kindPipeline, n, nil
	}

	if n, ok := b.webhookARNIndexStore(region)[resourceARN]; ok {
		return kindWebhook, n, nil
	}

	return "", "", ErrNotFound
}

// ListTagsForResource returns the sorted tags for a pipeline by ARN.
// Returns ResourceNotFoundException when the ARN refers to a non-pipeline resource.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	kind, name, err := b.resolveResourceARN(region, resourceARN)
	if err != nil {
		return nil, err
	}

	switch kind {
	case kindPipeline:
		return tagsToSortedSlice(b.pipelinesStore(region)[name].Tags), nil
	case kindWebhook:
		return tagsToSortedSlice(b.webhooksStore(region)[name].Tags), nil
	default:
		return nil, fmt.Errorf("%w: ARN %q", ErrResourceNotFound, resourceARN)
	}
}

// TagResource adds or updates tags on a pipeline by ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	kind, name, err := b.resolveResourceARN(region, resourceARN)
	if err != nil {
		return err
	}

	switch kind {
	case kindPipeline:
		p := b.pipelinesStore(region)[name]
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}

		for _, t := range tags {
			p.Tags[t.Key] = t.Value
		}
	case kindWebhook:
		wh := b.webhooksStore(region)[name]
		if wh.Tags == nil {
			wh.Tags = make(map[string]string)
		}

		for _, t := range tags {
			wh.Tags[t.Key] = t.Value
		}
	default:
		return fmt.Errorf("%w: ARN %q is not a taggable resource", ErrResourceNotFound, resourceARN)
	}

	return nil
}

// UntagResource removes tags from a pipeline by ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	kind, name, err := b.resolveResourceARN(region, resourceARN)
	if err != nil {
		return err
	}

	switch kind {
	case kindPipeline:
		p := b.pipelinesStore(region)[name]
		for _, k := range tagKeys {
			delete(p.Tags, k)
		}
	case kindWebhook:
		wh := b.webhooksStore(region)[name]
		for _, k := range tagKeys {
			delete(wh.Tags, k)
		}
	default:
		return fmt.Errorf("%w: ARN %q is not a taggable resource", ErrResourceNotFound, resourceARN)
	}

	return nil
}

func copyPipeline(p *Pipeline) *Pipeline {
	tagsCopy := make(map[string]string, len(p.Tags))
	maps.Copy(tagsCopy, p.Tags)

	out := *p
	out.Tags = tagsCopy
	out.Declaration = copyDeclaration(p.Declaration)

	return &out
}

// tagsToSortedSlice converts a tag map to a deterministically-sorted slice of Tag.
func tagsToSortedSlice(kv map[string]string) []Tag {
	keys := collections.SortedKeys(kv)

	tags := make([]Tag, 0, len(kv))
	for _, k := range keys {
		tags = append(tags, Tag{Key: k, Value: kv[k]})
	}

	return tags
}

// AddPipelineInternal seeds a pipeline directly into the backend's default region (for testing).
func (b *InMemoryBackend) AddPipelineInternal(decl PipelineDeclaration, tags map[string]string) *Pipeline {
	b.mu.Lock("AddPipelineInternal")
	defer b.mu.Unlock()

	store := b.pipelinesStore(b.region)
	arnIndex := b.pipelineARNIndexStore(b.region)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	if decl.Version == 0 {
		decl.Version = 1
	}

	p := &Pipeline{
		Declaration: decl,
		Metadata: PipelineMetadata{
			PipelineArn: b.buildPipelineARN(b.region, decl.Name),
			Created:     now,
			Updated:     now,
		},
		Tags: tagsCopy,
	}
	store[decl.Name] = p
	arnIndex[p.Metadata.PipelineArn] = decl.Name

	return copyPipeline(p)
}

// AddCustomActionTypeInternal seeds a custom action type into the backend's default region (for testing).
func (b *InMemoryBackend) AddCustomActionTypeInternal(cat *CustomActionType) {
	b.mu.Lock("AddCustomActionTypeInternal")
	defer b.mu.Unlock()

	key := customActionTypeKey{Category: cat.Category, Provider: cat.Provider, Version: cat.Version}
	b.customActionTypesStore(b.region)[key] = copyCustomActionType(cat)
}

// GetStageTransitionState returns the disabled state for a stage transition, or nil if enabled.
func (b *InMemoryBackend) GetStageTransitionState(
	ctx context.Context,
	pipelineName, stageName, transitionType string,
) *StageTransitionState {
	b.mu.RLock("GetStageTransitionState")
	defer b.mu.RUnlock()

	key := stageTransitionKey{
		PipelineName:   pipelineName,
		StageName:      stageName,
		TransitionType: transitionType,
	}

	state, ok := b.stageTransitionsStore(getRegion(ctx, b.region))[key]
	if !ok {
		return nil
	}

	cp := *state

	return &cp
}

// --- Custom Action Type operations ---

// CreateCustomActionType stores a new custom action type.
func (b *InMemoryBackend) CreateCustomActionType(
	ctx context.Context,
	cat *CustomActionType,
) (*CustomActionType, error) {
	b.mu.Lock("CreateCustomActionType")
	defer b.mu.Unlock()

	store := b.customActionTypesStore(getRegion(ctx, b.region))
	key := customActionTypeKey{Category: cat.Category, Provider: cat.Provider, Version: cat.Version}

	if _, exists := store[key]; exists {
		return nil, fmt.Errorf("%w: custom action type %q/%q/%q already exists",
			ErrAlreadyExists, cat.Category, cat.Provider, cat.Version)
	}

	if cat.Owner == "" {
		cat.Owner = keyOwnerCustom
	}

	cp := copyCustomActionType(cat)
	store[key] = cp

	return copyCustomActionType(cp), nil
}

// DeleteCustomActionType removes a custom action type.
// Returns ResourceInUseException if any pipeline references the type.
func (b *InMemoryBackend) DeleteCustomActionType(ctx context.Context, category, provider, version string) error {
	b.mu.Lock("DeleteCustomActionType")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.customActionTypesStore(region)
	key := customActionTypeKey{Category: category, Provider: provider, Version: version}

	if _, ok := store[key]; !ok {
		return fmt.Errorf("%w: custom action type %q/%q/%q", ErrActionTypeNotFound, category, provider, version)
	}

	// Check that no pipeline references this action type.
	for pName, p := range b.pipelinesStore(region) {
		for _, stage := range p.Declaration.Stages {
			for _, action := range stage.Actions {
				at := action.ActionTypeID
				if at.Category == category && at.Provider == provider && at.Version == version {
					return fmt.Errorf("%w: action type %q/%q/%q is in use by pipeline %q",
						ErrResourceInUse, category, provider, version, pName)
				}
			}
		}
	}

	delete(store, key)

	return nil
}

// GetActionType retrieves a custom action type.
func (b *InMemoryBackend) GetActionType(
	ctx context.Context,
	category, owner, provider, version string,
) (*CustomActionType, error) {
	b.mu.RLock("GetActionType")
	defer b.mu.RUnlock()

	key := customActionTypeKey{Category: category, Provider: provider, Version: version}

	cat, ok := b.customActionTypesStore(getRegion(ctx, b.region))[key]
	if !ok {
		return nil, fmt.Errorf("%w: action type %q/%q/%q/%q", ErrActionTypeNotFound, category, owner, provider, version)
	}

	return copyCustomActionType(cat), nil
}

// --- Job operations ---

// AcknowledgeJob acknowledges that a job worker has received a job.
// Returns InProgress if Nonce matches; otherwise returns current status unchanged.
func (b *InMemoryBackend) AcknowledgeJob(ctx context.Context, jobID, nonce string) (string, error) {
	b.mu.Lock("AcknowledgeJob")
	defer b.mu.Unlock()

	job, ok := b.jobsStore(getRegion(ctx, b.region))[jobID]
	if !ok {
		return "", fmt.Errorf("%w: job %q", ErrJobNotFound, jobID)
	}

	if job.Nonce == nonce {
		job.Status = statusInProgress
	}

	return job.Status, nil
}

// AcknowledgeThirdPartyJob acknowledges that a third-party job worker has received a job.
func (b *InMemoryBackend) AcknowledgeThirdPartyJob(
	ctx context.Context,
	jobID, nonce, clientToken string,
) (string, error) {
	b.mu.Lock("AcknowledgeThirdPartyJob")
	defer b.mu.Unlock()

	job, ok := b.jobsStore(getRegion(ctx, b.region))[jobID]
	if !ok {
		return "", fmt.Errorf("%w: third-party job %q with client token %q", ErrJobNotFound, jobID, clientToken)
	}

	if job.Nonce == nonce {
		job.Status = statusInProgress
	}

	return job.Status, nil
}

// GetJobDetails returns details for a job by ID.
func (b *InMemoryBackend) GetJobDetails(ctx context.Context, jobID string) (*Job, error) {
	b.mu.RLock("GetJobDetails")
	defer b.mu.RUnlock()

	job, ok := b.jobsStore(getRegion(ctx, b.region))[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: job %q", ErrJobNotFound, jobID)
	}

	cp := *job

	return &cp, nil
}

// AddJobInternal seeds a job into the backend's default region (for testing).
func (b *InMemoryBackend) AddJobInternal(job *Job) {
	b.mu.Lock("AddJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.jobsStore(b.region)[cp.ID] = &cp
}

// --- Webhook operations ---

// DeleteWebhook removes a webhook by name (idempotent).
func (b *InMemoryBackend) DeleteWebhook(ctx context.Context, name string) error {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.webhooksStore(region)

	if wh, ok := store[name]; ok {
		delete(b.webhookARNIndexStore(region), wh.ARN)
	}

	delete(store, name)

	return nil
}

// DeregisterWebhookWithThirdParty clears the third-party registration flag on a webhook.
func (b *InMemoryBackend) DeregisterWebhookWithThirdParty(ctx context.Context, name string) error {
	b.mu.Lock("DeregisterWebhookWithThirdParty")
	defer b.mu.Unlock()

	if wh, ok := b.webhooksStore(getRegion(ctx, b.region))[name]; ok {
		wh.RegisteredWithThirdParty = false
	}

	return nil
}

// AddWebhookInternal seeds a webhook into the backend's default region (for testing).
func (b *InMemoryBackend) AddWebhookInternal(wh *Webhook) {
	b.mu.Lock("AddWebhookInternal")
	defer b.mu.Unlock()

	cp := *wh
	if cp.ARN == "" {
		cp.ARN = b.buildWebhookARN(b.region, cp.Name)
	}

	b.webhooksStore(b.region)[cp.Name] = &cp
	b.webhookARNIndexStore(b.region)[cp.ARN] = cp.Name
}

// --- Stage transition operations ---

// DisableStageTransition disables a stage transition and records the reason.
// Returns StageNotFoundException if stageName does not exist in the pipeline.
func (b *InMemoryBackend) DisableStageTransition(
	ctx context.Context,
	pipelineName, stageName, transitionType, reason string,
) error {
	b.mu.Lock("DisableStageTransition")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[pipelineName]
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	if !pipelineHasStage(p, stageName) {
		return fmt.Errorf("%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName)
	}

	key := stageTransitionKey{PipelineName: pipelineName, StageName: stageName, TransitionType: transitionType}
	b.stageTransitionsStore(region)[key] = &StageTransitionState{
		PipelineName:   pipelineName,
		StageName:      stageName,
		TransitionType: transitionType,
		Reason:         reason,
		Disabled:       true,
	}

	return nil
}

// EnableStageTransition re-enables a stage transition.
func (b *InMemoryBackend) EnableStageTransition(
	ctx context.Context,
	pipelineName, stageName, transitionType string,
) error {
	b.mu.Lock("EnableStageTransition")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region)[pipelineName]; !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	key := stageTransitionKey{PipelineName: pipelineName, StageName: stageName, TransitionType: transitionType}
	delete(b.stageTransitionsStore(region), key)

	return nil
}

// pipelineHasStage returns true if the pipeline contains a stage with the given name.
func pipelineHasStage(p *Pipeline, stageName string) bool {
	for _, s := range p.Declaration.Stages {
		if s.Name == stageName {
			return true
		}
	}

	return false
}

func copyCustomActionType(c *CustomActionType) *CustomActionType {
	cp := *c

	if c.Tags != nil {
		cp.Tags = make(map[string]string, len(c.Tags))
		maps.Copy(cp.Tags, c.Tags)
	}

	if c.ConfigurationProperties != nil {
		cp.ConfigurationProperties = make([]ActionConfigurationProperty, len(c.ConfigurationProperties))
		copy(cp.ConfigurationProperties, c.ConfigurationProperties)
	}

	if c.Settings != nil {
		s := *c.Settings
		cp.Settings = &s
	}

	return &cp
}

// copyDeclaration deep-copies a PipelineDeclaration so callers cannot mutate
// the backend's stored stages, actions, or configuration maps.
func copyDeclaration(d PipelineDeclaration) PipelineDeclaration {
	out := d
	out.Stages = copyStages(d.Stages)
	out.Variables = copyVariables(d.Variables)
	out.Triggers = copyTriggers(d.Triggers)

	if d.ArtifactStores != nil {
		out.ArtifactStores = make(map[string]ArtifactStore, len(d.ArtifactStores))
		maps.Copy(out.ArtifactStores, d.ArtifactStores)
	}

	return out
}

func copyVariables(vars []PipelineVariable) []PipelineVariable {
	if vars == nil {
		return nil
	}

	out := make([]PipelineVariable, len(vars))
	copy(out, vars)

	return out
}

func copyTriggers(triggers []Trigger) []Trigger {
	if triggers == nil {
		return nil
	}

	out := make([]Trigger, len(triggers))
	copy(out, triggers)

	return out
}

func copyStages(stages []Stage) []Stage {
	if stages == nil {
		return nil
	}

	out := make([]Stage, len(stages))
	for i, s := range stages {
		out[i] = Stage{
			Name:        s.Name,
			Type:        s.Type,
			Actions:     copyActions(s.Actions),
			BeforeEntry: copyCondition(s.BeforeEntry),
			OnFailure:   copyCondition(s.OnFailure),
			OnSuccess:   copyCondition(s.OnSuccess),
		}
	}

	return out
}

func copyCondition(c *Condition) *Condition {
	if c == nil {
		return nil
	}

	cp := *c
	if c.Rules != nil {
		cp.Rules = make([]Rule, len(c.Rules))
		copy(cp.Rules, c.Rules)
	}

	return &cp
}

func copyActions(actions []Action) []Action {
	if actions == nil {
		return nil
	}

	out := make([]Action, len(actions))
	for i, a := range actions {
		actionCopy := a
		actionCopy.Configuration = copyStringMap(a.Configuration)
		actionCopy.InputArtifacts = copyArtifactRefs(a.InputArtifacts)
		actionCopy.OutputArtifacts = copyArtifactRefs(a.OutputArtifacts)
		out[i] = actionCopy
	}

	return out
}

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func copyArtifactRefs(refs []ArtifactRef) []ArtifactRef {
	if refs == nil {
		return nil
	}

	out := make([]ArtifactRef, len(refs))
	copy(out, refs)

	return out
}

// --- Pipeline execution operations ---

// PipelineExecution represents a stored pipeline execution.
type PipelineExecution struct {
	PipelineName        string `json:"pipelineName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	Status              string `json:"status"`
	Trigger             string `json:"trigger,omitempty"`
	PipelineVersion     int    `json:"pipelineVersion"`
}

// StartPipelineExecution starts and stores a new execution of a pipeline.
func (b *InMemoryBackend) StartPipelineExecution(ctx context.Context, pipelineName string) (*PipelineExecution, error) {
	b.mu.Lock("StartPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[pipelineName]
	if !ok {
		return nil, ErrNotFound
	}

	exec := &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: uuid.NewString(),
		Status:              statusInProgress,
		PipelineVersion:     p.Declaration.Version,
	}

	execs := b.executionsStore(region)
	execs[pipelineName] = append(execs[pipelineName], exec)

	// Record an action execution for every action in the pipeline so that
	// ListActionExecutions reflects the work performed by this execution.
	now := time.Now().UTC()

	actionExecs := b.actionExecutionsStore(region)

	for _, stage := range p.Declaration.Stages {
		for _, action := range stage.Actions {
			ae := &ActionExecution{
				PipelineExecutionID: exec.PipelineExecutionID,
				ActionExecutionID:   uuid.NewString(),
				StageName:           stage.Name,
				ActionName:          action.Name,
				Status:              statusSucceeded,
				StartTime:           now,
				LastUpdateTime:      now,
			}
			actionExecs[pipelineName] = append(actionExecs[pipelineName], ae)
		}
	}

	cp := *exec

	return &cp, nil
}

// GetPipelineExecution returns the stored execution by pipeline name and execution ID.
func (b *InMemoryBackend) GetPipelineExecution(
	ctx context.Context,
	pipelineName, executionID string,
) (*PipelineExecution, error) {
	b.mu.RLock("GetPipelineExecution")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region)[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	for _, exec := range b.executionsStore(region)[pipelineName] {
		if exec.PipelineExecutionID == executionID {
			cp := *exec

			return &cp, nil
		}
	}

	// Return a stub for unknown execution IDs to maintain backward compatibility.
	return &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: executionID,
		Status:              "Succeeded",
	}, nil
}

// StopPipelineExecution stops an in-progress pipeline execution.
func (b *InMemoryBackend) StopPipelineExecution(
	ctx context.Context,
	pipelineName, executionID, reason string,
) (*PipelineExecution, error) {
	b.mu.Lock("StopPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region)[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	_ = reason

	for _, exec := range b.executionsStore(region)[pipelineName] {
		if exec.PipelineExecutionID == executionID {
			exec.Status = "Stopping"
			cp := *exec

			return &cp, nil
		}
	}

	return &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: executionID,
		Status:              "Stopping",
	}, nil
}

// ListPipelineExecutions returns stored executions for a pipeline, most recent first.
func (b *InMemoryBackend) ListPipelineExecutions(
	ctx context.Context,
	pipelineName string,
) ([]PipelineExecution, error) {
	b.mu.RLock("ListPipelineExecutions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region)[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	stored := b.executionsStore(region)[pipelineName]
	out := make([]PipelineExecution, len(stored))

	// Return in reverse order (most recent first).
	for i, e := range stored {
		out[len(stored)-1-i] = *e
	}

	return out, nil
}

// StageState represents the state of a pipeline stage.
type StageState struct {
	InboundTransitionState  *StageTransitionState
	OutboundTransitionState *StageTransitionState
	StageName               string
	ActionStates            []map[string]any
}

// GetPipelineState returns the current state of each stage in a pipeline.
func (b *InMemoryBackend) GetPipelineState(ctx context.Context, pipelineName string) ([]StageState, error) {
	b.mu.RLock("GetPipelineState")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region)[pipelineName]
	if !ok {
		return nil, ErrNotFound
	}

	transitions := b.stageTransitionsStore(region)

	states := make([]StageState, len(p.Declaration.Stages))
	for i, stage := range p.Declaration.Stages {
		inKey := stageTransitionKey{
			PipelineName: pipelineName, StageName: stage.Name, TransitionType: transitionTypeInbound,
		}
		outKey := stageTransitionKey{
			PipelineName: pipelineName, StageName: stage.Name, TransitionType: transitionTypeOutbound,
		}

		var inState, outState *StageTransitionState
		if ts, found := transitions[inKey]; found {
			tsCopy := *ts
			inState = &tsCopy
		}

		if ts, found := transitions[outKey]; found {
			tsCopy := *ts
			outState = &tsCopy
		}

		actionExecs := b.actionExecutionsStore(region)[pipelineName]
		actionStates := make([]map[string]any, len(stage.Actions))
		for j, action := range stage.Actions {
			state := map[string]any{
				"actionName": action.Name,
			}
			// Walk backwards to find the most recent execution for this stage/action pair.
			for _, ae := range slices.Backward(actionExecs) {
				if ae.StageName == stage.Name && ae.ActionName == action.Name {
					state["latestExecution"] = map[string]any{
						"actionExecutionId": ae.ActionExecutionID,
						keyStatus:           ae.Status,
						"startTime":         float64(ae.StartTime.Unix()),
						"lastUpdateTime":    float64(ae.LastUpdateTime.Unix()),
					}

					break
				}
			}
			actionStates[j] = state
		}

		states[i] = StageState{
			StageName:               stage.Name,
			InboundTransitionState:  inState,
			OutboundTransitionState: outState,
			ActionStates:            actionStates,
		}
	}

	return states, nil
}

// RetryStageExecution retries a failed stage in a pipeline.
func (b *InMemoryBackend) RetryStageExecution(
	ctx context.Context,
	pipelineName, stageName, executionID string,
) (*PipelineExecution, error) {
	b.mu.RLock("RetryStageExecution")
	defer b.mu.RUnlock()

	if _, ok := b.pipelinesStore(getRegion(ctx, b.region))[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	_ = stageName

	return &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: executionID,
		Status:              statusInProgress,
	}, nil
}

// RollbackStage rolls back a stage to a previous successful execution.
func (b *InMemoryBackend) RollbackStage(
	ctx context.Context,
	pipelineName, stageName, targetExecutionID string,
) (*PipelineExecution, error) {
	b.mu.RLock("RollbackStage")
	defer b.mu.RUnlock()

	if _, ok := b.pipelinesStore(getRegion(ctx, b.region))[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	_ = stageName
	_ = targetExecutionID

	return &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: uuid.NewString(),
		Status:              statusInProgress,
	}, nil
}

// OverrideStageCondition overrides a stage condition.
func (b *InMemoryBackend) OverrideStageCondition(
	ctx context.Context,
	pipelineName, stageName, executionID string,
) error {
	b.mu.RLock("OverrideStageCondition")
	defer b.mu.RUnlock()

	if _, ok := b.pipelinesStore(getRegion(ctx, b.region))[pipelineName]; !ok {
		return ErrNotFound
	}

	_ = stageName
	_ = executionID

	return nil
}

// ListWebhooks returns all webhooks in the request region, sorted by name.
func (b *InMemoryBackend) ListWebhooks(ctx context.Context) []*Webhook {
	b.mu.RLock("ListWebhooks")
	defer b.mu.RUnlock()

	store := b.webhooksStore(getRegion(ctx, b.region))

	result := make([]*Webhook, 0, len(store))
	for _, wh := range store {
		cp := *wh
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// PutWebhook creates or updates a webhook with full definition fields.
func (b *InMemoryBackend) PutWebhook(ctx context.Context, wh *Webhook) (*Webhook, error) {
	b.mu.Lock("PutWebhook")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.webhooksStore(region)

	cp := *wh
	cp.ARN = b.buildWebhookARN(region, wh.Name)
	cp.URL = fmt.Sprintf("https://webhooks.%s.codepipeline.aws.a2z.com/trigger?t=%s",
		region, uuid.NewString())

	if existing, ok := store[wh.Name]; ok {
		// Preserve URL on update.
		cp.URL = existing.URL
	}

	store[cp.Name] = &cp
	b.webhookARNIndexStore(region)[cp.ARN] = cp.Name

	result := cp

	return &result, nil
}

// RegisterWebhookWithThirdParty registers a webhook with a third-party provider.
func (b *InMemoryBackend) RegisterWebhookWithThirdParty(ctx context.Context, name string) error {
	b.mu.Lock("RegisterWebhookWithThirdParty")
	defer b.mu.Unlock()

	wh, ok := b.webhooksStore(getRegion(ctx, b.region))[name]
	if !ok {
		return ErrWebhookNotFound
	}

	wh.RegisteredWithThirdParty = true

	return nil
}

// PollForJobs returns available queued jobs matching the given ActionTypeID.
func (b *InMemoryBackend) PollForJobs(ctx context.Context, category, owner, provider, version string) ([]*Job, error) {
	b.mu.RLock("PollForJobs")
	defer b.mu.RUnlock()

	store := b.jobsStore(getRegion(ctx, b.region))

	result := make([]*Job, 0, len(store))

	for _, job := range store {
		if job.Status != "Queued" {
			continue
		}

		at := job.ActionTypeID
		if at.Category != category || at.Provider != provider || at.Version != version {
			continue
		}

		if owner != "" && at.Owner != "" && at.Owner != owner {
			continue
		}

		cp := *job
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// PollForThirdPartyJobs returns available third-party jobs.
func (b *InMemoryBackend) PollForThirdPartyJobs(
	ctx context.Context,
	category, provider, version string,
) ([]*Job, error) {
	return b.PollForJobs(ctx, category, "ThirdParty", provider, version)
}

// GetThirdPartyJobDetails returns details for a third-party job.
func (b *InMemoryBackend) GetThirdPartyJobDetails(ctx context.Context, jobID, clientToken string) (*Job, error) {
	b.mu.RLock("GetThirdPartyJobDetails")
	defer b.mu.RUnlock()

	job, ok := b.jobsStore(getRegion(ctx, b.region))[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}

	_ = clientToken

	cp := *job

	return &cp, nil
}

// PutJobSuccessResult acknowledges job success.
func (b *InMemoryBackend) PutJobSuccessResult(ctx context.Context, jobID string) error {
	b.mu.Lock("PutJobSuccessResult")
	defer b.mu.Unlock()

	job, ok := b.jobsStore(getRegion(ctx, b.region))[jobID]
	if !ok {
		return ErrJobNotFound
	}

	job.Status = "Succeeded"

	return nil
}

// PutJobFailureResult acknowledges job failure.
func (b *InMemoryBackend) PutJobFailureResult(ctx context.Context, jobID, message string) error {
	b.mu.Lock("PutJobFailureResult")
	defer b.mu.Unlock()

	job, ok := b.jobsStore(getRegion(ctx, b.region))[jobID]
	if !ok {
		return ErrJobNotFound
	}

	_ = message
	job.Status = "Failed"

	return nil
}

// PutThirdPartyJobSuccessResult acknowledges third-party job success.
func (b *InMemoryBackend) PutThirdPartyJobSuccessResult(ctx context.Context, jobID, _ string) error {
	return b.PutJobSuccessResult(ctx, jobID)
}

// PutThirdPartyJobFailureResult acknowledges third-party job failure.
func (b *InMemoryBackend) PutThirdPartyJobFailureResult(ctx context.Context, jobID, _, message string) error {
	return b.PutJobFailureResult(ctx, jobID, message)
}

// PutActionRevision puts an action revision for a pipeline source action.
func (b *InMemoryBackend) PutActionRevision(ctx context.Context, pipelineName, stageName, actionName string) error {
	b.mu.RLock("PutActionRevision")
	defer b.mu.RUnlock()

	if _, ok := b.pipelinesStore(getRegion(ctx, b.region))[pipelineName]; !ok {
		return ErrNotFound
	}

	_ = stageName
	_ = actionName

	return nil
}

// PutApprovalResult submits a manual approval for a pipeline action.
func (b *InMemoryBackend) PutApprovalResult(
	ctx context.Context,
	pipelineName, stageName, actionName, status, summary string,
) error {
	b.mu.RLock("PutApprovalResult")
	defer b.mu.RUnlock()

	if _, ok := b.pipelinesStore(getRegion(ctx, b.region))[pipelineName]; !ok {
		return ErrNotFound
	}

	_ = stageName
	_ = actionName
	_ = status
	_ = summary

	return nil
}

// UpdateActionType updates an action type definition with full fields.
func (b *InMemoryBackend) UpdateActionType(ctx context.Context, cat *CustomActionType) error {
	b.mu.Lock("UpdateActionType")
	defer b.mu.Unlock()

	store := b.customActionTypesStore(getRegion(ctx, b.region))
	key := customActionTypeKey{
		Category: cat.Category,
		Provider: cat.Provider,
		Version:  cat.Version,
	}

	if _, ok := store[key]; !ok {
		return ErrActionTypeNotFound
	}

	cp := copyCustomActionType(cat)
	store[key] = cp

	return nil
}

// ListActionTypes returns all registered action types in the request region.
func (b *InMemoryBackend) ListActionTypes(ctx context.Context) []*CustomActionType {
	b.mu.RLock("ListActionTypes")
	defer b.mu.RUnlock()

	store := b.customActionTypesStore(getRegion(ctx, b.region))

	result := make([]*CustomActionType, 0, len(store))

	for _, cat := range store {
		result = append(result, copyCustomActionType(cat))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Provider < result[j].Provider
	})

	return result
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

// ListActionExecutions returns the recorded action executions for a pipeline,
// most recent first. An optional pipelineExecutionId filters to a single run.
func (b *InMemoryBackend) ListActionExecutions(
	ctx context.Context,
	pipelineName, pipelineExecutionID string,
) ([]map[string]any, error) {
	b.mu.RLock("ListActionExecutions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region)[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	stored := b.actionExecutionsStore(region)[pipelineName]
	out := make([]map[string]any, 0, len(stored))

	// Iterate in reverse so the most recent execution appears first.
	for _, ae := range slices.Backward(stored) {
		if pipelineExecutionID != "" && ae.PipelineExecutionID != pipelineExecutionID {
			continue
		}

		out = append(out, map[string]any{
			keyPipelineExecutionID: ae.PipelineExecutionID,
			"actionExecutionId":    ae.ActionExecutionID,
			"stageName":            ae.StageName,
			"actionName":           ae.ActionName,
			"startTime":            float64(ae.StartTime.Unix()),
			"lastUpdateTime":       float64(ae.LastUpdateTime.Unix()),
			keyStatus:              ae.Status,
		})
	}

	return out, nil
}

// ListRuleExecutions returns rule executions for a pipeline. The emulator does
// not run condition rules, so this returns an empty (but valid) list for a known
// pipeline and ErrNotFound otherwise.
func (b *InMemoryBackend) ListRuleExecutions(ctx context.Context, pipelineName string) ([]map[string]any, error) {
	b.mu.RLock("ListRuleExecutions")
	defer b.mu.RUnlock()

	if _, ok := b.pipelinesStore(getRegion(ctx, b.region))[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	return []map[string]any{}, nil
}

// ListRuleTypes returns the AWS-managed CodePipeline rule types. These mirror
// the built-in condition rule providers AWS exposes.
func (b *InMemoryBackend) ListRuleTypes() []map[string]any {
	providers := []string{"Deployment", "LambdaInvoke", "CloudWatchAlarm", "VariableCheck"}

	out := make([]map[string]any, 0, len(providers))

	for _, provider := range providers {
		out = append(out, map[string]any{
			"id": map[string]any{
				"category": "Rule",
				"owner":    ruleOwnerAWS,
				"provider": provider,
				"version":  "1",
			},
		})
	}

	return out
}

// ListDeployActionExecutionTargets returns the deploy targets for an action
// execution. The emulator does not model deployment targets, so it returns an
// empty (but valid) list for a known pipeline and ErrNotFound otherwise.
func (b *InMemoryBackend) ListDeployActionExecutionTargets(
	ctx context.Context,
	pipelineName, executionID string,
) ([]map[string]any, error) {
	b.mu.RLock("ListDeployActionExecutionTargets")
	defer b.mu.RUnlock()

	if _, ok := b.pipelinesStore(getRegion(ctx, b.region))[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	_ = executionID

	return []map[string]any{}, nil
}
