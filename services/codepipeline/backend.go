// Package codepipeline provides an in-memory implementation of the AWS CodePipeline service.
package codepipeline

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

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
)

var (
	// ErrNotFound is returned when a pipeline resource does not exist.
	ErrNotFound = awserr.New("PipelineNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
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
type InMemoryBackend struct {
	pipelines         map[string]*Pipeline
	pipelineARNIndex  map[string]string // ARN → pipeline name
	customActionTypes map[customActionTypeKey]*CustomActionType
	jobs              map[string]*Job     // jobID → Job
	webhooks          map[string]*Webhook // name → Webhook
	webhookARNIndex   map[string]string   // ARN → webhook name
	stageTransitions  map[stageTransitionKey]*StageTransitionState
	executions        map[string][]*PipelineExecution // pipelineName → executions
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipelines:         make(map[string]*Pipeline),
		pipelineARNIndex:  make(map[string]string),
		customActionTypes: make(map[customActionTypeKey]*CustomActionType),
		jobs:              make(map[string]*Job),
		webhooks:          make(map[string]*Webhook),
		webhookARNIndex:   make(map[string]string),
		stageTransitions:  make(map[stageTransitionKey]*StageTransitionState),
		executions:        make(map[string][]*PipelineExecution),
		accountID:         accountID,
		region:            region,
		mu:                lockmetrics.New("codepipeline-" + region),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state in the backend, resetting it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.pipelines = make(map[string]*Pipeline)
	b.pipelineARNIndex = make(map[string]string)
	b.customActionTypes = make(map[customActionTypeKey]*CustomActionType)
	b.jobs = make(map[string]*Job)
	b.webhooks = make(map[string]*Webhook)
	b.webhookARNIndex = make(map[string]string)
	b.stageTransitions = make(map[stageTransitionKey]*StageTransitionState)
	b.executions = make(map[string][]*PipelineExecution)
}

func (b *InMemoryBackend) buildPipelineARN(name string) string {
	return arn.Build("codepipeline", b.region, b.accountID, name)
}

func (b *InMemoryBackend) buildWebhookARN(name string) string {
	return arn.Build("codepipeline", b.region, b.accountID, "webhook:"+name)
}

// CreatePipeline creates a new CodePipeline pipeline.
func (b *InMemoryBackend) CreatePipeline(decl PipelineDeclaration, tags map[string]string) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	if _, exists := b.pipelines[decl.Name]; exists {
		return nil, fmt.Errorf("%w: pipeline %q already exists", ErrAlreadyExists, decl.Name)
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
			PipelineArn: b.buildPipelineARN(decl.Name),
			Created:     now,
			Updated:     now,
		},
		Tags: tagsCopy,
	}
	b.pipelines[decl.Name] = p
	b.pipelineARNIndex[p.Metadata.PipelineArn] = decl.Name

	return copyPipeline(p), nil
}

// GetPipeline returns the pipeline with the given name.
func (b *InMemoryBackend) GetPipeline(name string) (*Pipeline, error) {
	b.mu.RLock("GetPipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	return copyPipeline(p), nil
}

// UpdatePipeline replaces the pipeline declaration.
// If decl.Version is non-zero it must match the current version (optimistic concurrency).
func (b *InMemoryBackend) UpdatePipeline(decl PipelineDeclaration) (*Pipeline, error) {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines[decl.Name]
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
func (b *InMemoryBackend) DeletePipeline(name string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines[name]
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	delete(b.pipelineARNIndex, p.Metadata.PipelineArn)
	delete(b.pipelines, name)
	delete(b.executions, name)

	// Cascade: remove disabled stage transitions for this pipeline.
	for key := range b.stageTransitions {
		if key.PipelineName == name {
			delete(b.stageTransitions, key)
		}
	}

	return nil
}

// ListPipelines returns a sorted summary of all pipelines.
func (b *InMemoryBackend) ListPipelines() []PipelineSummary {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.pipelines))
	for name := range b.pipelines {
		names = append(names, name)
	}

	sort.Strings(names)

	summaries := make([]PipelineSummary, 0, len(b.pipelines))
	for _, name := range names {
		p := b.pipelines[name]
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

// resolveResourceARN looks up a resource by ARN, returning its type ("pipeline" or "webhook")
// and name. Returns ErrResourceNotFound if ARN refers to a webhook, ErrNotFound if unknown.
func (b *InMemoryBackend) resolveResourceARN(resourceARN string) (string, string, error) {
	if n, ok := b.pipelineARNIndex[resourceARN]; ok {
		return kindPipeline, n, nil
	}

	if n, ok := b.webhookARNIndex[resourceARN]; ok {
		return "webhook", n, nil
	}

	return "", "", ErrNotFound
}

// ListTagsForResource returns the sorted tags for a pipeline by ARN.
// Returns ResourceNotFoundException when the ARN refers to a non-pipeline resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	kind, name, err := b.resolveResourceARN(resourceARN)
	if err != nil {
		return nil, err
	}

	switch kind {
	case kindPipeline:
		return tagsToSortedSlice(b.pipelines[name].Tags), nil
	case "webhook":
		// Webhooks support tagging but we don't store tags on them yet;
		// return empty slice for now.
		return []Tag{}, nil
	default:
		return nil, fmt.Errorf("%w: ARN %q", ErrResourceNotFound, resourceARN)
	}
}

// TagResource adds or updates tags on a pipeline by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	kind, name, err := b.resolveResourceARN(resourceARN)
	if err != nil {
		return err
	}

	if kind != kindPipeline {
		return fmt.Errorf("%w: ARN %q is not a pipeline", ErrResourceNotFound, resourceARN)
	}

	p := b.pipelines[name]
	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	for _, t := range tags {
		p.Tags[t.Key] = t.Value
	}

	return nil
}

// UntagResource removes tags from a pipeline by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	kind, name, err := b.resolveResourceARN(resourceARN)
	if err != nil {
		return err
	}

	if kind != kindPipeline {
		return fmt.Errorf("%w: ARN %q is not a pipeline", ErrResourceNotFound, resourceARN)
	}

	p := b.pipelines[name]

	for _, k := range tagKeys {
		delete(p.Tags, k)
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
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	tags := make([]Tag, 0, len(kv))
	for _, k := range keys {
		tags = append(tags, Tag{Key: k, Value: kv[k]})
	}

	return tags
}

// AddPipelineInternal seeds a pipeline directly into the backend (for testing).
func (b *InMemoryBackend) AddPipelineInternal(decl PipelineDeclaration, tags map[string]string) *Pipeline {
	b.mu.Lock("AddPipelineInternal")
	defer b.mu.Unlock()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	if decl.Version == 0 {
		decl.Version = 1
	}

	p := &Pipeline{
		Declaration: decl,
		Metadata: PipelineMetadata{
			PipelineArn: b.buildPipelineARN(decl.Name),
			Created:     now,
			Updated:     now,
		},
		Tags: tagsCopy,
	}
	b.pipelines[decl.Name] = p
	b.pipelineARNIndex[p.Metadata.PipelineArn] = decl.Name

	return copyPipeline(p)
}

// AddCustomActionTypeInternal seeds a custom action type directly into the backend (for testing).
func (b *InMemoryBackend) AddCustomActionTypeInternal(cat *CustomActionType) {
	b.mu.Lock("AddCustomActionTypeInternal")
	defer b.mu.Unlock()

	key := customActionTypeKey{Category: cat.Category, Provider: cat.Provider, Version: cat.Version}
	b.customActionTypes[key] = copyCustomActionType(cat)
}

// GetStageTransitionState returns the disabled state for a stage transition, or nil if enabled.
func (b *InMemoryBackend) GetStageTransitionState(
	pipelineName, stageName, transitionType string,
) *StageTransitionState {
	b.mu.RLock("GetStageTransitionState")
	defer b.mu.RUnlock()

	key := stageTransitionKey{
		PipelineName:   pipelineName,
		StageName:      stageName,
		TransitionType: transitionType,
	}

	state, ok := b.stageTransitions[key]
	if !ok {
		return nil
	}

	cp := *state

	return &cp
}

// --- Custom Action Type operations ---

// CreateCustomActionType stores a new custom action type.
func (b *InMemoryBackend) CreateCustomActionType(cat *CustomActionType) (*CustomActionType, error) {
	b.mu.Lock("CreateCustomActionType")
	defer b.mu.Unlock()

	key := customActionTypeKey{Category: cat.Category, Provider: cat.Provider, Version: cat.Version}

	if _, exists := b.customActionTypes[key]; exists {
		return nil, fmt.Errorf("%w: custom action type %q/%q/%q already exists",
			ErrAlreadyExists, cat.Category, cat.Provider, cat.Version)
	}

	if cat.Owner == "" {
		cat.Owner = "Custom"
	}

	cp := copyCustomActionType(cat)
	b.customActionTypes[key] = cp

	return copyCustomActionType(cp), nil
}

// DeleteCustomActionType removes a custom action type.
// Returns ResourceInUseException if any pipeline references the type.
func (b *InMemoryBackend) DeleteCustomActionType(category, provider, version string) error {
	b.mu.Lock("DeleteCustomActionType")
	defer b.mu.Unlock()

	key := customActionTypeKey{Category: category, Provider: provider, Version: version}

	if _, ok := b.customActionTypes[key]; !ok {
		return fmt.Errorf("%w: custom action type %q/%q/%q", ErrActionTypeNotFound, category, provider, version)
	}

	// Check that no pipeline references this action type.
	for pName, p := range b.pipelines {
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

	delete(b.customActionTypes, key)

	return nil
}

// GetActionType retrieves a custom action type.
func (b *InMemoryBackend) GetActionType(category, owner, provider, version string) (*CustomActionType, error) {
	b.mu.RLock("GetActionType")
	defer b.mu.RUnlock()

	key := customActionTypeKey{Category: category, Provider: provider, Version: version}

	cat, ok := b.customActionTypes[key]
	if !ok {
		return nil, fmt.Errorf("%w: action type %q/%q/%q/%q", ErrActionTypeNotFound, category, owner, provider, version)
	}

	return copyCustomActionType(cat), nil
}

// --- Job operations ---

// AcknowledgeJob acknowledges that a job worker has received a job.
// Returns InProgress if Nonce matches; otherwise returns current status unchanged.
func (b *InMemoryBackend) AcknowledgeJob(jobID, nonce string) (string, error) {
	b.mu.Lock("AcknowledgeJob")
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return "", fmt.Errorf("%w: job %q", ErrJobNotFound, jobID)
	}

	if job.Nonce == nonce {
		job.Status = statusInProgress
	}

	return job.Status, nil
}

// AcknowledgeThirdPartyJob acknowledges that a third-party job worker has received a job.
func (b *InMemoryBackend) AcknowledgeThirdPartyJob(jobID, nonce, clientToken string) (string, error) {
	b.mu.Lock("AcknowledgeThirdPartyJob")
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return "", fmt.Errorf("%w: third-party job %q with client token %q", ErrJobNotFound, jobID, clientToken)
	}

	if job.Nonce == nonce {
		job.Status = statusInProgress
	}

	return job.Status, nil
}

// GetJobDetails returns details for a job by ID.
func (b *InMemoryBackend) GetJobDetails(jobID string) (*Job, error) {
	b.mu.RLock("GetJobDetails")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: job %q", ErrJobNotFound, jobID)
	}

	cp := *job

	return &cp, nil
}

// AddJobInternal seeds a job directly into the backend (for testing).
func (b *InMemoryBackend) AddJobInternal(job *Job) {
	b.mu.Lock("AddJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.jobs[cp.ID] = &cp
}

// --- Webhook operations ---

// DeleteWebhook removes a webhook by name (idempotent).
func (b *InMemoryBackend) DeleteWebhook(name string) error {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	if wh, ok := b.webhooks[name]; ok {
		delete(b.webhookARNIndex, wh.ARN)
	}

	delete(b.webhooks, name)

	return nil
}

// DeregisterWebhookWithThirdParty clears the third-party registration flag on a webhook.
func (b *InMemoryBackend) DeregisterWebhookWithThirdParty(name string) error {
	b.mu.Lock("DeregisterWebhookWithThirdParty")
	defer b.mu.Unlock()

	if wh, ok := b.webhooks[name]; ok {
		wh.RegisteredWithThirdParty = false
	}

	return nil
}

// AddWebhookInternal seeds a webhook directly into the backend (for testing).
func (b *InMemoryBackend) AddWebhookInternal(wh *Webhook) {
	b.mu.Lock("AddWebhookInternal")
	defer b.mu.Unlock()

	cp := *wh
	if cp.ARN == "" {
		cp.ARN = b.buildWebhookARN(cp.Name)
	}

	b.webhooks[cp.Name] = &cp
	b.webhookARNIndex[cp.ARN] = cp.Name
}

// --- Stage transition operations ---

// DisableStageTransition disables a stage transition and records the reason.
// Returns StageNotFoundException if stageName does not exist in the pipeline.
func (b *InMemoryBackend) DisableStageTransition(pipelineName, stageName, transitionType, reason string) error {
	b.mu.Lock("DisableStageTransition")
	defer b.mu.Unlock()

	p, ok := b.pipelines[pipelineName]
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	if !pipelineHasStage(p, stageName) {
		return fmt.Errorf("%w: stage %q not found in pipeline %q", ErrStageNotFound, stageName, pipelineName)
	}

	key := stageTransitionKey{PipelineName: pipelineName, StageName: stageName, TransitionType: transitionType}
	b.stageTransitions[key] = &StageTransitionState{
		PipelineName:   pipelineName,
		StageName:      stageName,
		TransitionType: transitionType,
		Reason:         reason,
		Disabled:       true,
	}

	return nil
}

// EnableStageTransition re-enables a stage transition.
func (b *InMemoryBackend) EnableStageTransition(pipelineName, stageName, transitionType string) error {
	b.mu.Lock("EnableStageTransition")
	defer b.mu.Unlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, pipelineName)
	}

	key := stageTransitionKey{PipelineName: pipelineName, StageName: stageName, TransitionType: transitionType}
	delete(b.stageTransitions, key)

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
func (b *InMemoryBackend) StartPipelineExecution(pipelineName string) (*PipelineExecution, error) {
	b.mu.Lock("StartPipelineExecution")
	defer b.mu.Unlock()

	p, ok := b.pipelines[pipelineName]
	if !ok {
		return nil, ErrNotFound
	}

	exec := &PipelineExecution{
		PipelineName:        pipelineName,
		PipelineExecutionID: uuid.NewString(),
		Status:              statusInProgress,
		PipelineVersion:     p.Declaration.Version,
	}

	b.executions[pipelineName] = append(b.executions[pipelineName], exec)

	cp := *exec

	return &cp, nil
}

// GetPipelineExecution returns the stored execution by pipeline name and execution ID.
func (b *InMemoryBackend) GetPipelineExecution(pipelineName, executionID string) (*PipelineExecution, error) {
	b.mu.RLock("GetPipelineExecution")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	for _, exec := range b.executions[pipelineName] {
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
func (b *InMemoryBackend) StopPipelineExecution(pipelineName, executionID, reason string) (*PipelineExecution, error) {
	b.mu.Lock("StopPipelineExecution")
	defer b.mu.Unlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	_ = reason

	for _, exec := range b.executions[pipelineName] {
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
func (b *InMemoryBackend) ListPipelineExecutions(pipelineName string) ([]PipelineExecution, error) {
	b.mu.RLock("ListPipelineExecutions")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	stored := b.executions[pipelineName]
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
func (b *InMemoryBackend) GetPipelineState(pipelineName string) ([]StageState, error) {
	b.mu.RLock("GetPipelineState")
	defer b.mu.RUnlock()

	p, ok := b.pipelines[pipelineName]
	if !ok {
		return nil, ErrNotFound
	}

	states := make([]StageState, len(p.Declaration.Stages))
	for i, stage := range p.Declaration.Stages {
		inKey := stageTransitionKey{
			PipelineName: pipelineName, StageName: stage.Name, TransitionType: transitionTypeInbound,
		}
		outKey := stageTransitionKey{
			PipelineName: pipelineName, StageName: stage.Name, TransitionType: transitionTypeOutbound,
		}

		var inState, outState *StageTransitionState
		if ts, found := b.stageTransitions[inKey]; found {
			tsCopy := *ts
			inState = &tsCopy
		}

		if ts, found := b.stageTransitions[outKey]; found {
			tsCopy := *ts
			outState = &tsCopy
		}

		actionStates := make([]map[string]any, len(stage.Actions))
		for j, action := range stage.Actions {
			actionStates[j] = map[string]any{
				"actionName": action.Name,
			}
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
func (b *InMemoryBackend) RetryStageExecution(pipelineName, stageName, executionID string) (*PipelineExecution, error) {
	b.mu.RLock("RetryStageExecution")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
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
func (b *InMemoryBackend) RollbackStage(pipelineName, stageName, targetExecutionID string) (*PipelineExecution, error) {
	b.mu.RLock("RollbackStage")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
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
func (b *InMemoryBackend) OverrideStageCondition(pipelineName, stageName, executionID string) error {
	b.mu.RLock("OverrideStageCondition")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return ErrNotFound
	}

	_ = stageName
	_ = executionID

	return nil
}

// ListWebhooks returns all webhooks in the backend, sorted by name.
func (b *InMemoryBackend) ListWebhooks() []*Webhook {
	b.mu.RLock("ListWebhooks")
	defer b.mu.RUnlock()

	result := make([]*Webhook, 0, len(b.webhooks))
	for _, wh := range b.webhooks {
		cp := *wh
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// PutWebhook creates or updates a webhook with full definition fields.
func (b *InMemoryBackend) PutWebhook(wh *Webhook) (*Webhook, error) {
	b.mu.Lock("PutWebhook")
	defer b.mu.Unlock()

	cp := *wh
	cp.ARN = b.buildWebhookARN(wh.Name)
	cp.URL = fmt.Sprintf("https://webhooks.%s.codepipeline.aws.a2z.com/trigger?t=%s",
		b.region, uuid.NewString())

	if existing, ok := b.webhooks[wh.Name]; ok {
		// Preserve URL on update.
		cp.URL = existing.URL
	}

	b.webhooks[cp.Name] = &cp
	b.webhookARNIndex[cp.ARN] = cp.Name

	result := cp

	return &result, nil
}

// RegisterWebhookWithThirdParty registers a webhook with a third-party provider.
func (b *InMemoryBackend) RegisterWebhookWithThirdParty(name string) error {
	b.mu.Lock("RegisterWebhookWithThirdParty")
	defer b.mu.Unlock()

	wh, ok := b.webhooks[name]
	if !ok {
		return ErrWebhookNotFound
	}

	wh.RegisteredWithThirdParty = true

	return nil
}

// PollForJobs returns available queued jobs matching the given ActionTypeID.
func (b *InMemoryBackend) PollForJobs(category, owner, provider, version string) ([]*Job, error) {
	b.mu.RLock("PollForJobs")
	defer b.mu.RUnlock()

	result := make([]*Job, 0, len(b.jobs))

	for _, job := range b.jobs {
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
func (b *InMemoryBackend) PollForThirdPartyJobs(category, provider, version string) ([]*Job, error) {
	return b.PollForJobs(category, "ThirdParty", provider, version)
}

// GetThirdPartyJobDetails returns details for a third-party job.
func (b *InMemoryBackend) GetThirdPartyJobDetails(jobID, clientToken string) (*Job, error) {
	b.mu.RLock("GetThirdPartyJobDetails")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}

	_ = clientToken

	cp := *job

	return &cp, nil
}

// PutJobSuccessResult acknowledges job success.
func (b *InMemoryBackend) PutJobSuccessResult(jobID string) error {
	b.mu.Lock("PutJobSuccessResult")
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}

	job.Status = "Succeeded"

	return nil
}

// PutJobFailureResult acknowledges job failure.
func (b *InMemoryBackend) PutJobFailureResult(jobID, message string) error {
	b.mu.Lock("PutJobFailureResult")
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}

	_ = message
	job.Status = "Failed"

	return nil
}

// PutThirdPartyJobSuccessResult acknowledges third-party job success.
func (b *InMemoryBackend) PutThirdPartyJobSuccessResult(jobID, _ string) error {
	return b.PutJobSuccessResult(jobID)
}

// PutThirdPartyJobFailureResult acknowledges third-party job failure.
func (b *InMemoryBackend) PutThirdPartyJobFailureResult(jobID, _, message string) error {
	return b.PutJobFailureResult(jobID, message)
}

// PutActionRevision puts an action revision for a pipeline source action.
func (b *InMemoryBackend) PutActionRevision(pipelineName, stageName, actionName string) error {
	b.mu.RLock("PutActionRevision")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return ErrNotFound
	}

	_ = stageName
	_ = actionName

	return nil
}

// PutApprovalResult submits a manual approval for a pipeline action.
func (b *InMemoryBackend) PutApprovalResult(pipelineName, stageName, actionName, status, summary string) error {
	b.mu.RLock("PutApprovalResult")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return ErrNotFound
	}

	_ = stageName
	_ = actionName
	_ = status
	_ = summary

	return nil
}

// UpdateActionType updates an action type definition with full fields.
func (b *InMemoryBackend) UpdateActionType(cat *CustomActionType) error {
	b.mu.Lock("UpdateActionType")
	defer b.mu.Unlock()

	key := customActionTypeKey{
		Category: cat.Category,
		Provider: cat.Provider,
		Version:  cat.Version,
	}

	if _, ok := b.customActionTypes[key]; !ok {
		return ErrActionTypeNotFound
	}

	cp := copyCustomActionType(cat)
	b.customActionTypes[key] = cp

	return nil
}

// ListActionTypes returns all registered action types.
func (b *InMemoryBackend) ListActionTypes() []*CustomActionType {
	b.mu.RLock("ListActionTypes")
	defer b.mu.RUnlock()

	result := make([]*CustomActionType, 0, len(b.customActionTypes))

	for _, cat := range b.customActionTypes {
		result = append(result, copyCustomActionType(cat))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Provider < result[j].Provider
	})

	return result
}

// ListActionExecutions returns stub action executions for a pipeline.
func (b *InMemoryBackend) ListActionExecutions(pipelineName string) ([]map[string]any, error) {
	b.mu.RLock("ListActionExecutions")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	return []map[string]any{}, nil
}

// ListRuleExecutions returns stub rule executions for a pipeline.
func (b *InMemoryBackend) ListRuleExecutions(pipelineName string) ([]map[string]any, error) {
	b.mu.RLock("ListRuleExecutions")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	return []map[string]any{}, nil
}

// ListRuleTypes returns empty rule types (stub).
func (b *InMemoryBackend) ListRuleTypes() []map[string]any {
	return []map[string]any{}
}

// ListDeployActionExecutionTargets returns stub targets.
func (b *InMemoryBackend) ListDeployActionExecutionTargets(pipelineName, executionID string) ([]map[string]any, error) {
	b.mu.RLock("ListDeployActionExecutionTargets")
	defer b.mu.RUnlock()

	if _, ok := b.pipelines[pipelineName]; !ok {
		return nil, ErrNotFound
	}

	_ = executionID

	return []map[string]any{}, nil
}
