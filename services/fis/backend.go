package fis

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ----------------------------------------
// Sentinel errors
// ----------------------------------------

// ErrTemplateNotFound is returned when an experiment template is not found.
var ErrTemplateNotFound = errors.New("ExperimentTemplateNotFound")

// ErrExperimentNotFound is returned when an experiment is not found.
var ErrExperimentNotFound = errors.New("ExperimentNotFound")

// ErrActionNotFound is returned when a FIS action is not found.
var ErrActionNotFound = errors.New("ActionNotFound")

// ErrTargetResourceTypeNotFound is returned when a target resource type is not found.
var ErrTargetResourceTypeNotFound = errors.New("TargetResourceTypeNotFound")

// ErrExperimentNotRunning is returned when trying to stop an experiment that is not running.
var ErrExperimentNotRunning = errors.New("ExperimentNotRunning")

// ----------------------------------------
// Status constants
// ----------------------------------------

const (
	statusPending   = "pending"
	statusRunning   = "running"
	statusStopping  = "stopping"
	statusStopped   = "stopped"
	statusCompleted = "completed"
	statusFailed    = "failed"
)

const (
	actionStatusPending   = "pending"
	actionStatusRunning   = "running"
	actionStatusCompleted = "completed"
	actionStatusStopped   = "stopped"
	actionStatusFailed    = "failed"
)

// ----------------------------------------
// ID / ARN helpers
// ----------------------------------------

const idChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// generateID creates a random ID with the given prefix followed by 22 alphanumeric characters.
func generateID(prefix string) string {
	const length = 22
	b := make([]byte, length)

	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(idChars))))
		if err != nil {
			b[i] = idChars[0]

			continue
		}

		b[i] = idChars[n.Int64()]
	}

	return prefix + string(b)
}

// toUnix converts a [time.Time] to a Unix second float64 (same format as AWS SDK).
const nanoToSeconds = float64(time.Second)

func toUnix(t time.Time) float64 {
	return float64(t.UnixNano()) / nanoToSeconds
}

func toUnixPtr(t *time.Time) *float64 {
	if t == nil {
		return nil
	}

	v := toUnix(*t)

	return &v
}

// ----------------------------------------
// StorageBackend interface
// ----------------------------------------

// StorageBackend is the interface implemented by the FIS in-memory store.
type StorageBackend interface {
	// Template operations
	CreateExperimentTemplate(
		input *createExperimentTemplateRequest,
		accountID, region string,
	) (*ExperimentTemplate, error)
	GetExperimentTemplate(id string) (*ExperimentTemplate, error)
	UpdateExperimentTemplate(id string, input *updateExperimentTemplateRequest) (*ExperimentTemplate, error)
	DeleteExperimentTemplate(id string) error
	ListExperimentTemplates() ([]*ExperimentTemplate, error)

	// Experiment operations
	StartExperiment(ctx context.Context, input *startExperimentRequest, accountID, region string) (*Experiment, error)
	GetExperiment(id string) (*Experiment, error)
	StopExperiment(id string) (*Experiment, error)
	ListExperiments() ([]*Experiment, error)

	// Action / target-resource-type discovery
	ListActions() []ActionSummary
	GetAction(id string) (*ActionSummary, error)
	ListTargetResourceTypes() []TargetResourceTypeSummary
	GetTargetResourceType(resourceType string) (*TargetResourceTypeSummary, error)

	// Tag operations
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, keys []string) error

	// SetFaultStore injects the chaos FaultStore used for inject-api-* actions.
	SetFaultStore(store *chaos.FaultStore)

	// SetActionProviders registers external service action providers.
	SetActionProviders(providers []service.FISActionProvider)
}

// ----------------------------------------
// InMemoryBackend implementation
// ----------------------------------------

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	templates       map[string]*ExperimentTemplate
	experiments     map[string]*Experiment
	faultStore      *chaos.FaultStore
	accountID       string
	region          string
	actionProviders []service.FISActionProvider
	mu              sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		templates:   make(map[string]*ExperimentTemplate),
		experiments: make(map[string]*Experiment),
		accountID:   accountID,
		region:      region,
	}
}

// SetFaultStore injects the chaos FaultStore.
func (b *InMemoryBackend) SetFaultStore(store *chaos.FaultStore) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.faultStore = store
}

// SetActionProviders registers external FIS action providers discovered from the registry.
func (b *InMemoryBackend) SetActionProviders(providers []service.FISActionProvider) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.actionProviders = providers
}

// ----------------------------------------
// ExperimentTemplate CRUD
// ----------------------------------------

// CreateExperimentTemplate creates a new experiment template.
func (b *InMemoryBackend) CreateExperimentTemplate(
	input *createExperimentTemplateRequest,
	accountID, region string,
) (*ExperimentTemplate, error) {
	id := generateID("EXT")
	arnStr := arn.Build("fis", region, accountID, fmt.Sprintf("experiment-template/%s", id))

	now := time.Now()
	tpl := &ExperimentTemplate{
		ID:             id,
		Arn:            arnStr,
		Description:    input.Description,
		RoleArn:        input.RoleArn,
		Tags:           copyStringMap(input.Tags),
		Targets:        convertTargetDTOs(input.Targets),
		Actions:        convertActionDTOs(input.Actions),
		StopConditions: convertStopConditionDTOs(input.StopConditions),
		CreationTime:   now,
		LastUpdateTime: now,
	}

	if input.LogConfiguration != nil {
		tpl.LogConfiguration = convertTemplateLogConfigDTO(input.LogConfiguration)
	}

	if input.ExperimentOptions != nil {
		tpl.ExperimentOptions = &ExperimentTemplateExperimentOptions{
			AccountTargeting:          input.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: input.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.templates[id] = tpl

	return tpl, nil
}

// GetExperimentTemplate retrieves an experiment template by ID.
func (b *InMemoryBackend) GetExperimentTemplate(id string) (*ExperimentTemplate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tpl, ok := b.templates[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, id)
	}

	return tpl, nil
}

// UpdateExperimentTemplate updates an existing experiment template.
func (b *InMemoryBackend) UpdateExperimentTemplate(
	id string,
	input *updateExperimentTemplateRequest,
) (*ExperimentTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tpl, ok := b.templates[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, id)
	}

	if input.Description != "" {
		tpl.Description = input.Description
	}

	if input.RoleArn != "" {
		tpl.RoleArn = input.RoleArn
	}

	if input.Targets != nil {
		tpl.Targets = convertTargetDTOs(input.Targets)
	}

	if input.Actions != nil {
		tpl.Actions = convertActionDTOs(input.Actions)
	}

	if input.StopConditions != nil {
		tpl.StopConditions = convertStopConditionDTOs(input.StopConditions)
	}

	if input.LogConfiguration != nil {
		tpl.LogConfiguration = convertTemplateLogConfigDTO(input.LogConfiguration)
	}

	if input.ExperimentOptions != nil {
		tpl.ExperimentOptions = &ExperimentTemplateExperimentOptions{
			AccountTargeting:          input.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: input.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	tpl.LastUpdateTime = time.Now()

	return tpl, nil
}

// DeleteExperimentTemplate deletes an experiment template by ID.
func (b *InMemoryBackend) DeleteExperimentTemplate(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.templates[id]; !ok {
		return fmt.Errorf("%w: %s", ErrTemplateNotFound, id)
	}

	delete(b.templates, id)

	return nil
}

// ListExperimentTemplates returns all experiment templates.
func (b *InMemoryBackend) ListExperimentTemplates() ([]*ExperimentTemplate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*ExperimentTemplate, 0, len(b.templates))
	for _, tpl := range b.templates {
		result = append(result, tpl)
	}

	return result, nil
}

// ----------------------------------------
// Experiment lifecycle
// ----------------------------------------

// StartExperiment creates and starts a new experiment from a template.
func (b *InMemoryBackend) StartExperiment(
	ctx context.Context,
	input *startExperimentRequest,
	accountID, region string,
) (*Experiment, error) {
	b.mu.RLock()
	tpl, ok := b.templates[input.ExperimentTemplateID]
	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, input.ExperimentTemplateID)
	}

	id := generateID("EXP")
	arnStr := arn.Build("fis", region, accountID, fmt.Sprintf("experiment/%s", id))

	// Build resolved targets from template (simplified: copy ARNs directly).
	targets := make(map[string]ExperimentTarget, len(tpl.Targets))
	for name, t := range tpl.Targets {
		targets[name] = ExperimentTarget{
			ResourceType: t.ResourceType,
			ResourceArns: append([]string(nil), t.ResourceArns...),
			Parameters:   copyStringMap(t.Parameters),
		}
	}

	// Build action state.
	actions := make(map[string]ExperimentAction, len(tpl.Actions))
	for name, a := range tpl.Actions {
		actions[name] = ExperimentAction{
			ActionID:   a.ActionID,
			Parameters: copyStringMap(a.Parameters),
			Targets:    copyStringMap(a.Targets),
			Status:     ExperimentActionStatus{Status: actionStatusPending},
		}
	}

	// Copy stop conditions.
	stopConditions := make([]ExperimentStopCondition, len(tpl.StopConditions))
	for i, sc := range tpl.StopConditions {
		stopConditions[i] = ExperimentStopCondition(sc)
	}

	// Copy log configuration.
	var logConfig *ExperimentLogConfiguration
	if tpl.LogConfiguration != nil {
		logConfig = &ExperimentLogConfiguration{
			LogSchemaVersion: tpl.LogConfiguration.LogSchemaVersion,
		}

		if tpl.LogConfiguration.CloudWatchLogsConfiguration != nil {
			logConfig.CloudWatchLogsConfiguration = &ExperimentCloudWatchLogsConfiguration{
				LogGroupArn: tpl.LogConfiguration.CloudWatchLogsConfiguration.LogGroupArn,
			}
		}

		if tpl.LogConfiguration.S3Configuration != nil {
			logConfig.S3Configuration = &ExperimentS3Configuration{
				BucketName: tpl.LogConfiguration.S3Configuration.BucketName,
				Prefix:     tpl.LogConfiguration.S3Configuration.Prefix,
			}
		}
	}

	// Copy experiment options.
	var expOptions *ExperimentExperimentOptions
	if tpl.ExperimentOptions != nil {
		expOptions = &ExperimentExperimentOptions{
			AccountTargeting:          tpl.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: tpl.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	// expCtx is cancelled either by StopExperiment (via exp.cancel) or when the parent ctx ends.
	// The cancel function is intentionally stored in exp.cancel for deferred use by StopExperiment.
	//nolint:gosec // cancel stored in exp.cancel and called by StopExperiment
	expCtx, cancel := context.WithCancel(ctx)

	exp := &Experiment{
		ID:                   id,
		Arn:                  arnStr,
		ExperimentTemplateID: tpl.ID,
		RoleArn:              tpl.RoleArn,
		Status:               ExperimentStatus{Status: statusPending},
		Targets:              targets,
		Actions:              actions,
		StopConditions:       stopConditions,
		LogConfiguration:     logConfig,
		ExperimentOptions:    expOptions,
		Tags:                 copyStringMap(input.Tags),
		StartTime:            time.Now(),
		cancel:               cancel,
	}

	b.mu.Lock()
	b.experiments[id] = exp
	b.mu.Unlock()

	// Run the experiment lifecycle in the background.
	go b.runExperiment(expCtx, id, tpl)

	return exp, nil
}

// GetExperiment retrieves an experiment by ID.
func (b *InMemoryBackend) GetExperiment(id string) (*Experiment, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	exp, ok := b.experiments[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	return exp, nil
}

// StopExperiment stops a running experiment.
func (b *InMemoryBackend) StopExperiment(id string) (*Experiment, error) {
	b.mu.Lock()

	exp, ok := b.experiments[id]
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	if exp.Status.Status != statusPending && exp.Status.Status != statusRunning {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExperimentNotRunning, id)
	}

	// Signal the background goroutine to stop.
	if exp.cancel != nil {
		exp.cancel()
	}

	b.mu.Unlock()

	return exp, nil
}

// ListExperiments returns all experiments.
func (b *InMemoryBackend) ListExperiments() ([]*Experiment, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Experiment, 0, len(b.experiments))
	for _, exp := range b.experiments {
		result = append(result, exp)
	}

	return result, nil
}

// ----------------------------------------
// Action / target resource type discovery
// ----------------------------------------

// ListActions returns all available FIS actions: built-in + service-provided.
func (b *InMemoryBackend) ListActions() []ActionSummary {
	b.mu.RLock()
	providers := b.actionProviders
	b.mu.RUnlock()

	all := builtinActionSummaries(b.accountID, b.region)

	for _, p := range providers {
		for _, def := range p.FISActions() {
			all = append(all, actionDefToSummary(def, b.accountID, b.region))
		}
	}

	return all
}

// GetAction returns a single action by ID.
func (b *InMemoryBackend) GetAction(id string) (*ActionSummary, error) {
	all := b.ListActions()

	for _, a := range all {
		if a.ID == id {
			cp := a

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrActionNotFound, id)
}

// ListTargetResourceTypes returns all known target resource types.
func (b *InMemoryBackend) ListTargetResourceTypes() []TargetResourceTypeSummary {
	b.mu.RLock()
	providers := b.actionProviders
	b.mu.RUnlock()

	seen := make(map[string]TargetResourceTypeSummary)

	// Built-in types.
	for _, rt := range builtinTargetResourceTypes() {
		seen[rt.ResourceType] = rt
	}

	// From action providers.
	for _, p := range providers {
		for _, def := range p.FISActions() {
			if def.TargetType == "" {
				continue
			}

			if _, exists := seen[def.TargetType]; !exists {
				seen[def.TargetType] = TargetResourceTypeSummary{
					ResourceType: def.TargetType,
				}
			}
		}
	}

	result := make([]TargetResourceTypeSummary, 0, len(seen))
	for _, rt := range seen {
		result = append(result, rt)
	}

	return result
}

// GetTargetResourceType returns a single target resource type by resource type string.
func (b *InMemoryBackend) GetTargetResourceType(resourceType string) (*TargetResourceTypeSummary, error) {
	all := b.ListTargetResourceTypes()

	for _, rt := range all {
		if rt.ResourceType == resourceType {
			cp := rt

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrTargetResourceTypeNotFound, resourceType)
}

// ----------------------------------------
// Tag operations
// ----------------------------------------

// ListTagsForResource returns tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Check templates.
	for _, tpl := range b.templates {
		if tpl.Arn == resourceARN {
			return copyStringMap(tpl.Tags), nil
		}
	}

	// Check experiments.
	for _, exp := range b.experiments {
		if exp.Arn == resourceARN {
			return copyStringMap(exp.Tags), nil
		}
	}

	return map[string]string{}, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check templates.
	for _, tpl := range b.templates {
		if tpl.Arn == resourceARN {
			if tpl.Tags == nil {
				tpl.Tags = make(map[string]string)
			}

			maps.Copy(tpl.Tags, tags)

			return nil
		}
	}

	// Check experiments.
	for _, exp := range b.experiments {
		if exp.Arn == resourceARN {
			if exp.Tags == nil {
				exp.Tags = make(map[string]string)
			}

			maps.Copy(exp.Tags, tags)

			return nil
		}
	}

	return nil
}

// UntagResource removes specific tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check templates.
	for _, tpl := range b.templates {
		if tpl.Arn == resourceARN {
			for _, k := range keys {
				delete(tpl.Tags, k)
			}

			return nil
		}
	}

	// Check experiments.
	for _, exp := range b.experiments {
		if exp.Arn == resourceARN {
			for _, k := range keys {
				delete(exp.Tags, k)
			}

			return nil
		}
	}

	return nil
}

// ----------------------------------------
// Experiment goroutine
// ----------------------------------------

// runExperiment manages the full lifecycle of a single experiment.
func (b *InMemoryBackend) runExperiment(ctx context.Context, expID string, tpl *ExperimentTemplate) {
	// Transition PENDING → RUNNING.
	b.setExperimentStatus(expID, statusRunning, "")
	b.setAllActionStatuses(expID, actionStatusRunning)

	// Collect chaos fault rules and other actions to execute.
	faultRules, externalActions, maxDuration := b.prepareActions(tpl)

	// Apply chaos fault rules.
	if len(faultRules) > 0 && b.getFaultStore() != nil {
		b.getFaultStore().AppendRules(faultRules)
	}

	// Execute external service actions (EC2 stop, etc.).
	for _, ea := range externalActions {
		b.executeExternalAction(ctx, ea)
	}

	// Wait for duration, stop signal, or context cancellation.
	var timer <-chan time.Time

	if maxDuration > 0 {
		timer = time.After(maxDuration)
	}

	select {
	case <-ctx.Done():
		// Manually stopped or context cancelled.
		b.cleanupActions(faultRules, expID, statusStopped, actionStatusStopped)
	case <-timer:
		// All actions completed naturally.
		b.cleanupActions(faultRules, expID, statusCompleted, actionStatusCompleted)
	}
}

// prepareActions returns the chaos fault rules, external actions, and the maximum duration
// across all actions in the template.
func (b *InMemoryBackend) prepareActions(tpl *ExperimentTemplate) ([]chaos.FaultRule, []externalAction, time.Duration) {
	var faultRules []chaos.FaultRule
	var externalActions []externalAction

	var maxDuration time.Duration

	for _, action := range tpl.Actions {
		dur := parseISODuration(action.Parameters["duration"])
		if dur > maxDuration {
			maxDuration = dur
		}

		switch {
		case strings.HasPrefix(action.ActionID, "aws:fis:inject-api-"):
			faultRules = append(faultRules, buildFaultRules(action)...)
		case action.ActionID == "aws:fis:wait":
			// Wait action — only the duration matters; it's already captured above.
		default:
			externalActions = append(externalActions, externalAction{
				actionID:   action.ActionID,
				params:     copyStringMap(action.Parameters),
				targets:    action.Targets,
				duration:   dur,
				tplTargets: tpl.Targets,
			})
		}
	}

	return faultRules, externalActions, maxDuration
}

// externalAction carries the data needed to call an external FISActionProvider.
type externalAction struct {
	params     map[string]string
	targets    map[string]string
	tplTargets map[string]ExperimentTemplateTarget
	actionID   string
	duration   time.Duration
}

// executeExternalAction calls the appropriate FISActionProvider for a non-built-in action.
func (b *InMemoryBackend) executeExternalAction(ctx context.Context, ea externalAction) {
	b.mu.RLock()
	providers := b.actionProviders
	b.mu.RUnlock()

	// Resolve target ARNs.
	var targetARNs []string

	for targetKey, targetName := range ea.targets {
		if tgt, ok := ea.tplTargets[targetKey]; ok {
			targetARNs = append(targetARNs, tgt.ResourceArns...)
		} else if tgtByName, ok2 := ea.tplTargets[targetName]; ok2 {
			targetARNs = append(targetARNs, tgtByName.ResourceArns...)
		}
	}

	exec := service.FISActionExecution{
		ActionID:   ea.actionID,
		Parameters: ea.params,
		Targets:    targetARNs,
		Duration:   ea.duration,
	}

	for _, p := range providers {
		for _, def := range p.FISActions() {
			if def.ActionID == ea.actionID {
				_ = p.ExecuteFISAction(ctx, exec)

				return
			}
		}
	}
}

// cleanupActions removes fault rules and sets the final experiment status.
func (b *InMemoryBackend) cleanupActions(faultRules []chaos.FaultRule, expID, expStatus, actionStatus string) {
	if len(faultRules) > 0 && b.getFaultStore() != nil {
		b.getFaultStore().DeleteRules(faultRules)
	}

	now := time.Now()
	b.mu.Lock()

	if exp, ok := b.experiments[expID]; ok {
		exp.Status = ExperimentStatus{Status: expStatus}
		exp.EndTime = &now

		for name, action := range exp.Actions {
			action.Status = ExperimentActionStatus{Status: actionStatus}
			endTime := now
			action.EndTime = &endTime
			exp.Actions[name] = action
		}
	}

	b.mu.Unlock()
}

// setExperimentStatus atomically updates an experiment's status.
func (b *InMemoryBackend) setExperimentStatus(id, status, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if exp, ok := b.experiments[id]; ok {
		exp.Status = ExperimentStatus{Status: status, Reason: reason}
	}
}

// setAllActionStatuses atomically sets all actions in an experiment to the given status.
func (b *InMemoryBackend) setAllActionStatuses(expID, status string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if exp, ok := b.experiments[expID]; ok {
		now := time.Now()

		for name, action := range exp.Actions {
			action.Status = ExperimentActionStatus{Status: status}
			action.StartTime = &now
			exp.Actions[name] = action
		}
	}
}

// getFaultStore safely returns the fault store (may be nil).
func (b *InMemoryBackend) getFaultStore() *chaos.FaultStore {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.faultStore
}

// ----------------------------------------
// Conversion helpers
// ----------------------------------------

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func convertTargetDTOs(in map[string]experimentTemplateTargetDTO) map[string]ExperimentTemplateTarget {
	if in == nil {
		return map[string]ExperimentTemplateTarget{}
	}

	out := make(map[string]ExperimentTemplateTarget, len(in))

	for name, dto := range in {
		filters := make([]ExperimentTemplateTargetFilter, len(dto.Filters))
		for i, f := range dto.Filters {
			filters[i] = ExperimentTemplateTargetFilter(f)
		}

		out[name] = ExperimentTemplateTarget{
			ResourceType:  dto.ResourceType,
			SelectionMode: dto.SelectionMode,
			ResourceArns:  append([]string(nil), dto.ResourceArns...),
			ResourceTags:  copyStringMap(dto.ResourceTags),
			Filters:       filters,
			Parameters:    copyStringMap(dto.Parameters),
		}
	}

	return out
}

func convertActionDTOs(in map[string]experimentTemplateActionDTO) map[string]ExperimentTemplateAction {
	if in == nil {
		return map[string]ExperimentTemplateAction{}
	}

	out := make(map[string]ExperimentTemplateAction, len(in))

	for name, dto := range in {
		out[name] = ExperimentTemplateAction{
			ActionID:    dto.ActionID,
			Description: dto.Description,
			Parameters:  copyStringMap(dto.Parameters),
			StartAfter:  append([]string(nil), dto.StartAfter...),
			Targets:     copyStringMap(dto.Targets),
		}
	}

	return out
}

func convertStopConditionDTOs(in []experimentTemplateStopConditionDTO) []ExperimentTemplateStopCondition {
	if in == nil {
		return []ExperimentTemplateStopCondition{}
	}

	out := make([]ExperimentTemplateStopCondition, len(in))
	for i, dto := range in {
		out[i] = ExperimentTemplateStopCondition(dto)
	}

	return out
}

func convertTemplateLogConfigDTO(dto *experimentTemplateLogConfigurationDTO) *ExperimentTemplateLogConfiguration {
	if dto == nil {
		return nil
	}

	lc := &ExperimentTemplateLogConfiguration{
		LogSchemaVersion: dto.LogSchemaVersion,
	}

	if dto.CloudWatchLogsConfiguration != nil {
		lc.CloudWatchLogsConfiguration = &ExperimentTemplateCloudWatchLogsConfiguration{
			LogGroupArn: dto.CloudWatchLogsConfiguration.LogGroupArn,
		}
	}

	if dto.S3Configuration != nil {
		lc.S3Configuration = &ExperimentTemplateS3Configuration{
			BucketName: dto.S3Configuration.BucketName,
			Prefix:     dto.S3Configuration.Prefix,
		}
	}

	return lc
}
