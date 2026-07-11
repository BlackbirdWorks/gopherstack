package fis

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusDisengaged = "disengaged"
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

// ErrResourceNotFound is returned when a tagged resource ARN is not known.
var ErrResourceNotFound = errors.New("ResourceNotFound")

// ErrSafetyLeverNotFound is returned when the safety lever ID does not match this account.
var ErrSafetyLeverNotFound = errors.New("SafetyLeverNotFound")

// ErrSafetyLeverEngaged is returned when StartExperiment is blocked by an engaged safety lever.
var ErrSafetyLeverEngaged = errors.New("SafetyLeverEngaged")

// ErrTooManyExperiments is returned when the experiment count would exceed the cap.
var ErrTooManyExperiments = errors.New("ServiceQuotaExceededException")

// ErrValidation is returned when a required field is missing or has an invalid value.
var ErrValidation = errors.New("ValidationException")

// ----------------------------------------
// Compile-time interface check
// ----------------------------------------

var _ StorageBackend = (*InMemoryBackend)(nil)

// ----------------------------------------
// Status constants
// ----------------------------------------

const (
	statusPending    = "pending"
	statusInitiating = "initiating"
	statusRunning    = "running"
	statusCompleting = "completing"
	statusStopping   = "stopping"
	statusStopped    = "stopped"
	statusCompleted  = "completed"
	statusFailed     = "failed"
)

const (
	actionStatusPending    = "pending"
	actionStatusInitiating = "initiating"
	actionStatusRunning    = "running"
	actionStatusCompleting = "completing"
	actionStatusCompleted  = "completed"
	actionStatusStopped    = "stopped"
	actionStatusFailed     = "failed"
	actionStatusCancelled  = "cancelled"
	actionStatusSkipped    = "skipped"
)

// ----------------------------------------
// ID / ARN helpers
// ----------------------------------------

const idChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// idTotalLength is the total length of generated IDs including the prefix.
// AWS FIS uses 16-character IDs (e.g., "EXT2zP9aBcDeFgHi").
const idTotalLength = 16

// maxExperiments is the maximum number of experiments that can exist concurrently.
const maxExperiments = 1000

// maxTagsPerResource is the maximum number of tags allowed per resource.
const maxTagsPerResource = 50

// maxTagKeyLen / maxTagValueLen are the AWS tag size limits.
const (
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

// generateID creates a random ID with the given prefix so that total length == idTotalLength.
func generateID(prefix string) string {
	length := idTotalLength - len(prefix)
	length = max(length, 1)

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

	// Phase 3 — resolved targets
	ListExperimentResolvedTargets(id string) ([]ExperimentResolvedTarget, error)

	// Phase 3 — safety lever
	GetSafetyLever(id string) (*SafetyLever, error)
	UpdateSafetyLeverState(id string, input *updateSafetyLeverStateRequest) (*SafetyLever, error)

	// Action / target-resource-type discovery
	ListActions() []ActionSummary
	GetAction(id string) (*ActionSummary, error)
	ListTargetResourceTypes() []TargetResourceTypeSummary
	GetTargetResourceType(resourceType string) (*TargetResourceTypeSummary, error)

	// Tag operations
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, keys []string) error

	// Target account configuration operations (template-scoped)
	CreateTargetAccountConfiguration(
		templateID, accountID, roleArn, description string,
	) (*TargetAccountConfiguration, error)
	DeleteTargetAccountConfiguration(templateID, accountID string) (*TargetAccountConfiguration, error)
	GetTargetAccountConfiguration(templateID, accountID string) (*TargetAccountConfiguration, error)
	UpdateTargetAccountConfiguration(
		templateID, accountID string,
		roleArn, description *string,
	) (*TargetAccountConfiguration, error)
	ListTargetAccountConfigurations(templateID string) ([]*TargetAccountConfiguration, error)

	// Experiment target account configuration operations (experiment-scoped, read-only)
	GetExperimentTargetAccountConfiguration(
		experimentID, accountID string,
	) (*ExperimentTargetAccountConfiguration, error)
	ListExperimentTargetAccountConfigurations(experimentID string) ([]*ExperimentTargetAccountConfiguration, error)

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
	registry                       *store.Registry
	templates                      *store.Table[ExperimentTemplate]
	templatesByArn                 *store.Index[ExperimentTemplate]
	experiments                    *store.Table[Experiment]
	experimentsByArn               *store.Index[Experiment]
	targetAccountConfigs           *store.Table[TargetAccountConfiguration]
	targetAccountConfigsByTemplate *store.Index[TargetAccountConfiguration]
	tplClientTokens                map[string]string // clientToken → templateID
	expClientTokens                map[string]string // clientToken → experimentID
	faultStore                     *chaos.FaultStore
	safetyLever                    *SafetyLever
	mu                             *lockmetrics.RWMutex
	svcCtx                         context.Context
	accountID                      string
	region                         string
	actionProviders                []service.FISActionProvider
}

// NewInMemoryBackend creates a new InMemoryBackend with a background service context.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose experiment goroutines
// are parented by svcCtx so they are cancelled on server shutdown. If svcCtx is nil,
// [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	safetyLeverARN := arn.Build("fis", region, accountID, "safety-lever/"+accountID)

	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		tplClientTokens: make(map[string]string),
		expClientTokens: make(map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("fis"),
		svcCtx:          svcCtx,
		safetyLever: &SafetyLever{
			ID:    accountID,
			Arn:   safetyLeverARN,
			Tags:  make(map[string]string),
			State: SafetyLeverState{Status: statusDisengaged},
		},
	}

	registerAllTables(b)

	return b
}

// Reset clears all in-memory state, cancelling any running experiments.
// The safety lever is re-initialised to its default disengaged state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, exp := range b.experiments.All() {
		if exp.cancel != nil {
			exp.cancel()
		}
	}

	safetyLeverARN := arn.Build("fis", b.region, b.accountID, "safety-lever/"+b.accountID)

	b.registry.ResetAll()
	b.tplClientTokens = make(map[string]string)
	b.expClientTokens = make(map[string]string)
	b.safetyLever = &SafetyLever{
		ID:    b.accountID,
		Arn:   safetyLeverARN,
		Tags:  make(map[string]string),
		State: SafetyLeverState{Status: statusDisengaged},
	}
}

// SetFaultStore injects the chaos FaultStore.
func (b *InMemoryBackend) SetFaultStore(store *chaos.FaultStore) {
	b.mu.Lock("SetFaultStore")
	defer b.mu.Unlock()

	b.faultStore = store
}

// SetActionProviders registers external FIS action providers discovered from the registry.
func (b *InMemoryBackend) SetActionProviders(providers []service.FISActionProvider) {
	b.mu.Lock("SetActionProviders")
	defer b.mu.Unlock()

	b.actionProviders = providers
}

// ----------------------------------------
// Template validation
// ----------------------------------------

// selectionModeRe matches valid FIS selectionMode values: ALL, COUNT(N), PERCENT(N).
var selectionModeRe = regexp.MustCompile(`^(ALL|COUNT\(\d+\)|PERCENT\(\d{1,3}(\.\d+)?\))$`)

// maxDescriptionLen is the maximum allowed length for template/experiment descriptions.
const maxDescriptionLen = 512

// maxClientTokenLen is the maximum allowed length for idempotency client tokens.
const maxClientTokenLen = 64

// validateTemplate checks that a create request meets AWS FIS requirements.
func validateTemplate(input *createExperimentTemplateRequest) error {
	if strings.TrimSpace(input.RoleArn) == "" {
		return fmt.Errorf("%w: roleArn is required", ErrValidation)
	}

	if !isValidRoleArn(input.RoleArn) {
		return fmt.Errorf("%w: roleArn must be a valid IAM role ARN (arn:aws:iam::{account}:role/...)", ErrValidation)
	}

	if len(input.Description) > maxDescriptionLen {
		return fmt.Errorf(
			"%w: description must be at most %d characters; got %d",
			ErrValidation, maxDescriptionLen, len(input.Description),
		)
	}

	if len(input.ClientToken) > maxClientTokenLen {
		return fmt.Errorf(
			"%w: clientToken must be at most %d characters",
			ErrValidation, maxClientTokenLen,
		)
	}

	if len(input.StopConditions) == 0 {
		return fmt.Errorf("%w: stopConditions is required", ErrValidation)
	}

	if err := validateStopConditions(input.StopConditions); err != nil {
		return err
	}

	if err := validateTargets(input.Targets); err != nil {
		return err
	}

	if err := validateActions(input.Actions, input.Targets); err != nil {
		return err
	}

	if len(input.Tags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: tags must have at most %d entries; got %d",
			ErrValidation, maxTagsPerResource, len(input.Tags),
		)
	}

	return nil
}

// validateUpdateTemplate checks that an update request meets AWS FIS requirements.
func validateUpdateTemplate(input *updateExperimentTemplateRequest) error {
	if input.RoleArn != "" && !isValidRoleArn(input.RoleArn) {
		return fmt.Errorf("%w: roleArn must be a valid IAM role ARN (arn:aws:iam::{account}:role/...)", ErrValidation)
	}

	if len(input.Description) > maxDescriptionLen {
		return fmt.Errorf(
			"%w: description must be at most %d characters; got %d",
			ErrValidation, maxDescriptionLen, len(input.Description),
		)
	}

	if input.StopConditions != nil {
		if len(input.StopConditions) == 0 {
			return fmt.Errorf("%w: stopConditions must not be empty when provided", ErrValidation)
		}

		if err := validateStopConditions(input.StopConditions); err != nil {
			return err
		}
	}

	if input.Targets != nil {
		if err := validateTargets(input.Targets); err != nil {
			return err
		}
	}

	if input.Actions != nil {
		if err := validateActions(input.Actions, input.Targets); err != nil {
			return err
		}
	}

	return nil
}

// validateStopConditions validates the stop conditions slice.
// Source must be "none" or a CloudWatch alarm ARN.
func validateStopConditions(conditions []experimentTemplateStopConditionDTO) error {
	for i, sc := range conditions {
		src := strings.TrimSpace(sc.Source)
		if src == "" {
			return fmt.Errorf("%w: stopConditions[%d].source is required", ErrValidation, i)
		}

		switch {
		case src == "none":
			if strings.TrimSpace(sc.Value) != "" {
				return fmt.Errorf(
					"%w: stopConditions[%d]: value must be empty when source is \"none\"",
					ErrValidation, i,
				)
			}
		case strings.HasPrefix(src, "aws:cloudwatch:alarm"):
			// valid CloudWatch alarm source
		default:
			return fmt.Errorf(
				"%w: stopConditions[%d].source must be \"none\" or a CloudWatch alarm ARN; got %q",
				ErrValidation, i, src,
			)
		}
	}

	return nil
}

// validateTargets validates the target map.
func validateTargets(targets map[string]experimentTemplateTargetDTO) error {
	for name, tgt := range targets {
		if strings.TrimSpace(tgt.ResourceType) == "" {
			return fmt.Errorf("%w: target %q: resourceType is required", ErrValidation, name)
		}

		if strings.TrimSpace(tgt.SelectionMode) == "" {
			return fmt.Errorf("%w: target %q: selectionMode is required", ErrValidation, name)
		}

		if !selectionModeRe.MatchString(tgt.SelectionMode) {
			return fmt.Errorf(
				"%w: target %q: selectionMode must be ALL, COUNT(N), or PERCENT(N); got %q",
				ErrValidation, name, tgt.SelectionMode,
			)
		}

		// Validate filters.
		for j, f := range tgt.Filters {
			if strings.TrimSpace(f.Path) == "" {
				return fmt.Errorf("%w: target %q: filter[%d].path is required", ErrValidation, name, j)
			}

			if len(f.Values) == 0 {
				return fmt.Errorf("%w: target %q: filter[%d].values must not be empty", ErrValidation, name, j)
			}
		}
	}

	return nil
}

// validateActions validates the action map.
func validateActions(
	actions map[string]experimentTemplateActionDTO,
	targets map[string]experimentTemplateTargetDTO,
) error {
	for name, action := range actions {
		if err := validateAction(name, action, actions, targets); err != nil {
			return err
		}
	}

	// Detect cycles in startAfter dependencies.
	if err := detectActionCycles(actions); err != nil {
		return err
	}

	return nil
}

// validateAction validates a single action's identifier, references, and parameters.
func validateAction(
	name string,
	action experimentTemplateActionDTO,
	actions map[string]experimentTemplateActionDTO,
	targets map[string]experimentTemplateTargetDTO,
) error {
	if strings.TrimSpace(action.ActionID) == "" {
		return fmt.Errorf("%w: action %q: actionId is required", ErrValidation, name)
	}

	if err := validateActionReferences(name, action, actions, targets); err != nil {
		return err
	}

	return validateActionDuration(name, action)
}

// validateActionReferences validates an action's startAfter and target references.
func validateActionReferences(
	name string,
	action experimentTemplateActionDTO,
	actions map[string]experimentTemplateActionDTO,
	targets map[string]experimentTemplateTargetDTO,
) error {
	for _, depName := range action.StartAfter {
		if _, ok := actions[depName]; !ok {
			return fmt.Errorf(
				"%w: action %q: startAfter references undefined action %q",
				ErrValidation, name, depName,
			)
		}
	}

	for _, tgtName := range action.Targets {
		if _, ok := targets[tgtName]; !ok {
			return fmt.Errorf(
				"%w: action %q references undefined target %q",
				ErrValidation, name, tgtName,
			)
		}
	}

	return nil
}

// validateActionDuration validates the duration parameter requirements for an action.
func validateActionDuration(name string, action experimentTemplateActionDTO) error {
	// aws:fis:wait requires duration.
	if action.ActionID == actionIDWait && strings.TrimSpace(action.Parameters["duration"]) == "" {
		return fmt.Errorf(
			"%w: action %q: %s requires the duration parameter",
			ErrValidation, name, actionIDWait,
		)
	}

	// Validate duration parameter format when present.
	if dur, ok := action.Parameters["duration"]; ok && dur != "" {
		if !isValidISODuration(dur) {
			return fmt.Errorf(
				"%w: action %q: duration parameter %q is not a valid ISO 8601 duration",
				ErrValidation, name, dur,
			)
		}
	}

	return nil
}

// detectActionCycles returns an error if the startAfter dependency graph has a cycle.
func detectActionCycles(actions map[string]experimentTemplateActionDTO) error {
	const (
		unvisited = 0
		inStack   = 1
		done      = 2
	)

	state := make(map[string]int, len(actions))

	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case inStack:
			return fmt.Errorf("%w: action dependency cycle detected involving action %q", ErrValidation, name)
		case done:
			return nil
		}

		state[name] = inStack

		for _, dep := range actions[name].StartAfter {
			if err := visit(dep); err != nil {
				return err
			}
		}

		state[name] = done

		return nil
	}

	for name := range actions {
		if err := visit(name); err != nil {
			return err
		}
	}

	return nil
}

// iamAccountIDLen is the fixed length of an AWS account ID (12 decimal digits).
const iamAccountIDLen = 12

// isValidRoleArn checks that the string looks like an IAM role ARN with a valid 12-digit account ID.
func isValidRoleArn(s string) bool {
	const prefix = "arn:aws:iam::"
	if !strings.HasPrefix(s, prefix) {
		return false
	}

	rest := s[len(prefix):]
	colon := strings.Index(rest, ":")

	if colon != iamAccountIDLen {
		return false
	}

	for _, c := range rest[:12] {
		if c < '0' || c > '9' {
			return false
		}
	}

	return strings.HasPrefix(rest[colon:], ":role/")
}

// ----------------------------------------
// ExperimentTemplate CRUD
// ----------------------------------------

// CreateExperimentTemplate creates a new experiment template.
func (b *InMemoryBackend) CreateExperimentTemplate(
	input *createExperimentTemplateRequest,
	accountID, region string,
) (*ExperimentTemplate, error) {
	if err := validateTemplate(input); err != nil {
		return nil, err
	}

	// Check clientToken idempotency before generating a new ID.
	if input.ClientToken != "" {
		b.mu.RLock("CreateExperimentTemplate-idempotency")
		existingID, ok := b.tplClientTokens[input.ClientToken]
		b.mu.RUnlock()

		if ok {
			return b.GetExperimentTemplate(existingID)
		}
	}

	id := generateID("EXT")
	arnStr := arn.Build("fis", region, accountID, "experiment-template/"+id)

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

	b.mu.Lock("CreateExperimentTemplate")
	defer b.mu.Unlock()

	b.templates.Put(tpl)

	if input.ClientToken != "" {
		b.tplClientTokens[input.ClientToken] = id
	}

	return tpl, nil
}

// GetExperimentTemplate retrieves an experiment template by ID.
func (b *InMemoryBackend) GetExperimentTemplate(id string) (*ExperimentTemplate, error) {
	b.mu.RLock("GetExperimentTemplate")
	defer b.mu.RUnlock()

	tpl, ok := b.templates.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, id)
	}

	cp := cloneTemplate(tpl)

	return cp, nil
}

// UpdateExperimentTemplate updates an existing experiment template.
func (b *InMemoryBackend) UpdateExperimentTemplate(
	id string,
	input *updateExperimentTemplateRequest,
) (*ExperimentTemplate, error) {
	if err := validateUpdateTemplate(input); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateExperimentTemplate")
	defer b.mu.Unlock()

	tpl, ok := b.templates.Get(id)
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

	return cloneTemplate(tpl), nil
}

// DeleteExperimentTemplate deletes an experiment template by ID, including its target account configurations.
func (b *InMemoryBackend) DeleteExperimentTemplate(id string) error {
	b.mu.Lock("DeleteExperimentTemplate")
	defer b.mu.Unlock()

	if !b.templates.Has(id) {
		return fmt.Errorf("%w: %s", ErrTemplateNotFound, id)
	}

	b.templates.Delete(id)

	// Clone the index group before deleting in the loop: Delete mutates the
	// index's backing slice for this group, which would otherwise invalidate
	// iteration over the very slice being ranged.
	cfgs := slices.Clone(b.targetAccountConfigsByTemplate.Get(id))
	for _, cfg := range cfgs {
		b.targetAccountConfigs.Delete(targetAccountConfigKey(cfg.ExperimentTemplateID, cfg.AccountID))
	}

	// Drop idempotency-token entries pointing at this template so the map
	// cannot grow unbounded across long-lived backends.
	for tok, tid := range b.tplClientTokens {
		if tid == id {
			delete(b.tplClientTokens, tok)
		}
	}

	return nil
}

// ListExperimentTemplates returns all experiment templates sorted by ID.
func (b *InMemoryBackend) ListExperimentTemplates() ([]*ExperimentTemplate, error) {
	b.mu.RLock("ListExperimentTemplates")
	defer b.mu.RUnlock()

	all := b.templates.All()
	result := make([]*ExperimentTemplate, 0, len(all))

	for _, tpl := range all {
		result = append(result, cloneTemplate(tpl))
	}

	slices.SortFunc(result, func(a, b *ExperimentTemplate) int { return strings.Compare(a.ID, b.ID) })

	return result, nil
}

// ----------------------------------------
// Experiment lifecycle
// ----------------------------------------

// StartExperiment creates and starts a new experiment from a template.
func (b *InMemoryBackend) StartExperiment(
	_ context.Context,
	input *startExperimentRequest,
	accountID, region string,
) (*Experiment, error) {
	// Check clientToken idempotency first (read-only fast path).
	if input.ClientToken != "" {
		b.mu.RLock("StartExperiment-idempotency")
		existingID, ok := b.expClientTokens[input.ClientToken]
		b.mu.RUnlock()

		if ok {
			return b.GetExperiment(existingID)
		}
	}

	b.mu.RLock("StartExperiment")
	tpl, ok := b.templates.Get(input.ExperimentTemplateID)
	leverEngaged := b.safetyLever != nil && b.safetyLever.State.Status == "engaged"
	experimentCount := b.experiments.Len()
	tplAccountCount := len(b.targetAccountConfigsByTemplate.Get(input.ExperimentTemplateID))
	b.mu.RUnlock()

	if leverEngaged {
		return nil, fmt.Errorf("%w: safety lever is engaged", ErrSafetyLeverEngaged)
	}

	if experimentCount >= maxExperiments {
		return nil, fmt.Errorf(
			"%w: experiment count would exceed the limit of %d",
			ErrTooManyExperiments,
			maxExperiments,
		)
	}

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, input.ExperimentTemplateID)
	}

	id := generateID("EXP")
	arnStr := arn.Build("fis", region, accountID, "experiment/"+id)

	// expCtx derives from b.svcCtx — NOT the HTTP request context — so the experiment
	// goroutine is not cancelled when the HTTP response is sent, but IS cancelled on shutdown.
	expCtx, cancel := context.WithCancel(b.svcCtx)
	exp := buildExperimentFromTemplate(id, arnStr, tpl, input.Tags, cancel)
	exp.TargetAccountConfigurationsCount = tplAccountCount

	// Clone the template BEFORE passing to the goroutine so template updates don't race.
	tplForRun := cloneTemplate(tpl)

	b.mu.Lock("StartExperiment")
	b.experiments.Put(exp)

	if input.ClientToken != "" {
		b.expClientTokens[input.ClientToken] = id
	}

	// Take the snapshot while holding the lock, before launching the goroutine,
	// so the background goroutine cannot mutate exp while we're reading it.
	snapshot := cloneExperiment(exp)
	b.mu.Unlock()

	// Run the experiment lifecycle in the background.
	go b.runExperiment(expCtx, id, tplForRun)

	return snapshot, nil
}

// buildExperimentFromTemplate constructs a new Experiment from a template and input tags.
func buildExperimentFromTemplate(
	id, arnStr string,
	tpl *ExperimentTemplate,
	inputTags map[string]string,
	cancel context.CancelFunc,
) *Experiment {
	targets := make(map[string]ExperimentTarget, len(tpl.Targets))
	for name, t := range tpl.Targets {
		targets[name] = ExperimentTarget{
			ResourceType: t.ResourceType,
			ResourceArns: append([]string(nil), t.ResourceArns...),
			Parameters:   copyStringMap(t.Parameters),
		}
	}

	actions := make(map[string]ExperimentAction, len(tpl.Actions))
	for name, a := range tpl.Actions {
		actions[name] = ExperimentAction{
			ActionID:   a.ActionID,
			Parameters: copyStringMap(a.Parameters),
			Targets:    copyStringMap(a.Targets),
			Status:     ExperimentActionStatus{Status: actionStatusPending},
		}
	}

	stopConditions := make([]ExperimentStopCondition, len(tpl.StopConditions))
	for i, sc := range tpl.StopConditions {
		stopConditions[i] = ExperimentStopCondition(sc)
	}

	logConfig := copyLogConfiguration(tpl.LogConfiguration)

	var expOptions *ExperimentExperimentOptions
	if tpl.ExperimentOptions != nil {
		expOptions = &ExperimentExperimentOptions{
			AccountTargeting:          tpl.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: tpl.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	// expCtx derives from svcCtx — NOT the HTTP request context — so the experiment
	// goroutine is not cancelled when the HTTP response is sent.
	// cancel is passed in from StartExperiment and stored on the returned experiment.

	now := time.Now()

	return &Experiment{
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
		Tags:                 copyStringMap(inputTags),
		CreationTime:         now,
		StartTime:            now,
		cancel:               cancel,
	}
}

// copyLogConfiguration deep-copies a template log configuration into its experiment equivalent.
func copyLogConfiguration(tplLog *ExperimentTemplateLogConfiguration) *ExperimentLogConfiguration {
	if tplLog == nil {
		return nil
	}

	lc := &ExperimentLogConfiguration{LogSchemaVersion: tplLog.LogSchemaVersion}

	if tplLog.CloudWatchLogsConfiguration != nil {
		lc.CloudWatchLogsConfiguration = &ExperimentCloudWatchLogsConfiguration{
			LogGroupArn: tplLog.CloudWatchLogsConfiguration.LogGroupArn,
		}
	}

	if tplLog.S3Configuration != nil {
		lc.S3Configuration = &ExperimentS3Configuration{
			BucketName: tplLog.S3Configuration.BucketName,
			Prefix:     tplLog.S3Configuration.Prefix,
		}
	}

	return lc
}

// GetExperiment retrieves an experiment by ID.
func (b *InMemoryBackend) GetExperiment(id string) (*Experiment, error) {
	b.mu.RLock("GetExperiment")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	return cloneExperiment(exp), nil
}

// StopExperiment stops a running experiment.
func (b *InMemoryBackend) StopExperiment(id string) (*Experiment, error) {
	b.mu.Lock("StopExperiment")

	exp, ok := b.experiments.Get(id)
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	s := exp.Status.Status
	if s != statusPending && s != statusInitiating && s != statusRunning && s != statusCompleting {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExperimentNotRunning, id)
	}

	// Signal the background goroutine to stop.
	if exp.cancel != nil {
		exp.cancel()
	}

	// Immediately reflect the transition to stopping in the response.
	exp.Status = ExperimentStatus{Status: statusStopping}

	snap := cloneExperiment(exp)
	b.mu.Unlock()

	return snap, nil
}

// ListExperiments returns all experiments sorted by ID.
func (b *InMemoryBackend) ListExperiments() ([]*Experiment, error) {
	b.mu.RLock("ListExperiments")
	defer b.mu.RUnlock()

	all := b.experiments.All()
	result := make([]*Experiment, 0, len(all))

	for _, exp := range all {
		result = append(result, cloneExperiment(exp))
	}

	slices.SortFunc(result, func(a, b *Experiment) int { return strings.Compare(a.ID, b.ID) })

	return result, nil
}

// StopAllExperiments cancels every running experiment goroutine.
// Called during graceful shutdown to prevent goroutine leaks.
func (b *InMemoryBackend) StopAllExperiments() {
	b.mu.Lock("StopAllExperiments")
	defer b.mu.Unlock()

	for _, exp := range b.experiments.All() {
		if exp.cancel != nil {
			exp.cancel()
		}
	}
}

// ----------------------------------------
// Phase 3 — Resolved Targets
// ----------------------------------------

// ListExperimentResolvedTargets returns the resolved resource ARN counts for each
// named target group in the given experiment.
func (b *InMemoryBackend) ListExperimentResolvedTargets(id string) ([]ExperimentResolvedTarget, error) {
	b.mu.RLock("ListExperimentResolvedTargets")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	resolved := make([]ExperimentResolvedTarget, 0, len(exp.Targets))

	for name, tgt := range exp.Targets {
		arns := make([]string, len(tgt.ResourceArns))
		copy(arns, tgt.ResourceArns)
		resolved = append(resolved, ExperimentResolvedTarget{
			ResourceType:         tgt.ResourceType,
			TargetName:           name,
			ResolvedArns:         arns,
			TargetResourcesCount: len(tgt.ResourceArns),
		})
	}

	slices.SortFunc(
		resolved,
		func(a, b ExperimentResolvedTarget) int { return strings.Compare(a.TargetName, b.TargetName) },
	)

	return resolved, nil
}

// ----------------------------------------
// Phase 3 — Safety Lever
// ----------------------------------------

// resolveSafetyLeverID maps the "default" alias to the backend's account ID.
func (b *InMemoryBackend) resolveSafetyLeverID(id string) string {
	if id == "default" {
		return b.accountID
	}

	return id
}

// GetSafetyLever returns the current state of the account's safety lever.
// Accepts either the account ID or the conventional "default" alias.
func (b *InMemoryBackend) GetSafetyLever(id string) (*SafetyLever, error) {
	b.mu.RLock("GetSafetyLever")
	defer b.mu.RUnlock()

	if b.safetyLever == nil {
		return nil, fmt.Errorf("%w: %s", ErrSafetyLeverNotFound, id)
	}

	cp := *b.safetyLever

	return &cp, nil
}

// UpdateSafetyLeverState updates the state of the account's safety lever.
// Accepts either the account ID or the conventional "default" alias.
// Setting status to "engaged" blocks new experiments from starting.
func (b *InMemoryBackend) UpdateSafetyLeverState(
	id string,
	input *updateSafetyLeverStateRequest,
) (*SafetyLever, error) {
	status := input.UpdateSafetyLeverStateInput.Status
	if status != statusDisengaged && status != "engaged" {
		return nil, fmt.Errorf(
			"%w: safetyLever status must be \"engaged\" or \"disengaged\"; got %q",
			ErrValidation, status,
		)
	}

	b.mu.Lock("UpdateSafetyLeverState")
	defer b.mu.Unlock()

	resolved := b.resolveSafetyLeverID(id)

	if b.safetyLever == nil || b.safetyLever.ID != resolved {
		return nil, fmt.Errorf("%w: %s", ErrSafetyLeverNotFound, id)
	}

	b.safetyLever.State = SafetyLeverState{
		Status: status,
		Reason: input.UpdateSafetyLeverStateInput.Reason,
	}

	cp := *b.safetyLever

	return &cp, nil
}

// ----------------------------------------
// Action / target resource type discovery
// ----------------------------------------

// ListActions returns all available FIS actions: built-in + service-provided, sorted by ID.
// Built-in actions take precedence over provider-supplied actions with the same ID (dedup by ID).
func (b *InMemoryBackend) ListActions() []ActionSummary {
	b.mu.RLock("ListActions")
	providers := b.actionProviders
	b.mu.RUnlock()

	seen := make(map[string]ActionSummary)

	for _, a := range builtinActionSummaries(b.accountID, b.region) {
		seen[a.ID] = a
	}

	for _, p := range providers {
		for _, def := range p.FISActions() {
			if _, exists := seen[def.ActionID]; !exists {
				seen[def.ActionID] = actionDefToSummary(def, b.accountID, b.region)
			}
		}
	}

	all := make([]ActionSummary, 0, len(seen))
	for _, a := range seen {
		all = append(all, a)
	}

	slices.SortFunc(all, func(a, b ActionSummary) int { return strings.Compare(a.ID, b.ID) })

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
	b.mu.RLock("ListTargetResourceTypes")
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

	slices.SortFunc(
		result,
		func(a, b TargetResourceTypeSummary) int { return strings.Compare(a.ResourceType, b.ResourceType) },
	)

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
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.safetyLever != nil && b.safetyLever.Arn == resourceARN {
		return copyStringMap(b.safetyLever.Tags), nil
	}

	if tpl, ok := b.templateByArn(resourceARN); ok {
		return copyStringMap(tpl.Tags), nil
	}

	if exp, ok := b.experimentByArn(resourceARN); ok {
		return copyStringMap(exp.Tags), nil
	}

	return nil, fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}

// ErrTooManyTags is returned when adding tags would exceed the 50-tag limit.
var ErrTooManyTags = errors.New("TooManyTagsException")

// validateTags checks tag key/value constraints per AWS limits.
func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key %q: length must be 1-%d characters",
				ErrValidation, k, maxTagKeyLen,
			)
		}

		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key %q: keys with prefix \"aws:\" are reserved", ErrValidation, k)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag key %q: value length must be 0-%d characters",
				ErrValidation, k, maxTagValueLen,
			)
		}
	}

	return nil
}

// applyTags merges newTags into dest, enforcing the per-resource quota.
// dest must be non-nil. Returns ErrTooManyTags if the quota would be exceeded.
func applyTags(dest *map[string]string, newTags map[string]string, resourceARN string) error {
	if *dest == nil {
		*dest = make(map[string]string)
	}

	if len(*dest)+len(newTags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource %s already has the maximum of %d tags",
			ErrTooManyTags, resourceARN, maxTagsPerResource,
		)
	}

	maps.Copy(*dest, newTags)

	return nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	return b.applyTagsLocked(resourceARN, tags)
}

// applyTagsLocked merges tags into the resource identified by resourceARN.
// Must be called with b.mu held for write.
func (b *InMemoryBackend) applyTagsLocked(resourceARN string, tags map[string]string) error {
	if b.safetyLever != nil && b.safetyLever.Arn == resourceARN {
		return applyTags(&b.safetyLever.Tags, tags, resourceARN)
	}

	if tpl, ok := b.templateByArn(resourceARN); ok {
		return applyTags(&tpl.Tags, tags, resourceARN)
	}

	if exp, ok := b.experimentByArn(resourceARN); ok {
		return applyTags(&exp.Tags, tags, resourceARN)
	}

	return fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}

// UntagResource removes specific tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.safetyLever != nil && b.safetyLever.Arn == resourceARN {
		for _, k := range keys {
			delete(b.safetyLever.Tags, k)
		}

		return nil
	}

	if tpl, ok := b.templateByArn(resourceARN); ok {
		for _, k := range keys {
			delete(tpl.Tags, k)
		}

		return nil
	}

	if exp, ok := b.experimentByArn(resourceARN); ok {
		for _, k := range keys {
			delete(exp.Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}

// ----------------------------------------
// Target Account Configuration operations
// ----------------------------------------

// ErrTargetAccountConfigNotFound is returned when a target account configuration is not found.
var ErrTargetAccountConfigNotFound = errors.New("TargetAccountConfigurationNotFound")

// CreateTargetAccountConfiguration creates or replaces a target account configuration for the given template.
func (b *InMemoryBackend) CreateTargetAccountConfiguration(
	templateID, accountID, roleArn, description string,
) (*TargetAccountConfiguration, error) {
	if strings.TrimSpace(roleArn) == "" {
		return nil, fmt.Errorf("%w: roleArn is required", ErrValidation)
	}

	b.mu.Lock("CreateTargetAccountConfiguration")
	defer b.mu.Unlock()

	if !b.templates.Has(templateID) {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, templateID)
	}

	cfg := &TargetAccountConfiguration{
		ExperimentTemplateID: templateID,
		AccountID:            accountID,
		RoleArn:              roleArn,
		Description:          description,
	}

	b.targetAccountConfigs.Put(cfg)

	cp := *cfg

	return &cp, nil
}

// DeleteTargetAccountConfiguration deletes a target account configuration.
func (b *InMemoryBackend) DeleteTargetAccountConfiguration(
	templateID, accountID string,
) (*TargetAccountConfiguration, error) {
	b.mu.Lock("DeleteTargetAccountConfiguration")
	defer b.mu.Unlock()

	key := targetAccountConfigKey(templateID, accountID)

	cfg, ok := b.targetAccountConfigs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: template=%s account=%s", ErrTargetAccountConfigNotFound, templateID, accountID)
	}

	cp := *cfg

	b.targetAccountConfigs.Delete(key)

	return &cp, nil
}

// GetTargetAccountConfiguration returns a single target account configuration by template ID and account ID.
func (b *InMemoryBackend) GetTargetAccountConfiguration(
	templateID, accountID string,
) (*TargetAccountConfiguration, error) {
	b.mu.RLock("GetTargetAccountConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.targetAccountConfigs.Get(targetAccountConfigKey(templateID, accountID))
	if !ok {
		return nil, fmt.Errorf("%w: template=%s account=%s", ErrTargetAccountConfigNotFound, templateID, accountID)
	}

	cp := *cfg

	return &cp, nil
}

// UpdateTargetAccountConfiguration updates an existing target account configuration.
// Only non-nil pointer fields are applied.
func (b *InMemoryBackend) UpdateTargetAccountConfiguration(
	templateID, accountID string,
	roleArn, description *string,
) (*TargetAccountConfiguration, error) {
	b.mu.Lock("UpdateTargetAccountConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.targetAccountConfigs.Get(targetAccountConfigKey(templateID, accountID))
	if !ok {
		return nil, fmt.Errorf("%w: template=%s account=%s", ErrTargetAccountConfigNotFound, templateID, accountID)
	}

	if roleArn != nil {
		cfg.RoleArn = *roleArn
	}

	if description != nil {
		cfg.Description = *description
	}

	cp := *cfg

	return &cp, nil
}

// ListTargetAccountConfigurations returns all target account configurations for a template, sorted by account ID.
func (b *InMemoryBackend) ListTargetAccountConfigurations(templateID string) ([]*TargetAccountConfiguration, error) {
	b.mu.RLock("ListTargetAccountConfigurations")
	defer b.mu.RUnlock()

	if !b.templates.Has(templateID) {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, templateID)
	}

	cfgs := b.targetAccountConfigsByTemplate.Get(templateID)
	result := make([]*TargetAccountConfiguration, 0, len(cfgs))

	for _, cfg := range cfgs {
		cp := *cfg
		result = append(result, &cp)
	}

	slices.SortFunc(
		result,
		func(a, b *TargetAccountConfiguration) int { return strings.Compare(a.AccountID, b.AccountID) },
	)

	return result, nil
}

// GetExperimentTargetAccountConfiguration returns the target account configuration for a running experiment.
// It resolves the configuration from the experiment's source template.
func (b *InMemoryBackend) GetExperimentTargetAccountConfiguration(
	experimentID, accountID string,
) (*ExperimentTargetAccountConfiguration, error) {
	b.mu.RLock("GetExperimentTargetAccountConfiguration")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(experimentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, experimentID)
	}

	cfg, ok := b.targetAccountConfigs.Get(targetAccountConfigKey(exp.ExperimentTemplateID, accountID))
	if !ok {
		return nil, fmt.Errorf("%w: experiment=%s account=%s", ErrTargetAccountConfigNotFound, experimentID, accountID)
	}

	return &ExperimentTargetAccountConfiguration{
		ExperimentID: experimentID,
		AccountID:    cfg.AccountID,
		Description:  cfg.Description,
		RoleArn:      cfg.RoleArn,
	}, nil
}

// ListExperimentTargetAccountConfigurations lists all target account configurations for a running experiment,
// sorted by account ID. It resolves configurations from the experiment's source template.
func (b *InMemoryBackend) ListExperimentTargetAccountConfigurations(
	experimentID string,
) ([]*ExperimentTargetAccountConfiguration, error) {
	b.mu.RLock("ListExperimentTargetAccountConfigurations")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(experimentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, experimentID)
	}

	cfgs := b.targetAccountConfigsByTemplate.Get(exp.ExperimentTemplateID)
	result := make([]*ExperimentTargetAccountConfiguration, 0, len(cfgs))

	for _, cfg := range cfgs {
		result = append(result, &ExperimentTargetAccountConfiguration{
			ExperimentID: experimentID,
			AccountID:    cfg.AccountID,
			Description:  cfg.Description,
			RoleArn:      cfg.RoleArn,
		})
	}

	slices.SortFunc(result, func(a, b *ExperimentTargetAccountConfiguration) int {
		return strings.Compare(a.AccountID, b.AccountID)
	})

	return result, nil
}

// ----------------------------------------
// Experiment goroutine
// ----------------------------------------

// lifecycleDelay is the short pause between lifecycle state transitions so that
// SDK polling can observe intermediate states (initiating, completing, stopping).
const lifecycleDelay = 10 * time.Millisecond

// runExperiment manages the full lifecycle of a single experiment.
func (b *InMemoryBackend) runExperiment(ctx context.Context, expID string, tpl *ExperimentTemplate) {
	// PENDING → INITIATING.
	b.setExperimentStatus(expID, statusInitiating)
	b.setAllActionStatuses(expID, actionStatusInitiating)

	initiatingTimer := time.NewTimer(lifecycleDelay)
	defer initiatingTimer.Stop()
	select {
	case <-ctx.Done():
		b.cleanupActions(nil, expID, statusStopped, actionStatusCancelled)

		return
	case <-initiatingTimer.C:
	}

	// INITIATING → RUNNING.
	b.setExperimentStatus(expID, statusRunning)
	b.setAllActionStatuses(expID, actionStatusRunning)

	// Build fault rules and run actions respecting startAfter dependencies.
	faultRules, maxDuration, failReason := b.executeActionsOrdered(ctx, expID, tpl)

	if failReason != "" {
		b.cleanupActions(faultRules, expID, statusFailed, actionStatusFailed)

		return
	}

	// Wait for duration, stop signal, or context cancellation.
	// If maxDuration is 0 (e.g. all actions are immediate/non-timed), complete right away.
	if maxDuration == 0 {
		b.setExperimentStatus(expID, statusCompleting)
		b.setAllActionStatuses(expID, actionStatusCompleting)

		completingTimer := time.NewTimer(lifecycleDelay)
		defer completingTimer.Stop()
		select {
		case <-ctx.Done():
			b.cleanupActions(faultRules, expID, statusStopped, actionStatusStopped)

			return
		case <-completingTimer.C:
		}

		b.cleanupActions(faultRules, expID, statusCompleted, actionStatusCompleted)

		return
	}

	timer := time.NewTimer(maxDuration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		// Manually stopped or context cancelled — transition through stopping.
		b.setExperimentStatus(expID, statusStopping)
		b.cleanupActions(faultRules, expID, statusStopped, actionStatusStopped)
	case <-timer.C:
		// All actions completed naturally — transition through completing.
		b.setExperimentStatus(expID, statusCompleting)
		b.setAllActionStatuses(expID, actionStatusCompleting)

		finalTimer := time.NewTimer(lifecycleDelay)
		defer finalTimer.Stop()
		select {
		case <-ctx.Done():
			b.cleanupActions(faultRules, expID, statusStopped, actionStatusStopped)
		case <-finalTimer.C:
			b.cleanupActions(faultRules, expID, statusCompleted, actionStatusCompleted)
		}
	}
}

// executeActionsOrdered executes template actions in startAfter dependency order.
// Chaos fault rules are applied first, then external actions run in topological order.
// Returns accumulated fault rules, the maximum action duration, and a non-empty failure reason on error.
func (b *InMemoryBackend) executeActionsOrdered(
	ctx context.Context,
	expID string,
	tpl *ExperimentTemplate,
) ([]chaos.FaultRule, time.Duration, string) {
	var faultRules []chaos.FaultRule

	var maxDuration time.Duration

	// Sort actions into topological order respecting startAfter.
	ordered := topoSortActions(tpl.Actions)

	// Track which action names have completed so downstream deps can be released.
	completed := make(map[string]bool, len(tpl.Actions))

	for _, name := range ordered {
		action := tpl.Actions[name]

		// Check context before each action.
		select {
		case <-ctx.Done():
			return faultRules, maxDuration, ""
		default:
		}

		// Wait for all startAfter dependencies.
		for _, dep := range action.StartAfter {
			if !completed[dep] {
				// Dep should already be done since we process in topo order,
				// but guard against topo sort edge cases.
				continue
			}
		}

		dur := parseISODuration(action.Parameters["duration"])
		if dur > maxDuration {
			maxDuration = dur
		}

		switch {
		case strings.HasPrefix(action.ActionID, "aws:fis:inject-api-"):
			faultRules = append(faultRules, buildFaultRules(action)...)
			// Apply immediately so faults are active as soon as possible.
			if len(faultRules) > 0 && b.getFaultStore() != nil {
				b.getFaultStore().AppendRules(buildFaultRules(action))
			}
		case action.ActionID == actionIDWait:
			// Wait action — duration already captured above.
		default:
			ea := externalAction{
				actionID:   action.ActionID,
				params:     copyStringMap(action.Parameters),
				targets:    action.Targets,
				duration:   dur,
				tplTargets: tpl.Targets,
			}

			b.setActionStatus(expID, name, actionStatusRunning)

			if err := b.executeExternalAction(ctx, ea); err != nil {
				b.markExperimentFailed(expID, err.Error())

				return faultRules, maxDuration, err.Error()
			}

			b.setActionStatus(expID, name, actionStatusCompleted)
		}

		completed[name] = true
	}

	return faultRules, maxDuration, ""
}

// topoSortActions returns action names in a topological order respecting startAfter.
// Actions with no dependencies come first; actions whose dependencies are all earlier come later.
// The result is deterministic: within the same dependency level, actions are sorted by name.
func topoSortActions(actions map[string]ExperimentTemplateAction) []string {
	inDegree := make(map[string]int, len(actions))
	dependents := make(map[string][]string, len(actions)) // name → names that depend on it

	for name := range actions {
		if _, ok := inDegree[name]; !ok {
			inDegree[name] = 0
		}
	}

	for name, action := range actions {
		for _, dep := range action.StartAfter {
			inDegree[name]++
			dependents[dep] = append(dependents[dep], name)
		}
	}

	// Collect zero-in-degree nodes, sorted for determinism.
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	slices.Sort(queue)

	result := make([]string, 0, len(actions))

	for len(queue) > 0 {
		// Pop front.
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)

		// Reduce in-degree for dependents.
		next := make([]string, 0)

		for _, dep := range dependents[cur] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				next = append(next, dep)
			}
		}

		slices.Sort(next)
		queue = append(queue, next...)
	}

	return result
}

// setActionStatus atomically updates a single action's status.
func (b *InMemoryBackend) setActionStatus(expID, actionName, status string) {
	b.mu.Lock("setActionStatus")
	defer b.mu.Unlock()

	if exp, ok := b.experiments.Get(expID); ok {
		if action, ok2 := exp.Actions[actionName]; ok2 {
			action.Status = ExperimentActionStatus{Status: status}
			exp.Actions[actionName] = action
		}
	}
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
// Returns an error if the provider reports a failure.
func (b *InMemoryBackend) executeExternalAction(ctx context.Context, ea externalAction) error {
	b.mu.RLock("executeExternalAction")
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
				return p.ExecuteFISAction(ctx, exec)
			}
		}
	}

	return nil
}

// cleanupActions removes fault rules and sets the final experiment status.
// It also calls exp.cancel() to release the context and prevent goroutine leaks.
func (b *InMemoryBackend) cleanupActions(faultRules []chaos.FaultRule, expID, expStatus, actionStatus string) {
	if len(faultRules) > 0 && b.getFaultStore() != nil {
		b.getFaultStore().DeleteRules(faultRules)
	}

	now := time.Now()
	b.mu.Lock("cleanupActions")

	if exp, ok := b.experiments.Get(expID); ok {
		exp.Status = ExperimentStatus{Status: expStatus}
		exp.EndTime = &now

		for name, action := range exp.Actions {
			action.Status = ExperimentActionStatus{Status: actionStatus}
			endTime := now
			action.EndTime = &endTime
			exp.Actions[name] = action
		}

		// Release context resources; safe to call multiple times.
		if exp.cancel != nil {
			exp.cancel()
		}
	}

	b.mu.Unlock()
}

// setExperimentStatus atomically updates an experiment's status.
func (b *InMemoryBackend) setExperimentStatus(id, status string) {
	b.mu.Lock("setExperimentStatus")
	defer b.mu.Unlock()

	if exp, ok := b.experiments.Get(id); ok {
		exp.Status = ExperimentStatus{Status: status}
	}
}

// setAllActionStatuses atomically sets all actions in an experiment to the given status.
func (b *InMemoryBackend) setAllActionStatuses(expID, status string) {
	b.mu.Lock("setAllActionStatuses")
	defer b.mu.Unlock()

	if exp, ok := b.experiments.Get(expID); ok {
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
	b.mu.RLock("getFaultStore")
	defer b.mu.RUnlock()

	return b.faultStore
}

// markExperimentFailed sets an experiment and all its actions to failed with a reason.
func (b *InMemoryBackend) markExperimentFailed(expID, reason string) {
	b.mu.Lock("markExperimentFailed")
	defer b.mu.Unlock()

	exp, ok := b.experiments.Get(expID)
	if !ok {
		return
	}

	now := time.Now()
	exp.Status = ExperimentStatus{Status: statusFailed, Reason: reason}
	exp.EndTime = &now

	for name, action := range exp.Actions {
		if action.Status.Status == actionStatusRunning || action.Status.Status == actionStatusPending {
			action.Status = ExperimentActionStatus{Status: actionStatusFailed, Reason: reason}
			endTime := now
			action.EndTime = &endTime
			exp.Actions[name] = action
		}
	}
}

// ----------------------------------------
// Deep copy helpers
// ----------------------------------------

// cloneTemplate returns a deep copy of an ExperimentTemplate.
func cloneTemplate(tpl *ExperimentTemplate) *ExperimentTemplate {
	cp := *tpl

	cp.Tags = copyStringMap(tpl.Tags)

	if tpl.Targets != nil {
		cp.Targets = make(map[string]ExperimentTemplateTarget, len(tpl.Targets))

		for k, v := range tpl.Targets {
			t := v
			t.ResourceArns = append([]string(nil), v.ResourceArns...)
			t.ResourceTags = copyStringMap(v.ResourceTags)
			t.Parameters = copyStringMap(v.Parameters)

			filters := make([]ExperimentTemplateTargetFilter, len(v.Filters))
			for i, f := range v.Filters {
				filters[i] = ExperimentTemplateTargetFilter{
					Path:   f.Path,
					Values: append([]string(nil), f.Values...),
				}
			}

			t.Filters = filters
			cp.Targets[k] = t
		}
	}

	if tpl.Actions != nil {
		cp.Actions = make(map[string]ExperimentTemplateAction, len(tpl.Actions))

		for k, v := range tpl.Actions {
			a := v
			a.Parameters = copyStringMap(v.Parameters)
			a.Targets = copyStringMap(v.Targets)
			a.StartAfter = append([]string(nil), v.StartAfter...)
			cp.Actions[k] = a
		}
	}

	if tpl.StopConditions != nil {
		cp.StopConditions = append([]ExperimentTemplateStopCondition(nil), tpl.StopConditions...)
	}

	if tpl.LogConfiguration != nil {
		lc := *tpl.LogConfiguration
		if tpl.LogConfiguration.CloudWatchLogsConfiguration != nil {
			cwl := *tpl.LogConfiguration.CloudWatchLogsConfiguration
			lc.CloudWatchLogsConfiguration = &cwl
		}

		if tpl.LogConfiguration.S3Configuration != nil {
			s3 := *tpl.LogConfiguration.S3Configuration
			lc.S3Configuration = &s3
		}

		cp.LogConfiguration = &lc
	}

	if tpl.ExperimentOptions != nil {
		opt := *tpl.ExperimentOptions
		cp.ExperimentOptions = &opt
	}

	return &cp
}

// cloneExperiment returns a snapshot of an Experiment safe to return outside the lock.
// The cancel field is intentionally NOT copied.
func cloneExperiment(exp *Experiment) *Experiment {
	cp := *exp
	cp.cancel = nil
	cp.CreationTime = exp.CreationTime
	cp.Tags = copyStringMap(exp.Tags)

	if exp.Targets != nil {
		cp.Targets = make(map[string]ExperimentTarget, len(exp.Targets))

		for k, v := range exp.Targets {
			t := v
			t.ResourceArns = append([]string(nil), v.ResourceArns...)
			t.Parameters = copyStringMap(v.Parameters)
			cp.Targets[k] = t
		}
	}

	if exp.Actions != nil {
		cp.Actions = make(map[string]ExperimentAction, len(exp.Actions))

		for k, v := range exp.Actions {
			a := v
			a.Parameters = copyStringMap(v.Parameters)
			a.Targets = copyStringMap(v.Targets)

			if v.StartTime != nil {
				st := *v.StartTime
				a.StartTime = &st
			}

			if v.EndTime != nil {
				et := *v.EndTime
				a.EndTime = &et
			}

			cp.Actions[k] = a
		}
	}

	if exp.StopConditions != nil {
		cp.StopConditions = append([]ExperimentStopCondition(nil), exp.StopConditions...)
	}

	if exp.EndTime != nil {
		et := *exp.EndTime
		cp.EndTime = &et
	}

	if exp.LogConfiguration != nil {
		lc := *exp.LogConfiguration
		if exp.LogConfiguration.CloudWatchLogsConfiguration != nil {
			cwl := *exp.LogConfiguration.CloudWatchLogsConfiguration
			lc.CloudWatchLogsConfiguration = &cwl
		}

		if exp.LogConfiguration.S3Configuration != nil {
			s3 := *exp.LogConfiguration.S3Configuration
			lc.S3Configuration = &s3
		}

		cp.LogConfiguration = &lc
	}

	if exp.ExperimentOptions != nil {
		opt := *exp.ExperimentOptions
		cp.ExperimentOptions = &opt
	}

	return &cp
}

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
