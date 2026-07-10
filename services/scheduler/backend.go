package scheduler

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// ScheduleOption applies an optional field to a schedule during creation or update.
type ScheduleOption func(*Schedule)

// applyScheduleOptions applies all options to the schedule.
func applyScheduleOptions(opts []ScheduleOption, s *Schedule) {
	for _, o := range opts {
		o(s)
	}
}

// WithStartDate sets the optional start date on a schedule.
func WithStartDate(t time.Time) ScheduleOption {
	return func(s *Schedule) { s.StartDate = &t }
}

// WithEndDate sets the optional end date on a schedule.
func WithEndDate(t time.Time) ScheduleOption {
	return func(s *Schedule) { s.EndDate = &t }
}

// WithActionAfterCompletion sets what happens after the schedule completes (DELETE or NONE).
func WithActionAfterCompletion(action string) ScheduleOption {
	return func(s *Schedule) { s.ActionAfterCompletion = action }
}

// WithKmsKeyArn sets an optional customer-managed KMS key ARN on a schedule.
func WithKmsKeyArn(arn string) ScheduleOption {
	return func(s *Schedule) { s.KmsKeyArn = arn }
}

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const (
	defaultGroupName         = "default"
	scheduleGroupStateActive = "ACTIVE"
	scheduleStateEnabled     = "ENABLED"
	scheduleStateDisabled    = "DISABLED"

	// flexibleTimeWindowModeOff means no flexible time window is used.
	flexibleTimeWindowModeOff = "OFF"
	// flexibleTimeWindowModeFlexible means a flexible time window is applied.
	flexibleTimeWindowModeFlexible = "FLEXIBLE"

	// cronFieldCount is the number of space-separated fields a valid EventBridge
	// Scheduler cron() expression must contain:
	// minutes hours day-of-month month day-of-week year.
	cronFieldCount = 6

	// Name validation limits.
	scheduleNameMaxLen = 64
	// RetryPolicy field limits per AWS spec.
	retryPolicyMinEventAge = 60
	retryPolicyMaxEventAge = 86400
	retryPolicyMaxAttempts = 185
)

// validNameRE matches valid schedule/group names: 1-64 chars, [0-9a-zA-Z-_.].
var validNameRE = regexp.MustCompile(`^[0-9a-zA-Z\-_.]+$`)

type FlexibleTimeWindow struct {
	Mode                   string `json:"mode"`
	MaximumWindowInMinutes int    `json:"maximumWindowInMinutes,omitempty"`
}

// RetryPolicy configures retry behaviour for a schedule target.
// MaximumEventAgeInSeconds: 60-86400 (default 86400).
// MaximumRetryAttempts: 0-185 (default 185).
type RetryPolicy struct {
	MaximumEventAgeInSeconds int `json:"maximumEventAgeInSeconds"`
	MaximumRetryAttempts     int `json:"maximumRetryAttempts"`
}

// DeadLetterConfig holds the ARN of an SQS queue used as a dead-letter queue.
type DeadLetterConfig struct {
	Arn string `json:"arn"`
}

// InputTransformer maps input path expressions to a template string.
type InputTransformer struct {
	InputPathsMap map[string]string `json:"inputPathsMap,omitempty"`
	InputTemplate string            `json:"inputTemplate"`
}

// EventBridgeParameters holds parameters for EventBridge bus targets.
type EventBridgeParameters struct {
	DetailType string `json:"detailType"`
	Source     string `json:"source"`
}

// KinesisParameters holds parameters for Kinesis stream targets.
type KinesisParameters struct {
	PartitionKey string `json:"partitionKey"`
}

// SqsParameters holds parameters for SQS targets.
type SqsParameters struct {
	MessageGroupID string `json:"messageGroupId,omitempty"`
}

// SageMakerPipelineParameter is a name/value pair for SageMaker pipeline execution.
type SageMakerPipelineParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// SageMakerPipelineParameters holds parameters for SageMaker pipeline targets.
type SageMakerPipelineParameters struct {
	PipelineParameterList []SageMakerPipelineParameter `json:"pipelineParameterList,omitempty"`
}

// EcsAwsvpcConfiguration holds VPC networking options for ECS tasks.
type EcsAwsvpcConfiguration struct {
	AssignPublicIP string   `json:"assignPublicIp,omitempty"`
	SecurityGroups []string `json:"securityGroups,omitempty"`
	Subnets        []string `json:"subnets"`
}

// EcsNetworkConfiguration holds the awsvpc network configuration for ECS tasks.
type EcsNetworkConfiguration struct {
	AwsvpcConfiguration *EcsAwsvpcConfiguration `json:"awsvpcConfiguration,omitempty"`
}

// EcsCapacityProviderStrategyItem is one entry in an ECS capacity provider strategy.
type EcsCapacityProviderStrategyItem struct {
	CapacityProvider string `json:"capacityProvider"`
	Base             int    `json:"base,omitempty"`
	Weight           int    `json:"weight,omitempty"`
}

// EcsPlacementConstraint constrains ECS task placement.
type EcsPlacementConstraint struct {
	Expression string `json:"expression,omitempty"`
	Type       string `json:"type,omitempty"`
}

// EcsPlacementStrategy defines ECS task placement strategy.
type EcsPlacementStrategy struct {
	Field string `json:"field,omitempty"`
	Type  string `json:"type,omitempty"`
}

// EcsTag is a key/value tag applied to the ECS task at launch time.
type EcsTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// EcsParameters holds parameters for ECS task targets.
type EcsParameters struct {
	NetworkConfiguration     *EcsNetworkConfiguration          `json:"networkConfiguration,omitempty"`
	PropagateTags            string                            `json:"propagateTags,omitempty"`
	TaskDefinitionArn        string                            `json:"taskDefinitionArn"`
	LaunchType               string                            `json:"launchType,omitempty"`
	PlatformVersion          string                            `json:"platformVersion,omitempty"`
	Group                    string                            `json:"group,omitempty"`
	ReferenceID              string                            `json:"referenceId,omitempty"`
	PlacementConstraints     []EcsPlacementConstraint          `json:"placementConstraints,omitempty"`
	PlacementStrategy        []EcsPlacementStrategy            `json:"placementStrategy,omitempty"`
	Tags                     []EcsTag                          `json:"tags,omitempty"`
	CapacityProviderStrategy []EcsCapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	TaskCount                int                               `json:"taskCount,omitempty"`
	EnableECSManagedTags     bool                              `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                              `json:"enableExecuteCommand,omitempty"`
}

type Target struct {
	RetryPolicy                 *RetryPolicy                 `json:"retryPolicy,omitempty"`
	DeadLetterConfig            *DeadLetterConfig            `json:"deadLetterConfig,omitempty"`
	InputTransformer            *InputTransformer            `json:"inputTransformer,omitempty"`
	EventBridgeParameters       *EventBridgeParameters       `json:"eventBridgeParameters,omitempty"`
	KinesisParameters           *KinesisParameters           `json:"kinesisParameters,omitempty"`
	SqsParameters               *SqsParameters               `json:"sqsParameters,omitempty"`
	SageMakerPipelineParameters *SageMakerPipelineParameters `json:"sageMakerPipelineParameters,omitempty"`
	EcsParameters               *EcsParameters               `json:"ecsParameters,omitempty"`
	// Input is an optional custom event payload sent to the target instead of the default
	// scheduler event. When empty the runner constructs a default EventBridge Scheduler event.
	Input   string `json:"input,omitempty"`
	ARN     string `json:"arn"`
	RoleARN string `json:"roleARN"`
}

// Schedule represents an EventBridge Scheduler schedule.
type Schedule struct {
	Tags                       *tags.Tags         `json:"tags,omitempty"`
	CreationDate               time.Time          `json:"creationDate"`
	LastModificationDate       time.Time          `json:"lastModificationDate"`
	StartDate                  *time.Time         `json:"startDate,omitempty"`
	EndDate                    *time.Time         `json:"endDate,omitempty"`
	Target                     Target             `json:"target"`
	Name                       string             `json:"name"`
	ARN                        string             `json:"arn"`
	ScheduleExpression         string             `json:"scheduleExpression"`
	ScheduleExpressionTimezone string             `json:"scheduleExpressionTimezone,omitempty"`
	Description                string             `json:"description,omitempty"`
	GroupName                  string             `json:"groupName"`
	State                      string             `json:"state"`
	ActionAfterCompletion      string             `json:"actionAfterCompletion,omitempty"`
	KmsKeyArn                  string             `json:"kmsKeyArn,omitempty"`
	AccountID                  string             `json:"accountID"`
	Region                     string             `json:"region"`
	FlexibleTimeWindow         FlexibleTimeWindow `json:"flexibleTimeWindow"`
}

// ScheduleGroup represents an EventBridge Scheduler schedule group.
type ScheduleGroup struct {
	CreationDate         time.Time  `json:"creationDate"`
	LastModificationDate time.Time  `json:"lastModificationDate"`
	Tags                 *tags.Tags `json:"tags,omitempty"`
	Description          string     `json:"description,omitempty"`
	Name                 string     `json:"name"`
	ARN                  string     `json:"arn"`
	State                string     `json:"state"`
}

// InMemoryBackend stores schedules and schedule groups nested by region (multiple
// regions can each have a same-named schedule/group, fully isolated from one another).
// Phase 3.3 of the datalayer refactor replaces the region-nested
// map[string]map[string]*T pair for each resource with a single flat *store.Table[T],
// keyed by the composite "region|id" string (see regionKey below), with companion
// *store.Index values grouping entries by region (replacing the old outer map
// nesting) and by "region|ARN" (replacing the old per-region ARN reverse map used by
// TagResource/UntagResource/ListTagsForResource). See store_setup.go.
//
// Schedule already carries its own exported Region field (part of the AWS wire
// shape), so schedules is registered directly off that field. ScheduleGroup has no
// such field -- its ARN always embeds the region it was created in (see
// CreateScheduleGroup/seedDefaultGroup), so scheduleGroupRegionOf derives it from
// there instead of adding a new field.
//
// Both tables are "dirty" in the store_setup.go/persistence.go sense: their Tags
// field is a live *tags.Tags (backed by a Prometheus-instrumented map), which
// persistence.go swaps for a plain map[string]string via a DTO on Snapshot/Restore
// to preserve the existing per-resource Prometheus metric naming -- so neither
// table is registered on a shared b.registry; each is built with store.New only.
type InMemoryBackend struct {
	schedules              *store.Table[Schedule]
	schedulesByRegion      *store.Index[Schedule]
	schedulesByARN         *store.Index[Schedule]
	scheduleGroups         *store.Table[ScheduleGroup]
	scheduleGroupsByRegion *store.Index[ScheduleGroup]
	scheduleGroupsByARN    *store.Index[ScheduleGroup]
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("scheduler"),
	}
	registerAllTables(b)
	// Seed the built-in "default" group in the default region.
	b.ensureRegionGroupsSeeded(region)

	return b
}

// regionKey builds the composite store.Table primary key ("region|id") shared by
// both region-qualified tables registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// ensureRegionGroupsSeeded seeds the built-in "default" schedule group in region the
// first time the region is touched. It replicates the pre-conversion
// scheduleGroupsStore's lazy-init side effect: since the default group can never be
// deleted (see DeleteScheduleGroup), a region having zero groups is equivalent to
// "never seeded" and vice versa. Callers must hold b.mu (or equivalent).
func (b *InMemoryBackend) ensureRegionGroupsSeeded(region string) {
	if len(b.scheduleGroupsByRegion.Get(region)) == 0 {
		b.seedDefaultGroup(region)
	}
}

// scheduleGroupRegionOf returns the region a ScheduleGroup belongs to, derived from
// its ARN (always built with the owning region -- see CreateScheduleGroup/
// seedDefaultGroup), falling back to the backend's own default region for a
// malformed/test-injected ARN. Mirrors the pre-conversion AddScheduleGroupInternal's
// regionFromARN(g.ARN, b.region) call.
func (b *InMemoryBackend) scheduleGroupRegionOf(g *ScheduleGroup) string {
	return regionFromARN(g.ARN, b.region)
}

// scheduleGroupTableKeyFn is the store.Table primary key function for scheduleGroups.
func (b *InMemoryBackend) scheduleGroupTableKeyFn(g *ScheduleGroup) string {
	return regionKey(b.scheduleGroupRegionOf(g), g.Name)
}

// scheduleGroupARNIndexKeyFn is the scheduleGroupsByARN index key function.
func (b *InMemoryBackend) scheduleGroupARNIndexKeyFn(g *ScheduleGroup) string {
	return regionKey(b.scheduleGroupRegionOf(g), g.ARN)
}

// seedDefaultGroup creates the built-in "default" schedule group in the given region.
// It is invoked from ensureRegionGroupsSeeded the first time a region is seen (and
// from Reset/Restore). Callers must hold b.mu (or be in single-threaded setup).
func (b *InMemoryBackend) seedDefaultGroup(region string) {
	now := time.Now().UTC()
	groupARN := arn.Build("scheduler", region, b.accountID, "schedule-group/"+defaultGroupName)
	g := &ScheduleGroup{
		Name:                 defaultGroupName,
		ARN:                  groupARN,
		State:                scheduleGroupStateActive,
		CreationDate:         now,
		LastModificationDate: now,
		Tags:                 tags.New("scheduler.schedulegroup." + defaultGroupName + ".tags"),
	}
	b.scheduleGroups.Put(g)
}

// scheduleKey returns the composite map key for a schedule: "groupName/name".
func scheduleKey(groupName, name string) string {
	return groupName + "/" + name
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// CreateSchedule creates a new schedule in the named group.
func (b *InMemoryBackend) CreateSchedule(
	ctx context.Context,
	name, groupName, expr, description, timezone string,
	target Target,
	state string,
	ftw FlexibleTimeWindow,
	opts ...ScheduleOption,
) (*Schedule, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}

	if expr == "" {
		return nil, fmt.Errorf("%w: ScheduleExpression is required", ErrValidation)
	}

	if err := validateScheduleExpression(expr); err != nil {
		return nil, err
	}

	if target.ARN == "" {
		return nil, fmt.Errorf("%w: Target.Arn is required", ErrValidation)
	}

	if target.RoleARN == "" {
		return nil, fmt.Errorf("%w: Target.RoleArn is required", ErrValidation)
	}

	if ftw.Mode == "" {
		return nil, fmt.Errorf("%w: FlexibleTimeWindow.Mode is required", ErrValidation)
	}

	if err := validateScheduleState(state); err != nil {
		return nil, err
	}

	if err := validateFlexibleTimeWindow(ftw); err != nil {
		return nil, err
	}

	if err := validateTarget(target); err != nil {
		return nil, err
	}

	if groupName == "" {
		groupName = defaultGroupName
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateSchedule")
	defer b.mu.Unlock()

	b.ensureRegionGroupsSeeded(region)

	if _, ok := b.scheduleGroups.Get(regionKey(region, groupName)); !ok {
		return nil, fmt.Errorf("%w: schedule group %s not found", ErrNotFound, groupName)
	}

	tableKey := regionKey(region, scheduleKey(groupName, name))
	if b.schedules.Has(tableKey) {
		return nil, fmt.Errorf("%w: schedule %s already exists in group %s", ErrAlreadyExists, name, groupName)
	}

	schedARN := arn.Build("scheduler", region, b.accountID, "schedule/"+groupName+"/"+name)
	now := time.Now().UTC()
	s := &Schedule{
		Name:                       name,
		GroupName:                  groupName,
		ARN:                        schedARN,
		ScheduleExpression:         expr,
		ScheduleExpressionTimezone: timezone,
		Description:                description,
		Target:                     target,
		State:                      state,
		FlexibleTimeWindow:         ftw,
		AccountID:                  b.accountID,
		Region:                     region,
		CreationDate:               now,
		LastModificationDate:       now,
		Tags:                       tags.New("scheduler.schedule." + groupName + "." + name + ".tags"),
	}
	applyScheduleOptions(opts, s)
	b.schedules.Put(s)

	return cloneSchedule(s), nil
}

// GetSchedule returns a schedule by name and group.
func (b *InMemoryBackend) GetSchedule(ctx context.Context, name, groupName string) (*Schedule, error) {
	if groupName == "" {
		groupName = defaultGroupName
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetSchedule")
	defer b.mu.RUnlock()

	s, ok := b.schedules.Get(regionKey(region, scheduleKey(groupName, name)))
	if !ok {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}

	return cloneSchedule(s), nil
}

// ListSchedules returns schedules optionally filtered by group name, name prefix, and state.
// When maxResults > 0 and nextToken is non-empty it resumes after the token (last seen name).
// Returns the page of schedules and the next continuation token (empty when no more results).
func (b *InMemoryBackend) ListSchedules(
	ctx context.Context,
	groupName, namePrefix, state, nextToken string,
	maxResults int,
) ([]*Schedule, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListSchedules")
	defer b.mu.RUnlock()

	schedules := b.schedulesByRegion.Get(region)
	list := make([]*Schedule, 0, len(schedules))

	for _, s := range schedules {
		if groupName != "" && s.GroupName != groupName {
			continue
		}

		if namePrefix != "" && !strings.HasPrefix(s.Name, namePrefix) {
			continue
		}

		if state != "" && s.State != state {
			continue
		}

		list = append(list, cloneSchedule(s))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return paginate(list, func(s *Schedule) string { return s.GroupName + "/" + s.Name }, nextToken, maxResults)
}

// DeleteSchedule removes a schedule by name and group.
func (b *InMemoryBackend) DeleteSchedule(ctx context.Context, name, groupName string) error {
	if groupName == "" {
		groupName = defaultGroupName
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteSchedule")
	defer b.mu.Unlock()

	tableKey := regionKey(region, scheduleKey(groupName, name))

	s, ok := b.schedules.Get(tableKey)
	if !ok {
		return fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}

	b.schedules.Delete(tableKey)
	s.Tags.Close()

	return nil
}

// UpdateSchedule updates an existing schedule.
func (b *InMemoryBackend) UpdateSchedule(
	ctx context.Context,
	name, groupName, expr, description, timezone string,
	target Target,
	state string,
	ftw FlexibleTimeWindow,
	opts ...ScheduleOption,
) (*Schedule, error) {
	if expr == "" {
		return nil, fmt.Errorf("%w: ScheduleExpression is required", ErrValidation)
	}

	if err := validateScheduleExpression(expr); err != nil {
		return nil, err
	}

	if target.ARN == "" {
		return nil, fmt.Errorf("%w: Target.Arn is required", ErrValidation)
	}

	if target.RoleARN == "" {
		return nil, fmt.Errorf("%w: Target.RoleArn is required", ErrValidation)
	}

	if ftw.Mode == "" {
		return nil, fmt.Errorf("%w: FlexibleTimeWindow.Mode is required", ErrValidation)
	}

	if err := validateScheduleState(state); err != nil {
		return nil, err
	}

	if err := validateFlexibleTimeWindow(ftw); err != nil {
		return nil, err
	}

	if err := validateTarget(target); err != nil {
		return nil, err
	}

	if groupName == "" {
		groupName = defaultGroupName
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateSchedule")
	defer b.mu.Unlock()

	s, ok := b.schedules.Get(regionKey(region, scheduleKey(groupName, name)))
	if !ok {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}

	s.ScheduleExpression = expr
	s.ScheduleExpressionTimezone = timezone
	s.Description = description
	s.Target = target
	s.State = state
	s.FlexibleTimeWindow = ftw
	s.LastModificationDate = time.Now().UTC()
	applyScheduleOptions(opts, s)

	return cloneSchedule(s), nil
}

func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kv map[string]string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		s.Tags.Merge(kv)

		return nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		g.Tags.Merge(kv)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		s.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		g.Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		return s.Tags.Clone(), nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		return g.Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// scheduleByARN resolves a schedule by its ARN within region via the byARN index,
// returning nil when no schedule matches. Callers must hold b.mu. An ARN uniquely
// identifies a schedule, so at most one entry is ever grouped under the index key.
func (b *InMemoryBackend) scheduleByARN(region, resourceARN string) *Schedule {
	if matches := b.schedulesByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return matches[0]
	}

	return nil
}

// scheduleGroupByARN resolves a schedule group by its ARN within region via the
// byARN index, returning nil when no group matches. Callers must hold b.mu.
func (b *InMemoryBackend) scheduleGroupByARN(region, resourceARN string) *ScheduleGroup {
	if matches := b.scheduleGroupsByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return matches[0]
	}

	return nil
}

// Reset clears all in-memory state and re-seeds the default schedule group
// in the backend's default region.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, s := range b.schedules.All() {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	for _, g := range b.scheduleGroups.All() {
		if g.Tags != nil {
			g.Tags.Close()
		}
	}

	b.schedules.Reset()
	b.scheduleGroups.Reset()
	// Re-seed the built-in "default" group in the default region.
	b.ensureRegionGroupsSeeded(b.region)
}

// CreateScheduleGroup creates a new schedule group with the given name and optional tags.
func (b *InMemoryBackend) CreateScheduleGroup(
	ctx context.Context,
	name, description string,
	initialTags map[string]string,
) (*ScheduleGroup, error) {
	if err := validateGroupName(name); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateScheduleGroup")
	defer b.mu.Unlock()

	b.ensureRegionGroupsSeeded(region)

	if b.scheduleGroups.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: schedule group %s already exists", ErrAlreadyExists, name)
	}

	groupARN := arn.Build("scheduler", region, b.accountID, "schedule-group/"+name)
	now := time.Now().UTC()
	g := &ScheduleGroup{
		Name:                 name,
		ARN:                  groupARN,
		Description:          description,
		State:                scheduleGroupStateActive,
		CreationDate:         now,
		LastModificationDate: now,
		Tags:                 tags.New("scheduler.schedulegroup." + name + ".tags"),
	}
	g.Tags.Merge(initialTags)
	b.scheduleGroups.Put(g)

	return cloneScheduleGroup(g), nil
}

// GetScheduleGroup returns the schedule group with the given name.
func (b *InMemoryBackend) GetScheduleGroup(ctx context.Context, name string) (*ScheduleGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetScheduleGroup")
	defer b.mu.RUnlock()

	b.ensureRegionGroupsSeeded(region)

	g, ok := b.scheduleGroups.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: schedule group %s not found", ErrNotFound, name)
	}

	return cloneScheduleGroup(g), nil
}

// DeleteScheduleGroup removes the schedule group with the given name.
// The built-in "default" group cannot be deleted.
// All schedules within the group are also deleted.
func (b *InMemoryBackend) DeleteScheduleGroup(ctx context.Context, name string) error {
	if name == defaultGroupName {
		return fmt.Errorf("%w: cannot delete the default schedule group", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteScheduleGroup")
	defer b.mu.Unlock()

	g, ok := b.scheduleGroups.Get(regionKey(region, name))
	if !ok {
		return fmt.Errorf("%w: schedule group %s not found", ErrNotFound, name)
	}

	// Cascade-delete all schedules belonging to this group.
	for _, s := range b.schedulesByRegion.Get(region) {
		if s.GroupName == name {
			b.schedules.Delete(regionKey(region, scheduleKey(s.GroupName, s.Name)))
			if s.Tags != nil {
				s.Tags.Close()
			}
		}
	}

	b.scheduleGroups.Delete(regionKey(region, name))
	g.Tags.Close()

	return nil
}

// ListScheduleGroups returns schedule groups optionally filtered by name prefix.
// When maxResults > 0 and nextToken is non-empty it resumes after the token (last seen name).
// Returns the page of groups and the next continuation token (empty when no more results).
func (b *InMemoryBackend) ListScheduleGroups(
	ctx context.Context, namePrefix, nextToken string, maxResults int,
) ([]*ScheduleGroup, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListScheduleGroups")
	defer b.mu.RUnlock()

	b.ensureRegionGroupsSeeded(region)

	groups := b.scheduleGroupsByRegion.Get(region)
	list := make([]*ScheduleGroup, 0, len(groups))

	for _, g := range groups {
		if namePrefix != "" && !strings.HasPrefix(g.Name, namePrefix) {
			continue
		}

		list = append(list, cloneScheduleGroup(g))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return paginate(list, func(g *ScheduleGroup) string { return g.Name }, nextToken, maxResults)
}

// paginate slices a sorted list by maxResults starting after nextToken.
// keyFn extracts the opaque token key from each element.
// Returns the page and the continuation token (empty when no more results).
func paginate[T any](list []T, keyFn func(T) string, nextToken string, maxResults int) ([]T, string) {
	if nextToken != "" {
		start := 0

		for i, item := range list {
			if keyFn(item) == nextToken {
				start = i + 1

				break
			}
		}

		list = list[start:]
	}

	if maxResults <= 0 || maxResults >= len(list) {
		return list, ""
	}

	page := list[:maxResults]

	return page, keyFn(page[len(page)-1])
}

// AddScheduleInternal inserts a schedule directly for testing purposes.
// Must only be used from test code.
func (b *InMemoryBackend) AddScheduleInternal(s *Schedule) {
	b.mu.Lock("AddScheduleInternal")
	defer b.mu.Unlock()

	if s.GroupName == "" {
		s.GroupName = defaultGroupName
	}

	if s.Tags == nil {
		s.Tags = tags.New("scheduler.schedule." + s.GroupName + "." + s.Name + ".tags")
	}

	if s.Region == "" {
		s.Region = b.region
	}

	// s.Region is the outer half of the composite table key (see schedulesKeyFn),
	// so it must be set before Put.
	b.schedules.Put(s)
}

// AddScheduleGroupInternal inserts a schedule group directly for testing purposes.
// Must only be used from test code.
func (b *InMemoryBackend) AddScheduleGroupInternal(g *ScheduleGroup) {
	b.mu.Lock("AddScheduleGroupInternal")
	defer b.mu.Unlock()

	if g.Tags == nil {
		g.Tags = tags.New("scheduler.schedulegroup." + g.Name + ".tags")
	}

	// Seed the built-in "default" group for the group's region first (matching the
	// pre-conversion scheduleGroupsStore's lazy-init side effect), then insert g.
	// The group's region is derived from its ARN by the table's key function.
	b.ensureRegionGroupsSeeded(b.scheduleGroupRegionOf(g))
	b.scheduleGroups.Put(g)
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

// cloneSchedule returns a deep copy of a schedule (including a snapshot of its Tags).
func cloneSchedule(s *Schedule) *Schedule {
	cp := *s
	cp.Tags = nil

	if s.Tags != nil {
		cp.Tags = tags.FromMap("scheduler.schedule."+s.GroupName+"."+s.Name+".tags.clone", s.Tags.Clone())
	}

	// Deep-copy optional time pointer fields.
	if s.StartDate != nil {
		t := *s.StartDate
		cp.StartDate = &t
	}

	if s.EndDate != nil {
		t := *s.EndDate
		cp.EndDate = &t
	}

	return &cp
}

// cloneScheduleGroup returns a deep copy of a schedule group (including a snapshot of its Tags).
func cloneScheduleGroup(g *ScheduleGroup) *ScheduleGroup {
	cp := *g
	cp.Tags = nil

	if g.Tags != nil {
		cp.Tags = tags.FromMap("scheduler.schedulegroup."+g.Name+".tags.clone", g.Tags.Clone())
	}

	return &cp
}

// validateScheduleExpression checks that the ScheduleExpression has a valid prefix and format.
// AWS Scheduler accepts: rate(value unit), cron(fields), at(datetime).
// A cron expression must have exactly 6 space-separated fields inside cron(...).
func validateScheduleExpression(expr string) error {
	switch {
	case strings.HasPrefix(expr, "rate("):
		if !strings.HasSuffix(expr, ")") {
			return fmt.Errorf("%w: ScheduleExpression rate expression must end with ')'", ErrValidation)
		}
	case strings.HasPrefix(expr, "at("):
		if !strings.HasSuffix(expr, ")") {
			return fmt.Errorf("%w: ScheduleExpression at expression must end with ')'", ErrValidation)
		}
	case strings.HasPrefix(expr, "cron("):
		if !strings.HasSuffix(expr, ")") {
			return fmt.Errorf("%w: ScheduleExpression cron expression must end with ')'", ErrValidation)
		}
		inner := expr[len("cron(") : len(expr)-1]
		fields := strings.Fields(inner)
		if len(fields) != cronFieldCount {
			return fmt.Errorf(
				"%w: ScheduleExpression cron expression must have exactly %d fields "+
					"(minutes hours day-of-month month day-of-week year), got %d",
				ErrValidation,
				cronFieldCount,
				len(fields),
			)
		}
	default:
		return fmt.Errorf(
			"%w: ScheduleExpression must start with rate(), cron(), or at(); got %q",
			ErrValidation, expr,
		)
	}

	return nil
}

// validateScheduleState returns ErrValidation if state is not a valid value.
// An empty string is allowed (the handler sets a default).
func validateScheduleState(state string) error {
	switch state {
	case scheduleStateEnabled, scheduleStateDisabled, "":
		return nil
	default:
		return fmt.Errorf("%w: State must be ENABLED or DISABLED, got %q", ErrValidation, state)
	}
}

// validateFlexibleTimeWindowMode returns ErrValidation if mode is not OFF or FLEXIBLE.
func validateFlexibleTimeWindowMode(mode string) error {
	switch mode {
	case flexibleTimeWindowModeOff, flexibleTimeWindowModeFlexible:
		return nil
	default:
		return fmt.Errorf("%w: FlexibleTimeWindow.Mode must be OFF or FLEXIBLE, got %q", ErrValidation, mode)
	}
}

// validateFlexibleTimeWindow validates both the mode and the required window size.
// When Mode is FLEXIBLE, MaximumWindowInMinutes must be positive (1–1440).
func validateFlexibleTimeWindow(ftw FlexibleTimeWindow) error {
	if err := validateFlexibleTimeWindowMode(ftw.Mode); err != nil {
		return err
	}

	if ftw.Mode == flexibleTimeWindowModeFlexible && ftw.MaximumWindowInMinutes <= 0 {
		return fmt.Errorf(
			"%w: FlexibleTimeWindow.MaximumWindowInMinutes is required and must be >= 1 when Mode is FLEXIBLE",
			ErrValidation,
		)
	}

	return nil
}

// validateName checks that name is non-empty, matches [0-9a-zA-Z-_.], and is at most 64 chars.
func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if len(name) > scheduleNameMaxLen {
		return fmt.Errorf("%w: Name must be 1-64 characters, got %d", ErrValidation, len(name))
	}

	if !validNameRE.MatchString(name) {
		return fmt.Errorf("%w: Name must match [0-9a-zA-Z-_.], got %q", ErrValidation, name)
	}

	return nil
}

// validateGroupName validates a schedule group name, also rejecting "default" as a creation target.
func validateGroupName(name string) error {
	if err := validateName(name); err != nil {
		return err
	}

	if name == defaultGroupName {
		return fmt.Errorf("%w: cannot create a group named %q (reserved)", ErrValidation, defaultGroupName)
	}

	return nil
}

// validateRetryPolicy validates RetryPolicy field ranges.
func validateRetryPolicy(rp *RetryPolicy) error {
	if rp == nil {
		return nil
	}

	if rp.MaximumEventAgeInSeconds != 0 &&
		(rp.MaximumEventAgeInSeconds < retryPolicyMinEventAge || rp.MaximumEventAgeInSeconds > retryPolicyMaxEventAge) {
		return fmt.Errorf(
			"%w: RetryPolicy.MaximumEventAgeInSeconds must be 60-86400, got %d",
			ErrValidation,
			rp.MaximumEventAgeInSeconds,
		)
	}

	if rp.MaximumRetryAttempts < 0 || rp.MaximumRetryAttempts > retryPolicyMaxAttempts {
		return fmt.Errorf(
			"%w: RetryPolicy.MaximumRetryAttempts must be 0-185, got %d",
			ErrValidation,
			rp.MaximumRetryAttempts,
		)
	}

	return nil
}

// validateTarget validates target-specific parameter constraints.
func validateTarget(target Target) error {
	if err := validateRetryPolicy(target.RetryPolicy); err != nil {
		return err
	}

	if target.DeadLetterConfig != nil && target.DeadLetterConfig.Arn != "" {
		if !strings.HasPrefix(target.DeadLetterConfig.Arn, "arn:aws:sqs:") {
			return fmt.Errorf("%w: DeadLetterConfig.Arn must be an SQS ARN (arn:aws:sqs:...)", ErrValidation)
		}
	}

	if target.KinesisParameters != nil && target.KinesisParameters.PartitionKey == "" {
		return fmt.Errorf("%w: KinesisParameters.PartitionKey is required for Kinesis targets", ErrValidation)
	}

	if target.EventBridgeParameters != nil {
		if target.EventBridgeParameters.DetailType == "" {
			return fmt.Errorf("%w: EventBridgeParameters.DetailType is required", ErrValidation)
		}

		if target.EventBridgeParameters.Source == "" {
			return fmt.Errorf("%w: EventBridgeParameters.Source is required", ErrValidation)
		}
	}

	if target.EcsParameters != nil && target.EcsParameters.TaskDefinitionArn == "" {
		return fmt.Errorf("%w: EcsParameters.TaskDefinitionArn is required", ErrValidation)
	}

	return nil
}
