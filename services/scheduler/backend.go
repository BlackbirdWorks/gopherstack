package scheduler

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

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
	MessageGroupId string `json:"messageGroupId,omitempty"`
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

// EcsParameters holds parameters for ECS task targets.
type EcsParameters struct {
	TaskDefinitionArn    string `json:"taskDefinitionArn"`
	LaunchType           string `json:"launchType,omitempty"`
	PlatformVersion      string `json:"platformVersion,omitempty"`
	Group                string `json:"group,omitempty"`
	PropagateTags        string `json:"propagateTags,omitempty"`
	ReferenceId          string `json:"referenceId,omitempty"`
	TaskCount            int    `json:"taskCount,omitempty"`
	EnableECSManagedTags bool   `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand bool   `json:"enableExecuteCommand,omitempty"`
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

type InMemoryBackend struct {
	schedules             map[string]*Schedule
	scheduleARNIndex      map[string]string // ARN → schedule name (group/name composite key)
	scheduleGroups        map[string]*ScheduleGroup
	scheduleGroupARNIndex map[string]string // ARN → schedule group name
	mu                    *lockmetrics.RWMutex
	accountID             string
	region                string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		schedules:             make(map[string]*Schedule),
		scheduleARNIndex:      make(map[string]string),
		scheduleGroups:        make(map[string]*ScheduleGroup),
		scheduleGroupARNIndex: make(map[string]string),
		accountID:             accountID,
		region:                region,
		mu:                    lockmetrics.New("scheduler"),
	}
	b.seedDefaultGroup()

	return b
}

// seedDefaultGroup creates the built-in "default" schedule group.
// Must be called without the mutex held.
func (b *InMemoryBackend) seedDefaultGroup() {
	now := time.Now().UTC()
	groupARN := arn.Build("scheduler", b.region, b.accountID, "schedule-group/"+defaultGroupName)
	g := &ScheduleGroup{
		Name:                 defaultGroupName,
		ARN:                  groupARN,
		State:                scheduleGroupStateActive,
		CreationDate:         now,
		LastModificationDate: now,
		Tags:                 tags.New("scheduler.schedulegroup." + defaultGroupName + ".tags"),
	}
	b.scheduleGroups[defaultGroupName] = g
	b.scheduleGroupARNIndex[groupARN] = defaultGroupName
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

	if err := validateFlexibleTimeWindowMode(ftw.Mode); err != nil {
		return nil, err
	}

	if err := validateTarget(target); err != nil {
		return nil, err
	}

	if groupName == "" {
		groupName = defaultGroupName
	}

	b.mu.Lock("CreateSchedule")
	defer b.mu.Unlock()

	if _, ok := b.scheduleGroups[groupName]; !ok {
		return nil, fmt.Errorf("%w: schedule group %s not found", ErrNotFound, groupName)
	}

	key := scheduleKey(groupName, name)
	if _, ok := b.schedules[key]; ok {
		return nil, fmt.Errorf("%w: schedule %s already exists in group %s", ErrAlreadyExists, name, groupName)
	}

	schedARN := arn.Build("scheduler", b.region, b.accountID, "schedule/"+groupName+"/"+name)
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
		Region:                     b.region,
		CreationDate:               now,
		LastModificationDate:       now,
		Tags:                       tags.New("scheduler.schedule." + groupName + "." + name + ".tags"),
	}
	applyScheduleOptions(opts, s)
	b.schedules[key] = s
	b.scheduleARNIndex[schedARN] = key

	return cloneSchedule(s), nil
}

// GetSchedule returns a schedule by name and group.
func (b *InMemoryBackend) GetSchedule(name, groupName string) (*Schedule, error) {
	if groupName == "" {
		groupName = defaultGroupName
	}

	b.mu.RLock("GetSchedule")
	defer b.mu.RUnlock()

	s, ok := b.schedules[scheduleKey(groupName, name)]
	if !ok {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}

	return cloneSchedule(s), nil
}

// ListSchedules returns schedules optionally filtered by group name, name prefix, and state.
// When maxResults > 0 and nextToken is non-empty it resumes after the token (last seen name).
// Returns the page of schedules and the next continuation token (empty when no more results).
func (b *InMemoryBackend) ListSchedules(
	groupName, namePrefix, state, nextToken string,
	maxResults int,
) ([]*Schedule, string) {
	b.mu.RLock("ListSchedules")
	defer b.mu.RUnlock()

	list := make([]*Schedule, 0, len(b.schedules))

	for _, s := range b.schedules {
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
func (b *InMemoryBackend) DeleteSchedule(name, groupName string) error {
	if groupName == "" {
		groupName = defaultGroupName
	}

	b.mu.Lock("DeleteSchedule")
	defer b.mu.Unlock()

	key := scheduleKey(groupName, name)

	s, ok := b.schedules[key]
	if !ok {
		return fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}

	delete(b.scheduleARNIndex, s.ARN)
	delete(b.schedules, key)
	s.Tags.Close()

	return nil
}

// UpdateSchedule updates an existing schedule.
func (b *InMemoryBackend) UpdateSchedule(
	name, groupName, expr, description, timezone string,
	target Target,
	state string,
	ftw FlexibleTimeWindow,
	opts ...ScheduleOption,
) (*Schedule, error) {
	if state != "" {
		if err := validateScheduleState(state); err != nil {
			return nil, err
		}
	}

	if ftw.Mode != "" {
		if err := validateFlexibleTimeWindowMode(ftw.Mode); err != nil {
			return nil, err
		}
	}

	if err := validateTarget(target); err != nil {
		return nil, err
	}

	if groupName == "" {
		groupName = defaultGroupName
	}

	b.mu.Lock("UpdateSchedule")
	defer b.mu.Unlock()

	s, ok := b.schedules[scheduleKey(groupName, name)]
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

func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if key, ok := b.scheduleARNIndex[resourceARN]; ok {
		b.schedules[key].Tags.Merge(kv)

		return nil
	}

	if name, ok := b.scheduleGroupARNIndex[resourceARN]; ok {
		b.scheduleGroups[name].Tags.Merge(kv)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if key, ok := b.scheduleARNIndex[resourceARN]; ok {
		b.schedules[key].Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.scheduleGroupARNIndex[resourceARN]; ok {
		b.scheduleGroups[name].Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if key, ok := b.scheduleARNIndex[resourceARN]; ok {
		return b.schedules[key].Tags.Clone(), nil
	}

	if name, ok := b.scheduleGroupARNIndex[resourceARN]; ok {
		return b.scheduleGroups[name].Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// Reset clears all in-memory state and re-seeds the default schedule group.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, s := range b.schedules {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	for _, g := range b.scheduleGroups {
		if g.Tags != nil {
			g.Tags.Close()
		}
	}

	b.schedules = make(map[string]*Schedule)
	b.scheduleARNIndex = make(map[string]string)
	b.scheduleGroups = make(map[string]*ScheduleGroup)
	b.scheduleGroupARNIndex = make(map[string]string)
	b.seedDefaultGroup()
}

// CreateScheduleGroup creates a new schedule group with the given name and optional tags.
func (b *InMemoryBackend) CreateScheduleGroup(
	name, description string,
	initialTags map[string]string,
) (*ScheduleGroup, error) {
	if err := validateGroupName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateScheduleGroup")
	defer b.mu.Unlock()

	if _, ok := b.scheduleGroups[name]; ok {
		return nil, fmt.Errorf("%w: schedule group %s already exists", ErrAlreadyExists, name)
	}

	groupARN := arn.Build("scheduler", b.region, b.accountID, "schedule-group/"+name)
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
	b.scheduleGroups[name] = g
	b.scheduleGroupARNIndex[groupARN] = name

	return cloneScheduleGroup(g), nil
}

// GetScheduleGroup returns the schedule group with the given name.
func (b *InMemoryBackend) GetScheduleGroup(name string) (*ScheduleGroup, error) {
	b.mu.RLock("GetScheduleGroup")
	defer b.mu.RUnlock()

	g, ok := b.scheduleGroups[name]
	if !ok {
		return nil, fmt.Errorf("%w: schedule group %s not found", ErrNotFound, name)
	}

	return cloneScheduleGroup(g), nil
}

// DeleteScheduleGroup removes the schedule group with the given name.
// The built-in "default" group cannot be deleted.
// All schedules within the group are also deleted.
func (b *InMemoryBackend) DeleteScheduleGroup(name string) error {
	if name == defaultGroupName {
		return fmt.Errorf("%w: cannot delete the default schedule group", ErrValidation)
	}

	b.mu.Lock("DeleteScheduleGroup")
	defer b.mu.Unlock()

	g, ok := b.scheduleGroups[name]
	if !ok {
		return fmt.Errorf("%w: schedule group %s not found", ErrNotFound, name)
	}

	// Cascade-delete all schedules belonging to this group.
	for key, s := range b.schedules {
		if s.GroupName == name {
			delete(b.scheduleARNIndex, s.ARN)
			delete(b.schedules, key)
			if s.Tags != nil {
				s.Tags.Close()
			}
		}
	}

	delete(b.scheduleGroupARNIndex, g.ARN)
	delete(b.scheduleGroups, name)
	g.Tags.Close()

	return nil
}

// ListScheduleGroups returns schedule groups optionally filtered by name prefix.
// When maxResults > 0 and nextToken is non-empty it resumes after the token (last seen name).
// Returns the page of groups and the next continuation token (empty when no more results).
func (b *InMemoryBackend) ListScheduleGroups(namePrefix, nextToken string, maxResults int) ([]*ScheduleGroup, string) {
	b.mu.RLock("ListScheduleGroups")
	defer b.mu.RUnlock()

	list := make([]*ScheduleGroup, 0, len(b.scheduleGroups))

	for _, g := range b.scheduleGroups {
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

	key := scheduleKey(s.GroupName, s.Name)
	b.schedules[key] = s
	b.scheduleARNIndex[s.ARN] = key
}

// AddScheduleGroupInternal inserts a schedule group directly for testing purposes.
// Must only be used from test code.
func (b *InMemoryBackend) AddScheduleGroupInternal(g *ScheduleGroup) {
	b.mu.Lock("AddScheduleGroupInternal")
	defer b.mu.Unlock()

	if g.Tags == nil {
		g.Tags = tags.New("scheduler.schedulegroup." + g.Name + ".tags")
	}

	b.scheduleGroups[g.Name] = g
	b.scheduleGroupARNIndex[g.ARN] = g.Name
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
