package scheduler

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const (
	defaultGroupName         = "default"
	scheduleGroupStateActive = "ACTIVE"
)

type FlexibleTimeWindow struct {
	Mode                   string `json:"mode"`
	MaximumWindowInMinutes int    `json:"maximumWindowInMinutes,omitempty"`
}

type Target struct {
	// Input is an optional custom event payload sent to the target instead of the default
	// scheduler event. When empty the runner constructs a default EventBridge Scheduler event.
	Input   string `json:"input,omitempty"`
	ARN     string `json:"arn"`
	RoleARN string `json:"roleARN"`
}

type Schedule struct {
	Tags               *tags.Tags         `json:"tags,omitempty"`
	Target             Target             `json:"target"`
	Name               string             `json:"name"`
	ARN                string             `json:"arn"`
	ScheduleExpression string             `json:"scheduleExpression"`
	State              string             `json:"state"`
	AccountID          string             `json:"accountID"`
	Region             string             `json:"region"`
	FlexibleTimeWindow FlexibleTimeWindow `json:"flexibleTimeWindow"`
}

// ScheduleGroup represents an EventBridge Scheduler schedule group.
type ScheduleGroup struct {
	CreationDate         time.Time  `json:"creationDate"`
	LastModificationDate time.Time  `json:"lastModificationDate"`
	Tags                 *tags.Tags `json:"tags,omitempty"`
	Name                 string     `json:"name"`
	ARN                  string     `json:"arn"`
	State                string     `json:"state"`
}

type InMemoryBackend struct {
	schedules             map[string]*Schedule
	scheduleARNIndex      map[string]string // ARN → schedule name
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

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateSchedule creates a new schedule.
// The Tags field in the returned Schedule points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) CreateSchedule(
	name, expr string,
	target Target,
	state string,
	ftw FlexibleTimeWindow,
) (*Schedule, error) {
	b.mu.Lock("CreateSchedule")
	defer b.mu.Unlock()

	if _, ok := b.schedules[name]; ok {
		return nil, fmt.Errorf("%w: schedule %s already exists", ErrAlreadyExists, name)
	}

	schedARN := arn.Build("scheduler", b.region, b.accountID, "schedule/default/"+name)
	s := &Schedule{
		Name:               name,
		ARN:                schedARN,
		ScheduleExpression: expr,
		Target:             target,
		State:              state,
		FlexibleTimeWindow: ftw,
		AccountID:          b.accountID,
		Region:             b.region,
		Tags:               tags.New("scheduler.group." + name + ".tags"),
	}
	b.schedules[name] = s
	b.scheduleARNIndex[schedARN] = name
	cp := *s

	return &cp, nil
}

// GetSchedule returns a schedule by name.
// The Tags field in the returned Schedule points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) GetSchedule(name string) (*Schedule, error) {
	b.mu.RLock("GetSchedule")
	defer b.mu.RUnlock()

	s, ok := b.schedules[name]
	if !ok {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}
	cp := *s

	return &cp, nil
}

// ListSchedules returns all schedules.
// The Tags field in each returned Schedule points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) ListSchedules() []*Schedule {
	b.mu.RLock("ListSchedules")
	defer b.mu.RUnlock()

	list := make([]*Schedule, 0, len(b.schedules))
	for _, s := range b.schedules {
		cp := *s
		list = append(list, &cp)
	}

	return list
}

func (b *InMemoryBackend) DeleteSchedule(name string) error {
	b.mu.Lock("DeleteSchedule")
	defer b.mu.Unlock()

	s, ok := b.schedules[name]
	if !ok {
		return fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}

	delete(b.scheduleARNIndex, s.ARN)
	delete(b.schedules, name)
	s.Tags.Close()

	return nil
}

// UpdateSchedule updates an existing schedule.
// The Tags field in the returned Schedule points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) UpdateSchedule(
	name, expr string,
	target Target,
	state string,
	ftw FlexibleTimeWindow,
) (*Schedule, error) {
	b.mu.Lock("UpdateSchedule")
	defer b.mu.Unlock()

	s, ok := b.schedules[name]
	if !ok {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}
	s.ScheduleExpression = expr
	s.Target = target
	s.State = state
	s.FlexibleTimeWindow = ftw
	cp := *s

	return &cp, nil
}

func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if name, ok := b.scheduleARNIndex[resourceARN]; ok {
		b.schedules[name].Tags.Merge(kv)

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

	if name, ok := b.scheduleARNIndex[resourceARN]; ok {
		b.schedules[name].Tags.DeleteKeys(tagKeys)

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

	if name, ok := b.scheduleARNIndex[resourceARN]; ok {
		return b.schedules[name].Tags.Clone(), nil
	}

	if name, ok := b.scheduleGroupARNIndex[resourceARN]; ok {
		return b.scheduleGroups[name].Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// Reset clears all in-memory state. It closes all schedule Tags to release
// Prometheus metrics before discarding the schedules map.
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
func (b *InMemoryBackend) CreateScheduleGroup(name string, initialTags map[string]string) (*ScheduleGroup, error) {
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
		State:                scheduleGroupStateActive,
		CreationDate:         now,
		LastModificationDate: now,
		Tags:                 tags.New("scheduler.schedulegroup." + name + ".tags"),
	}
	g.Tags.Merge(initialTags)
	b.scheduleGroups[name] = g
	b.scheduleGroupARNIndex[groupARN] = name
	cp := *g

	return &cp, nil
}

// GetScheduleGroup returns the schedule group with the given name.
func (b *InMemoryBackend) GetScheduleGroup(name string) (*ScheduleGroup, error) {
	b.mu.RLock("GetScheduleGroup")
	defer b.mu.RUnlock()

	g, ok := b.scheduleGroups[name]
	if !ok {
		return nil, fmt.Errorf("%w: schedule group %s not found", ErrNotFound, name)
	}
	cp := *g

	return &cp, nil
}

// DeleteScheduleGroup removes the schedule group with the given name.
// The built-in "default" group cannot be deleted.
func (b *InMemoryBackend) DeleteScheduleGroup(name string) error {
	b.mu.Lock("DeleteScheduleGroup")
	defer b.mu.Unlock()

	if name == defaultGroupName {
		return fmt.Errorf("%w: cannot delete the default schedule group", ErrValidation)
	}

	g, ok := b.scheduleGroups[name]
	if !ok {
		return fmt.Errorf("%w: schedule group %s not found", ErrNotFound, name)
	}

	delete(b.scheduleGroupARNIndex, g.ARN)
	delete(b.scheduleGroups, name)
	g.Tags.Close()

	return nil
}

// ListScheduleGroups returns all schedule groups.
func (b *InMemoryBackend) ListScheduleGroups() []*ScheduleGroup {
	b.mu.RLock("ListScheduleGroups")
	defer b.mu.RUnlock()

	list := make([]*ScheduleGroup, 0, len(b.scheduleGroups))
	for _, g := range b.scheduleGroups {
		cp := *g
		list = append(list, &cp)
	}

	return list
}
