package scheduler

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
)

type FlexibleTimeWindow struct {
	Mode                   string `json:"mode"`
	MaximumWindowInMinutes int    `json:"maximumWindowInMinutes,omitempty"`
}

type Target struct {
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

type InMemoryBackend struct {
	schedules        map[string]*Schedule
	scheduleARNIndex map[string]string // ARN → schedule name
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		schedules:        make(map[string]*Schedule),
		scheduleARNIndex: make(map[string]string),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("scheduler"),
	}
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

	name, ok := b.scheduleARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	b.schedules[name].Tags.Merge(kv)

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name, ok := b.scheduleARNIndex[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return b.schedules[name].Tags.Clone(), nil
}
