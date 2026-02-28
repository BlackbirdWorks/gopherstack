package scheduler

import (
	"fmt"
	"maps"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
)

type FlexibleTimeWindow struct {
	Mode                   string
	MaximumWindowInMinutes int
}

type Target struct {
	ARN     string
	RoleARN string
}

type Schedule struct {
	Tags               map[string]string
	Target             Target
	Name               string
	ARN                string
	ScheduleExpression string
	State              string
	AccountID          string
	Region             string
	FlexibleTimeWindow FlexibleTimeWindow
}

type InMemoryBackend struct {
	schedules map[string]*Schedule
	accountID string
	region    string
	mu        sync.RWMutex
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		schedules: make(map[string]*Schedule),
		accountID: accountID,
		region:    region,
	}
}

func (b *InMemoryBackend) CreateSchedule(
	name, expr string,
	target Target,
	state string,
	ftw FlexibleTimeWindow,
) (*Schedule, error) {
	b.mu.Lock()
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
		Tags:               make(map[string]string),
	}
	b.schedules[name] = s
	cp := *s
	cp.Tags = make(map[string]string)
	maps.Copy(cp.Tags, s.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) GetSchedule(name string) (*Schedule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s, ok := b.schedules[name]
	if !ok {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}
	cp := *s
	cp.Tags = make(map[string]string)
	maps.Copy(cp.Tags, s.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) ListSchedules() []*Schedule {
	b.mu.RLock()
	defer b.mu.RUnlock()

	list := make([]*Schedule, 0, len(b.schedules))
	for _, s := range b.schedules {
		cp := *s
		cp.Tags = make(map[string]string)
		maps.Copy(cp.Tags, s.Tags)
		list = append(list, &cp)
	}

	return list
}

func (b *InMemoryBackend) DeleteSchedule(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.schedules[name]; !ok {
		return fmt.Errorf("%w: schedule %s not found", ErrNotFound, name)
	}
	delete(b.schedules, name)

	return nil
}

func (b *InMemoryBackend) UpdateSchedule(
	name, expr string,
	target Target,
	state string,
	ftw FlexibleTimeWindow,
) (*Schedule, error) {
	b.mu.Lock()
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
	cp.Tags = make(map[string]string)
	maps.Copy(cp.Tags, s.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, s := range b.schedules {
		if s.ARN == arn {
			maps.Copy(s.Tags, tags)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, arn)
}

func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, s := range b.schedules {
		if s.ARN == arn {
			tags := make(map[string]string, len(s.Tags))
			maps.Copy(tags, s.Tags)

			return tags, nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, arn)
}
