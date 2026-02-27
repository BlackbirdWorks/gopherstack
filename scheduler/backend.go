package scheduler

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNotFound      = errors.New("ResourceNotFoundException")
	ErrAlreadyExists = errors.New("ConflictException")
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
	Name               string
	ARN                string
	ScheduleExpression string
	Target             Target
	State              string
	FlexibleTimeWindow FlexibleTimeWindow
	AccountID          string
	Region             string
	Tags               map[string]string
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

func (b *InMemoryBackend) CreateSchedule(name, expr string, target Target, state string, ftw FlexibleTimeWindow) (*Schedule, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.schedules[name]; ok {
		return nil, fmt.Errorf("%w: schedule %s already exists", ErrAlreadyExists, name)
	}

	arn := fmt.Sprintf("arn:aws:scheduler:%s:%s:schedule/default/%s", b.region, b.accountID, name)
	s := &Schedule{
		Name:               name,
		ARN:                arn,
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
	for k, v := range s.Tags {
		cp.Tags[k] = v
	}
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
	for k, v := range s.Tags {
		cp.Tags[k] = v
	}
	return &cp, nil
}

func (b *InMemoryBackend) ListSchedules() []*Schedule {
	b.mu.RLock()
	defer b.mu.RUnlock()

	list := make([]*Schedule, 0, len(b.schedules))
	for _, s := range b.schedules {
		cp := *s
		cp.Tags = make(map[string]string)
		for k, v := range s.Tags {
			cp.Tags[k] = v
		}
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

func (b *InMemoryBackend) UpdateSchedule(name, expr string, target Target, state string, ftw FlexibleTimeWindow) (*Schedule, error) {
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
	for k, v := range s.Tags {
		cp.Tags[k] = v
	}
	return &cp, nil
}

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, s := range b.schedules {
		if s.ARN == arn {
			for k, v := range tags {
				s.Tags[k] = v
			}
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
			for k, v := range s.Tags {
				tags[k] = v
			}
			return tags, nil
		}
	}
	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, arn)
}
