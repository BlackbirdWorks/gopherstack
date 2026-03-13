package pipes

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// stateRunning is the running state for a pipe.
	stateRunning = "RUNNING"
	// stateStopped is the stopped state for a pipe.
	stateStopped = "STOPPED"
)

var (
	// ErrNotFound is returned when a pipe does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a pipe already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
)

// Pipe represents an EventBridge Pipe.
type Pipe struct {
	LastModifiedTime time.Time         `json:"lastModifiedTime"`
	CreationTime     time.Time         `json:"creationTime"`
	Tags             map[string]string `json:"tags,omitempty"`
	Description      string            `json:"description,omitempty"`
	Source           string            `json:"source"`
	Target           string            `json:"target"`
	RoleARN          string            `json:"roleArn"`
	DesiredState     string            `json:"desiredState"`
	CurrentState     string            `json:"currentState"`
	AccountID        string            `json:"accountID"`
	Region           string            `json:"region"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
}

// InMemoryBackend is the in-memory storage for the Pipes service.
type InMemoryBackend struct {
	pipes     map[string]*Pipe
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory Pipes backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipes:     make(map[string]*Pipe),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("pipes"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreatePipe creates a new pipe.
func (b *InMemoryBackend) CreatePipe(
	name, roleARN, source, target, description, desiredState string,
	tags map[string]string,
) (*Pipe, error) {
	b.mu.Lock("CreatePipe")
	defer b.mu.Unlock()

	if _, ok := b.pipes[name]; ok {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, name)
	}

	if desiredState == "" {
		desiredState = stateRunning
	}

	now := time.Now()
	pipeARN := arn.Build("pipes", b.region, b.accountID, "pipe/"+name)
	p := &Pipe{
		Name:             name,
		ARN:              pipeARN,
		RoleARN:          roleARN,
		Source:           source,
		Target:           target,
		Description:      description,
		DesiredState:     desiredState,
		CurrentState:     desiredState,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.pipes[name] = p
	cp := *p

	return &cp, nil
}

// GetPipe returns a pipe by name.
func (b *InMemoryBackend) GetPipe(name string) (*Pipe, error) {
	b.mu.RLock("GetPipe")
	defer b.mu.RUnlock()

	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	cp := *p

	return &cp, nil
}

// ListPipes returns all pipes.
func (b *InMemoryBackend) ListPipes() []*Pipe {
	b.mu.RLock("ListPipes")
	defer b.mu.RUnlock()

	list := make([]*Pipe, 0, len(b.pipes))
	for _, p := range b.pipes {
		cp := *p
		list = append(list, &cp)
	}

	return list
}

// UpdatePipe updates an existing pipe.
func (b *InMemoryBackend) UpdatePipe(name, roleARN, target, description string) (*Pipe, error) {
	b.mu.Lock("UpdatePipe")
	defer b.mu.Unlock()

	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	if roleARN != "" {
		p.RoleARN = roleARN
	}

	if target != "" {
		p.Target = target
	}

	p.Description = description
	p.LastModifiedTime = time.Now()
	cp := *p

	return &cp, nil
}

// DeletePipe deletes a pipe by name.
func (b *InMemoryBackend) DeletePipe(name string) error {
	b.mu.Lock("DeletePipe")
	defer b.mu.Unlock()

	if _, ok := b.pipes[name]; !ok {
		return fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	delete(b.pipes, name)

	return nil
}

// StartPipe transitions a pipe to the RUNNING state.
func (b *InMemoryBackend) StartPipe(name string) (*Pipe, error) {
	b.mu.Lock("StartPipe")
	defer b.mu.Unlock()

	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	p.DesiredState = stateRunning
	p.CurrentState = stateRunning
	p.LastModifiedTime = time.Now()
	cp := *p

	return &cp, nil
}

// StopPipe transitions a pipe to the STOPPED state.
func (b *InMemoryBackend) StopPipe(name string) (*Pipe, error) {
	b.mu.Lock("StopPipe")
	defer b.mu.Unlock()

	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	p.DesiredState = stateStopped
	p.CurrentState = stateStopped
	p.LastModifiedTime = time.Now()
	cp := *p

	return &cp, nil
}

// TagResource adds or updates tags on a pipe identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, p := range b.pipes {
		if p.ARN == resourceARN {
			p.Tags = mergeTags(p.Tags, kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes specified tag keys from a pipe.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, p := range b.pipes {
		if p.ARN == resourceARN {
			for _, k := range keys {
				delete(p.Tags, k)
			}

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a pipe identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, p := range b.pipes {
		if p.ARN == resourceARN {
			result := make(map[string]string, len(p.Tags))
			maps.Copy(result, p.Tags)

			return result, nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)

	maps.Copy(result, incoming)

	return result
}
