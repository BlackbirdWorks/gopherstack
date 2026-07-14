package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CreatePipe creates a new EventBridge Pipe.
func (b *InMemoryBackend) CreatePipe(
	ctx context.Context, //nolint:revive // existing issue.
	input CreatePipeInput,
) (*Pipe, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}
	if input.SourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrInvalidParameter)
	}
	if input.TargetArn == "" {
		return nil, fmt.Errorf("%w: TargetArn is required", ErrInvalidParameter)
	}
	if input.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrInvalidParameter)
	}

	desiredState := input.DesiredState
	if desiredState == "" {
		desiredState = "RUNNING"
	}

	b.mu.Lock("CreatePipe")
	defer b.mu.Unlock()

	if b.pipesTable().Has(input.Name) {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	pipe := &Pipe{
		Arn:              b.pipeARN(input.Name),
		Name:             input.Name,
		Description:      input.Description,
		DesiredState:     desiredState,
		CurrentState:     "CREATING",
		SourceArn:        input.SourceArn,
		TargetArn:        input.TargetArn,
		RoleArn:          input.RoleArn,
		EnrichmentArn:    input.EnrichmentArn,
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.pipesTable().Put(pipe)

	cp := *pipe
	// Transition CREATING → RUNNING immediately (in-process simulation).
	pipe.CurrentState = desiredState

	return &cp, nil
}

// DeletePipe removes an EventBridge Pipe.
func (b *InMemoryBackend) DeletePipe(ctx context.Context, name string) error { //nolint:revive // existing issue.
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeletePipe")
	defer b.mu.Unlock()

	pipe, exists := b.pipesTable().Get(name)
	if !exists {
		return fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	pipe.CurrentState = "DELETING"
	b.pipesTable().Delete(name)

	return nil
}

// DescribePipe returns a single EventBridge Pipe by name.
func (b *InMemoryBackend) DescribePipe(
	ctx context.Context, //nolint:revive // existing issue.
	name string,
) (*Pipe, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribePipe")
	defer b.mu.RUnlock()

	pipe, exists := b.pipesTable().Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	cp := *pipe

	return &cp, nil
}

// ListPipes returns EventBridge Pipes optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListPipes(
	ctx context.Context, //nolint:revive // existing issue.
	namePrefix, nextToken string,
) ([]Pipe, string, error) {
	b.mu.RLock("ListPipes")
	defer b.mu.RUnlock()

	all := make([]Pipe, 0, b.pipesTable().Len())
	for _, p := range b.pipesTable().All() {
		if namePrefix == "" || strings.HasPrefix(p.Name, namePrefix) {
			all = append(all, *p)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdatePipe updates an existing EventBridge Pipe.
func (b *InMemoryBackend) UpdatePipe(
	ctx context.Context, //nolint:revive // existing issue.
	input UpdatePipeInput,
) (*Pipe, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdatePipe")
	defer b.mu.Unlock()

	pipe, exists := b.pipesTable().Get(input.Name)
	if !exists {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, input.Name)
	}

	if input.Description != "" {
		pipe.Description = input.Description
	}
	if input.RoleArn != "" {
		pipe.RoleArn = input.RoleArn
	}
	if input.TargetArn != "" {
		pipe.TargetArn = input.TargetArn
	}
	if input.EnrichmentArn != "" {
		pipe.EnrichmentArn = input.EnrichmentArn
	}
	if input.DesiredState != "" {
		pipe.DesiredState = input.DesiredState
		pipe.CurrentState = input.DesiredState
	}
	pipe.LastModifiedTime = time.Now()

	cp := *pipe

	return &cp, nil
}
