package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrExperimentNotFound is returned when an experiment does not exist.
	ErrExperimentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrExperimentAlreadyExists is returned when an experiment already exists.
	ErrExperimentAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// Experiment represents a SageMaker Experiment.
type Experiment struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ExperimentName   string            `json:"ExperimentName"`
	ExperimentArn    string            `json:"ExperimentArn"`
	DisplayName      string            `json:"DisplayName,omitempty"`
	Description      string            `json:"Description,omitempty"`
}

func cloneExperiment(e *Experiment) *Experiment {
	cp := *e
	cp.Tags = maps.Clone(e.Tags)

	return &cp
}

// CreateExperiment creates a new experiment.
func (b *InMemoryBackend) CreateExperiment(
	ctx context.Context,
	name, displayName, description string,
	tags map[string]string,
) (*Experiment, error) {
	b.mu.Lock("CreateExperiment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.experimentsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: experiment %s already exists", ErrExperimentAlreadyExists, name)
	}

	expArn := arn.Build("sagemaker", region, b.accountID, "experiment/"+name)
	now := time.Now()

	e := &Experiment{
		ExperimentName:   name,
		ExperimentArn:    expArn,
		DisplayName:      displayName,
		Description:      description,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.experimentsStore(region).Put(e)

	return cloneExperiment(e), nil
}

// DescribeExperiment returns an experiment by name.
func (b *InMemoryBackend) DescribeExperiment(ctx context.Context, name string) (*Experiment, error) {
	b.mu.RLock("DescribeExperiment")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	e, ok := b.experimentsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: experiment %q not found", ErrExperimentNotFound, name)
	}

	return cloneExperiment(e), nil
}

// ListExperiments returns all experiments.
func (b *InMemoryBackend) ListExperiments(ctx context.Context, nextToken string) ([]*Experiment, string) {
	b.mu.RLock("ListExperiments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.experimentsStoreRO(region), nextToken, cloneExperiment,
		func(a, b *Experiment) bool { return a.ExperimentName < b.ExperimentName })
}

// DeleteExperiment deletes an experiment.
func (b *InMemoryBackend) DeleteExperiment(ctx context.Context, name string) (*Experiment, error) {
	b.mu.Lock("DeleteExperiment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.experimentsStore(region)

	e, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: experiment %q not found", ErrExperimentNotFound, name)
	}

	cp := cloneExperiment(e)
	store.Delete(name)

	return cp, nil
}

// UpdateExperiment mutates DisplayName and Description on an experiment.
func (b *InMemoryBackend) UpdateExperiment(
	ctx context.Context,
	name, displayName, description string,
) (*Experiment, error) {
	b.mu.Lock("UpdateExperiment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	e, ok := b.experimentsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: experiment %q not found", ErrExperimentNotFound, name)
	}

	if displayName != "" {
		e.DisplayName = displayName
	}
	if description != "" {
		e.Description = description
	}
	e.LastModifiedTime = time.Now()

	return cloneExperiment(e), nil
}
