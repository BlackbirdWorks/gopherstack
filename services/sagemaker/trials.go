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
	// ErrTrialNotFound is returned when a trial does not exist.
	ErrTrialNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrTrialAlreadyExists is returned when a trial already exists.
	ErrTrialAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// Trial represents a SageMaker Trial.
type Trial struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	TrialName        string            `json:"TrialName"`
	TrialArn         string            `json:"TrialArn"`
	ExperimentName   string            `json:"ExperimentName"`
	DisplayName      string            `json:"DisplayName,omitempty"`
}

func cloneTrial(t *Trial) *Trial {
	cp := *t
	cp.Tags = maps.Clone(t.Tags)

	return &cp
}

// CreateTrial creates a new trial.
func (b *InMemoryBackend) CreateTrial(
	ctx context.Context,
	name, experimentName string,
	tags map[string]string,
) (*Trial, error) {
	b.mu.Lock("CreateTrial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.trialsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: trial %s already exists", ErrTrialAlreadyExists, name)
	}

	trialArn := arn.Build("sagemaker", region, b.accountID, "experiment-trial/"+name)
	now := time.Now()

	t := &Trial{
		TrialName:        name,
		TrialArn:         trialArn,
		ExperimentName:   experimentName,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.trialsStore(region).Put(t)

	return cloneTrial(t), nil
}

// DescribeTrial returns a trial by name.
func (b *InMemoryBackend) DescribeTrial(ctx context.Context, name string) (*Trial, error) {
	b.mu.RLock("DescribeTrial")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	t, ok := b.trialsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: trial %q not found", ErrTrialNotFound, name)
	}

	return cloneTrial(t), nil
}

// ListTrials returns all trials.
func (b *InMemoryBackend) ListTrials(ctx context.Context, nextToken string) ([]*Trial, string) {
	b.mu.RLock("ListTrials")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.trialsStoreRO(region), nextToken, cloneTrial,
		func(a, b *Trial) bool { return a.TrialName < b.TrialName })
}

// DeleteTrial deletes a trial.
func (b *InMemoryBackend) DeleteTrial(ctx context.Context, name string) (*Trial, error) {
	b.mu.Lock("DeleteTrial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.trialsStore(region)

	t, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: trial %q not found", ErrTrialNotFound, name)
	}

	cp := cloneTrial(t)
	store.Delete(name)

	return cp, nil
}

// UpdateTrial mutates DisplayName on a trial.
func (b *InMemoryBackend) UpdateTrial(ctx context.Context, name, displayName string) (*Trial, error) {
	b.mu.Lock("UpdateTrial")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	t, ok := b.trialsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: trial %q not found", ErrTrialNotFound, name)
	}

	if displayName != "" {
		t.DisplayName = displayName
	}
	t.LastModifiedTime = time.Now()

	return cloneTrial(t), nil
}
