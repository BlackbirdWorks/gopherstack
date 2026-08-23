package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
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
	CreationTime       time.Time           `json:"CreationTime"`
	LastModifiedTime   time.Time           `json:"LastModifiedTime"`
	Tags               map[string]string   `json:"Tags,omitempty"`
	MetadataProperties *MetadataProperties `json:"MetadataProperties,omitempty"`
	TrialName          string              `json:"TrialName"`
	TrialArn           string              `json:"TrialArn"`
	ExperimentName     string              `json:"ExperimentName"`
	DisplayName        string              `json:"DisplayName,omitempty"`
}

func cloneTrial(t *Trial) *Trial {
	cp := *t
	cp.Tags = maps.Clone(t.Tags)

	if t.MetadataProperties != nil {
		mp := *t.MetadataProperties
		cp.MetadataProperties = &mp
	}

	return &cp
}

// CreateTrial creates a new trial.
func (b *InMemoryBackend) CreateTrial(
	ctx context.Context,
	name, experimentName, displayName string,
	metadataProperties *MetadataProperties,
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
		TrialName:          name,
		TrialArn:           trialArn,
		ExperimentName:     experimentName,
		DisplayName:        displayName,
		MetadataProperties: metadataProperties,
		CreationTime:       now,
		LastModifiedTime:   now,
		Tags:               mergeTags(nil, tags),
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

// ListTrialsFilter narrows and orders the results of ListTrials
// (api_op_ListTrials.go:34-63). The op's own doc states both real defaults
// explicitly: SortBy is CreationTime, SortOrder is Descending.
type ListTrialsFilter struct {
	CreatedAfter       *time.Time
	CreatedBefore      *time.Time
	ExperimentName     string
	TrialComponentName string
	SortBy             string
	SortOrder          string
	MaxResults         int32
}

// ListTrials returns trials matching filter, sorted by filter.SortBy
// (default CreationTime) / filter.SortOrder (default Descending).
// trialNamesForComponent returns the set of trial names associated with
// trialComponentName, or nil if trialComponentName is empty (meaning the
// TrialComponentName filter is unset and every trial passes).
func (b *InMemoryBackend) trialNamesForComponent(region, trialComponentName string) map[string]bool {
	if trialComponentName == "" {
		return nil
	}

	names := map[string]bool{}

	for _, assoc := range b.trialComponentAssociationsStoreRO(region).All() {
		if assoc.TrialComponentName == trialComponentName {
			names[assoc.TrialName] = true
		}
	}

	return names
}

// trialMatchesFilter reports whether t satisfies filter's ExperimentName/
// allowedTrialNames (from TrialComponentName)/CreatedAfter/CreatedBefore.
func trialMatchesFilter(t *Trial, filter ListTrialsFilter, allowedTrialNames map[string]bool) bool {
	if filter.ExperimentName != "" && t.ExperimentName != filter.ExperimentName {
		return false
	}

	if allowedTrialNames != nil && !allowedTrialNames[t.TrialName] {
		return false
	}

	return timeWindowOK(t.CreationTime, filter.CreatedAfter, filter.CreatedBefore)
}

// lessTrial orders a before b by filter.SortBy (default CreationTime, tie-broken by name).
func lessTrial(a, b *Trial, sortBy string) bool {
	if sortBy == keyGenericName {
		return a.TrialName < b.TrialName
	}

	if a.CreationTime.Equal(b.CreationTime) {
		return a.TrialName < b.TrialName
	}

	return a.CreationTime.Before(b.CreationTime)
}

func (b *InMemoryBackend) ListTrials(
	ctx context.Context,
	nextToken string,
	filter ListTrialsFilter,
) ([]*Trial, string) {
	b.mu.RLock("ListTrials")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	allowedTrialNames := b.trialNamesForComponent(region, filter.TrialComponentName)

	list := make([]*Trial, 0, b.trialsStoreRO(region).Len())

	for _, t := range b.trialsStoreRO(region).All() {
		if trialMatchesFilter(t, filter, allowedTrialNames) {
			list = append(list, cloneTrial(t))
		}
	}

	desc := !strings.EqualFold(filter.SortOrder, "Ascending")
	sort.Slice(list, func(i, k int) bool {
		less := lessTrial(list[i], list[k], filter.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
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
