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
	// ErrFeatureGroupNotFound is returned when a feature group does not exist.
	ErrFeatureGroupNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrFeatureGroupAlreadyExists is returned when a feature group already exists.
	ErrFeatureGroupAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// FeatureDefinition holds the definition of a single feature.
type FeatureDefinition struct {
	FeatureName string `json:"FeatureName"`
	FeatureType string `json:"FeatureType,omitempty"`
}

// FeatureGroup represents a SageMaker Feature Store feature group.
type FeatureGroup struct {
	CreationTime                time.Time           `json:"CreationTime"`
	Tags                        map[string]string   `json:"Tags,omitempty"`
	FeatureGroupName            string              `json:"FeatureGroupName"`
	FeatureGroupArn             string              `json:"FeatureGroupArn"`
	RecordIdentifierFeatureName string              `json:"RecordIdentifierFeatureName,omitempty"`
	EventTimeFeatureName        string              `json:"EventTimeFeatureName,omitempty"`
	FeatureGroupStatus          string              `json:"FeatureGroupStatus"`
	Description                 string              `json:"Description,omitempty"`
	RoleArn                     string              `json:"RoleArn,omitempty"`
	FeatureDefinitions          []FeatureDefinition `json:"FeatureDefinitions,omitempty"`
}

func cloneFeatureGroup(fg *FeatureGroup) *FeatureGroup {
	cp := *fg
	cp.Tags = maps.Clone(fg.Tags)
	cp.FeatureDefinitions = make([]FeatureDefinition, len(fg.FeatureDefinitions))
	copy(cp.FeatureDefinitions, fg.FeatureDefinitions)

	return &cp
}

// CreateFeatureGroupOptions holds the parameters CreateFeatureGroup accepts.
type CreateFeatureGroupOptions struct {
	Tags                        map[string]string
	FeatureGroupName            string
	RecordIdentifierFeatureName string
	EventTimeFeatureName        string
	Description                 string
	RoleArn                     string
	FeatureDefinitions          []FeatureDefinition
}

// CreateFeatureGroup creates a new feature group.
func (b *InMemoryBackend) CreateFeatureGroup(
	ctx context.Context,
	opts CreateFeatureGroupOptions,
) (*FeatureGroup, error) {
	b.mu.Lock("CreateFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.featureGroupsStore(region).Get(opts.FeatureGroupName); ok {
		return nil, fmt.Errorf(
			"%w: feature group %s already exists",
			ErrFeatureGroupAlreadyExists,
			opts.FeatureGroupName,
		)
	}

	fgArn := arn.Build("sagemaker", region, b.accountID, "feature-group/"+opts.FeatureGroupName)
	storedDefs := make([]FeatureDefinition, len(opts.FeatureDefinitions))
	copy(storedDefs, opts.FeatureDefinitions)

	fg := &FeatureGroup{
		FeatureGroupName:            opts.FeatureGroupName,
		FeatureGroupArn:             fgArn,
		RecordIdentifierFeatureName: opts.RecordIdentifierFeatureName,
		EventTimeFeatureName:        opts.EventTimeFeatureName,
		Description:                 opts.Description,
		RoleArn:                     opts.RoleArn,
		FeatureDefinitions:          storedDefs,
		FeatureGroupStatus:          "Created",
		CreationTime:                time.Now(),
		Tags:                        mergeTags(nil, opts.Tags),
	}
	b.featureGroupsStore(region).Put(fg)

	return cloneFeatureGroup(fg), nil
}

// DescribeFeatureGroup returns a feature group by name.
func (b *InMemoryBackend) DescribeFeatureGroup(ctx context.Context, name string) (*FeatureGroup, error) {
	b.mu.RLock("DescribeFeatureGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	fg, ok := b.featureGroupsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	return cloneFeatureGroup(fg), nil
}

// ListFeatureGroups returns all feature groups.
func (b *InMemoryBackend) ListFeatureGroups(ctx context.Context, nextToken string) ([]*FeatureGroup, string) {
	b.mu.RLock("ListFeatureGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.featureGroupsStoreRO(region), nextToken, cloneFeatureGroup,
		func(a, b *FeatureGroup) bool { return a.FeatureGroupName < b.FeatureGroupName })
}

// DeleteFeatureGroup deletes a feature group.
func (b *InMemoryBackend) DeleteFeatureGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.featureGroupsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	store.Delete(name)

	return nil
}

// UpdateFeatureGroup mutates FeatureDefinitions on an existing feature group.
func (b *InMemoryBackend) UpdateFeatureGroup(
	ctx context.Context,
	name string,
	featureDefinitions []FeatureDefinition,
) (*FeatureGroup, error) {
	b.mu.Lock("UpdateFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	fg, ok := b.featureGroupsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	if len(featureDefinitions) > 0 {
		fg.FeatureDefinitions = append(fg.FeatureDefinitions, featureDefinitions...)
	}

	return cloneFeatureGroup(fg), nil
}
