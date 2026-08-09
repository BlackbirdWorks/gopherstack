package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrModelPackageNotFound is returned when a model package does not exist.
	ErrModelPackageNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelPackageGroupNotFound is returned when a model package group does not exist.
	ErrModelPackageGroupNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelPackageGroupHasPackages is returned when deleting a group that still has packages.
	ErrModelPackageGroupHasPackages = awserr.New("ConflictException", awserr.ErrConflict)
)

// ModelPackageBatchResult holds the result of describing a single model package in a batch.
type ModelPackageBatchResult struct {
	ModelPackage *ModelPackage
	ErrorCode    string
	ErrorMessage string
}

// BatchDescribeModelPackage returns descriptions of multiple model packages by ARN.
func (b *InMemoryBackend) BatchDescribeModelPackage(
	ctx context.Context,
	modelPackageArns []string,
) map[string]ModelPackageBatchResult {
	b.mu.RLock("BatchDescribeModelPackage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	mpStore := b.modelPackagesStoreRO(region)

	results := make(map[string]ModelPackageBatchResult, len(modelPackageArns))

	for _, arnStr := range modelPackageArns {
		mp, ok := mpStore.Get(arnStr)
		if !ok {
			results[arnStr] = ModelPackageBatchResult{
				ErrorCode:    "ValidationException",
				ErrorMessage: fmt.Sprintf("model package %q not found", arnStr),
			}

			continue
		}

		results[arnStr] = ModelPackageBatchResult{
			ModelPackage: cloneModelPackage(mp),
		}
	}

	return results
}

// AddModelPackageInternal adds a model package directly for testing.
func (b *InMemoryBackend) AddModelPackageInternal(ctx context.Context, mp *ModelPackage) {
	b.mu.Lock("AddModelPackageInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.modelPackagesStore(region).Put(mp)
	b.modelPackageARNIndexStore(region)[mp.ModelPackageArn] = mp.ModelPackageArn
}

// ---------------------------------------------------------------------------
// ModelPackageGroup
// ---------------------------------------------------------------------------

// ModelPackageGroup represents a SageMaker model package group.
type ModelPackageGroup struct {
	CreationTime                 time.Time         `json:"CreationTime"`
	Tags                         map[string]string `json:"Tags,omitempty"`
	ModelPackageGroupName        string            `json:"ModelPackageGroupName"`
	ModelPackageGroupArn         string            `json:"ModelPackageGroupArn"`
	ModelPackageGroupDescription string            `json:"ModelPackageGroupDescription,omitempty"`
	ModelPackageGroupStatus      string            `json:"ModelPackageGroupStatus"`
	// ResourcePolicy is the resource policy JSON document attached via
	// PutModelPackageGroupPolicy, if any.
	ResourcePolicy string `json:"ResourcePolicy,omitempty"`
}

func cloneModelPackageGroup(g *ModelPackageGroup) *ModelPackageGroup {
	cp := *g
	cp.Tags = maps.Clone(g.Tags)

	return &cp
}

// MarshalJSON emits CreationTime as an AWS awsjson1.1 epoch-seconds number
// rather than Go's default RFC3339 string — this struct is marshaled
// directly by handleDescribeModelPackageGroup.
func (g *ModelPackageGroup) MarshalJSON() ([]byte, error) {
	type alias ModelPackageGroup

	return json.Marshal(struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{
		alias:        (*alias)(g),
		CreationTime: epochSeconds(g.CreationTime),
	})
}

// UnmarshalJSON is the inverse of [ModelPackageGroup.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (g *ModelPackageGroup) UnmarshalJSON(data []byte) error {
	type alias ModelPackageGroup

	aux := struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{alias: (*alias)(g)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	g.CreationTime = timeFromEpochSeconds(aux.CreationTime)

	return nil
}

// CreateModelPackageGroup creates a new model package group.
func (b *InMemoryBackend) CreateModelPackageGroup(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*ModelPackageGroup, error) {
	b.mu.Lock("CreateModelPackageGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", ErrValidation)
	}

	if _, ok := b.modelPackageGroupsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: model package group %q already exists", ErrValidation, name)
	}

	groupARN := arn.Build("sagemaker", region, b.accountID, "model-package-group/"+name)

	g := &ModelPackageGroup{
		ModelPackageGroupName:        name,
		ModelPackageGroupArn:         groupARN,
		ModelPackageGroupDescription: description,
		ModelPackageGroupStatus:      algorithmStatusCompleted,
		Tags:                         mergeTags(nil, tags),
		CreationTime:                 time.Now(),
	}
	b.modelPackageGroupsStore(region).Put(g)
	b.modelPackageGroupARNIndexStore(region)[groupARN] = name

	return cloneModelPackageGroup(g), nil
}

// DescribeModelPackageGroup returns a model package group by name.
func (b *InMemoryBackend) DescribeModelPackageGroup(ctx context.Context, name string) (*ModelPackageGroup, error) {
	b.mu.RLock("DescribeModelPackageGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	return cloneModelPackageGroup(g), nil
}

// DeleteModelPackageGroup removes a model package group by name.
func (b *InMemoryBackend) DeleteModelPackageGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModelPackageGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	// AWS rejects deletion when model packages still exist in the group.
	for _, mp := range b.modelPackagesStore(region).All() {
		if mp.ModelPackageGroupName == name {
			return fmt.Errorf("%w: model package group %q has model packages and cannot be deleted",
				ErrModelPackageGroupHasPackages, name)
		}
	}

	store := b.modelPackageGroupsStore(region)
	store.Delete(name)
	delete(b.modelPackageGroupARNIndexStore(region), g.ModelPackageGroupArn)

	return nil
}

// ListModelPackageGroups returns all model package groups, sorted by name.
func (b *InMemoryBackend) ListModelPackageGroups(ctx context.Context, nextToken string) ([]*ModelPackageGroup, string) {
	b.mu.RLock("ListModelPackageGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.modelPackageGroupsStoreRO(region),
		nextToken,
		cloneModelPackageGroup,
		func(v *ModelPackageGroup) string { return v.ModelPackageGroupName },
	)
}

// ErrModelPackageGroupPolicyNotFound is returned when a model package group
// has no resource policy attached.
var ErrModelPackageGroupPolicyNotFound = awserr.New("ValidationException", awserr.ErrNotFound)

// GetModelPackageGroupPolicy returns the resource policy attached to a model
// package group.
func (b *InMemoryBackend) GetModelPackageGroupPolicy(ctx context.Context, name string) (string, error) {
	b.mu.RLock("GetModelPackageGroupPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStoreRO(region).Get(name)
	if !ok {
		return "", fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	if g.ResourcePolicy == "" {
		return "", fmt.Errorf(
			"%w: model package group %q has no resource policy attached",
			ErrModelPackageGroupPolicyNotFound, name,
		)
	}

	return g.ResourcePolicy, nil
}

// PutModelPackageGroupPolicy attaches (or replaces) the resource policy for a
// model package group.
func (b *InMemoryBackend) PutModelPackageGroupPolicy(
	ctx context.Context,
	name, policy string,
) (*ModelPackageGroup, error) {
	b.mu.Lock("PutModelPackageGroupPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	if policy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy is required", ErrValidation)
	}

	g.ResourcePolicy = policy

	return cloneModelPackageGroup(g), nil
}

// DeleteModelPackageGroupPolicy removes the resource policy from a model
// package group.
func (b *InMemoryBackend) DeleteModelPackageGroupPolicy(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModelPackageGroupPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	g.ResourcePolicy = ""

	return nil
}

// ---------------------------------------------------------------------------
// ModelPackage CRUD (name/ARN dual lookup)
// ---------------------------------------------------------------------------

// CreateModelPackage creates a model package.
func (b *InMemoryBackend) CreateModelPackage(
	ctx context.Context,
	name, groupName, description string,
	tags map[string]string,
) (*ModelPackage, error) {
	b.mu.Lock("CreateModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", ErrValidation)
	}

	mpARN := arn.Build("sagemaker", region, b.accountID, "model-package/"+name)

	if _, ok := b.modelPackagesStore(region).Get(mpARN); ok {
		return nil, fmt.Errorf("%w: model package %q already exists", ErrValidation, name)
	}

	mp := &ModelPackage{
		ModelPackageName:        name,
		ModelPackageArn:         mpARN,
		ModelPackageGroupName:   groupName,
		ModelPackageStatus:      "Completed",
		ModelPackageDescription: description,
		Tags:                    mergeTags(nil, tags),
		CreationTime:            time.Now(),
		ModelPackageStatusDetails: ModelPackageStatusDetails{
			ValidationStatuses: []ModelPackageStatusItem{},
		},
	}
	b.modelPackagesStore(region).Put(mp)
	b.modelPackageARNIndexStore(region)[name] = mpARN

	return cloneModelPackage(mp), nil
}

// DescribeModelPackage returns a model package by name or ARN.
func (b *InMemoryBackend) DescribeModelPackage(ctx context.Context, nameOrArn string) (*ModelPackage, error) {
	b.mu.RLock("DescribeModelPackage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	// Try direct ARN lookup first.
	if mp, ok := b.modelPackagesStoreRO(region).Get(nameOrArn); ok {
		return cloneModelPackage(mp), nil
	}

	// Try name → ARN index.
	if arnStr, ok := b.modelPackageARNIndexStoreRO(region)[nameOrArn]; ok {
		if mp, found := b.modelPackagesStoreRO(region).Get(arnStr); found {
			return cloneModelPackage(mp), nil
		}
	}

	return nil, fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
}

// DeleteModelPackage removes a model package by name or ARN.
func (b *InMemoryBackend) DeleteModelPackage(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	arnStr := nameOrArn
	if v, ok := b.modelPackageARNIndexStore(region)[nameOrArn]; ok {
		arnStr = v
	}

	if _, ok := b.modelPackagesStore(region).Get(arnStr); !ok {
		return fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
	}

	mp := tableGet(b.modelPackagesStore(region), arnStr)
	arnIdxStore := b.modelPackageARNIndexStore(region)
	delete(arnIdxStore, mp.ModelPackageName)
	mpStore := b.modelPackagesStore(region)
	mpStore.Delete(arnStr)

	return nil
}

// ListModelPackages returns model packages, optionally filtered by group name.
func (b *InMemoryBackend) ListModelPackages(
	ctx context.Context,
	groupName, nextToken string,
) ([]*ModelPackage, string) {
	b.mu.RLock("ListModelPackages")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var arns []string
	for _, mp := range b.modelPackagesStoreRO(region).All() {
		if groupName == "" || mp.ModelPackageGroupName == groupName {
			arns = append(arns, mp.ModelPackageArn)
		}
	}

	sort.Strings(arns)

	start := 0
	if nextToken != "" {
		for i, k := range arns {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(arns))

	out := make([]*ModelPackage, 0, end-start)
	for _, k := range arns[start:end] {
		out = append(out, cloneModelPackage(tableGet(b.modelPackagesStoreRO(region), k)))
	}

	next := ""
	if end < len(arns) {
		next = arns[end]
	}

	return out, next
}

// UpdateModelPackage updates the approval status of a model package (by name or ARN).
func (b *InMemoryBackend) UpdateModelPackage(
	ctx context.Context,
	nameOrArn, approvalStatus string,
) (*ModelPackage, error) {
	b.mu.Lock("UpdateModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	arnStr := nameOrArn
	if v, ok := b.modelPackageARNIndexStore(region)[nameOrArn]; ok {
		arnStr = v
	}

	mp, ok := b.modelPackagesStore(region).Get(arnStr)
	if !ok {
		return nil, fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
	}

	if approvalStatus != "" {
		mp.ModelApprovalStatus = approvalStatus
	}

	return cloneModelPackage(mp), nil
}
