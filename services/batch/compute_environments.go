package batch

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	fargateSpot = "FARGATE_SPOT"

	maxCENameLength = 128
)

// isValidCEType returns true if the given type is a valid compute environment type (MANAGED or UNMANAGED).
func isValidCEType(t string) bool {
	return t == "MANAGED" || t == "UNMANAGED"
}

// isValidComputeResourcesType returns true if the given type is a valid ComputeResources type.
func isValidComputeResourcesType(t string) bool {
	switch t {
	case "EC2", "SPOT", "FARGATE", fargateSpot:
		return true
	}

	return false
}

// isValidAllocationStrategy returns true if the given allocation strategy is valid.
func isValidAllocationStrategy(s string) bool {
	switch s {
	case "", "BEST_FIT", "BEST_FIT_PROGRESSIVE", "SPOT_CAPACITY_OPTIMIZED", "SPOT_PRICE_CAPACITY_OPTIMIZED":
		return true
	}

	return false
}

// lookupCEByNameOrARN returns a compute environment by name or ARN within region.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupCEByNameOrARN(region, nameOrARN string) (*ComputeEnvironment, bool) {
	if ce, ok := b.computeEnvironments.Get(regionKey(region, nameOrARN)); ok {
		return ce, true
	}

	for _, ce := range b.computeEnvironmentsByRegion.Get(region) {
		if ce.ComputeEnvironmentArn == nameOrARN {
			return ce, true
		}
	}

	return nil, false
}

// validateComputeResourcesForCreate checks the ComputeResources sub-fields
// accepted by CreateComputeEnvironment (type, allocation strategy, and their
// cross-field constraints with ceType).
func validateComputeResourcesForCreate(ceType string, computeResources *ComputeResources) error {
	if computeResources == nil {
		return nil
	}

	if computeResources.Type != "" && !isValidComputeResourcesType(computeResources.Type) {
		return fmt.Errorf(
			"%w: invalid computeResources.type %q (must be EC2, SPOT, FARGATE, or FARGATE_SPOT)",
			ErrValidation, computeResources.Type,
		)
	}

	if !isValidAllocationStrategy(computeResources.AllocationStrategy) {
		return fmt.Errorf(
			"%w: invalid allocationStrategy %q",
			ErrValidation, computeResources.AllocationStrategy,
		)
	}

	if (computeResources.Type == "SPOT" || computeResources.Type == "FARGATE_SPOT") &&
		computeResources.AllocationStrategy == "" {
		return fmt.Errorf(
			"%w: allocationStrategy is required for SPOT and FARGATE_SPOT compute resources",
			ErrValidation,
		)
	}

	if ceType == "UNMANAGED" && (computeResources.Type == "FARGATE" || computeResources.Type == "FARGATE_SPOT") {
		return fmt.Errorf(
			"%w: FARGATE and FARGATE_SPOT are not valid for UNMANAGED compute environments",
			ErrValidation,
		)
	}

	return nil
}

// validateCreateComputeEnvironmentInput validates the name, state, type, and
// compute resources accepted by CreateComputeEnvironment.
func validateCreateComputeEnvironmentInput(
	name, ceType, state string,
	computeResources *ComputeResources,
) error {
	if len(name) == 0 || len(name) > maxCENameLength {
		return fmt.Errorf(
			"%w: computeEnvironmentName must be between 1 and %d characters",
			ErrValidation, maxCENameLength,
		)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if !isValidCEType(ceType) {
		return fmt.Errorf(
			"%w: invalid compute environment type %q (must be MANAGED or UNMANAGED)",
			ErrValidation,
			ceType,
		)
	}

	return validateComputeResourcesForCreate(ceType, computeResources)
}

// cloneEksConfiguration performs a shallow copy of an EksConfiguration struct.
func cloneEksConfiguration(eksConfig *EksConfiguration) *EksConfiguration {
	if eksConfig == nil {
		return nil
	}

	cp := *eksConfig

	return &cp
}

// cloneUpdatePolicy performs a shallow copy of an UpdatePolicy struct.
func cloneUpdatePolicy(updatePolicy *UpdatePolicy) *UpdatePolicy {
	if updatePolicy == nil {
		return nil
	}

	cp := *updatePolicy

	return &cp
}

// CreateComputeEnvironment creates a new compute environment.
func (b *InMemoryBackend) CreateComputeEnvironment(
	ctx context.Context,
	name, ceType, state string,
	tags map[string]string,
	serviceRole string,
	computeResources *ComputeResources,
	eksConfig *EksConfiguration,
	updatePolicy *UpdatePolicy,
) (*ComputeEnvironment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateComputeEnvironment")
	defer b.mu.Unlock()

	if err := validateCreateComputeEnvironmentInput(name, ceType, state, computeResources); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if b.computeEnvironments.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: compute environment %s already exists", ErrAlreadyExists, name)
	}

	ceARN := arn.Build("batch", region, b.accountID, "compute-environment/"+name)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	if state == "" {
		state = stateEnabled
	}

	crCopy := cloneComputeResources(computeResources)
	eksCopy := cloneEksConfiguration(eksConfig)
	upCopy := cloneUpdatePolicy(updatePolicy)

	ce := &ComputeEnvironment{
		region:                 region,
		ComputeEnvironmentName: name,
		ComputeEnvironmentArn:  ceARN,
		Type:                   ceType,
		State:                  state,
		Status:                 statusValid,
		Tags:                   tagsCopy,
		ServiceRole:            serviceRole,
		ComputeResources:       crCopy,
		EksConfiguration:       eksCopy,
		UpdatePolicy:           upCopy,
	}
	b.computeEnvironments.Put(ce)
	b.cesByARN[ceARN] = name
	cp := *ce

	return &cp, nil
}

// cloneComputeResources performs a shallow copy of a ComputeResources struct.
func cloneComputeResources(cr *ComputeResources) *ComputeResources {
	if cr == nil {
		return nil
	}

	clone := *cr
	if cr.Tags != nil {
		clone.Tags = maps.Clone(cr.Tags)
	}

	if cr.Ec2Configuration != nil {
		ec2Config := make([]Ec2Configuration, len(cr.Ec2Configuration))
		copy(ec2Config, cr.Ec2Configuration)
		clone.Ec2Configuration = ec2Config
	}

	return &clone
}

// DescribeComputeEnvironments returns compute environments, optionally filtered by names/ARNs.
// When names is empty, results are paginated via maxResults/nextToken.
//
//nolint:dupl // Boilerplate pagination logic is similar to DescribeJobQueues
func (b *InMemoryBackend) DescribeComputeEnvironments(
	ctx context.Context,
	names []string,
	maxResults int32,
	nextToken string,
) ([]*ComputeEnvironment, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeComputeEnvironments")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		list := make([]*ComputeEnvironment, 0, len(names))

		for _, nameOrARN := range names {
			if ce, ok := b.lookupCEByNameOrARN(region, nameOrARN); ok {
				cp := *ce
				cp.Tags = tagsCloneOrEmpty(ce.Tags)
				list = append(list, &cp)
			}
		}

		return list, ""
	}

	sortedKeys := sortedNames(b.computeEnvironmentsByRegion.Get(region), func(ce *ComputeEnvironment) string {
		return ce.ComputeEnvironmentName
	})

	keys, next := paginateMapKeys(sortedKeys, nextToken, maxResults)
	out := make([]*ComputeEnvironment, 0, len(keys))

	for _, k := range keys {
		ce, _ := b.computeEnvironments.Get(regionKey(region, k))
		cp := *ce
		cp.Tags = tagsCloneOrEmpty(cp.Tags)
		out = append(out, &cp)
	}

	return out, next
}

// UpdateComputeEnvironment updates the state, service role, compute resources, and/or update policy.
func (b *InMemoryBackend) UpdateComputeEnvironment(
	ctx context.Context,
	nameOrARN, state, serviceRole string,
	computeResources *ComputeResources,
	updatePolicy *UpdatePolicy,
) (*ComputeEnvironment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateComputeEnvironment")
	defer b.mu.Unlock()

	ce, ok := b.lookupCEByNameOrARN(region, nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: compute environment %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return nil, fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if state != "" {
		ce.State = state
	}

	if serviceRole != "" {
		ce.ServiceRole = serviceRole
	}

	if computeResources != nil {
		ce.ComputeResources = cloneComputeResources(computeResources)
	}

	if updatePolicy != nil {
		up := *updatePolicy
		ce.UpdatePolicy = &up
	}

	cp := *ce

	return &cp, nil
}

// DeleteComputeEnvironment removes a compute environment.
func (b *InMemoryBackend) DeleteComputeEnvironment(ctx context.Context, nameOrARN string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteComputeEnvironment")
	defer b.mu.Unlock()

	ce, ok := b.lookupCEByNameOrARN(region, nameOrARN)
	if !ok {
		return fmt.Errorf("%w: compute environment %s not found", ErrNotFound, nameOrARN)
	}

	if ce.State != "DISABLED" {
		return fmt.Errorf(
			"%w: compute environment %s must be DISABLED before it can be deleted",
			ErrValidation,
			nameOrARN,
		)
	}

	// O(1) check via reverse index instead of scanning all job queues.
	if len(b.ceToQueues[ce.ComputeEnvironmentName]) > 0 || len(b.ceToQueues[ce.ComputeEnvironmentArn]) > 0 {
		return fmt.Errorf(
			"%w: compute environment %s is referenced by one or more job queues",
			ErrValidation,
			nameOrARN,
		)
	}

	b.computeEnvironments.Delete(regionKey(region, ce.ComputeEnvironmentName))
	delete(b.cesByARN, ce.ComputeEnvironmentArn)

	return nil
}
