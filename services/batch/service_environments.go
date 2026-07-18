package batch

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateServiceEnvironment creates a new service environment.
func (b *InMemoryBackend) CreateServiceEnvironment(
	ctx context.Context,
	name, envType, state string,
	tags map[string]string,
) (*ServiceEnvironment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateServiceEnvironment")
	defer b.mu.Unlock()

	if b.serviceEnvironments.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: service environment %s already exists", ErrAlreadyExists, name)
	}

	seARN := arn.Build("batch", region, b.accountID, "service-environment/"+name)

	if state == "" {
		state = stateEnabled
	}

	se := &ServiceEnvironment{
		region:                 region,
		ServiceEnvironmentName: name,
		ServiceEnvironmentArn:  seARN,
		ServiceEnvironmentType: envType,
		State:                  state,
		Status:                 statusValid,
		Tags:                   tagsCloneOrEmpty(tags),
	}
	b.serviceEnvironments.Put(se)
	cp := *se

	return &cp, nil
}

// DeleteServiceEnvironment removes a service environment by name or ARN.
func (b *InMemoryBackend) DeleteServiceEnvironment(ctx context.Context, nameOrARN string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteServiceEnvironment")
	defer b.mu.Unlock()

	se, ok := b.lookupServiceEnvironmentByNameOrARN(region, nameOrARN)
	if !ok {
		return fmt.Errorf("%w: service environment %s not found", ErrNotFound, nameOrARN)
	}

	b.serviceEnvironments.Delete(regionKey(region, se.ServiceEnvironmentName))

	return nil
}

// lookupServiceEnvironmentByNameOrARN returns a service environment by name or ARN within region.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupServiceEnvironmentByNameOrARN(region, nameOrARN string) (*ServiceEnvironment, bool) {
	if se, ok := b.serviceEnvironments.Get(regionKey(region, nameOrARN)); ok {
		return se, true
	}

	for _, se := range b.serviceEnvironmentsByRegion.Get(region) {
		if se.ServiceEnvironmentArn == nameOrARN {
			return se, true
		}
	}

	return nil, false
}

// DescribeServiceEnvironments returns service environments, optionally filtered by names/ARNs.
func (b *InMemoryBackend) DescribeServiceEnvironments(ctx context.Context, names []string) []*ServiceEnvironment {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeServiceEnvironments")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		group := b.serviceEnvironmentsByRegion.Get(region)
		list := make([]*ServiceEnvironment, 0, len(group))

		for _, se := range group {
			cp := *se
			cp.Tags = tagsCloneOrEmpty(se.Tags)
			list = append(list, &cp)
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].ServiceEnvironmentName < list[j].ServiceEnvironmentName
		})

		return list
	}

	list := make([]*ServiceEnvironment, 0, len(names))

	for _, nameOrARN := range names {
		if se, ok := b.lookupServiceEnvironmentByNameOrARN(region, nameOrARN); ok {
			cp := *se
			cp.Tags = tagsCloneOrEmpty(se.Tags)
			list = append(list, &cp)
		}
	}

	return list
}

// UpdateServiceEnvironment updates the state of a service environment.
func (b *InMemoryBackend) UpdateServiceEnvironment(
	ctx context.Context,
	nameOrARN, state string,
) (*ServiceEnvironment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateServiceEnvironment")
	defer b.mu.Unlock()

	se, ok := b.lookupServiceEnvironmentByNameOrARN(region, nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: service environment %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" {
		se.State = state
	}

	cp := *se
	cp.Tags = tagsCloneOrEmpty(se.Tags)

	return &cp, nil
}
