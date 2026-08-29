package batch

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	resourceTypeReplenishable    = "REPLENISHABLE"
	resourceTypeNonReplenishable = "NON_REPLENISHABLE"
)

// CreateConsumableResource creates a new consumable resource.
func (b *InMemoryBackend) CreateConsumableResource(
	ctx context.Context,
	name, resourceType string,
	totalQuantity int64,
	tags map[string]string,
) (*ConsumableResource, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateConsumableResource")
	defer b.mu.Unlock()

	if b.consumableResources.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: consumable resource %s already exists", ErrAlreadyExists, name)
	}

	if resourceType == "" {
		resourceType = resourceTypeReplenishable
	}

	if resourceType != resourceTypeReplenishable && resourceType != resourceTypeNonReplenishable {
		return nil, fmt.Errorf("%w: invalid resource type %s", ErrValidation, resourceType)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	crARN := arn.Build("batch", region, b.accountID, "consumable-resource/"+name)

	cr := &ConsumableResource{
		region:                 region,
		ConsumableResourceName: name,
		ConsumableResourceArn:  crARN,
		ResourceType:           resourceType,
		TotalQuantity:          totalQuantity,
		AvailableQuantity:      totalQuantity,
		InUseQuantity:          0,
		CreatedAt:              time.Now().UnixMilli(),
		Tags:                   tagsCloneOrEmpty(tags),
	}
	b.consumableResources.Put(cr)
	b.crsByARN[crARN] = name
	cp := *cr

	return &cp, nil
}

// DeleteConsumableResource removes a consumable resource by name or ARN.
func (b *InMemoryBackend) DeleteConsumableResource(ctx context.Context, nameOrARN string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteConsumableResource")
	defer b.mu.Unlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(region, nameOrARN)
	if !ok {
		return fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	b.consumableResources.Delete(regionKey(region, cr.ConsumableResourceName))
	delete(b.crsByARN, cr.ConsumableResourceArn)

	return nil
}

// DescribeConsumableResource returns details for a consumable resource identified by name or ARN.
func (b *InMemoryBackend) DescribeConsumableResource(
	ctx context.Context,
	nameOrARN string,
) (*ConsumableResource, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeConsumableResource")
	defer b.mu.RUnlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(region, nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	cp := *cr
	cp.Tags = tagsCloneOrEmpty(cr.Tags)

	return &cp, nil
}

// lookupConsumableResourceByNameOrARN returns a consumable resource by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupConsumableResourceByNameOrARN(region, nameOrARN string) (*ConsumableResource, bool) {
	if cr, ok := b.consumableResources.Get(regionKey(region, nameOrARN)); ok {
		return cr, true
	}

	for _, cr := range b.consumableResourcesByRegion.Get(region) {
		if cr.ConsumableResourceArn == nameOrARN {
			return cr, true
		}
	}

	return nil, false
}

// UpdateConsumableResource updates the quantity of a consumable resource.
func (b *InMemoryBackend) UpdateConsumableResource(
	ctx context.Context,
	nameOrARN, operation string,
	quantity int64,
) (*ConsumableResource, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateConsumableResource")
	defer b.mu.Unlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(region, nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	if quantity < 0 {
		return nil, fmt.Errorf("%w: quantity must be non-negative", ErrValidation)
	}

	if operation == "" {
		operation = "SET"
	}

	switch operation {
	case "SET":
		cr.TotalQuantity = quantity
		cr.AvailableQuantity = quantity
	case "ADD":
		cr.TotalQuantity += quantity
		cr.AvailableQuantity += quantity
	case "REMOVE":
		if quantity > cr.TotalQuantity {
			return nil, fmt.Errorf(
				"%w: cannot remove %d from total quantity %d",
				ErrValidation,
				quantity,
				cr.TotalQuantity,
			)
		}

		if quantity > cr.AvailableQuantity {
			return nil, fmt.Errorf(
				"%w: cannot remove %d from available quantity %d (in-use reservations block removal)",
				ErrValidation,
				quantity,
				cr.AvailableQuantity,
			)
		}

		cr.TotalQuantity -= quantity
		cr.AvailableQuantity -= quantity
	default:
		return nil, fmt.Errorf("%w: unsupported operation %s", ErrValidation, operation)
	}

	cp := *cr
	cp.Tags = tagsCloneOrEmpty(cr.Tags)

	return &cp, nil
}

// consumableResourceMatchesFilters reports whether cr satisfies every filter
// entry (AND across entries, OR within one entry's Values). Only
// CONSUMABLE_RESOURCE_NAME is a documented filter name for this op.
func consumableResourceMatchesFilters(cr *ConsumableResource, filters []KeyValueFilter) bool {
	for _, f := range filters {
		if f.Name != "CONSUMABLE_RESOURCE_NAME" {
			return false
		}

		matched := false

		for _, v := range f.Values {
			if filterValueMatches(cr.ConsumableResourceName, v, true) {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}

// ListConsumableResources returns all consumable resources sorted by name,
// optionally filtered by name (CONSUMABLE_RESOURCE_NAME, case-insensitive,
// trailing '*' is a prefix match -- api_op_ListConsumableResources.go).
func (b *InMemoryBackend) ListConsumableResources(ctx context.Context, filters []KeyValueFilter) []*ConsumableResource {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListConsumableResources")
	defer b.mu.RUnlock()

	group := b.consumableResourcesByRegion.Get(region)
	list := make([]*ConsumableResource, 0, len(group))

	for _, cr := range group {
		if !consumableResourceMatchesFilters(cr, filters) {
			continue
		}

		cp := *cr
		cp.Tags = tagsCloneOrEmpty(cr.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ConsumableResourceName < list[j].ConsumableResourceName
	})

	return list
}

// ListJobsByConsumableResource returns jobs that reference the named consumable resource
// via their ConsumableResourceProperties.
func (b *InMemoryBackend) ListJobsByConsumableResource(
	ctx context.Context,
	consumableResource string,
) ([]*Job, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListJobsByConsumableResource")
	defer b.mu.RUnlock()

	list := make([]*Job, 0)

	for _, j := range b.jobsByRegion.Get(region) {
		if jobReferencesConsumableResource(j, consumableResource) {
			cp := *j
			cp.Tags = tagsCloneOrEmpty(j.Tags)
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })

	return list, nil
}

// jobReferencesConsumableResource reports whether a job's ConsumableResourceProperties
// references the named consumable resource.
func jobReferencesConsumableResource(j *Job, consumableResource string) bool {
	if j.ConsumableResourceProperties == nil {
		return false
	}

	for _, crp := range j.ConsumableResourceProperties.ConsumableResourceList {
		if crp.ConsumableResource == consumableResource {
			return true
		}
	}

	return false
}
