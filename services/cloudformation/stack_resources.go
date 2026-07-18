package cloudformation

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// DescribeStackResource returns details for a single resource in a stack.
func (b *InMemoryBackend) DescribeStackResource(
	nameOrID, logicalID string,
) (*StackResource, error) {
	b.mu.RLock("DescribeStackResource")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	res, ok := b.resources[stack.StackID][logicalID]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return res, nil
}

// ListStackResources returns paginated summaries of all resources in a stack.
func (b *InMemoryBackend) ListStackResources(
	nameOrID, nextToken string,
) (page.Page[StackResourceSummary], error) {
	b.mu.RLock("ListStackResources")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return page.Page[StackResourceSummary]{}, ErrStackNotFound
	}

	resMap := b.resources[stack.StackID]
	summaries := make([]StackResourceSummary, 0, len(resMap))

	for _, res := range resMap {
		summaries = append(summaries, StackResourceSummary{
			Timestamp:          res.Timestamp,
			LogicalResourceID:  res.LogicalID,
			PhysicalResourceID: res.PhysicalID,
			ResourceType:       res.Type,
			ResourceStatus:     res.Status,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].LogicalResourceID < summaries[j].LogicalResourceID
	})

	return page.New(summaries, nextToken, 0, cfnDefaultPageSize), nil
}

// DescribeStackResources returns all resources for a stack (or matching a physical resource ID).
func (b *InMemoryBackend) DescribeStackResources(nameOrID string) ([]StackResource, error) {
	b.mu.RLock("DescribeStackResources")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	resMap := b.resources[stack.StackID]
	resources := make([]StackResource, 0, len(resMap))

	for _, res := range resMap {
		resources = append(resources, *res)
	}

	sort.Slice(resources, func(i, j int) bool {
		return resources[i].LogicalID < resources[j].LogicalID
	})

	return resources, nil
}

func (b *InMemoryBackend) SignalResource(stackName, logicalID, uniqueID, status string) error {
	b.mu.Lock("SignalResource")
	defer b.mu.Unlock()
	if _, ok := b.resolveStack(stackName); !ok {
		return fmt.Errorf("%w: %s", ErrStackNotFound, stackName)
	}
	key := stackName + "/" + logicalID
	b.signals[key] = append(b.signals[key], SignalRecord{
		UniqueID: uniqueID,
		Status:   status,
	})

	return nil
}
