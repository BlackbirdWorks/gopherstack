package servicediscovery

import (
	"fmt"
	"sort"
)

// GetOperation returns an operation by ID.
func (b *InMemoryBackend) GetOperation(id string) (*Operation, error) {
	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	op, ok := b.operations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: operation %s not found", ErrOperationNotFound, id)
	}

	cp := copyOperation(op)

	return &cp, nil
}

// ListOperations returns all operations sorted by ID, optionally filtered.
func (b *InMemoryBackend) ListOperations(filter ListOperationsFilter) []Operation {
	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	all := b.operations.All()
	result := make([]Operation, 0, len(all))

	for _, op := range all {
		if !filter.Status.matches(op.Status) {
			continue
		}

		if !filter.Type.matches(op.Type) {
			continue
		}

		if !filter.NamespaceID.matches(op.Targets[typeNamespace]) {
			continue
		}

		if !filter.ServiceID.matches(op.Targets[typeService]) {
			continue
		}

		if filter.UpdateDateStart != nil && op.UpdateDate.Before(*filter.UpdateDateStart) {
			continue
		}

		if filter.UpdateDateEnd != nil && op.UpdateDate.After(*filter.UpdateDateEnd) {
			continue
		}

		result = append(result, copyOperation(op))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result
}
