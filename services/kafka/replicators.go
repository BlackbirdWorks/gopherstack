package kafka

import (
	"context"
	"fmt"
	"slices"
)

// CreateReplicator creates a new MSK replicator.
func (b *InMemoryBackend) CreateReplicator(
	ctx context.Context,
	name, description, serviceExecutionRoleArn string,
	tags map[string]string,
) (*Replicator, error) {
	if name == "" {
		return nil, fmt.Errorf("replicatorName is required: %w", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateReplicator")
	defer b.mu.Unlock()

	for _, r := range b.replicatorsByRegion.Get(region) {
		if r.ReplicatorName == name {
			return nil, ErrAlreadyExists
		}
	}

	replicatorArn := b.replicatorARN(region, name)
	replicator := &Replicator{
		ReplicatorArn:           replicatorArn,
		ReplicatorName:          name,
		Description:             description,
		ServiceExecutionRoleArn: serviceExecutionRoleArn,
		ReplicatorState:         ReplicatorStateRunning,
		Tags:                    nonNilTagsCopy(tags),
	}
	b.replicators.Put(replicator)

	return cloneReplicator(replicator), nil
}

// DeleteReplicator deletes a replicator by ARN.
func (b *InMemoryBackend) DeleteReplicator(_ context.Context, replicatorArn string) error {
	b.mu.Lock("DeleteReplicator")
	defer b.mu.Unlock()

	if !b.replicators.Delete(replicatorArn) {
		return ErrNotFound
	}

	return nil
}

// DescribeReplicator retrieves a replicator by ARN.
func (b *InMemoryBackend) DescribeReplicator(_ context.Context, replicatorArn string) (*Replicator, error) {
	b.mu.RLock("DescribeReplicator")
	defer b.mu.RUnlock()

	r, ok := b.replicators.Get(replicatorArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneReplicator(r), nil
}

// ListReplicators returns all replicators in the request's region sorted by name.
func (b *InMemoryBackend) ListReplicators(ctx context.Context) []*Replicator {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListReplicators")
	defer b.mu.RUnlock()

	replicators := b.replicatorsByRegion.Get(region)
	out := make([]*Replicator, 0, len(replicators))
	for _, r := range replicators {
		out = append(out, cloneReplicator(r))
	}

	slices.SortFunc(out, func(a, b *Replicator) int {
		if a.ReplicatorName < b.ReplicatorName {
			return -1
		}
		if a.ReplicatorName > b.ReplicatorName {
			return 1
		}

		return 0
	})

	return out
}

// UpdateReplicationInfo updates the replicator description.
func (b *InMemoryBackend) UpdateReplicationInfo(
	_ context.Context,
	replicatorArn, description string,
) (*Replicator, error) {
	b.mu.Lock("UpdateReplicationInfo")
	defer b.mu.Unlock()

	r, ok := b.replicators.Get(replicatorArn)
	if !ok {
		return nil, ErrNotFound
	}

	r.Description = description

	return cloneReplicator(r), nil
}

// AddReplicatorInternal creates a replicator directly for testing purposes.
func (b *InMemoryBackend) AddReplicatorInternal(name string) *Replicator {
	b.mu.Lock("AddReplicatorInternal")
	defer b.mu.Unlock()

	replicatorArn := b.replicatorARN(b.region, name)
	replicator := &Replicator{
		ReplicatorArn:   replicatorArn,
		ReplicatorName:  name,
		ReplicatorState: ReplicatorStateRunning,
		Tags:            make(map[string]string),
	}
	b.replicators.Put(replicator)

	return cloneReplicator(replicator)
}

// cloneReplicator creates a deep copy of a Replicator.
func cloneReplicator(r *Replicator) *Replicator {
	return &Replicator{
		ReplicatorArn:           r.ReplicatorArn,
		ReplicatorName:          r.ReplicatorName,
		Description:             r.Description,
		ServiceExecutionRoleArn: r.ServiceExecutionRoleArn,
		ReplicatorState:         r.ReplicatorState,
		Tags:                    nonNilTagsCopy(r.Tags),
	}
}
