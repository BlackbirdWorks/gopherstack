package lightsail

// This file backs family Z (3 ops: GetOperation, GetOperations,
// GetOperationsForResource) plus the shared Operation-model bookkeeping
// every other family's mutating op relies on (PARITY.md section 2). An
// Operation is created Started (never a synchronously-fabricated terminal
// status) and this backend schedules a single asyncTransitionDelay hop to a
// terminal status via pkgs/worker, following services/eks's
// scheduleClusterActivation / services/grafana's analogous pattern --
// exactly the "real NotStarted/Started -> terminal transition" this task's
// audit calls out as the single most under-modelable-if-rushed part of this
// service.
//
// Decision (UNCONFIRMED by this SDK module, which lists both Completed and
// Succeeded with no per-op mapping documented, PARITY.md section 2): this
// backend always lands successful mutations on Succeeded, never Completed,
// for a single consistent terminal value across all 161 ops -- a defensible,
// documented simplification rather than a guessed per-op split.

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// newOperationLocked creates and stores a new Started Operation for
// resourceName/resourceType, scheduling its async transition to Succeeded.
// Callers must hold b.mu.
func (b *InMemoryBackend) newOperationLocked(opType, resourceName, resourceType string) *Operation {
	now := nowUTC()
	op := &Operation{
		ID:              newUUID(),
		Type:            opType,
		Status:          OperationStatusStarted,
		ResourceName:    resourceName,
		ResourceType:    resourceType,
		Location:        b.resourceLocationLocked(resourceType),
		CreatedAt:       now,
		StatusChangedAt: now,
		IsTerminal:      false,
	}
	b.operations.Put(op)
	b.scheduleOperationSuccessLocked(op.ID)

	return op.clone()
}

// newOperationsLocked creates one Operation per resourceName, all sharing
// opType -- the shape every batch-style mutating op (CreateInstances,
// DeleteKnownHostKeys, ...) returns per PARITY.md section 2.
func (b *InMemoryBackend) newOperationsLocked(opType, resourceType string, resourceNames []string) []Operation {
	out := make([]Operation, 0, len(resourceNames))

	for _, name := range resourceNames {
		out = append(out, *b.newOperationLocked(opType, name, resourceType))
	}

	return out
}

// resourceLocationLocked returns the Location an Operation for a resource
// of the given kind should report. Distribution always reports the literal
// us-east-1 region (PARITY.md 4.7) and Domain the "global" segment
// (PARITY.md 5.1); every other kind reports this backend's own configured
// region/AZ.
func (b *InMemoryBackend) resourceLocationLocked(resourceType string) ResourceLocation {
	switch resourceType {
	case ResourceTypeDistribution:
		return ResourceLocation{RegionName: distributionRegion}
	case ResourceTypeDomain:
		return ResourceLocation{RegionName: domainGlobalRegion}
	default:
		return ResourceLocation{RegionName: b.region, AvailabilityZone: availabilityZoneA(b.region)}
	}
}

// scheduleOperationSuccessLocked schedules op (by id) to transition from
// Started to Succeeded after asyncTransitionDelay. Always re-fetches the
// live *Operation from the table before mutating (rather than closing over
// the pointer newOperationLocked returned, which is a throwaway clone) --
// this is the deep-copy discipline this task calls out: every read hands
// back a clone, so the timer must mutate the table's own live pointer, not
// a caller-visible copy.
func (b *InMemoryBackend) scheduleOperationSuccessLocked(id string) {
	b.work.After("OperationTransition", asyncTransitionDelay, func() {
		b.mu.Lock("Operation-async-success")
		defer b.mu.Unlock()

		if op, ok := b.operations.Get(id); ok && op.Status == OperationStatusStarted {
			op.Status = OperationStatusSucceeded
			op.StatusChangedAt = nowUTC()
			op.IsTerminal = true
		}
	})
}

// GetOperation returns the Operation with the given id.
func (b *InMemoryBackend) GetOperation(id string) (*Operation, error) {
	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	op, ok := b.operations.Get(id)
	if !ok {
		return nil, notFoundError("Operation", id)
	}

	return op.clone(), nil
}

// GetOperations returns every Operation, paginated.
func (b *InMemoryBackend) GetOperations(token string) (page.Page[*Operation], error) {
	b.mu.RLock("GetOperations")
	defer b.mu.RUnlock()

	all := b.operations.All()
	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.Before(all[j].CreatedAt) })

	out := make([]*Operation, len(all))
	for i, o := range all {
		out[i] = o.clone()
	}

	if err := page.ValidateToken(token); err != nil {
		return page.Page[*Operation]{}, validationError(err.Error())
	}

	return page.New(out, token, 0, defaultPageLimit), nil
}

// GetOperationsForResource returns every Operation recorded against
// resourceName, paginated.
func (b *InMemoryBackend) GetOperationsForResource(resourceName, token string) (page.Page[*Operation], error) {
	b.mu.RLock("GetOperationsForResource")
	defer b.mu.RUnlock()

	all := b.opsByResource.Get(resourceName)
	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.Before(all[j].CreatedAt) })

	out := make([]*Operation, len(all))
	for i, o := range all {
		out[i] = o.clone()
	}

	if err := page.ValidateToken(token); err != nil {
		return page.Page[*Operation]{}, validationError(err.Error())
	}

	return page.New(out, token, 0, defaultPageLimit), nil
}
