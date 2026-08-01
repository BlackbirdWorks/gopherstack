package outposts

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func validateInstancePools(pools []instanceTypeCapacityWire) error {
	if len(pools) == 0 {
		return validationError("InstancePools is required")
	}

	for _, p := range pools {
		if p.InstanceType == "" {
			return validationError("InstancePools[].InstanceType is required")
		}

		if p.Count <= 0 {
			return validationError("InstancePools[].Count must be positive")
		}
	}

	return nil
}

func isValidTaskActionOnBlockingInstances(v string) bool {
	switch v {
	case "", "WAIT_FOR_EVACUATION", "FAIL_TASK":
		return true
	default:
		return false
	}
}

// activeCapacityTaskExistsLocked reports whether outpostID already has a
// REQUESTED capacity task for orderID -- StartCapacityTask's own doc
// comment: only one active capacity task is allowed per (order, Outpost)
// pair at a time. Callers must hold b.mu.
func (b *InMemoryBackend) activeCapacityTaskExistsLocked(outpostID, orderID string) bool {
	for _, t := range b.capacityTasksByOutpost.Get(outpostID) {
		if t.OrderID == orderID && t.Status == CapacityTaskStatusRequested {
			return true
		}
	}

	return false
}

// StartCapacityTask starts a new capacity task against outpostIdentifier's
// Outpost. A non-dry-run task is REQUESTED and completes asynchronously
// (see scheduleCapacityTaskCompletion); a dry run never mutates state and
// completes synchronously, matching StartCapacityTaskInput.DryRun's doc
// comment ("does not make any changes to your plan").
func (b *InMemoryBackend) StartCapacityTask(
	outpostIdentifier string, req *startCapacityTaskRequest,
) (*CapacityTask, error) {
	if err := validateInstancePools(req.InstancePools); err != nil {
		return nil, err
	}

	if !isValidTaskActionOnBlockingInstances(req.TaskActionOnBlockingInstances) {
		return nil, validationError("invalid TaskActionOnBlockingInstances: " + req.TaskActionOnBlockingInstances)
	}

	b.mu.Lock("StartCapacityTask")
	defer b.mu.Unlock()

	o, ok := b.resolveOutpostLocked(outpostIdentifier)
	if !ok {
		return nil, notFoundError(resourceOutpost, outpostIdentifier)
	}

	if req.AssetId != "" {
		a, assetOK := b.assets.Get(req.AssetId)
		if !assetOK || a.OutpostID != o.ID {
			return nil, notFoundError(resourceAsset, req.AssetId)
		}
	}

	if req.OrderId != "" {
		ord, orderOK := b.orders.Get(req.OrderId)
		if !orderOK || ord.OutpostID != o.ID {
			return nil, notFoundError(resourceOrder, req.OrderId)
		}
	}

	if b.activeCapacityTaskExistsLocked(o.ID, req.OrderId) {
		return nil, conflictError("an active capacity task already exists for this Outpost/order")
	}

	now := time.Now().UTC()

	pools := make([]InstanceTypeCapacity, 0, len(req.InstancePools))
	for _, p := range req.InstancePools {
		pools = append(pools, InstanceTypeCapacity(p))
	}

	t := &CapacityTask{
		ID:                            newCapacityTaskID(),
		OutpostID:                     o.ID,
		AssetID:                       req.AssetId,
		OrderID:                       req.OrderId,
		Status:                        CapacityTaskStatusRequested,
		CreationDate:                  now,
		LastModifiedDate:              now,
		DryRun:                        req.DryRun,
		InstancesToExclude:            fromInstancesToExcludeWire(req.InstancesToExclude),
		RequestedInstancePools:        pools,
		TaskActionOnBlockingInstances: req.TaskActionOnBlockingInstances,
	}

	b.capacityTasks.Put(t)

	if req.DryRun {
		t.Status = CapacityTaskStatusCompleted
		t.CompletionDate = now
	} else {
		b.scheduleCapacityTaskCompletion(t.ID)
	}

	return t.clone(), nil
}

// clone returns a deep copy of t, so the returned CapacityTask shares no
// mutable memory with the backend's stored copy. Failed and
// InstancesToExclude are pointers and RequestedInstancePools is a slice;
// cloning them all keeps a caller's copy stable even if the backend later
// mutates the stored task or its pointed-to state.
func (t *CapacityTask) clone() *CapacityTask {
	cp := *t
	cp.Failed = t.Failed.clone()
	cp.InstancesToExclude = t.InstancesToExclude.clone()
	cp.RequestedInstancePools = append([]InstanceTypeCapacity(nil), t.RequestedInstancePools...)

	return &cp
}

// clone returns a deep copy of f, or nil if f is nil.
func (f *CapacityTaskFailure) clone() *CapacityTaskFailure {
	if f == nil {
		return nil
	}

	cp := *f

	return &cp
}

// clone returns a deep copy of e, or nil if e is nil.
func (e *InstancesToExclude) clone() *InstancesToExclude {
	if e == nil {
		return nil
	}

	return &InstancesToExclude{
		AccountIDs: cloneStrs(e.AccountIDs),
		Instances:  cloneStrs(e.Instances),
		Services:   cloneStrs(e.Services),
	}
}

// scheduleCapacityTaskCompletion schedules a single-hop async transition of
// capacity task id from REQUESTED to COMPLETED, at which point it applies
// RequestedInstancePools onto the target Asset's
// ComputeAttributes.InstanceTypeCapacities -- the real capacity-ledger
// mutation GetOutpostInstanceTypes later reads. WAITING_FOR_EVACUATION never
// occurs here: it requires a live blocking EC2 instance, and this backend
// has no cross-service instance-placement data (see
// ListBlockingInstancesForCapacityTask, always empty) -- see PARITY.md.
func (b *InMemoryBackend) scheduleCapacityTaskCompletion(id string) {
	b.work.After("CapacityTaskCompletion", capacityTaskTransitionDelay, func() {
		b.mu.Lock("CapacityTaskCompletion-async")
		defer b.mu.Unlock()

		t, ok := b.capacityTasks.Get(id)
		if !ok || t.Status != CapacityTaskStatusRequested {
			return
		}

		now := time.Now().UTC()
		t.Status = CapacityTaskStatusCompleted
		t.CompletionDate = now
		t.LastModifiedDate = now

		if t.AssetID == "" {
			return
		}

		a, ok := b.assets.Get(t.AssetID)
		if !ok || a.ComputeAttributes == nil {
			return
		}

		for _, pool := range t.RequestedInstancePools {
			mergeInstanceTypeCapacity(a.ComputeAttributes, pool)
		}
	})
}

// mergeInstanceTypeCapacity adds pool's Count onto the matching InstanceType
// entry in ca.InstanceTypeCapacities, appending a new entry if none exists.
func mergeInstanceTypeCapacity(ca *ComputeAttributes, pool InstanceTypeCapacity) {
	for i := range ca.InstanceTypeCapacities {
		if ca.InstanceTypeCapacities[i].InstanceType == pool.InstanceType {
			ca.InstanceTypeCapacities[i].Count += pool.Count

			return
		}
	}

	ca.InstanceTypeCapacities = append(ca.InstanceTypeCapacities, pool)
}

// GetCapacityTask returns a copy of the capacity task identified by
// (outpostIdentifier, capacityTaskID).
func (b *InMemoryBackend) GetCapacityTask(outpostIdentifier, capacityTaskID string) (*CapacityTask, error) {
	b.mu.RLock("GetCapacityTask")
	defer b.mu.RUnlock()

	o, ok := b.resolveOutpostLocked(outpostIdentifier)
	if !ok {
		return nil, notFoundError(resourceOutpost, outpostIdentifier)
	}

	t, ok := b.capacityTasks.Get(capacityTaskID)
	if !ok || t.OutpostID != o.ID {
		return nil, notFoundError(resourceCapacityTask, capacityTaskID)
	}

	return t.clone(), nil
}

// CancelCapacityTask cancels the capacity task identified by
// (outpostIdentifier, capacityTaskID). Rejected (ConflictException) once
// the task has already reached a terminal state. This backend transitions
// directly to CANCELLED rather than pausing at the transient
// CANCELLATION_IN_PROGRESS state -- a documented simplification, since
// cancellation here is synchronous (there is no real hardware-side cleanup
// to wait for) -- see PARITY.md.
func (b *InMemoryBackend) CancelCapacityTask(outpostIdentifier, capacityTaskID string) error {
	b.mu.Lock("CancelCapacityTask")
	defer b.mu.Unlock()

	o, ok := b.resolveOutpostLocked(outpostIdentifier)
	if !ok {
		return notFoundError(resourceOutpost, outpostIdentifier)
	}

	t, ok := b.capacityTasks.Get(capacityTaskID)
	if !ok || t.OutpostID != o.ID {
		return notFoundError(resourceCapacityTask, capacityTaskID)
	}

	if t.Status != CapacityTaskStatusRequested {
		return conflictError("capacity task is not cancellable in status: " + t.Status)
	}

	t.Status = CapacityTaskStatusCancelled
	t.LastModifiedDate = time.Now().UTC()

	return nil
}

// capacityTaskFilter holds ListCapacityTasks' optional filters.
type capacityTaskFilter struct {
	outpostIdentifierFilter string
	statuses                []string
}

// ListCapacityTasks returns a page of capacity task summaries matching f.
func (b *InMemoryBackend) ListCapacityTasks(f capacityTaskFilter, token string, limit int) page.Page[*CapacityTask] {
	b.mu.RLock("ListCapacityTasks")
	defer b.mu.RUnlock()

	var all []*CapacityTask

	if f.outpostIdentifierFilter != "" {
		if o, ok := b.resolveOutpostLocked(f.outpostIdentifierFilter); ok {
			all = append(all, b.capacityTasksByOutpost.Get(o.ID)...)
		}
	} else {
		all = b.capacityTasks.Snapshot()
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	filtered := make([]*CapacityTask, 0, len(all))

	for _, t := range all {
		if len(f.statuses) == 0 || containsStr(f.statuses, t.Status) {
			// Clone before returning: these are the live, backend-owned
			// pointers, and scheduleCapacityTaskCompletion mutates
			// Status/CompletionDate/LastModifiedDate on them in place after
			// this call returns and the lock is released -- see clone's doc
			// comment.
			filtered = append(filtered, t.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit)
}

// ListBlockingInstancesForCapacityTask validates that (outpostIdentifier,
// capacityTaskID) resolves to a real capacity task, then always returns an
// empty result -- this backend has no cross-service EC2-on-Outposts
// placement data (see PARITY.md's "EC2 capacity coupling" gap), so a
// correctly-empty result after real validation is not a stub (see
// parity-principles.md).
func (b *InMemoryBackend) ListBlockingInstancesForCapacityTask(outpostIdentifier, capacityTaskID string) error {
	b.mu.RLock("ListBlockingInstancesForCapacityTask")
	defer b.mu.RUnlock()

	o, ok := b.resolveOutpostLocked(outpostIdentifier)
	if !ok {
		return notFoundError(resourceOutpost, outpostIdentifier)
	}

	t, ok := b.capacityTasks.Get(capacityTaskID)
	if !ok || t.OutpostID != o.ID {
		return notFoundError(resourceCapacityTask, capacityTaskID)
	}

	return nil
}
