package lightsail

// This file backs family F (4 ops: GetAutoSnapshots, DeleteAutoSnapshot,
// EnableAddOn, DisableAddOn). AddOnType has exactly 2 values (AutoSnapshot,
// StopInstanceOnIdle, PARITY.md family F) and every add-on op targets a
// ResourceName that may be either an Instance or a Disk -- resolved via the
// global activeNames index (store.go) rather than two near-duplicate op
// implementations.

import (
	"fmt"
	"sort"
	"time"
)

const (
	OperationTypeEnableAddOn = "EnableAddOn"
	opTypeDisableAddOn       = "DisableAddOn"
	opTypeDeleteAutoSnapshot = "DeleteAutoSnapshot"

	// autoSnapshotCadence is AWS's real once-daily AutoSnapshot interval.
	autoSnapshotCadence = 24 * time.Hour

	// autoSnapshotRetentionCount is AWS's documented retention depth: the
	// latest 7 daily snapshots are kept before the oldest is replaced.
	autoSnapshotRetentionCount = 7
)

// applyAddOnRequestLocked returns addOns with req applied (added, or
// replacing an existing entry of the same Type). Callers must hold b.mu.
func applyAddOnRequestLocked(addOns []AddOn, req AddOnRequest) []AddOn {
	entry := AddOn{Name: req.Type, Status: "Enabled"}

	switch req.Type {
	case AddOnTypeAutoSnapshot:
		entry.SnapshotTimeOfDay = req.AutoSnapshotTimeOfDay
		entry.NextSnapshotTimeOfDay = req.AutoSnapshotTimeOfDay
	case AddOnTypeStopInstanceOnIdle:
		entry.Duration = req.StopInstanceOnIdleDuration
		entry.Threshold = req.StopInstanceOnIdleThreshold
	}

	out := make([]AddOn, 0, len(addOns)+1)
	replaced := false

	for _, a := range addOns {
		if a.Name == req.Type {
			out = append(out, entry)
			replaced = true

			continue
		}

		out = append(out, a)
	}

	if !replaced {
		out = append(out, entry)
	}

	return out
}

// removeAddOnLocked returns addOns with any entry named addOnType removed.
func removeAddOnLocked(addOns []AddOn, addOnType string) []AddOn {
	out := make([]AddOn, 0, len(addOns))

	for _, a := range addOns {
		if a.Name != addOnType {
			out = append(out, a)
		}
	}

	return out
}

// GetAutoSnapshots returns resourceName's AutoSnapshot history and resource
// kind (Instance or Disk). resourceType.
func (b *InMemoryBackend) GetAutoSnapshots(resourceName string) ([]AutoSnapshotDetails, string, error) {
	b.mu.RLock("GetAutoSnapshots")
	defer b.mu.RUnlock()

	kind, ok := b.activeNames[resourceName]
	if !ok {
		return nil, "", notFoundError("resource", resourceName)
	}

	switch kind {
	case ResourceTypeInstance:
		i, found := b.instances.Get(resourceName)
		if !found {
			return nil, "", notFoundError("Instance", resourceName)
		}

		return cloneAutoSnapshots(i.AutoSnapshots), kind, nil
	case ResourceTypeDisk:
		d, found := b.disks.Get(resourceName)
		if !found {
			return nil, "", notFoundError("Disk", resourceName)
		}

		return cloneAutoSnapshots(d.AutoSnapshots), kind, nil
	default:
		return nil, "", validationError(fmt.Sprintf("resource %s is not an Instance or Disk", resourceName))
	}
}

// DeleteAutoSnapshot removes the dated AutoSnapshot entry for resourceName.
func (b *InMemoryBackend) DeleteAutoSnapshot(resourceName, date string) ([]Operation, error) {
	b.mu.Lock("DeleteAutoSnapshot")
	defer b.mu.Unlock()

	kind, ok := b.activeNames[resourceName]
	if !ok {
		return nil, notFoundError("resource", resourceName)
	}

	switch kind {
	case ResourceTypeInstance:
		i, found := b.instances.Get(resourceName)
		if !found {
			return nil, notFoundError("Instance", resourceName)
		}

		i.AutoSnapshots = deleteAutoSnapshotByDate(i.AutoSnapshots, date)
	case ResourceTypeDisk:
		d, found := b.disks.Get(resourceName)
		if !found {
			return nil, notFoundError("Disk", resourceName)
		}

		d.AutoSnapshots = deleteAutoSnapshotByDate(d.AutoSnapshots, date)
	default:
		return nil, validationError(fmt.Sprintf("resource %s is not an Instance or Disk", resourceName))
	}

	return b.newOperationsLocked(opTypeDeleteAutoSnapshot, kind, []string{resourceName}), nil
}

func deleteAutoSnapshotByDate(in []AutoSnapshotDetails, date string) []AutoSnapshotDetails {
	out := make([]AutoSnapshotDetails, 0, len(in))

	for _, s := range in {
		if s.Date != date {
			out = append(out, s)
		}
	}

	return out
}

// hasAutoSnapshotAddOn reports whether addOns already contains an
// AutoSnapshot entry.
func hasAutoSnapshotAddOn(addOns []AddOn) bool {
	for _, a := range addOns {
		if a.Name == AddOnTypeAutoSnapshot {
			return true
		}
	}

	return false
}

// EnableAddOn enables/updates req on resourceName (Instance or Disk). First
// enabling AutoSnapshot also starts a real recurring daily cadence.
func (b *InMemoryBackend) EnableAddOn(resourceName string, req AddOnRequest) ([]Operation, error) {
	b.mu.Lock("EnableAddOn")
	defer b.mu.Unlock()

	kind, ok := b.activeNames[resourceName]
	if !ok {
		return nil, notFoundError("resource", resourceName)
	}

	var firstAutoSnapshot bool

	switch kind {
	case ResourceTypeInstance:
		i, found := b.instances.Get(resourceName)
		if !found {
			return nil, notFoundError("Instance", resourceName)
		}

		firstAutoSnapshot = req.Type == AddOnTypeAutoSnapshot && !hasAutoSnapshotAddOn(i.AddOns)
		i.AddOns = applyAddOnRequestLocked(i.AddOns, req)
	case ResourceTypeDisk:
		d, found := b.disks.Get(resourceName)
		if !found {
			return nil, notFoundError("Disk", resourceName)
		}

		firstAutoSnapshot = req.Type == AddOnTypeAutoSnapshot && !hasAutoSnapshotAddOn(d.AddOns)
		d.AddOns = applyAddOnRequestLocked(d.AddOns, req)
	default:
		return nil, validationError(fmt.Sprintf("resource %s is not an Instance or Disk", resourceName))
	}

	if req.Type == AddOnTypeAutoSnapshot {
		b.appendAutoSnapshotLocked(kind, resourceName)

		if firstAutoSnapshot {
			b.scheduleAutoSnapshotCadenceLocked(kind, resourceName)
		}
	}

	return b.newOperationsLocked(OperationTypeEnableAddOn, kind, []string{resourceName}), nil
}

// appendAutoSnapshotLocked records one dated entry for resourceName,
// evicting the oldest past autoSnapshotRetentionCount. Callers hold b.mu.
func (b *InMemoryBackend) appendAutoSnapshotLocked(kind, resourceName string) {
	entry := AutoSnapshotDetails{
		Date: nowUTC().Format("20060102"), CreatedAt: nowUTC(), Status: AutoSnapshotStatusSuccess,
	}

	switch kind {
	case ResourceTypeInstance:
		if i, found := b.instances.Get(resourceName); found {
			i.AutoSnapshots = trimAutoSnapshots(append(i.AutoSnapshots, entry))
		}
	case ResourceTypeDisk:
		if d, found := b.disks.Get(resourceName); found {
			d.AutoSnapshots = trimAutoSnapshots(append(d.AutoSnapshots, entry))
		}
	}
}

// trimAutoSnapshots keeps only the newest autoSnapshotRetentionCount entries.
func trimAutoSnapshots(in []AutoSnapshotDetails) []AutoSnapshotDetails {
	if len(in) <= autoSnapshotRetentionCount {
		return in
	}

	return in[len(in)-autoSnapshotRetentionCount:]
}

// autoSnapshotEnabledLocked reports whether resourceName still exists and
// still has the AutoSnapshot add-on enabled. Callers must hold b.mu.
func (b *InMemoryBackend) autoSnapshotEnabledLocked(kind, resourceName string) bool {
	switch kind {
	case ResourceTypeInstance:
		i, found := b.instances.Get(resourceName)

		return found && hasAutoSnapshotAddOn(i.AddOns)
	case ResourceTypeDisk:
		d, found := b.disks.Get(resourceName)

		return found && hasAutoSnapshotAddOn(d.AddOns)
	default:
		return false
	}
}

// scheduleAutoSnapshotCadenceLocked reschedules itself every
// autoSnapshotCadence until disabled. Callers must hold b.mu.
func (b *InMemoryBackend) scheduleAutoSnapshotCadenceLocked(kind, resourceName string) {
	b.work.After("AutoSnapshotCadence", autoSnapshotCadence, func() {
		b.mu.Lock("AutoSnapshotCadence")
		defer b.mu.Unlock()

		if !b.autoSnapshotEnabledLocked(kind, resourceName) {
			return
		}

		b.appendAutoSnapshotLocked(kind, resourceName)
		b.scheduleAutoSnapshotCadenceLocked(kind, resourceName)
	})
}

// DisableAddOn removes addOnType from resourceName (Instance or Disk).
func (b *InMemoryBackend) DisableAddOn(resourceName, addOnType string) ([]Operation, error) {
	b.mu.Lock("DisableAddOn")
	defer b.mu.Unlock()

	kind, ok := b.activeNames[resourceName]
	if !ok {
		return nil, notFoundError("resource", resourceName)
	}

	switch kind {
	case ResourceTypeInstance:
		i, found := b.instances.Get(resourceName)
		if !found {
			return nil, notFoundError("Instance", resourceName)
		}

		i.AddOns = removeAddOnLocked(i.AddOns, addOnType)
	case ResourceTypeDisk:
		d, found := b.disks.Get(resourceName)
		if !found {
			return nil, notFoundError("Disk", resourceName)
		}

		d.AddOns = removeAddOnLocked(d.AddOns, addOnType)
	default:
		return nil, validationError(fmt.Sprintf("resource %s is not an Instance or Disk", resourceName))
	}

	return b.newOperationsLocked(opTypeDisableAddOn, kind, []string{resourceName}), nil
}

// sortAutoSnapshots sorts by Date for deterministic output.
func sortAutoSnapshots(in []AutoSnapshotDetails) {
	sort.Slice(in, func(i, j int) bool { return in[i].Date < in[j].Date })
}
