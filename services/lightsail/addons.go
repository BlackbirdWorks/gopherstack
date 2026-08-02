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
)

const (
	OperationTypeEnableAddOn = "EnableAddOn"
	opTypeDisableAddOn       = "DisableAddOn"
	opTypeDeleteAutoSnapshot = "DeleteAutoSnapshot"
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

// EnableAddOn enables/updates req on resourceName (Instance or Disk).
func (b *InMemoryBackend) EnableAddOn(resourceName string, req AddOnRequest) ([]Operation, error) {
	b.mu.Lock("EnableAddOn")
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

		i.AddOns = applyAddOnRequestLocked(i.AddOns, req)

		if req.Type == AddOnTypeAutoSnapshot {
			i.AutoSnapshots = append(i.AutoSnapshots, AutoSnapshotDetails{
				Date: nowUTC().Format("20060102"), CreatedAt: nowUTC(), Status: AutoSnapshotStatusSuccess,
			})
		}
	case ResourceTypeDisk:
		d, found := b.disks.Get(resourceName)
		if !found {
			return nil, notFoundError("Disk", resourceName)
		}

		d.AddOns = applyAddOnRequestLocked(d.AddOns, req)

		if req.Type == AddOnTypeAutoSnapshot {
			d.AutoSnapshots = append(d.AutoSnapshots, AutoSnapshotDetails{
				Date: nowUTC().Format("20060102"), CreatedAt: nowUTC(), Status: AutoSnapshotStatusSuccess,
			})
		}
	default:
		return nil, validationError(fmt.Sprintf("resource %s is not an Instance or Disk", resourceName))
	}

	return b.newOperationsLocked(OperationTypeEnableAddOn, kind, []string{resourceName}), nil
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
