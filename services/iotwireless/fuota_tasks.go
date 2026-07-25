package iotwireless

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func fuotaTaskARN(region, accountID, id string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("FuotaTask/%s", id))
}

// copyFuotaTask returns a shallow copy of ft with independent Tags and
// LoRaWAN maps.
func copyFuotaTask(ft *FuotaTask) *FuotaTask {
	cp := *ft
	cp.Tags = make(map[string]string, len(ft.Tags))
	maps.Copy(cp.Tags, ft.Tags)
	cp.LoRaWAN = copyAnyMap(ft.LoRaWAN)

	return &cp
}

// CreateFuotaTask creates a new FUOTA task.
func (b *InMemoryBackend) CreateFuotaTask(
	accountID, region, name, description, firmwareUpdateImage, firmwareUpdateRole, descriptor string,
	fragmentIntervalMS, fragmentSizeBytes, redundancyPercent int32,
	loRaWAN map[string]any,
	tags map[string]string,
) (*FuotaTask, error) {
	b.mu.Lock("CreateFuotaTask")
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := fuotaTaskARN(region, accountID, id)

	ft := &FuotaTask{
		ID:                  id,
		ARN:                 arn,
		Name:                name,
		Description:         description,
		FirmwareUpdateImage: firmwareUpdateImage,
		FirmwareUpdateRole:  firmwareUpdateRole,
		Descriptor:          descriptor,
		FragmentIntervalMS:  fragmentIntervalMS,
		FragmentSizeBytes:   fragmentSizeBytes,
		RedundancyPercent:   redundancyPercent,
		LoRaWAN:             loRaWAN,
		Status:              "Pending",
		Tags:                newTagsCopy(tags),
		CreatedAt:           time.Now(),
		AccountID:           accountID,
		Region:              region,
	}

	b.fuotaTasks.Put(ft)
	b.storeResourceTagsLocked(arn, tags)

	return copyFuotaTask(ft), nil
}

// GetFuotaTask returns a FUOTA task by ID.
func (b *InMemoryBackend) GetFuotaTask(accountID, region, id string) (*FuotaTask, error) {
	b.mu.RLock("GetFuotaTask")
	defer b.mu.RUnlock()

	ft, ok := b.fuotaTasks.Get(compositeKey(accountID, region, id))
	if !ok {
		return nil, ErrFuotaTaskNotFound
	}

	return copyFuotaTask(ft), nil
}

// ListFuotaTasks returns all FUOTA tasks for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListFuotaTasks(accountID, region string) []*FuotaTask {
	b.mu.RLock("ListFuotaTasks")
	defer b.mu.RUnlock()

	all := b.fuotaTasks.All()
	result := make([]*FuotaTask, 0, len(all))

	for _, ft := range all {
		if ft.AccountID == accountID && ft.Region == region {
			result = append(result, copyFuotaTask(ft))
		}
	}

	slices.SortFunc(result, func(a, b *FuotaTask) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteFuotaTask deletes a FUOTA task by ID.
func (b *InMemoryBackend) DeleteFuotaTask(accountID, region, id string) error {
	b.mu.Lock("DeleteFuotaTask")
	defer b.mu.Unlock()

	key := compositeKey(accountID, region, id)

	ft, ok := b.fuotaTasks.Get(key)
	if !ok {
		return ErrFuotaTaskNotFound
	}

	delete(b.resourceTags, ft.ARN)
	delete(b.fuotaTaskDevices, id)
	delete(b.fuotaTaskMulticast, id)
	b.fuotaTasks.Delete(key)

	return nil
}

// UpdateFuotaTask updates mutable fields on an existing FUOTA task.
func (b *InMemoryBackend) UpdateFuotaTask(accountID, region, id, name, description string) error {
	b.mu.Lock("UpdateFuotaTask")
	defer b.mu.Unlock()

	ft, ok := b.fuotaTasks.Get(compositeKey(accountID, region, id))
	if !ok {
		return ErrFuotaTaskNotFound
	}

	if name != "" {
		ft.Name = name
	}

	ft.Description = description

	return nil
}

// AssociateMulticastGroupWithFuotaTask records the association of a
// multicast group with a FUOTA task. A FUOTA task can have several
// associated multicast groups (ListMulticastGroupsByFuotaTask returns a
// list), so this adds to a per-task set rather than overwriting a single slot.
func (b *InMemoryBackend) AssociateMulticastGroupWithFuotaTask(fuotaTaskID, multicastGroupID string) error {
	b.mu.Lock("AssociateMulticastGroupWithFuotaTask")
	defer b.mu.Unlock()

	if b.fuotaTaskMulticast[fuotaTaskID] == nil {
		b.fuotaTaskMulticast[fuotaTaskID] = make(map[string]bool)
	}

	b.fuotaTaskMulticast[fuotaTaskID][multicastGroupID] = true

	return nil
}

// AssociateWirelessDeviceWithFuotaTask records the association of a
// wireless device with a FUOTA task. A FUOTA task can have several
// associated devices (ListWirelessDevices' FuotaTaskId filter implies more
// than one), so this adds to a per-task set rather than overwriting a
// single slot.
func (b *InMemoryBackend) AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, wirelessDeviceID string) error {
	b.mu.Lock("AssociateWirelessDeviceWithFuotaTask")
	defer b.mu.Unlock()

	if b.fuotaTaskDevices[fuotaTaskID] == nil {
		b.fuotaTaskDevices[fuotaTaskID] = make(map[string]bool)
	}

	b.fuotaTaskDevices[fuotaTaskID][wirelessDeviceID] = true

	return nil
}

// ListFuotaTaskDeviceIDs returns the IDs of wireless devices currently
// associated with a FUOTA task, sorted for deterministic output.
func (b *InMemoryBackend) ListFuotaTaskDeviceIDs(fuotaTaskID string) []string {
	b.mu.RLock("ListFuotaTaskDeviceIDs")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.fuotaTaskDevices[fuotaTaskID]))
	for id := range b.fuotaTaskDevices[fuotaTaskID] {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

// AddFuotaTaskInternal inserts a FuotaTask directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddFuotaTaskInternal(accountID, region string, ft *FuotaTask) {
	b.mu.Lock("AddFuotaTaskInternal")
	defer b.mu.Unlock()

	cp := copyFuotaTask(ft)
	cp.AccountID = accountID
	cp.Region = region
	b.fuotaTasks.Put(cp)
	b.storeResourceTagsLocked(ft.ARN, ft.Tags)
}

// --- FuotaTask extended operations ---

// StartFuotaTask transitions the FUOTA task's Status to
// "FuotaSession_Waiting" (types.FuotaTaskStatusFuotaSessionWaiting), matching
// real AWS's lifecycle for a task moved out of "Pending" by StartFuotaTask.
// Previously this corrupted FirmwareUpdateRole by overwriting it with a
// fabricated status string instead of tracking status as its own field.
func (b *InMemoryBackend) StartFuotaTask(accountID, region, id string) error {
	b.mu.Lock("StartFuotaTask")
	defer b.mu.Unlock()

	ft, ok := b.fuotaTasks.Get(compositeKey(accountID, region, id))
	if !ok {
		return ErrFuotaTaskNotFound
	}

	ft.Status = "FuotaSession_Waiting"

	return nil
}

// DisassociateWirelessDeviceFromFuotaTask removes a single device from a
// FUOTA task's association set. wirelessDeviceID is the {WirelessDeviceId}
// path segment of DELETE /fuota-tasks/{Id}/wireless-devices/{WirelessDeviceId};
// an empty value falls back to clearing every device associated with the
// task, preserving the prior behavior.
func (b *InMemoryBackend) DisassociateWirelessDeviceFromFuotaTask(fuotaTaskID, wirelessDeviceID string) error {
	b.mu.Lock("DisassociateWirelessDeviceFromFuotaTask")
	defer b.mu.Unlock()

	if wirelessDeviceID == "" {
		delete(b.fuotaTaskDevices, fuotaTaskID)

		return nil
	}

	delete(b.fuotaTaskDevices[fuotaTaskID], wirelessDeviceID)

	return nil
}

// ListMulticastGroupsByFuotaTask returns every multicast group associated
// with a FUOTA task.
func (b *InMemoryBackend) ListMulticastGroupsByFuotaTask(
	accountID, region, fuotaTaskID string,
) []*MulticastGroup {
	b.mu.RLock("ListMulticastGroupsByFuotaTask")
	defer b.mu.RUnlock()

	mgIDs := make([]string, 0, len(b.fuotaTaskMulticast[fuotaTaskID]))
	for id := range b.fuotaTaskMulticast[fuotaTaskID] {
		mgIDs = append(mgIDs, id)
	}

	slices.Sort(mgIDs)

	result := make([]*MulticastGroup, 0, len(mgIDs))

	for _, mgID := range mgIDs {
		if mg, ok := b.multicastGroups.Get(compositeKey(accountID, region, mgID)); ok {
			result = append(result, copyMulticastGroup(mg))
		}
	}

	return result
}

// DisassociateMulticastGroupFromFuotaTask removes a single multicast group
// from a FUOTA task's association set. multicastGroupID is the
// {MulticastGroupId} path segment of DELETE
// /fuota-tasks/{Id}/multicast-groups/{MulticastGroupId}; an empty value
// falls back to clearing every group associated with the task, preserving
// the prior behavior.
func (b *InMemoryBackend) DisassociateMulticastGroupFromFuotaTask(fuotaTaskID, multicastGroupID string) error {
	b.mu.Lock("DisassociateMulticastGroupFromFuotaTask")
	defer b.mu.Unlock()

	if multicastGroupID == "" {
		delete(b.fuotaTaskMulticast, fuotaTaskID)

		return nil
	}

	delete(b.fuotaTaskMulticast[fuotaTaskID], multicastGroupID)

	return nil
}
