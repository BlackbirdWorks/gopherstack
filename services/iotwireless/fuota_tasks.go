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

// copyFuotaTask returns a shallow copy of ft with an independent Tags map.
func copyFuotaTask(ft *FuotaTask) *FuotaTask {
	cp := *ft
	cp.Tags = make(map[string]string, len(ft.Tags))
	maps.Copy(cp.Tags, ft.Tags)

	return &cp
}

// CreateFuotaTask creates a new FUOTA task.
func (b *InMemoryBackend) CreateFuotaTask(
	accountID, region, name, description, firmwareUpdateImage, firmwareUpdateRole string,
	tags map[string]string,
) (*FuotaTask, error) {
	b.mu.Lock()
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
	b.mu.RLock()
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
	b.mu.RLock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	key := compositeKey(accountID, region, id)

	ft, ok := b.fuotaTasks.Get(key)
	if !ok {
		return ErrFuotaTaskNotFound
	}

	delete(b.resourceTags, ft.ARN)
	b.fuotaTasks.Delete(key)

	return nil
}

// UpdateFuotaTask updates mutable fields on an existing FUOTA task.
func (b *InMemoryBackend) UpdateFuotaTask(accountID, region, id, name, description string) error {
	b.mu.Lock()
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

// AssociateMulticastGroupWithFuotaTask records the association of a multicast group with a FUOTA task.
func (b *InMemoryBackend) AssociateMulticastGroupWithFuotaTask(fuotaTaskID, multicastGroupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fuotaTaskMulticast[fuotaTaskID] = multicastGroupID

	return nil
}

// AssociateWirelessDeviceWithFuotaTask records the association of a wireless device with a FUOTA task.
func (b *InMemoryBackend) AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, wirelessDeviceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fuotaTaskDevices[fuotaTaskID] = wirelessDeviceID

	return nil
}

// AddFuotaTaskInternal inserts a FuotaTask directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddFuotaTaskInternal(accountID, region string, ft *FuotaTask) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyFuotaTask(ft)
	cp.AccountID = accountID
	cp.Region = region
	b.fuotaTasks.Put(cp)
	b.storeResourceTagsLocked(ft.ARN, ft.Tags)
}

// --- FuotaTask extended operations ---

// StartFuotaTask sets the FUOTA task status to FUOTA_SESSION_STARTED.
func (b *InMemoryBackend) StartFuotaTask(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ft, ok := b.fuotaTasks.Get(compositeKey(accountID, region, id))
	if !ok {
		return ErrFuotaTaskNotFound
	}

	ft.FirmwareUpdateRole = "FUOTA_SESSION_STARTED" // reuse field to track status

	return nil
}

// DisassociateWirelessDeviceFromFuotaTask removes the association of a wireless device from a FUOTA task.
func (b *InMemoryBackend) DisassociateWirelessDeviceFromFuotaTask(fuotaTaskID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.fuotaTaskDevices, fuotaTaskID)

	return nil
}

// ListMulticastGroupsByFuotaTask returns multicast groups linked to a FUOTA task.
func (b *InMemoryBackend) ListMulticastGroupsByFuotaTask(
	accountID, region, fuotaTaskID string,
) []*MulticastGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	mgID, ok := b.fuotaTaskMulticast[fuotaTaskID]
	if !ok {
		return []*MulticastGroup{}
	}

	mg, ok := b.multicastGroups.Get(compositeKey(accountID, region, mgID))
	if !ok {
		return []*MulticastGroup{}
	}

	return []*MulticastGroup{copyMulticastGroup(mg)}
}

// DisassociateMulticastGroupFromFuotaTask removes the association of a multicast group from a FUOTA task.
func (b *InMemoryBackend) DisassociateMulticastGroupFromFuotaTask(fuotaTaskID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.fuotaTaskMulticast, fuotaTaskID)

	return nil
}
