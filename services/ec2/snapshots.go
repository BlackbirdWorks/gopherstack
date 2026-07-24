package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CopySnapshot creates a new snapshot as a copy of an existing one.
func (b *InMemoryBackend) CopySnapshot(sourceSnapshotID, description string) (*Snapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceSnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CopySnapshot")
	defer b.mu.Unlock()

	src, ok := b.snapshots.Get(sourceSnapshotID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, sourceSnapshotID)
	}

	desc := description
	if desc == "" {
		desc = "Copy of " + sourceSnapshotID
	}

	snap := &Snapshot{
		SnapshotID:  "snap-" + uuid.New().String()[:17],
		VolumeID:    src.VolumeID,
		Description: desc,
		State:       stateCompleted,
		Progress:    snapshotProgress100,
		StartTime:   time.Now().UTC(),
		VolumeSize:  src.VolumeSize,
		Encrypted:   src.Encrypted,
		KmsKeyID:    src.KmsKeyID,
	}
	b.snapshots.Put(snap)

	return snap, nil
}

// ---- CreateSnapshots ----

// SnapshotEntry is used by CreateSnapshots to specify a volume.
type SnapshotEntry struct {
	VolumeID    string `json:"volumeID,omitempty"`
	SnapshotID  string `json:"snapshotID,omitempty"`
	Description string `json:"description,omitempty"`
	State       string `json:"state,omitempty"`
}

// CreateSnapshots creates one snapshot per volumeID in the list.
func (b *InMemoryBackend) CreateSnapshots(
	volumeIDs []string,
	description string,
) ([]*Snapshot, error) {
	if len(volumeIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshots")
	defer b.mu.Unlock()

	for _, vid := range volumeIDs {
		if _, ok := b.volumes.Get(vid); !ok {
			return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, vid)
		}
	}

	snaps := make([]*Snapshot, 0, len(volumeIDs))
	for _, vid := range volumeIDs {
		vol, _ := b.volumes.Get(vid)
		snap := &Snapshot{
			SnapshotID:  "snap-" + uuid.New().String()[:17],
			VolumeID:    vid,
			Description: description,
			State:       stateCompleted,
			Progress:    snapshotProgress100,
			StartTime:   time.Now().UTC(),
			VolumeSize:  vol.Size,
			Encrypted:   vol.Encrypted,
			KmsKeyID:    vol.KmsKeyID,
		}
		b.snapshots.Put(snap)
		snaps = append(snaps, snap)
	}

	return snaps, nil
}

// ---- Snapshot block public access ----

// GetSnapshotBlockPublicAccessState returns the account-level block state.
// Returns "block-all-sharing" (default) if never explicitly set.
func (b *InMemoryBackend) GetSnapshotBlockPublicAccessState() string {
	b.mu.RLock("GetSnapshotBlockPublicAccessState")
	defer b.mu.RUnlock()

	if b.snapshotBlockPublicAccess == "" {
		return stateBlockAllSharing
	}

	return b.snapshotBlockPublicAccess
}

// EnableSnapshotBlockPublicAccess sets the block state to block-all-sharing or block-new-sharing.
func (b *InMemoryBackend) EnableSnapshotBlockPublicAccess(state string) error {
	if state != stateBlockAllSharing && state != "block-new-sharing" {
		return fmt.Errorf(
			"%w: State must be "+stateBlockAllSharing+" or block-new-sharing",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("EnableSnapshotBlockPublicAccess")
	defer b.mu.Unlock()

	b.snapshotBlockPublicAccess = state

	return nil
}

// DisableSnapshotBlockPublicAccess sets the block state to unblocked.
func (b *InMemoryBackend) DisableSnapshotBlockPublicAccess() {
	b.mu.Lock("DisableSnapshotBlockPublicAccess")
	defer b.mu.Unlock()

	b.snapshotBlockPublicAccess = "unblocked"
}

// ---- Snapshot tier ----

// SnapshotTierItem contains tier info for a single snapshot.
type SnapshotTierItem struct {
	SnapshotID  string `json:"snapshotID,omitempty"`
	VolumeID    string `json:"volumeID,omitempty"`
	StorageTier string `json:"storageTier,omitempty"`
}

// DescribeSnapshotTierStatus returns tier info for given snapshot IDs (or all).
func (b *InMemoryBackend) DescribeSnapshotTierStatus(ids []string) []SnapshotTierItem {
	b.mu.RLock("DescribeSnapshotTierStatus")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []SnapshotTierItem
	for _, snap := range b.snapshots.All() {
		if len(filter) > 0 && !filter[snap.SnapshotID] {
			continue
		}
		tier := b.snapshotTiers[snap.SnapshotID]
		if tier == "" {
			tier = "standard"
		}
		out = append(out, SnapshotTierItem{
			SnapshotID:  snap.SnapshotID,
			VolumeID:    snap.VolumeID,
			StorageTier: tier,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })

	return out
}

// ModifySnapshotTier updates the storage tier for a snapshot ("archive" or "standard").
func (b *InMemoryBackend) ModifySnapshotTier(snapshotID, storageTier string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySnapshotTier")
	defer b.mu.Unlock()

	if _, ok := b.snapshots.Get(snapshotID); !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	b.snapshotTiers[snapshotID] = storageTier

	return nil
}

// ResetSnapshotAttribute resets the createVolumePermission attribute of a snapshot.
func (b *InMemoryBackend) ResetSnapshotAttribute(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetSnapshotAttribute")
	defer b.mu.Unlock()

	if _, ok := b.snapshots.Get(snapshotID); !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	delete(b.snapshotAttributes, snapshotID)

	return nil
}

// ---- CreateDefaultVpc ----

// LockSnapshot locks a snapshot to prevent deletion.
func (b *InMemoryBackend) LockSnapshot(
	snapshotID, lockMode string,
	durationDays int,
) (*SnapshotLock, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("LockSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshots.Get(snapshotID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if _, locked := b.snapshotLocks.Get(snapshotID); locked {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotAlreadyLocked, snapshotID)
	}

	now := time.Now().UTC()
	lock := &SnapshotLock{
		SnapshotID:       snapshotID,
		LockState:        lockMode,
		LockCreatedOn:    now,
		LockDurationDays: durationDays,
	}
	if durationDays > 0 {
		lock.LockExpiresOn = now.AddDate(0, 0, durationDays)
	}
	b.snapshotLocks.Put(lock)

	return lock, nil
}

// UnlockSnapshot removes the lock from a snapshot.
func (b *InMemoryBackend) UnlockSnapshot(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnlockSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshots.Get(snapshotID); !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if _, locked := b.snapshotLocks.Get(snapshotID); !locked {
		return fmt.Errorf("%w: %s", ErrSnapshotNotLocked, snapshotID)
	}
	b.snapshotLocks.Delete(snapshotID)

	return nil
}

// DescribeLockedSnapshots returns locked snapshots, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeLockedSnapshots(ids []string) []*SnapshotLock {
	b.mu.RLock("DescribeLockedSnapshots")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*SnapshotLock
	for _, lock := range b.snapshotLocks.All() {
		if len(filter) > 0 && !filter[lock.SnapshotID] {
			continue
		}
		cp := *lock
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })

	return out
}

// ---- CopyVolumes ----

// CopyVolumesResult holds the result of copying a volume.
type CopyVolumesResult struct {
	SourceVolumeID string `json:"sourceVolumeID,omitempty"`
	DestVolumeID   string `json:"destVolumeID,omitempty"`
}

// ListSnapshotsInRecycleBin returns soft-deleted snapshots.
func (b *InMemoryBackend) ListSnapshotsInRecycleBin(snapshotIDs []string) []*Snapshot {
	b.mu.RLock("ListSnapshotsInRecycleBin")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(snapshotIDs))
	for _, id := range snapshotIDs {
		filter[id] = true
	}

	var out []*Snapshot
	for _, snap := range b.recycleBinSnapshots.All() {
		if len(filter) > 0 && !filter[snap.SnapshotID] {
			continue
		}
		cp := *snap
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })

	return out
}

// RestoreSnapshotFromRecycleBin restores a snapshot from recycle bin.
func (b *InMemoryBackend) RestoreSnapshotFromRecycleBin(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreSnapshotFromRecycleBin")
	defer b.mu.Unlock()

	snap, ok := b.recycleBinSnapshots.Get(snapshotID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	b.snapshots.Put(snap)
	b.recycleBinSnapshots.Delete(snapshotID)

	return nil
}

// RestoreSnapshotTier restores a snapshot from archive tier to standard.
func (b *InMemoryBackend) RestoreSnapshotTier(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreSnapshotTier")
	defer b.mu.Unlock()

	if _, ok := b.snapshots.Get(snapshotID); !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	b.snapshotTiers[snapshotID] = stateDefaultCredit

	return nil
}

// ---- Import snapshot ----

// ImportSnapshot creates an import task for a snapshot.
func (b *InMemoryBackend) ImportSnapshot(description string) (*SnapshotImportTask, error) {
	b.mu.Lock("ImportSnapshot")
	defer b.mu.Unlock()

	task := &SnapshotImportTask{
		ImportTaskID: "import-snap-" + uuid.New().String()[:8],
		Description:  description,
		Status:       stateTaskCompleted,
	}
	b.snapshotImportTasks.Put(task)

	return task, nil
}

// DescribeImportSnapshotTasks returns import snapshot tasks.
func (b *InMemoryBackend) DescribeImportSnapshotTasks(taskIDs []string) []*SnapshotImportTask {
	b.mu.RLock("DescribeImportSnapshotTasks")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(taskIDs))
	for _, id := range taskIDs {
		filter[id] = true
	}

	var out []*SnapshotImportTask
	for _, t := range b.snapshotImportTasks.All() {
		if len(filter) > 0 && !filter[t.ImportTaskID] {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImportTaskID < out[j].ImportTaskID })

	return out
}

// ---- Fast launch / fast snapshot restores ----

// EnableFastSnapshotRestores enables fast snapshot restores for given AZ/snapshot combos.
func (b *InMemoryBackend) EnableFastSnapshotRestores(
	snapshotIDs, availabilityZones []string,
) error {
	b.mu.Lock("EnableFastSnapshotRestores")
	defer b.mu.Unlock()

	for _, snap := range snapshotIDs {
		for _, az := range availabilityZones {
			b.fastSnapshotRestores[snap+":"+az] = true
		}
	}

	return nil
}

// DisableFastSnapshotRestores disables fast snapshot restores.
func (b *InMemoryBackend) DisableFastSnapshotRestores(
	snapshotIDs, availabilityZones []string,
) error {
	b.mu.Lock("DisableFastSnapshotRestores")
	defer b.mu.Unlock()

	for _, snap := range snapshotIDs {
		for _, az := range availabilityZones {
			delete(b.fastSnapshotRestores, snap+":"+az)
		}
	}

	return nil
}

// FastSnapshotRestoreItem holds fast snapshot restore config for a snapshot/AZ combo.
type FastSnapshotRestoreItem struct {
	SnapshotID       string `json:"snapshotID,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	State            string `json:"state,omitempty"`
}

// DescribeFastSnapshotRestores returns fast snapshot restore enabled items.
func (b *InMemoryBackend) DescribeFastSnapshotRestores() []FastSnapshotRestoreItem {
	b.mu.RLock("DescribeFastSnapshotRestores")
	defer b.mu.RUnlock()

	var out []FastSnapshotRestoreItem
	for key := range b.fastSnapshotRestores {
		// Key is "snapshotID:az"
		parts := splitKey(key)
		if len(parts) == magicSplitLen {
			out = append(out, FastSnapshotRestoreItem{
				SnapshotID:       parts[0],
				AvailabilityZone: parts[1],
				State:            stateEnabledFastLaunch,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SnapshotID != out[j].SnapshotID {
			return out[i].SnapshotID < out[j].SnapshotID
		}

		return out[i].AvailabilityZone < out[j].AvailabilityZone
	})

	return out
}

// splitKey splits a colon-separated key string into two parts.
func splitKey(key string) []string {
	for i, c := range key {
		if c == ':' {
			return []string{key[:i], key[i+1:]}
		}
	}

	return []string{key}
}

// ---- GetPasswordData ----

// CreateSnapshot creates an EBS snapshot from a volume.
func (b *InMemoryBackend) CreateSnapshot(volumeID, description string) (*Snapshot, error) {
	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	vol, ok := b.volumes.Get(volumeID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	snap := &Snapshot{
		SnapshotID:  "snap-" + uuid.New().String()[:17],
		VolumeID:    volumeID,
		Description: description,
		State:       "completed",
		Progress:    "100%",
		StartTime:   time.Now().UTC(),
		VolumeSize:  vol.Size,
		Encrypted:   vol.Encrypted,
		KmsKeyID:    vol.KmsKeyID,
	}
	b.snapshots.Put(snap)

	cp := *snap

	return &cp, nil
}

// DescribeSnapshots returns snapshots, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeSnapshots(ids []string) []*Snapshot {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*Snapshot, 0, b.snapshots.Len())
	for _, snap := range b.snapshots.All() {
		if len(idSet) > 0 && !idSet[snap.SnapshotID] {
			continue
		}

		cp := *snap
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].SnapshotID < out[j].SnapshotID
	})

	return out
}

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(id string) error {
	if id == "" {
		return fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshots.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
	}
	b.snapshots.Delete(id)
	delete(b.tags, id)

	return nil
}

// ---- AMI lifecycle ----

// DescribeSnapshotsSorted returns snapshots sorted by snapshot ID.
func (b *InMemoryBackend) DescribeSnapshotsSorted(ids []string) []*Snapshot {
	return b.DescribeSnapshots(ids)
}
