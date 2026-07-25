package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedVolume struct {
	CreationTime            time.Time         `json:"creationTime"`
	Tags                    map[string]string `json:"tags"`
	VolumeID                string            `json:"volumeId"`
	VolumeType              string            `json:"volumeType"`
	FileSystemID            string            `json:"fileSystemId"`
	StorageVirtualMachineID string            `json:"storageVirtualMachineId,omitempty"`
	Name                    string            `json:"name"`
	Lifecycle               string            `json:"lifecycle"`
	ResourceARN             string            `json:"resourceArn"`
}

func (v *storedVolume) toPublic() *Volume {
	return &Volume{
		CreationTime:            epochTime(v.CreationTime),
		VolumeID:                v.VolumeID,
		VolumeType:              v.VolumeType,
		FileSystemID:            v.FileSystemID,
		StorageVirtualMachineID: v.StorageVirtualMachineID,
		Name:                    v.Name,
		Lifecycle:               v.Lifecycle,
		ResourceARN:             v.ResourceARN,
		Tags:                    tagsMapToSlice(v.Tags),
	}
}

type createVolumeInput struct {
	VolumeType              string `json:"VolumeType"`
	FileSystemID            string `json:"FileSystemId,omitempty"`
	StorageVirtualMachineID string `json:"StorageVirtualMachineId,omitempty"`
	Name                    string `json:"Name"`
	Tags                    []Tag  `json:"Tags,omitempty"`
}

// CreateVolume creates a volume.
func (b *InMemoryBackend) CreateVolume(input *createVolumeInput) (*Volume, error) {
	if input.VolumeType == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVolume")
	defer b.mu.Unlock()

	if input.FileSystemID != "" {
		if !b.fileSystems.Has(input.FileSystemID) {
			return nil, ErrFileSystemNotFound
		}
	}

	if input.StorageVirtualMachineID != "" {
		if !b.storageVirtualMachines.Has(input.StorageVirtualMachineID) {
			return nil, ErrStorageVirtualMachineNotFound
		}
	}

	id := "fsvol-" + uuid.New().String()[:16]
	arn := b.volumeARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	v := &storedVolume{
		CreationTime:            now,
		Tags:                    tags,
		VolumeID:                id,
		VolumeType:              input.VolumeType,
		FileSystemID:            input.FileSystemID,
		StorageVirtualMachineID: input.StorageVirtualMachineID,
		Name:                    input.Name,
		Lifecycle:               lifecycleAvailable,
		ResourceARN:             arn,
	}

	b.volumes.Put(v)
	b.tags[arn] = tags

	return v.toPublic(), nil
}

type createVolumeFromBackupInput struct {
	BackupID                string `json:"BackupId"`
	VolumeType              string `json:"VolumeType,omitempty"`
	StorageVirtualMachineID string `json:"StorageVirtualMachineId,omitempty"`
	Name                    string `json:"Name"`
	Tags                    []Tag  `json:"Tags,omitempty"`
}

// CreateVolumeFromBackup creates a volume from a backup.
func (b *InMemoryBackend) CreateVolumeFromBackup(input *createVolumeFromBackupInput) (*Volume, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVolumeFromBackup")
	defer b.mu.Unlock()

	src, ok := b.backups.Get(input.BackupID)
	if !ok {
		return nil, ErrBackupNotFound
	}

	id := "fsvol-" + uuid.New().String()[:16]
	arn := b.volumeARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	volType := input.VolumeType
	if volType == "" {
		volType = "ONTAP"
	}

	v := &storedVolume{
		CreationTime:            now,
		Tags:                    tags,
		VolumeID:                id,
		VolumeType:              volType,
		FileSystemID:            src.FileSystemID,
		StorageVirtualMachineID: input.StorageVirtualMachineID,
		Name:                    input.Name,
		Lifecycle:               lifecycleAvailable,
		ResourceARN:             arn,
	}

	b.volumes.Put(v)
	b.tags[arn] = tags

	return v.toPublic(), nil
}

// DeleteVolume removes a volume.
func (b *InMemoryBackend) DeleteVolume(volumeID string) error {
	b.mu.Lock("DeleteVolume")
	defer b.mu.Unlock()

	if !b.volumes.Has(volumeID) {
		return ErrVolumeNotFound
	}

	b.deleteVolumeLocked(volumeID)

	return nil
}

// deleteVolumeLocked removes a volume and cascades to its snapshots, so no
// ghost Snapshot rows (pointing at a now-nonexistent VolumeId) survive the
// volume's deletion. Caller must already hold b.mu and have verified the
// volume exists.
func (b *InMemoryBackend) deleteVolumeLocked(volumeID string) {
	v, ok := b.volumes.Get(volumeID)
	if !ok {
		return
	}

	var snapshotIDs []string

	b.snapshots.Range(func(s *storedSnapshot) bool {
		if s.VolumeID == volumeID {
			snapshotIDs = append(snapshotIDs, s.SnapshotID)
		}

		return true
	})

	for _, id := range snapshotIDs {
		if snap, found := b.snapshots.Get(id); found {
			delete(b.tags, snap.ResourceARN)
		}

		b.snapshots.Delete(id)
	}

	b.volumes.Delete(volumeID)
	delete(b.tags, v.ResourceARN)
}

// createOpenZFSRootVolumeLocked creates the backing root volume that real
// AWS auto-creates for every FSx for OpenZFS file system, and returns its
// VolumeId. Caller must already hold b.mu.
func (b *InMemoryBackend) createOpenZFSRootVolumeLocked(fs *storedFileSystem) string {
	id := "fsvol-" + uuid.New().String()[:16]
	arn := b.volumeARN(id)
	tags := make(map[string]string)

	v := &storedVolume{
		CreationTime: fs.CreationTime,
		Tags:         tags,
		VolumeID:     id,
		VolumeType:   fileSystemTypeOpenZFS,
		FileSystemID: fs.FileSystemID,
		Name:         openZFSRootVolumeName,
		Lifecycle:    lifecycleAvailable,
		ResourceARN:  arn,
	}

	b.volumes.Put(v)
	b.tags[arn] = tags

	return id
}

// DescribeVolumes returns volumes, optionally filtered by ID.
func (b *InMemoryBackend) DescribeVolumes( //nolint:dupl // existing issue.
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*Volume, string, error) {
	b.mu.RLock("DescribeVolumes")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedVolume

	if len(ids) > 0 {
		for _, id := range ids {
			v, ok := b.volumes.Get(id)
			if !ok {
				return nil, "", ErrVolumeNotFound
			}

			all = append(all, v)
		}
	} else {
		all = b.volumes.All()

		sort.Slice(all, func(i, j int) bool { return all[i].VolumeID < all[j].VolumeID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].VolumeID
	})

	result := make([]*Volume, end-start)
	for i, v := range all[start:end] {
		result[i] = v.toPublic()
	}

	return result, next, nil
}

type restoreVolumeFromSnapshotInput struct {
	VolumeID   string `json:"VolumeId"`
	SnapshotID string `json:"SnapshotId"`
}

// RestoreVolumeFromSnapshot restores a volume to a snapshot state.
func (b *InMemoryBackend) RestoreVolumeFromSnapshot(input *restoreVolumeFromSnapshotInput) (*Volume, error) {
	b.mu.Lock("RestoreVolumeFromSnapshot")
	defer b.mu.Unlock()

	v, ok := b.volumes.Get(input.VolumeID)
	if !ok {
		return nil, ErrVolumeNotFound
	}

	if !b.snapshots.Has(input.SnapshotID) {
		return nil, ErrSnapshotNotFound
	}

	return v.toPublic(), nil
}

type updateVolumeInput struct {
	VolumeID string `json:"VolumeId"`
	Name     string `json:"Name,omitempty"`
}

// UpdateVolume updates volume metadata.
func (b *InMemoryBackend) UpdateVolume(input *updateVolumeInput) (*Volume, error) {
	b.mu.Lock("UpdateVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes.Get(input.VolumeID)
	if !ok {
		return nil, ErrVolumeNotFound
	}

	if input.Name != "" {
		v.Name = input.Name
	}

	return v.toPublic(), nil
}

func (b *InMemoryBackend) volumeARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("volume/%s", id))
}
