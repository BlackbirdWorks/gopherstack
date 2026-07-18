package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedSnapshot struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	SnapshotID   string            `json:"snapshotId"`
	VolumeID     string            `json:"volumeId"`
	Name         string            `json:"name"`
	Lifecycle    string            `json:"lifecycle"`
	ResourceARN  string            `json:"resourceArn"`
}

func (s *storedSnapshot) toPublic() *Snapshot {
	return &Snapshot{
		CreationTime: epochTime(s.CreationTime),
		SnapshotID:   s.SnapshotID,
		VolumeID:     s.VolumeID,
		Name:         s.Name,
		Lifecycle:    s.Lifecycle,
		ResourceARN:  s.ResourceARN,
		Tags:         tagsMapToSlice(s.Tags),
	}
}

type createSnapshotInput struct {
	VolumeID string `json:"VolumeId"`
	Name     string `json:"Name"`
	Tags     []Tag  `json:"Tags,omitempty"`
}

// CreateSnapshot creates a snapshot of a volume.
func (b *InMemoryBackend) CreateSnapshot(input *createSnapshotInput) (*Snapshot, error) {
	if input.VolumeID == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	if !b.volumes.Has(input.VolumeID) {
		return nil, ErrVolumeNotFound
	}

	id := "fsvolsnap-" + uuid.New().String()[:12]
	arn := b.snapshotARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	s := &storedSnapshot{
		CreationTime: now,
		Tags:         tags,
		SnapshotID:   id,
		VolumeID:     input.VolumeID,
		Name:         input.Name,
		Lifecycle:    lifecycleAvailable,
		ResourceARN:  arn,
	}

	b.snapshots.Put(s)
	b.tags[arn] = tags

	return s.toPublic(), nil
}

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(snapshotID string) error {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	s, ok := b.snapshots.Get(snapshotID)
	if !ok {
		return ErrSnapshotNotFound
	}

	b.snapshots.Delete(snapshotID)
	delete(b.tags, s.ResourceARN)

	return nil
}

// DescribeSnapshots returns snapshots, optionally filtered by ID.
func (b *InMemoryBackend) DescribeSnapshots( //nolint:dupl // existing issue.
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*Snapshot, string, error) {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedSnapshot

	if len(ids) > 0 {
		for _, id := range ids {
			s, ok := b.snapshots.Get(id)
			if !ok {
				return nil, "", ErrSnapshotNotFound
			}

			all = append(all, s)
		}
	} else {
		all = b.snapshots.All()

		sort.Slice(all, func(i, j int) bool { return all[i].SnapshotID < all[j].SnapshotID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].SnapshotID
	})

	result := make([]*Snapshot, end-start)
	for i, s := range all[start:end] {
		result[i] = s.toPublic()
	}

	return result, next, nil
}

type updateSnapshotInput struct {
	SnapshotID string `json:"SnapshotId"`
	Name       string `json:"Name,omitempty"`
}

// UpdateSnapshot updates snapshot metadata.
func (b *InMemoryBackend) UpdateSnapshot(input *updateSnapshotInput) (*Snapshot, error) {
	b.mu.Lock("UpdateSnapshot")
	defer b.mu.Unlock()

	s, ok := b.snapshots.Get(input.SnapshotID)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if input.Name != "" {
		s.Name = input.Name
	}

	return s.toPublic(), nil
}

type copySnapshotAndUpdateVolumeInput struct {
	VolumeID         string `json:"VolumeId"`
	SourceSnapshotID string `json:"SourceSnapshotARN"`
}

// CopySnapshotAndUpdateVolume restores a volume to the state of a snapshot.
func (b *InMemoryBackend) CopySnapshotAndUpdateVolume(input *copySnapshotAndUpdateVolumeInput) (*Volume, error) {
	b.mu.Lock("CopySnapshotAndUpdateVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes.Get(input.VolumeID)
	if !ok {
		return nil, ErrVolumeNotFound
	}

	return v.toPublic(), nil
}

func (b *InMemoryBackend) snapshotARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("snapshot/%s", id))
}
