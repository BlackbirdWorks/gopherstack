package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Stored types
// ---------------------------------------------------------------------------

type storedDataRepositoryAssoc struct {
	CreationTime       time.Time         `json:"creationTime"`
	Tags               map[string]string `json:"tags"`
	AssociationID      string            `json:"associationId"`
	FileSystemID       string            `json:"fileSystemId"`
	FileSystemPath     string            `json:"fileSystemPath"`
	DataRepositoryPath string            `json:"dataRepositoryPath"`
	Lifecycle          string            `json:"lifecycle"`
	ResourceARN        string            `json:"resourceArn"`
}

func (a *storedDataRepositoryAssoc) toPublic() *DataRepositoryAssociation {
	return &DataRepositoryAssociation{
		CreationTime:       a.CreationTime,
		AssociationID:      a.AssociationID,
		FileSystemID:       a.FileSystemID,
		FileSystemPath:     a.FileSystemPath,
		DataRepositoryPath: a.DataRepositoryPath,
		Lifecycle:          a.Lifecycle,
		ResourceARN:        a.ResourceARN,
		Tags:               tagsMapToSlice(a.Tags),
	}
}

type storedDataRepositoryTask struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Paths        []string          `json:"paths,omitempty"`
	TaskID       string            `json:"taskId"`
	FileSystemID string            `json:"fileSystemId"`
	Type         string            `json:"type"`
	Lifecycle    string            `json:"lifecycle"`
	ResourceARN  string            `json:"resourceArn"`
}

func (t *storedDataRepositoryTask) toPublic() *DataRepositoryTask {
	return &DataRepositoryTask{
		CreationTime: t.CreationTime,
		TaskID:       t.TaskID,
		FileSystemID: t.FileSystemID,
		Type:         t.Type,
		Lifecycle:    t.Lifecycle,
		ResourceARN:  t.ResourceARN,
		Paths:        t.Paths,
		Tags:         tagsMapToSlice(t.Tags),
	}
}

type storedFileCache struct {
	CreationTime       time.Time         `json:"creationTime"`
	Tags               map[string]string `json:"tags"`
	FileCacheID        string            `json:"fileCacheId"`
	FileCacheType      string            `json:"fileCacheType"`
	Lifecycle          string            `json:"lifecycle"`
	ResourceARN        string            `json:"resourceArn"`
	StorageCapacityGiB int32             `json:"storageCapacityGiB,omitempty"`
}

func (c *storedFileCache) toPublic() *FileCache {
	return &FileCache{
		CreationTime:       c.CreationTime,
		FileCacheID:        c.FileCacheID,
		FileCacheType:      c.FileCacheType,
		Lifecycle:          c.Lifecycle,
		ResourceARN:        c.ResourceARN,
		StorageCapacityGiB: c.StorageCapacityGiB,
		Tags:               tagsMapToSlice(c.Tags),
	}
}

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
		CreationTime: s.CreationTime,
		SnapshotID:   s.SnapshotID,
		VolumeID:     s.VolumeID,
		Name:         s.Name,
		Lifecycle:    s.Lifecycle,
		ResourceARN:  s.ResourceARN,
		Tags:         tagsMapToSlice(s.Tags),
	}
}

type storedStorageVirtualMachine struct {
	CreationTime            time.Time         `json:"creationTime"`
	Tags                    map[string]string `json:"tags"`
	StorageVirtualMachineID string            `json:"storageVirtualMachineId"`
	FileSystemID            string            `json:"fileSystemId"`
	Name                    string            `json:"name"`
	Lifecycle               string            `json:"lifecycle"`
	ResourceARN             string            `json:"resourceArn"`
	Subtype                 string            `json:"subtype,omitempty"`
	RootVolumeSecurityStyle string            `json:"rootVolumeSecurityStyle,omitempty"`
}

func (s *storedStorageVirtualMachine) toPublic() *StorageVirtualMachine {
	return &StorageVirtualMachine{
		CreationTime:            s.CreationTime,
		StorageVirtualMachineID: s.StorageVirtualMachineID,
		FileSystemID:            s.FileSystemID,
		Name:                    s.Name,
		Lifecycle:               s.Lifecycle,
		ResourceARN:             s.ResourceARN,
		Subtype:                 s.Subtype,
		RootVolumeSecurityStyle: s.RootVolumeSecurityStyle,
		Tags:                    tagsMapToSlice(s.Tags),
	}
}

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
		CreationTime:            v.CreationTime,
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

type storedS3AccessPoint struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Name         string            `json:"name"`
	FileSystemID string            `json:"fileSystemId"`
	VolumeID     string            `json:"volumeId,omitempty"`
	Lifecycle    string            `json:"lifecycle"`
	ResourceARN  string            `json:"resourceArn"`
}

func (a *storedS3AccessPoint) toPublic() *S3AccessPoint {
	return &S3AccessPoint{
		CreationTime: a.CreationTime,
		Name:         a.Name,
		FileSystemID: a.FileSystemID,
		VolumeID:     a.VolumeID,
		Lifecycle:    a.Lifecycle,
		ResourceARN:  a.ResourceARN,
		Tags:         tagsMapToSlice(a.Tags),
	}
}

// ---------------------------------------------------------------------------
// Aliases
// ---------------------------------------------------------------------------

// AssociateFileSystemAliases adds DNS aliases to a file system.
func (b *InMemoryBackend) AssociateFileSystemAliases(fileSystemID string, aliases []string) ([]FileSystemAlias, error) {
	b.mu.Lock("AssociateFileSystemAliases")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return nil, ErrFileSystemNotFound
	}

	existing := make(map[string]struct{}, len(b.aliases[fileSystemID]))
	for _, a := range b.aliases[fileSystemID] {
		existing[a] = struct{}{}
	}

	for _, a := range aliases {
		if _, dup := existing[a]; !dup {
			b.aliases[fileSystemID] = append(b.aliases[fileSystemID], a)
			existing[a] = struct{}{}
		}
	}

	return aliasesToPublic(b.aliases[fileSystemID], "AVAILABLE"), nil
}

// DisassociateFileSystemAliases removes DNS aliases from a file system.
func (b *InMemoryBackend) DisassociateFileSystemAliases(
	fileSystemID string,
	aliases []string,
) ([]FileSystemAlias, error) {
	b.mu.Lock("DisassociateFileSystemAliases")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return nil, ErrFileSystemNotFound
	}

	remove := make(map[string]struct{}, len(aliases))
	for _, a := range aliases {
		remove[a] = struct{}{}
	}

	kept := b.aliases[fileSystemID][:0]
	for _, a := range b.aliases[fileSystemID] {
		if _, drop := remove[a]; !drop {
			kept = append(kept, a)
		}
	}

	b.aliases[fileSystemID] = kept

	return aliasesToPublic(aliases, "DELETING"), nil
}

// DescribeFileSystemAliases returns aliases for a file system.
func (b *InMemoryBackend) DescribeFileSystemAliases(
	fileSystemID string,
	maxResults int32,
	nextToken string,
) ([]FileSystemAlias, string, error) {
	b.mu.RLock("DescribeFileSystemAliases")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return nil, "", ErrFileSystemNotFound
	}

	all := b.aliases[fileSystemID]

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	start := 0
	if nextToken != "" {
		for i, a := range all {
			if a == nextToken {
				start = i
				break
			}
		}
	}

	end := min(start+int(maxResults), len(all))
	page := all[start:end]

	var next string
	if end < len(all) {
		next = all[end]
	}

	return aliasesToPublic(page, "AVAILABLE"), next, nil
}

func aliasesToPublic(names []string, lifecycle string) []FileSystemAlias {
	out := make([]FileSystemAlias, len(names))
	for i, n := range names {
		out[i] = FileSystemAlias{Name: n, Lifecycle: lifecycle}
	}

	return out
}

// ---------------------------------------------------------------------------
// Data Repository Associations
// ---------------------------------------------------------------------------

type createDataRepositoryAssociationInput struct {
	FileSystemID       string `json:"FileSystemId"`
	FileSystemPath     string `json:"FileSystemPath"`
	DataRepositoryPath string `json:"DataRepositoryPath"`
	Tags               []Tag  `json:"Tags,omitempty"`
}

// CreateDataRepositoryAssociation creates a data repository association.
func (b *InMemoryBackend) CreateDataRepositoryAssociation(
	input *createDataRepositoryAssociationInput,
) (*DataRepositoryAssociation, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataRepositoryAssociation")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[input.FileSystemID]; !ok {
		return nil, ErrFileSystemNotFound
	}

	id := "dra-" + uuid.New().String()[:17]
	arn := b.draARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	a := &storedDataRepositoryAssoc{
		CreationTime:       now,
		Tags:               tags,
		AssociationID:      id,
		FileSystemID:       input.FileSystemID,
		FileSystemPath:     input.FileSystemPath,
		DataRepositoryPath: input.DataRepositoryPath,
		Lifecycle:          lifecycleAvailable,
		ResourceARN:        arn,
	}

	b.dataRepositoryAssocs[id] = a
	b.tags[arn] = tags

	return a.toPublic(), nil
}

// DeleteDataRepositoryAssociation removes a data repository association.
func (b *InMemoryBackend) DeleteDataRepositoryAssociation(associationID string) error {
	b.mu.Lock("DeleteDataRepositoryAssociation")
	defer b.mu.Unlock()

	a, ok := b.dataRepositoryAssocs[associationID]
	if !ok {
		return ErrDataRepositoryAssociationNotFound
	}

	delete(b.dataRepositoryAssocs, associationID)
	delete(b.tags, a.ResourceARN)

	return nil
}

// DescribeDataRepositoryAssociations returns DRAs, optionally filtered by ID.
func (b *InMemoryBackend) DescribeDataRepositoryAssociations(
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*DataRepositoryAssociation, string, error) {
	b.mu.RLock("DescribeDataRepositoryAssociations")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedDataRepositoryAssoc

	if len(ids) > 0 {
		for _, id := range ids {
			a, ok := b.dataRepositoryAssocs[id]
			if !ok {
				return nil, "", ErrDataRepositoryAssociationNotFound
			}

			all = append(all, a)
		}
	} else {
		for _, a := range b.dataRepositoryAssocs {
			all = append(all, a)
		}

		sort.Slice(all, func(i, j int) bool { return all[i].AssociationID < all[j].AssociationID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].AssociationID
	})

	result := make([]*DataRepositoryAssociation, end-start)
	for i, a := range all[start:end] {
		result[i] = a.toPublic()
	}

	return result, next, nil
}

type updateDataRepositoryAssociationInput struct {
	AssociationID      string `json:"AssociationId"`
	FileSystemPath     string `json:"FileSystemPath,omitempty"`
	DataRepositoryPath string `json:"DataRepositoryPath,omitempty"`
}

// UpdateDataRepositoryAssociation updates a DRA's paths.
func (b *InMemoryBackend) UpdateDataRepositoryAssociation(
	input *updateDataRepositoryAssociationInput,
) (*DataRepositoryAssociation, error) {
	b.mu.Lock("UpdateDataRepositoryAssociation")
	defer b.mu.Unlock()

	a, ok := b.dataRepositoryAssocs[input.AssociationID]
	if !ok {
		return nil, ErrDataRepositoryAssociationNotFound
	}

	if input.FileSystemPath != "" {
		a.FileSystemPath = input.FileSystemPath
	}

	if input.DataRepositoryPath != "" {
		a.DataRepositoryPath = input.DataRepositoryPath
	}

	return a.toPublic(), nil
}

// ---------------------------------------------------------------------------
// Data Repository Tasks
// ---------------------------------------------------------------------------

type createDataRepositoryTaskInput struct {
	FileSystemID string   `json:"FileSystemId"`
	Type         string   `json:"Type"`
	Paths        []string `json:"Paths,omitempty"`
	Tags         []Tag    `json:"Tags,omitempty"`
}

// CreateDataRepositoryTask creates a data repository task.
func (b *InMemoryBackend) CreateDataRepositoryTask(input *createDataRepositoryTaskInput) (*DataRepositoryTask, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataRepositoryTask")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[input.FileSystemID]; !ok {
		return nil, ErrFileSystemNotFound
	}

	id := "task-" + uuid.New().String()[:17]
	arn := b.drtARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	t := &storedDataRepositoryTask{
		CreationTime: now,
		Tags:         tags,
		Paths:        input.Paths,
		TaskID:       id,
		FileSystemID: input.FileSystemID,
		Type:         input.Type,
		Lifecycle:    "EXECUTING",
		ResourceARN:  arn,
	}

	b.dataRepositoryTasks[id] = t

	return t.toPublic(), nil
}

// CancelDataRepositoryTask marks a task as cancelled.
func (b *InMemoryBackend) CancelDataRepositoryTask(taskID string) error {
	b.mu.Lock("CancelDataRepositoryTask")
	defer b.mu.Unlock()

	t, ok := b.dataRepositoryTasks[taskID]
	if !ok {
		return ErrDataRepositoryTaskNotFound
	}

	t.Lifecycle = "CANCELING"

	return nil
}

// DescribeDataRepositoryTasks returns tasks, optionally filtered by ID.
func (b *InMemoryBackend) DescribeDataRepositoryTasks(
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*DataRepositoryTask, string, error) {
	b.mu.RLock("DescribeDataRepositoryTasks")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedDataRepositoryTask

	if len(ids) > 0 {
		for _, id := range ids {
			t, ok := b.dataRepositoryTasks[id]
			if !ok {
				return nil, "", ErrDataRepositoryTaskNotFound
			}

			all = append(all, t)
		}
	} else {
		for _, t := range b.dataRepositoryTasks {
			all = append(all, t)
		}

		sort.Slice(all, func(i, j int) bool { return all[i].TaskID < all[j].TaskID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].TaskID
	})

	result := make([]*DataRepositoryTask, end-start)
	for i, t := range all[start:end] {
		result[i] = t.toPublic()
	}

	return result, next, nil
}

// ---------------------------------------------------------------------------
// File Caches
// ---------------------------------------------------------------------------

type createFileCacheInput struct {
	FileCacheType      string `json:"FileCacheType"`
	StorageCapacityGiB int32  `json:"StorageCapacityGiB,omitempty"`
	Tags               []Tag  `json:"Tags,omitempty"`
}

// CreateFileCache creates a file cache.
func (b *InMemoryBackend) CreateFileCache(input *createFileCacheInput) (*FileCache, error) {
	if input.FileCacheType == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFileCache")
	defer b.mu.Unlock()

	id := "fc-" + uuid.New().String()[:17]
	arn := b.fcARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	c := &storedFileCache{
		CreationTime:       now,
		Tags:               tags,
		FileCacheID:        id,
		FileCacheType:      input.FileCacheType,
		Lifecycle:          lifecycleAvailable,
		ResourceARN:        arn,
		StorageCapacityGiB: input.StorageCapacityGiB,
	}

	b.fileCaches[id] = c
	b.tags[arn] = tags

	return c.toPublic(), nil
}

// DeleteFileCache removes a file cache.
func (b *InMemoryBackend) DeleteFileCache(fileCacheID string) error {
	b.mu.Lock("DeleteFileCache")
	defer b.mu.Unlock()

	c, ok := b.fileCaches[fileCacheID]
	if !ok {
		return ErrFileCacheNotFound
	}

	delete(b.fileCaches, fileCacheID)
	delete(b.tags, c.ResourceARN)

	return nil
}

// DescribeFileCaches returns file caches, optionally filtered by ID.
func (b *InMemoryBackend) DescribeFileCaches(
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*FileCache, string, error) {
	b.mu.RLock("DescribeFileCaches")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedFileCache

	if len(ids) > 0 {
		for _, id := range ids {
			c, ok := b.fileCaches[id]
			if !ok {
				return nil, "", ErrFileCacheNotFound
			}

			all = append(all, c)
		}
	} else {
		for _, c := range b.fileCaches {
			all = append(all, c)
		}

		sort.Slice(all, func(i, j int) bool { return all[i].FileCacheID < all[j].FileCacheID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].FileCacheID
	})

	result := make([]*FileCache, end-start)
	for i, c := range all[start:end] {
		result[i] = c.toPublic()
	}

	return result, next, nil
}

type updateFileCacheInput struct {
	FileCacheID        string `json:"FileCacheId"`
	StorageCapacityGiB int32  `json:"StorageCapacityGiB,omitempty"`
}

// UpdateFileCache updates a file cache.
func (b *InMemoryBackend) UpdateFileCache(input *updateFileCacheInput) (*FileCache, error) {
	b.mu.Lock("UpdateFileCache")
	defer b.mu.Unlock()

	c, ok := b.fileCaches[input.FileCacheID]
	if !ok {
		return nil, ErrFileCacheNotFound
	}

	if input.StorageCapacityGiB > 0 {
		c.StorageCapacityGiB = input.StorageCapacityGiB
	}

	return c.toPublic(), nil
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

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

	if _, ok := b.volumes[input.VolumeID]; !ok {
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

	b.snapshots[id] = s
	b.tags[arn] = tags

	return s.toPublic(), nil
}

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(snapshotID string) error {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	s, ok := b.snapshots[snapshotID]
	if !ok {
		return ErrSnapshotNotFound
	}

	delete(b.snapshots, snapshotID)
	delete(b.tags, s.ResourceARN)

	return nil
}

// DescribeSnapshots returns snapshots, optionally filtered by ID.
func (b *InMemoryBackend) DescribeSnapshots(
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
			s, ok := b.snapshots[id]
			if !ok {
				return nil, "", ErrSnapshotNotFound
			}

			all = append(all, s)
		}
	} else {
		for _, s := range b.snapshots {
			all = append(all, s)
		}

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

	s, ok := b.snapshots[input.SnapshotID]
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

	v, ok := b.volumes[input.VolumeID]
	if !ok {
		return nil, ErrVolumeNotFound
	}

	return v.toPublic(), nil
}

// ---------------------------------------------------------------------------
// Storage Virtual Machines
// ---------------------------------------------------------------------------

type createStorageVirtualMachineInput struct {
	FileSystemID            string `json:"FileSystemId"`
	Name                    string `json:"Name"`
	Subtype                 string `json:"Subtype,omitempty"`
	RootVolumeSecurityStyle string `json:"RootVolumeSecurityStyle,omitempty"`
	Tags                    []Tag  `json:"Tags,omitempty"`
}

// CreateStorageVirtualMachine creates an SVM on an ONTAP file system.
func (b *InMemoryBackend) CreateStorageVirtualMachine(
	input *createStorageVirtualMachineInput,
) (*StorageVirtualMachine, error) {
	if input.FileSystemID == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateStorageVirtualMachine")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[input.FileSystemID]; !ok {
		return nil, ErrFileSystemNotFound
	}

	id := "svm-" + uuid.New().String()[:17]
	arn := b.svmARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	svm := &storedStorageVirtualMachine{
		CreationTime:            now,
		Tags:                    tags,
		StorageVirtualMachineID: id,
		FileSystemID:            input.FileSystemID,
		Name:                    input.Name,
		Lifecycle:               lifecycleAvailable,
		ResourceARN:             arn,
		Subtype:                 input.Subtype,
		RootVolumeSecurityStyle: input.RootVolumeSecurityStyle,
	}

	b.storageVirtualMachines[id] = svm
	b.tags[arn] = tags

	return svm.toPublic(), nil
}

// DeleteStorageVirtualMachine removes an SVM.
func (b *InMemoryBackend) DeleteStorageVirtualMachine(svmID string) error {
	b.mu.Lock("DeleteStorageVirtualMachine")
	defer b.mu.Unlock()

	svm, ok := b.storageVirtualMachines[svmID]
	if !ok {
		return ErrStorageVirtualMachineNotFound
	}

	delete(b.storageVirtualMachines, svmID)
	delete(b.tags, svm.ResourceARN)

	return nil
}

// DescribeStorageVirtualMachines returns SVMs, optionally filtered by ID.
func (b *InMemoryBackend) DescribeStorageVirtualMachines(
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*StorageVirtualMachine, string, error) {
	b.mu.RLock("DescribeStorageVirtualMachines")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedStorageVirtualMachine

	if len(ids) > 0 {
		for _, id := range ids {
			svm, ok := b.storageVirtualMachines[id]
			if !ok {
				return nil, "", ErrStorageVirtualMachineNotFound
			}

			all = append(all, svm)
		}
	} else {
		for _, svm := range b.storageVirtualMachines {
			all = append(all, svm)
		}

		sort.Slice(all, func(i, j int) bool {
			return all[i].StorageVirtualMachineID < all[j].StorageVirtualMachineID
		})
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].StorageVirtualMachineID
	})

	result := make([]*StorageVirtualMachine, end-start)
	for i, svm := range all[start:end] {
		result[i] = svm.toPublic()
	}

	return result, next, nil
}

type updateStorageVirtualMachineInput struct {
	StorageVirtualMachineID string `json:"StorageVirtualMachineId"`
	Subtype                 string `json:"Subtype,omitempty"`
}

// UpdateStorageVirtualMachine updates an SVM.
func (b *InMemoryBackend) UpdateStorageVirtualMachine(
	input *updateStorageVirtualMachineInput,
) (*StorageVirtualMachine, error) {
	b.mu.Lock("UpdateStorageVirtualMachine")
	defer b.mu.Unlock()

	svm, ok := b.storageVirtualMachines[input.StorageVirtualMachineID]
	if !ok {
		return nil, ErrStorageVirtualMachineNotFound
	}

	if input.Subtype != "" {
		svm.Subtype = input.Subtype
	}

	return svm.toPublic(), nil
}

// ---------------------------------------------------------------------------
// Volumes
// ---------------------------------------------------------------------------

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
		if _, ok := b.fileSystems[input.FileSystemID]; !ok {
			return nil, ErrFileSystemNotFound
		}
	}

	if input.StorageVirtualMachineID != "" {
		if _, ok := b.storageVirtualMachines[input.StorageVirtualMachineID]; !ok {
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

	b.volumes[id] = v
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

	src, ok := b.backups[input.BackupID]
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

	b.volumes[id] = v
	b.tags[arn] = tags

	return v.toPublic(), nil
}

// DeleteVolume removes a volume.
func (b *InMemoryBackend) DeleteVolume(volumeID string) error {
	b.mu.Lock("DeleteVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}

	delete(b.volumes, volumeID)
	delete(b.tags, v.ResourceARN)

	return nil
}

// DescribeVolumes returns volumes, optionally filtered by ID.
func (b *InMemoryBackend) DescribeVolumes(
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
			v, ok := b.volumes[id]
			if !ok {
				return nil, "", ErrVolumeNotFound
			}

			all = append(all, v)
		}
	} else {
		for _, v := range b.volumes {
			all = append(all, v)
		}

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

	v, ok := b.volumes[input.VolumeID]
	if !ok {
		return nil, ErrVolumeNotFound
	}

	if _, ok := b.snapshots[input.SnapshotID]; !ok {
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

	v, ok := b.volumes[input.VolumeID]
	if !ok {
		return nil, ErrVolumeNotFound
	}

	if input.Name != "" {
		v.Name = input.Name
	}

	return v.toPublic(), nil
}

// ---------------------------------------------------------------------------
// S3 Access Points
// ---------------------------------------------------------------------------

type createAndAttachS3AccessPointInput struct {
	Name         string `json:"Name"`
	FileSystemID string `json:"FileSystemId"`
	VolumeID     string `json:"VolumeId,omitempty"`
	Tags         []Tag  `json:"Tags,omitempty"`
}

// CreateAndAttachS3AccessPoint creates and attaches an S3 access point.
func (b *InMemoryBackend) CreateAndAttachS3AccessPoint(
	input *createAndAttachS3AccessPointInput,
) (*S3AccessPoint, error) {
	if input.Name == "" || input.FileSystemID == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateAndAttachS3AccessPoint")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[input.FileSystemID]; !ok {
		return nil, ErrFileSystemNotFound
	}

	arn := b.s3AccessPointARN(input.Name)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	ap := &storedS3AccessPoint{
		CreationTime: now,
		Tags:         tags,
		Name:         input.Name,
		FileSystemID: input.FileSystemID,
		VolumeID:     input.VolumeID,
		Lifecycle:    lifecycleAvailable,
		ResourceARN:  arn,
	}

	b.s3AccessPoints[input.Name] = ap
	b.tags[arn] = tags

	return ap.toPublic(), nil
}

// DetachAndDeleteS3AccessPoint removes an S3 access point.
func (b *InMemoryBackend) DetachAndDeleteS3AccessPoint(name, fileSystemID string) error {
	b.mu.Lock("DetachAndDeleteS3AccessPoint")
	defer b.mu.Unlock()

	ap, ok := b.s3AccessPoints[name]
	if !ok || ap.FileSystemID != fileSystemID {
		return ErrS3AccessPointNotFound
	}

	delete(b.s3AccessPoints, name)
	delete(b.tags, ap.ResourceARN)

	return nil
}

// DescribeS3AccessPointAttachments returns S3 access points.
func (b *InMemoryBackend) DescribeS3AccessPointAttachments(
	names []string,
	maxResults int32,
	nextToken string,
) ([]*S3AccessPoint, string, error) {
	b.mu.RLock("DescribeS3AccessPointAttachments")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedS3AccessPoint

	if len(names) > 0 {
		for _, name := range names {
			ap, ok := b.s3AccessPoints[name]
			if !ok {
				return nil, "", ErrS3AccessPointNotFound
			}

			all = append(all, ap)
		}
	} else {
		for _, ap := range b.s3AccessPoints {
			all = append(all, ap)
		}

		sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].Name
	})

	result := make([]*S3AccessPoint, end-start)
	for i, ap := range all[start:end] {
		result[i] = ap.toPublic()
	}

	return result, next, nil
}

// ---------------------------------------------------------------------------
// Shared VPC Configuration
// ---------------------------------------------------------------------------

// DescribeSharedVpcConfiguration returns the shared VPC configuration.
func (b *InMemoryBackend) DescribeSharedVpcConfiguration() (*SharedVpcConfiguration, error) {
	b.mu.RLock("DescribeSharedVpcConfiguration")
	defer b.mu.RUnlock()

	return &SharedVpcConfiguration{EnableSharedVpcOnFileSystemCreation: b.sharedVpcEnabled}, nil
}

type updateSharedVpcConfigurationInput struct {
	EnableSharedVpcOnFileSystemCreation string `json:"EnableSharedVpcOnFileSystemCreation"`
}

// UpdateSharedVpcConfiguration updates the shared VPC configuration.
func (b *InMemoryBackend) UpdateSharedVpcConfiguration(
	input *updateSharedVpcConfigurationInput,
) (*SharedVpcConfiguration, error) {
	b.mu.Lock("UpdateSharedVpcConfiguration")
	defer b.mu.Unlock()

	b.sharedVpcEnabled = input.EnableSharedVpcOnFileSystemCreation

	return &SharedVpcConfiguration{EnableSharedVpcOnFileSystemCreation: b.sharedVpcEnabled}, nil
}

// ---------------------------------------------------------------------------
// Miscellaneous
// ---------------------------------------------------------------------------

// ReleaseFileSystemNfsV3Locks releases NFS v3 locks on a file system.
func (b *InMemoryBackend) ReleaseFileSystemNfsV3Locks(fileSystemID string) error {
	b.mu.RLock("ReleaseFileSystemNfsV3Locks")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return ErrFileSystemNotFound
	}

	return nil
}

// StartMisconfiguredStateRecovery initiates recovery for a misconfigured file system.
func (b *InMemoryBackend) StartMisconfiguredStateRecovery(fileSystemID string) error {
	b.mu.Lock("StartMisconfiguredStateRecovery")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return ErrFileSystemNotFound
	}

	return nil
}

// ---------------------------------------------------------------------------
// ARN helpers
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) draARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:association/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) drtARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:task/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) fcARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:file-cache/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) snapshotARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:snapshot/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) svmARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:storage-virtual-machine/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) volumeARN(id string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:volume/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) s3AccessPointARN(name string) string {
	return fmt.Sprintf("arn:aws:fsx:%s:%s:s3-access-point/%s", b.region, b.accountID, name)
}

// ---------------------------------------------------------------------------
// Pagination helper
// ---------------------------------------------------------------------------

// paginate returns (start, end, nextToken) for a sorted slice of length n.
// keyFn maps index → token string.
func paginate(n, maxResults int, nextToken string, keyFn func(int) string) (int, int, string) {
	start := 0
	if nextToken != "" {
		for i := 0; i < n; i++ {
			if keyFn(i) == nextToken {
				start = i
				break
			}
		}
	}

	end := min(start+maxResults, n)

	var next string
	if end < n {
		next = keyFn(end)
	}

	return start, end, next
}
