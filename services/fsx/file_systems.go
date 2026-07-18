package fsx

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// storedFileSystem is the persisted form of a FileSystem.
// time.Time is first: non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedFileSystem struct {
	CreationTime        time.Time         `json:"creationTime"`
	Tags                map[string]string `json:"tags"`
	FileSystemID        string            `json:"fileSystemId"`
	FileSystemType      string            `json:"fileSystemType"`
	Lifecycle           string            `json:"lifecycle"`
	ResourceARN         string            `json:"resourceArn"`
	DNSName             string            `json:"dnsName,omitempty"`
	StorageType         string            `json:"storageType,omitempty"`
	VpcID               string            `json:"vpcId,omitempty"`
	OwnerID             string            `json:"ownerId,omitempty"`
	DeploymentType      string            `json:"deploymentType,omitempty"`
	MountName           string            `json:"mountName,omitempty"`
	SubnetIDs           []string          `json:"subnetIds,omitempty"`
	NetworkInterfaceIDs []string          `json:"networkInterfaceIds,omitempty"`
	StorageCapacityGiB  int32             `json:"storageCapacity,omitempty"`
}

func (s *storedFileSystem) toFileSystem() *FileSystem {
	fs := &FileSystem{
		CreationTime:        epochTime(s.CreationTime),
		Tags:                tagsMapToSlice(s.Tags),
		FileSystemID:        s.FileSystemID,
		FileSystemType:      s.FileSystemType,
		Lifecycle:           s.Lifecycle,
		ResourceARN:         s.ResourceARN,
		DNSName:             s.DNSName,
		StorageCapacityGiB:  s.StorageCapacityGiB,
		StorageType:         s.StorageType,
		VpcID:               s.VpcID,
		OwnersID:            s.OwnerID,
		SubnetIDs:           s.SubnetIDs,
		NetworkInterfaceIDs: s.NetworkInterfaceIDs,
	}

	// AWS always returns a LustreConfiguration block for Lustre file systems.
	// The terraform-provider-aws Read path treats a nil LustreConfiguration as
	// an empty result, so a Lustre file system must echo this back.
	if s.FileSystemType == fileSystemTypeLustre {
		fs.LustreConfiguration = &LustreConfiguration{
			DeploymentType: s.DeploymentType,
			MountName:      s.MountName,
			DataRepositoryConfiguration: &DataRepositoryConfiguration{
				Lifecycle: dataRepositoryLifecycleDisabled,
			},
		}
	}

	return fs
}

// createFileSystemInput holds parameters for CreateFileSystem.
type createFileSystemInput struct {
	LustreConfiguration *createLustreConfiguration `json:"LustreConfiguration,omitempty"`
	FileSystemType      string                     `json:"FileSystemType"`
	StorageType         string                     `json:"StorageType,omitempty"`
	VpcID               string                     `json:"VpcId,omitempty"`
	Tags                []Tag                      `json:"Tags,omitempty"`
	SubnetIDs           []string                   `json:"SubnetIds,omitempty"`
	StorageCapacityGiB  int32                      `json:"StorageCapacity,omitempty"`
}

// createLustreConfiguration mirrors the CreateFileSystemLustreConfiguration
// block sent by the AWS provider for Lustre file systems.
type createLustreConfiguration struct {
	DeploymentType string `json:"DeploymentType,omitempty"`
}

// CreateFileSystem creates a new file system.
func (b *InMemoryBackend) CreateFileSystem(input *createFileSystemInput) (*FileSystem, error) {
	if input.FileSystemType == "" {
		return nil, ErrValidation
	}

	minCapByType := map[string]int32{
		fileSystemTypeLustre:  minStorageCapacityLustre,
		fileSystemTypeWindows: minStorageCapacityWindows,
		fileSystemTypeONTAP:   minStorageCapacityONTAP,
		fileSystemTypeOpenZFS: minStorageCapacityOpenZFS,
	}
	minCap, ok := minCapByType[input.FileSystemType]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported FileSystemType %q", ErrValidation, input.FileSystemType)
	}

	if input.StorageCapacityGiB == 0 {
		input.StorageCapacityGiB = minCap
	} else if input.StorageCapacityGiB < minCap {
		return nil, fmt.Errorf(
			"%w: StorageCapacity %d GiB is below the minimum of %d GiB for %s",
			ErrValidation, input.StorageCapacityGiB, minCap, input.FileSystemType,
		)
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	id := "fs-" + uuid.New().String()[:17]
	arn := b.fsARN(id)
	now := time.Now().UTC()

	tags := tagsSliceToMap(input.Tags)

	fs := &storedFileSystem{
		CreationTime:        now,
		Tags:                tags,
		FileSystemID:        id,
		FileSystemType:      input.FileSystemType,
		Lifecycle:           lifecycleAvailable,
		ResourceARN:         arn,
		DNSName:             fmt.Sprintf("%s.fsx.%s.amazonaws.com", id, b.region),
		StorageCapacityGiB:  input.StorageCapacityGiB,
		StorageType:         input.StorageType,
		VpcID:               input.VpcID,
		OwnerID:             b.accountID,
		SubnetIDs:           input.SubnetIDs,
		NetworkInterfaceIDs: networkInterfaceIDsForSubnets(input.SubnetIDs),
	}

	if input.FileSystemType == fileSystemTypeLustre {
		fs.MountName = generateLustreMountName()
		if input.LustreConfiguration != nil {
			fs.DeploymentType = input.LustreConfiguration.DeploymentType
		}

		if fs.DeploymentType == "" {
			fs.DeploymentType = lustreDeploymentTypeScratch1
		}
	}

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	b.fileSystems.Put(fs)
	b.tags[arn] = tags

	return fs.toFileSystem(), nil
}

// generateLustreMountName returns a short, lowercase alphanumeric mount name in
// the style AWS assigns to Lustre file systems (e.g. "abcd1234").
func generateLustreMountName() string {
	raw := strings.ReplaceAll(uuid.New().String(), "-", "")
	if len(raw) > lustreMountNameLen {
		raw = raw[:lustreMountNameLen]
	}

	return raw
}

// networkInterfaceIDsForSubnets returns one synthetic ENI ID per subnet, as AWS
// attaches an elastic network interface to the file system in each subnet.
func networkInterfaceIDsForSubnets(subnetIDs []string) []string {
	if len(subnetIDs) == 0 {
		return nil
	}

	enis := make([]string, 0, len(subnetIDs))
	for range subnetIDs {
		enis = append(enis, "eni-"+strings.ReplaceAll(uuid.New().String(), "-", "")[:17])
	}

	return enis
}

// DescribeFileSystems returns file systems, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeFileSystems(
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*FileSystem, string, error) {
	b.mu.RLock("DescribeFileSystems")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedFileSystem

	if len(ids) > 0 {
		for _, id := range ids {
			fs, ok := b.fileSystems.Get(id)
			if !ok {
				return nil, "", ErrFileSystemNotFound
			}

			all = append(all, fs)
		}
	} else {
		all = b.fileSystems.All()

		sort.Slice(all, func(i, j int) bool { return all[i].FileSystemID < all[j].FileSystemID })
	}

	start := 0
	if nextToken != "" {
		for i, fs := range all {
			if fs.FileSystemID == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+int(maxResults), len(all))
	page := all[start:end]

	var next string
	if end < len(all) {
		next = all[end].FileSystemID
	}

	result := make([]*FileSystem, len(page))
	for i, fs := range page {
		result[i] = fs.toFileSystem()
	}

	return result, next, nil
}

// DeleteFileSystem removes a file system.
func (b *InMemoryBackend) DeleteFileSystem(fileSystemID string) error {
	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(fileSystemID)
	if !ok {
		return ErrFileSystemNotFound
	}

	b.fileSystems.Delete(fileSystemID)
	delete(b.tags, fs.ResourceARN)

	return nil
}

// updateFileSystemInput holds parameters for UpdateFileSystem.
type updateFileSystemInput struct {
	FileSystemID       string `json:"FileSystemId"`
	StorageCapacityGiB int32  `json:"StorageCapacity,omitempty"`
}

// UpdateFileSystem updates a file system's configuration.
func (b *InMemoryBackend) UpdateFileSystem(input *updateFileSystemInput) (*FileSystem, error) {
	b.mu.Lock("UpdateFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(input.FileSystemID)
	if !ok {
		return nil, ErrFileSystemNotFound
	}

	if input.StorageCapacityGiB > 0 {
		fs.StorageCapacityGiB = input.StorageCapacityGiB
	}

	return fs.toFileSystem(), nil
}

// createFileSystemFromBackupInput holds parameters for CreateFileSystemFromBackup.
type createFileSystemFromBackupInput struct {
	BackupID           string `json:"BackupId"`
	FileSystemType     string `json:"FileSystemType,omitempty"`
	StorageType        string `json:"StorageType,omitempty"`
	VpcID              string `json:"VpcId,omitempty"`
	Tags               []Tag  `json:"Tags,omitempty"`
	StorageCapacityGiB int32  `json:"StorageCapacity,omitempty"`
}

// CreateFileSystemFromBackup creates a new file system from an existing backup.
func (b *InMemoryBackend) CreateFileSystemFromBackup(input *createFileSystemFromBackupInput) (*FileSystem, error) {
	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFileSystemFromBackup")
	defer b.mu.Unlock()

	src, ok := b.backups.Get(input.BackupID)
	if !ok {
		return nil, ErrBackupNotFound
	}

	srcFS, _ := b.fileSystems.Get(src.FileSystemID)

	fsType := input.FileSystemType
	if fsType == "" && srcFS != nil {
		fsType = srcFS.FileSystemType
	}

	id := "fs-" + uuid.New().String()[:17]
	arn := b.fsARN(id)
	now := time.Now().UTC()

	capacity := input.StorageCapacityGiB
	if capacity == 0 && srcFS != nil {
		capacity = srcFS.StorageCapacityGiB
	}

	storageType := input.StorageType
	if storageType == "" && srcFS != nil {
		storageType = srcFS.StorageType
	}

	tags := tagsSliceToMap(input.Tags)

	fs := &storedFileSystem{
		CreationTime:       now,
		Tags:               tags,
		FileSystemID:       id,
		FileSystemType:     fsType,
		Lifecycle:          lifecycleAvailable,
		ResourceARN:        arn,
		StorageCapacityGiB: capacity,
		StorageType:        storageType,
		VpcID:              input.VpcID,
		OwnerID:            b.accountID,
	}

	b.fileSystems.Put(fs)
	b.tags[arn] = tags

	return fs.toFileSystem(), nil
}

func (b *InMemoryBackend) fsARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("file-system/%s", id))
}

// ---------------------------------------------------------------------------
// Aliases
// ---------------------------------------------------------------------------

// AssociateFileSystemAliases adds DNS aliases to a file system.
func (b *InMemoryBackend) AssociateFileSystemAliases(fileSystemID string, aliases []string) ([]FileSystemAlias, error) {
	b.mu.Lock("AssociateFileSystemAliases")
	defer b.mu.Unlock()

	if !b.fileSystems.Has(fileSystemID) {
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

	if !b.fileSystems.Has(fileSystemID) {
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

	if !b.fileSystems.Has(fileSystemID) {
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
// Miscellaneous
// ---------------------------------------------------------------------------

// ReleaseFileSystemNfsV3Locks releases NFS v3 locks on a file system.
func (b *InMemoryBackend) ReleaseFileSystemNfsV3Locks(fileSystemID string) error {
	b.mu.RLock("ReleaseFileSystemNfsV3Locks")
	defer b.mu.RUnlock()

	if !b.fileSystems.Has(fileSystemID) {
		return ErrFileSystemNotFound
	}

	return nil
}

// StartMisconfiguredStateRecovery initiates recovery for a misconfigured file system.
func (b *InMemoryBackend) StartMisconfiguredStateRecovery(fileSystemID string) error {
	b.mu.Lock("StartMisconfiguredStateRecovery")
	defer b.mu.Unlock()

	if !b.fileSystems.Has(fileSystemID) {
		return ErrFileSystemNotFound
	}

	return nil
}
