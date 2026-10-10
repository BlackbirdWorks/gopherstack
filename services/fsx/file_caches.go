package fsx

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedFileCache struct {
	CreationTime         time.Time         `json:"creationTime"`
	Tags                 map[string]string `json:"tags"`
	FileCacheID          string            `json:"fileCacheId"`
	FileCacheType        string            `json:"fileCacheType"`
	FileCacheTypeVersion string            `json:"fileCacheTypeVersion,omitempty"`
	KmsKeyID             string            `json:"kmsKeyId,omitempty"`
	Lifecycle            string            `json:"lifecycle"`
	ResourceARN          string            `json:"resourceArn"`

	CopyTagsToDRAs                   *bool    `json:"copyTagsToDras,omitempty"`
	LustreDeploymentType             string   `json:"lustreDeploymentType,omitempty"`
	LustreMountName                  string   `json:"lustreMountName,omitempty"`
	LustreWeeklyMaintenanceStartTime string   `json:"lustreWeeklyMaintenanceStartTime,omitempty"`
	SubnetIDs                        []string `json:"subnetIds,omitempty"`
	StorageCapacityGiB               int32    `json:"storageCapacityGiB,omitempty"`
	LustreMetadataStorageCapacity    int32    `json:"lustreMetadataStorageCapacity,omitempty"`
	LustrePerUnitStorageThroughput   int32    `json:"lustrePerUnitStorageThroughput,omitempty"`
}

// lustreConfiguration builds the FileCacheLustreConfiguration response block.
// CreateFileCache requires LustreConfiguration (see applyFileCacheLustreConfig),
// so this is always non-nil once a file cache exists.
func (c *storedFileCache) lustreConfiguration() *FileCacheLustreConfiguration {
	return &FileCacheLustreConfiguration{
		MetadataConfiguration: &FileCacheLustreMetadataConfiguration{
			StorageCapacity: c.LustreMetadataStorageCapacity,
		},
		DeploymentType:             c.LustreDeploymentType,
		MountName:                  c.LustreMountName,
		WeeklyMaintenanceStartTime: c.LustreWeeklyMaintenanceStartTime,
		PerUnitStorageThroughput:   c.LustrePerUnitStorageThroughput,
	}
}

func (b *InMemoryBackend) fileCacheDRAIDsLocked(cacheID string) []string {
	var ids []string

	b.dataRepositoryAssocs.Range(func(a *storedDataRepositoryAssoc) bool {
		if a.FileCacheID == cacheID {
			ids = append(ids, a.AssociationID)
		}

		return true
	})

	sort.Strings(ids)

	return ids
}

func (c *storedFileCache) toPublic(draIDs []string) *FileCache {
	return &FileCache{
		DataRepositoryAssociationIDs: draIDs,
		CopyTagsToDRAs:               c.CopyTagsToDRAs,
		CreationTime:                 epochTime(c.CreationTime),
		LustreConfiguration:          c.lustreConfiguration(),
		FileCacheID:                  c.FileCacheID,
		FileCacheType:                c.FileCacheType,
		FileCacheTypeVersion:         c.FileCacheTypeVersion,
		KmsKeyID:                     c.KmsKeyID,
		Lifecycle:                    c.Lifecycle,
		ResourceARN:                  c.ResourceARN,
		SubnetIDs:                    c.SubnetIDs,
		StorageCapacityGiB:           c.StorageCapacityGiB,
	}
}

// toPublicCreating renders the CreateFileCache response shape, which -- unlike
// DescribeFileCaches/UpdateFileCache's toPublic() above -- includes Tags (see
// FileCacheCreating in interfaces.go).
func (c *storedFileCache) toPublicCreating(draIDs []string) *FileCacheCreating {
	return &FileCacheCreating{
		DataRepositoryAssociationIDs: draIDs,
		CopyTagsToDRAs:               c.CopyTagsToDRAs,
		CreationTime:                 epochTime(c.CreationTime),
		LustreConfiguration:          c.lustreConfiguration(),
		FileCacheID:                  c.FileCacheID,
		FileCacheType:                c.FileCacheType,
		FileCacheTypeVersion:         c.FileCacheTypeVersion,
		KmsKeyID:                     c.KmsKeyID,
		Lifecycle:                    c.Lifecycle,
		ResourceARN:                  c.ResourceARN,
		SubnetIDs:                    c.SubnetIDs,
		StorageCapacityGiB:           c.StorageCapacityGiB,
		Tags:                         tagsMapToSlice(c.Tags),
	}
}

// createFileCacheLustreConfigurationInput mirrors
// CreateFileCacheLustreConfiguration (fsx@v1.68.4 types/types.go:574).
// DeploymentType, MetadataConfiguration, and PerUnitStorageThroughput are
// required members on the real SDK type.
type createFileCacheLustreConfigurationInput struct {
	MetadataConfiguration      *createFileCacheLustreMetadataInput `json:"MetadataConfiguration"`
	PerUnitStorageThroughput   *int32                              `json:"PerUnitStorageThroughput,omitempty"`
	DeploymentType             string                              `json:"DeploymentType,omitempty"`
	WeeklyMaintenanceStartTime string                              `json:"WeeklyMaintenanceStartTime,omitempty"`
}

// createFileCacheLustreMetadataInput mirrors
// FileCacheLustreMetadataConfiguration (fsx@v1.68.4 types/types.go:2550).
// StorageCapacity is a required member on the real SDK type.
type createFileCacheLustreMetadataInput struct {
	StorageCapacity *int32 `json:"StorageCapacity,omitempty"`
}

type createFileCacheInput struct {
	LustreConfiguration        *createFileCacheLustreConfigurationInput `json:"LustreConfiguration"`
	FileCacheType              string                                   `json:"FileCacheType"`
	FileCacheTypeVersion       string                                   `json:"FileCacheTypeVersion"`
	KmsKeyID                   string                                   `json:"KmsKeyId,omitempty"`
	ClientRequestToken         string                                   `json:"ClientRequestToken,omitempty"`
	Tags                       []Tag                                    `json:"Tags,omitempty"`
	SecurityGroupIDs           []string                                 `json:"SecurityGroupIds,omitempty"`
	DataRepositoryAssociations []fileCacheDRAInput                      `json:"DataRepositoryAssociations,omitempty"`

	CopyTagsToDRAs *bool `json:"CopyTagsToDataRepositoryAssociations,omitempty"`

	SubnetIDs          []string `json:"SubnetIds"`
	StorageCapacityGiB int32    `json:"StorageCapacity,omitempty"`
}

// applyFileCacheLustreConfig validates and applies cfg onto c.
// LustreConfiguration is a required CreateFileCacheInput member in effect
// (FileCacheType is always LUSTRE, and real AWS's MissingFileCacheConfiguration
// exception -- "A cache configuration is required for this operation." --
// covers exactly this case), matching the CreateFileSystem
// per-type-config-block-required pattern (see applyWindowsConfig et al. in
// file_systems.go): an absent block returns MissingFileCacheConfiguration, a
// present-but-incomplete block returns BadRequest.
func applyFileCacheLustreConfig(c *storedFileCache, cfg *createFileCacheLustreConfigurationInput) error {
	if cfg == nil {
		return ErrMissingFileCacheConfiguration
	}

	if cfg.DeploymentType == "" {
		return fmt.Errorf("%w: LustreConfiguration.DeploymentType is required", ErrValidation)
	}

	if cfg.MetadataConfiguration == nil || cfg.MetadataConfiguration.StorageCapacity == nil {
		return fmt.Errorf(
			"%w: LustreConfiguration.MetadataConfiguration.StorageCapacity is required", ErrValidation,
		)
	}

	if cfg.PerUnitStorageThroughput == nil {
		return fmt.Errorf("%w: LustreConfiguration.PerUnitStorageThroughput is required", ErrValidation)
	}

	if err := validateWeeklyMaintenanceTime(cfg.WeeklyMaintenanceStartTime); err != nil {
		return err
	}

	c.LustreDeploymentType = cfg.DeploymentType
	c.LustreWeeklyMaintenanceStartTime = cfg.WeeklyMaintenanceStartTime
	c.LustreMetadataStorageCapacity = *cfg.MetadataConfiguration.StorageCapacity
	c.LustrePerUnitStorageThroughput = *cfg.PerUnitStorageThroughput
	c.LustreMountName = generateLustreMountName()

	return nil
}

// CreateFileCache creates a file cache. FileCacheTypeVersion and SubnetIds
// are, along with FileCacheType/StorageCapacity, required
// CreateFileCacheInput members (verified against
// validateOpCreateFileCacheInput, validators.go) that the pre-fix request
// never read at all -- StorageCapacity was already wired.
func (b *InMemoryBackend) CreateFileCache(input *createFileCacheInput) (*FileCacheCreating, error) {
	if input.FileCacheType == "" {
		return nil, ErrValidation
	}

	if input.FileCacheTypeVersion == "" {
		return nil, fmt.Errorf("%w: FileCacheTypeVersion is required", ErrValidation)
	}

	if input.StorageCapacityGiB == 0 {
		return nil, fmt.Errorf("%w: StorageCapacity is required", ErrValidation)
	}

	if len(input.SubnetIDs) == 0 {
		return nil, fmt.Errorf("%w: SubnetIds is required", ErrValidation)
	}

	if err := validateCreateTags(input.Tags); err != nil {
		return nil, err
	}

	if err := validateSecurityGroupIDs(input.SecurityGroupIDs); err != nil {
		return nil, err
	}

	if err := validateFileCacheDRAs(input.DataRepositoryAssociations); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFileCache")
	defer b.mu.Unlock()

	fp, replayID, err := b.replayTokenLocked("CreateFileCache", input.ClientRequestToken, input)
	if err != nil {
		return nil, err
	}

	if existing, ok := b.fileCaches.Get(replayID); ok {
		return existing.toPublicCreating(b.fileCacheDRAIDsLocked(existing.FileCacheID)), nil
	}

	id := newFileCacheID()
	arn := b.fcARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	c := &storedFileCache{
		CreationTime:         now,
		Tags:                 tags,
		FileCacheID:          id,
		FileCacheType:        input.FileCacheType,
		FileCacheTypeVersion: input.FileCacheTypeVersion,
		KmsKeyID:             input.KmsKeyID,
		Lifecycle:            lifecycleAvailable,
		ResourceARN:          arn,
		SubnetIDs:            input.SubnetIDs,
		StorageCapacityGiB:   input.StorageCapacityGiB,
	}

	if err = applyFileCacheLustreConfig(c, input.LustreConfiguration); err != nil {
		return nil, err
	}

	c.CopyTagsToDRAs = input.CopyTagsToDRAs

	b.fileCaches.Put(c)
	b.tags[arn] = tags

	draIDs := b.createFileCacheDRAsLocked(c, input.DataRepositoryAssociations)

	b.recordTokenLocked("CreateFileCache", input.ClientRequestToken, fp, id)

	return c.toPublicCreating(draIDs), nil
}

// DeleteFileCache removes a file cache.
func (b *InMemoryBackend) DeleteFileCache(fileCacheID string) error {
	b.mu.Lock("DeleteFileCache")
	defer b.mu.Unlock()

	c, ok := b.fileCaches.Get(fileCacheID)
	if !ok {
		return ErrFileCacheNotFound
	}

	for _, id := range b.fileCacheDRAIDsLocked(fileCacheID) {
		if a, found := b.dataRepositoryAssocs.Get(id); found {
			delete(b.tags, a.ResourceARN)
		}

		b.dataRepositoryAssocs.Delete(id)
	}

	b.fileCaches.Delete(fileCacheID)
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
			c, ok := b.fileCaches.Get(id)
			if !ok {
				return nil, "", ErrFileCacheNotFound
			}

			all = append(all, c)
		}
	} else {
		all = b.fileCaches.All()

		sort.Slice(all, func(i, j int) bool { return all[i].FileCacheID < all[j].FileCacheID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].FileCacheID
	})

	result := make([]*FileCache, end-start)
	for i, c := range all[start:end] {
		result[i] = c.toPublic(b.fileCacheDRAIDsLocked(c.FileCacheID))
	}

	return result, next, nil
}

type updateFileCacheLustreInput struct {
	WeeklyMaintenanceStartTime string `json:"WeeklyMaintenanceStartTime,omitempty"`
}

type updateFileCacheInput struct {
	LustreConfiguration *updateFileCacheLustreInput `json:"LustreConfiguration,omitempty"`
	FileCacheID         string                      `json:"FileCacheId"`
	ClientRequestToken  string                      `json:"ClientRequestToken,omitempty"`
}

// UpdateFileCache updates a file cache.
func (b *InMemoryBackend) UpdateFileCache(input *updateFileCacheInput) (*FileCache, error) {
	b.mu.Lock("UpdateFileCache")
	defer b.mu.Unlock()

	fp, _, err := b.replayTokenLocked("UpdateFileCache", input.ClientRequestToken, input)
	if err != nil {
		return nil, err
	}

	c, ok := b.fileCaches.Get(input.FileCacheID)
	if !ok {
		return nil, ErrFileCacheNotFound
	}

	if cfg := input.LustreConfiguration; cfg != nil && cfg.WeeklyMaintenanceStartTime != "" {
		if err = validateWeeklyMaintenanceTime(cfg.WeeklyMaintenanceStartTime); err != nil {
			return nil, err
		}

		c.LustreWeeklyMaintenanceStartTime = cfg.WeeklyMaintenanceStartTime
	}

	b.recordTokenLocked("UpdateFileCache", input.ClientRequestToken, fp, c.FileCacheID)

	return c.toPublic(b.fileCacheDRAIDsLocked(c.FileCacheID)), nil
}

func (b *InMemoryBackend) fcARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("file-cache/%s", id))
}

// fileCacheDRAInput mirrors types.FileCacheDataRepositoryAssociation.
type fileCacheDRAInput struct {
	NFS                          *FileCacheNFSConfiguration `json:"NFS,omitempty"`
	DataRepositoryPath           string                     `json:"DataRepositoryPath"`
	FileCachePath                string                     `json:"FileCachePath"`
	DataRepositorySubdirectories []string                   `json:"DataRepositorySubdirectories,omitempty"`
}

func validateFileCacheDRAs(dras []fileCacheDRAInput) error {
	for _, d := range dras {
		if d.DataRepositoryPath == "" || d.FileCachePath == "" {
			return fmt.Errorf("%w: DataRepositoryAssociations need DataRepositoryPath and FileCachePath", ErrValidation)
		}

		if d.NFS != nil && d.NFS.Version != "NFS3" {
			return fmt.Errorf("%w: NFS.Version must be NFS3", ErrValidation)
		}
	}

	return nil
}

// createFileCacheDRAsLocked creates the data repository associations declared
// on CreateFileCache and returns their IDs. Caller must hold b.mu.
func (b *InMemoryBackend) createFileCacheDRAsLocked(c *storedFileCache, dras []fileCacheDRAInput) []string {
	ids := make([]string, 0, len(dras))

	copyTags := c.CopyTagsToDRAs != nil && *c.CopyTagsToDRAs

	for _, d := range dras {
		id := newDataRepositoryAssociationID()
		arn := b.draARN(id)
		tags := map[string]string{}

		if copyTags {
			maps.Copy(tags, c.Tags)
		}

		b.dataRepositoryAssocs.Put(&storedDataRepositoryAssoc{
			CreationTime:       c.CreationTime,
			Tags:               tags,
			AssociationID:      id,
			FileCacheID:        c.FileCacheID,
			FileCachePath:      d.FileCachePath,
			DataRepositoryPath: d.DataRepositoryPath,
			Subdirectories:     slices.Clone(d.DataRepositorySubdirectories),
			NFS:                cloneNFSConfig(d.NFS),
			Lifecycle:          lifecycleAvailable,
			ResourceARN:        arn,
		})
		b.tags[arn] = tags

		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

const weeklyMaintenanceLen = 7

func digitIn(c, lo, hi byte) bool { return c >= lo && c <= hi }

// validateWeeklyMaintenanceTime checks the "d:HH:MM" format (d is 1-7).
func validateWeeklyMaintenanceTime(v string) error {
	if v == "" {
		return nil
	}

	ok := len(v) == weeklyMaintenanceLen && digitIn(v[0], '1', '7') && v[1] == ':' && v[4] == ':' &&
		digitIn(v[2], '0', '2') && digitIn(v[3], '0', '9') && (v[2] != '2' || v[3] <= '3') &&
		digitIn(v[5], '0', '5') && digitIn(v[6], '0', '9')
	if !ok {
		return fmt.Errorf("%w: WeeklyMaintenanceStartTime %q must be formatted d:HH:MM", ErrValidation, v)
	}

	return nil
}
