package fsx

import (
	"fmt"
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
	Lifecycle            string            `json:"lifecycle"`
	ResourceARN          string            `json:"resourceArn"`
	SubnetIDs            []string          `json:"subnetIds,omitempty"`
	StorageCapacityGiB   int32             `json:"storageCapacityGiB,omitempty"`
}

func (c *storedFileCache) toPublic() *FileCache {
	return &FileCache{
		CreationTime:         epochTime(c.CreationTime),
		FileCacheID:          c.FileCacheID,
		FileCacheType:        c.FileCacheType,
		FileCacheTypeVersion: c.FileCacheTypeVersion,
		Lifecycle:            c.Lifecycle,
		ResourceARN:          c.ResourceARN,
		SubnetIDs:            c.SubnetIDs,
		StorageCapacityGiB:   c.StorageCapacityGiB,
		Tags:                 tagsMapToSlice(c.Tags),
	}
}

type createFileCacheInput struct {
	FileCacheType        string   `json:"FileCacheType"`
	FileCacheTypeVersion string   `json:"FileCacheTypeVersion"`
	Tags                 []Tag    `json:"Tags,omitempty"`
	SubnetIDs            []string `json:"SubnetIds"`
	StorageCapacityGiB   int32    `json:"StorageCapacity,omitempty"`
}

// CreateFileCache creates a file cache. FileCacheTypeVersion and SubnetIds
// are, along with FileCacheType/StorageCapacity, required
// CreateFileCacheInput members (verified against
// validateOpCreateFileCacheInput, validators.go) that the pre-fix request
// never read at all -- StorageCapacity was already wired.
func (b *InMemoryBackend) CreateFileCache(input *createFileCacheInput) (*FileCache, error) {
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

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFileCache")
	defer b.mu.Unlock()

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
		Lifecycle:            lifecycleAvailable,
		ResourceARN:          arn,
		SubnetIDs:            input.SubnetIDs,
		StorageCapacityGiB:   input.StorageCapacityGiB,
	}

	b.fileCaches.Put(c)
	b.tags[arn] = tags

	return c.toPublic(), nil
}

// DeleteFileCache removes a file cache.
func (b *InMemoryBackend) DeleteFileCache(fileCacheID string) error {
	b.mu.Lock("DeleteFileCache")
	defer b.mu.Unlock()

	c, ok := b.fileCaches.Get(fileCacheID)
	if !ok {
		return ErrFileCacheNotFound
	}

	b.fileCaches.Delete(fileCacheID)
	delete(b.tags, c.ResourceARN)

	return nil
}

// DescribeFileCaches returns file caches, optionally filtered by ID.
func (b *InMemoryBackend) DescribeFileCaches( //nolint:dupl // existing issue.
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

	c, ok := b.fileCaches.Get(input.FileCacheID)
	if !ok {
		return nil, ErrFileCacheNotFound
	}

	if input.StorageCapacityGiB > 0 {
		c.StorageCapacityGiB = input.StorageCapacityGiB
	}

	return c.toPublic(), nil
}

func (b *InMemoryBackend) fcARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("file-cache/%s", id))
}
