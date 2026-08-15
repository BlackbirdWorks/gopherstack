package rekognition

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	maxCollectionsPerPage = 4096
	maxCollectionIDLen    = 255
)

// validateCollectionID checks that a collection ID is non-empty and within AWS length limits.
func validateCollectionID(id string) error {
	if id == "" || len(id) > maxCollectionIDLen {
		return fmt.Errorf("%w: CollectionId must be between 1 and %d characters", ErrValidation, maxCollectionIDLen)
	}

	return nil
}

func (b *InMemoryBackend) collectionARN(collectionID string) string {
	return arn.Build("rekognition", b.region, b.accountID, fmt.Sprintf("collection/%s", collectionID))
}

// CreateCollection creates a new face collection.
func (b *InMemoryBackend) CreateCollection(collectionID string, tags map[string]string) (*Collection, error) {
	if err := validateCollectionID(collectionID); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCollection")
	defer b.mu.Unlock()

	if b.collections.Has(collectionID) {
		return nil, ErrCollectionAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	c := &storedCollection{
		CollectionID:      collectionID,
		CollectionARN:     b.collectionARN(collectionID),
		FaceModelVersion:  faceModelVersion,
		CreationTimestamp: time.Now(),
		Tags:              tagsCopy,
	}
	b.collections.Put(c)

	if len(tagsCopy) > 0 {
		b.tags[c.CollectionARN] = tagsCopy
	}

	return c.toCollection(), nil
}

// DeleteCollection deletes a face collection.
func (b *InMemoryBackend) DeleteCollection(collectionID string) error {
	b.mu.Lock("DeleteCollection")
	defer b.mu.Unlock()

	c, exists := b.collections.Get(collectionID)
	if !exists {
		return ErrCollectionNotFound
	}

	b.collections.Delete(collectionID)

	// Index result slices mutate under Delete, so clone before the delete loop.
	for _, f := range slices.Clone(b.facesByCollection.Get(collectionID)) {
		b.faces.Delete(f.FaceID)
	}

	delete(b.tags, c.CollectionARN)

	return nil
}

// DescribeCollection returns details about a collection.
func (b *InMemoryBackend) DescribeCollection(collectionID string) (*Collection, error) {
	b.mu.RLock("DescribeCollection")
	defer b.mu.RUnlock()

	c, exists := b.collections.Get(collectionID)
	if !exists {
		return nil, ErrCollectionNotFound
	}

	result := c.toCollection()
	result.Tags = b.tags[c.CollectionARN]
	result.UserCount = int64(len(b.usersByCollection.Get(collectionID)))

	return result, nil
}

// ListCollections returns a paginated list of collections.
func (b *InMemoryBackend) ListCollections(maxResults int32, nextToken string) ([]*Collection, string, error) {
	b.mu.RLock("ListCollections")
	defer b.mu.RUnlock()

	result, outToken := paginateTable(
		b.collections, maxResults, maxCollectionsPerPage, nextToken, collectionKeyFn, (*storedCollection).toCollection,
	)

	return result, outToken, nil
}
