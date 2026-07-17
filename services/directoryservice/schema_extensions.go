package directoryservice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// StartSchemaExtension starts a schema extension.
func (b *InMemoryBackend) StartSchemaExtension(
	ctx context.Context,
	directoryID, description, _ string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartSchemaExtension")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("e-%s", uuid.NewString()[:10])
	now := time.Now().UTC()
	b.schemaExtensionPut(&storedSchemaExtension{
		region:      region,
		ExtensionID: id,
		DirectoryID: directoryID,
		Description: description,
		Status:      "Completed",
		StartTime:   now,
		EndTime:     now,
	})

	return id, nil
}

// CancelSchemaExtension cancels a schema extension.
func (b *InMemoryBackend) CancelSchemaExtension(ctx context.Context, directoryID, schemaExtensionID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CancelSchemaExtension")
	defer b.mu.Unlock()

	ext, ok := b.schemaExtensionGet(region, schemaExtensionID)
	if !ok || ext.DirectoryID != directoryID {
		return ErrSchemaExtensionNotFound
	}

	ext.Status = "CancelInProgress"

	return nil
}

// ListSchemaExtensions returns schema extensions for a directory.
func (b *InMemoryBackend) ListSchemaExtensions(
	ctx context.Context,
	directoryID string,
	limit int32,
	nextToken string,
) ([]SchemaExtension, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListSchemaExtensions")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var all []storedSchemaExtension
	for _, e := range b.schemaExtensionsInRegion(region) {
		if e.DirectoryID == directoryID {
			all = append(all, *e)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ExtensionID < all[j].ExtensionID })

	start := 0
	if nextToken != "" {
		for i, e := range all {
			if e.ExtensionID == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(all))
	result := make([]SchemaExtension, 0, end-start)
	for _, e := range all[start:end] {
		result = append(result, SchemaExtension{
			StartTime:   e.StartTime,
			EndTime:     e.EndTime,
			ExtensionID: e.ExtensionID,
			DirectoryID: e.DirectoryID,
			Description: e.Description,
			Status:      e.Status,
		})
	}

	var outToken string
	if end < len(all) {
		outToken = all[end].ExtensionID
	}

	return result, outToken, nil
}
