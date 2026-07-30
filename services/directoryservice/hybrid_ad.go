package directoryservice

import (
	"context"
	"sort"

	"github.com/google/uuid"
)

// CreateHybridAD creates a Hybrid AD directory (stored as MicrosoftAD type).
func (b *InMemoryBackend) CreateHybridAD(
	ctx context.Context,
	name, shortName, description, _ string,
	edition DirectoryEdition,
	tags []Tag,
) (*Directory, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateHybridAD")
	defer b.mu.Unlock()

	if name == "" {
		return nil, "", ErrInvalidParameter
	}

	d := b.newStoredDirectory(
		region, name, shortName, description, DirectoryTypeMicrosoftAD, "", edition, "", nil, tags,
	)
	b.directoryPut(d)
	b.aliasesStore(region)[d.Alias] = d.DirectoryID

	requestID := uuid.NewString()
	b.hybridADUpdatePut(&storedHybridADUpdate{
		region:      region,
		RequestID:   requestID,
		DirectoryID: d.DirectoryID,
		Status:      "Updated", //nolint:goconst // existing issue.
	})

	cp := b.describeDirectory(d)

	return &cp, requestID, nil
}

// UpdateHybridAD updates a Hybrid AD directory.
func (b *InMemoryBackend) UpdateHybridAD(ctx context.Context, directoryID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateHybridAD")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	requestID := uuid.NewString()
	b.hybridADUpdatePut(&storedHybridADUpdate{
		region:      region,
		RequestID:   requestID,
		DirectoryID: directoryID,
		Status:      "Updated",
	})

	return requestID, nil
}

// DescribeHybridADUpdate returns hybrid AD update info for a directory.
func (b *InMemoryBackend) DescribeHybridADUpdate(
	ctx context.Context,
	directoryID string,
) ([]HybridADUpdateEntry, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeHybridADUpdate")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, ErrDirectoryNotFound
	}

	var result []HybridADUpdateEntry
	for _, u := range b.hybridADUpdatesInRegion(region) {
		if u.DirectoryID == directoryID {
			result = append(result, HybridADUpdateEntry{
				RequestID:   u.RequestID,
				DirectoryID: u.DirectoryID,
				Status:      u.Status,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RequestID < result[j].RequestID })

	return result, nil
}
