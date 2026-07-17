package directoryservice

import (
	"context"
	"slices"
	"sort"
	"time"
)

// AddRegion adds a region to a directory.
func (b *InMemoryBackend) AddRegion(ctx context.Context, directoryID, regionName string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddRegion")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	if _, exists := b.dsRegionGet(region, directoryID, regionName); exists {
		return ErrAliasAlreadyExists
	}

	b.dsRegionPut(&storedRegion{
		region:      region,
		DirectoryID: directoryID,
		RegionName:  regionName,
		RegionType:  "Additional",
		Status:      "Active",
		LaunchTime:  time.Now().UTC(),
	})

	return nil
}

// RemoveRegion removes a region from a directory.
func (b *InMemoryBackend) RemoveRegion(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveRegion")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	for _, r := range slices.Clone(b.dsRegionsInRegion(region)) {
		if r.DirectoryID == directoryID {
			b.dsRegions.Delete(regionKey(region, dsRegionID(r.DirectoryID, r.RegionName)))
		}
	}

	return nil
}

// DescribeRegions returns regions for a directory.
func (b *InMemoryBackend) DescribeRegions(
	ctx context.Context,
	directoryID, regionName, nextToken string, //nolint:revive // existing issue.
) ([]RegionDescription, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeRegions")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var all []storedRegion
	for _, r := range b.dsRegionsInRegion(region) {
		if r.DirectoryID != directoryID {
			continue
		}
		if regionName != "" && r.RegionName != regionName {
			continue
		}
		all = append(all, *r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].RegionName < all[j].RegionName })

	result := make([]RegionDescription, 0, len(all))
	for _, r := range all {
		result = append(result, RegionDescription{
			LaunchTime:  r.LaunchTime,
			DirectoryID: r.DirectoryID,
			RegionName:  r.RegionName,
			RegionType:  r.RegionType,
			Status:      r.Status,
		})
	}

	return result, "", nil
}
