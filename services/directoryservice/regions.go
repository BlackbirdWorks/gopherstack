package directoryservice

import (
	"context"
	"slices"
	"sort"
	"time"
)

// defaultRegionDomainControllers is the number of domain controllers AWS
// provisions by default when replicating a directory into a new additional
// Region (matches the Small/Standard-edition default used for the primary
// Region; the AddRegion API does not expose this as a request parameter).
const defaultRegionDomainControllers int32 = 2

// AddRegion adds a region to a directory.
func (b *InMemoryBackend) AddRegion(
	ctx context.Context,
	directoryID, regionName string,
	vpcSettings *DirectoryVpcSettings,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddRegion")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	if _, exists := b.dsRegionGet(region, directoryID, regionName); exists {
		return ErrDirectoryAlreadyInRegion
	}

	var storedVpc *storedVpcSettings
	if vpcSettings != nil {
		storedVpc = &storedVpcSettings{
			VpcID:             vpcSettings.VpcID,
			SubnetIDs:         vpcSettings.SubnetIDs,
			SecurityGroupIDs:  vpcSettings.SecurityGroupIDs,
			AvailabilityZones: vpcSettings.AvailabilityZones,
		}
	}

	now := time.Now().UTC()
	b.dsRegionPut(&storedRegion{
		region:                     region,
		DirectoryID:                directoryID,
		RegionName:                 regionName,
		RegionType:                 "Additional",
		Status:                     "Active",
		LaunchTime:                 now,
		StatusLastUpdatedDateTime:  now,
		VpcSettings:                storedVpc,
		DesiredNumberOfDomainCtrls: defaultRegionDomainControllers,
	})

	return nil
}

// RemoveRegion removes a region from a directory.
func (b *InMemoryBackend) RemoveRegion(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveRegion")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFoundDDNE
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
		return nil, "", ErrDirectoryNotFoundDDNE
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
		var vpcSettings *DirectoryVpcSettings
		if r.VpcSettings != nil {
			vpcSettings = &DirectoryVpcSettings{
				VpcID:             r.VpcSettings.VpcID,
				SubnetIDs:         r.VpcSettings.SubnetIDs,
				SecurityGroupIDs:  r.VpcSettings.SecurityGroupIDs,
				AvailabilityZones: r.VpcSettings.AvailabilityZones,
			}
		}
		result = append(result, RegionDescription{
			LaunchTime:                 r.LaunchTime,
			StatusLastUpdatedDateTime:  r.StatusLastUpdatedDateTime,
			VpcSettings:                vpcSettings,
			DirectoryID:                r.DirectoryID,
			RegionName:                 r.RegionName,
			RegionType:                 r.RegionType,
			Status:                     r.Status,
			DesiredNumberOfDomainCtrls: r.DesiredNumberOfDomainCtrls,
		})
	}

	return result, "", nil
}
