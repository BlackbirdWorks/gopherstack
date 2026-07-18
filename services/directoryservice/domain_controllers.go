package directoryservice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// DescribeDomainControllers returns domain controllers for a directory.
func (b *InMemoryBackend) DescribeDomainControllers(
	ctx context.Context,
	directoryID string,
	domainControllerIDs []string,
	limit int32,
	nextToken string,
) ([]DomainController, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeDomainControllers")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFound
	}

	filterSet := make(map[string]bool, len(domainControllerIDs))
	for _, id := range domainControllerIDs {
		filterSet[id] = true
	}

	var ids []string
	for _, dc := range b.domainControllersInRegion(region) {
		if dc.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[dc.ControllerID] {
			continue
		}
		ids = append(ids, dc.ControllerID)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]DomainController, 0, end-start)
	for _, id := range ids[start:end] {
		dc, _ := b.domainControllerGet(region, id)
		result = append(result, DomainController{
			ControllerID:     dc.ControllerID,
			DirectoryID:      dc.DirectoryID,
			Status:           dc.Status,
			AvailabilityZone: dc.AvailabilityZone,
			LaunchTime:       dc.LaunchTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// UpdateNumberOfDomainControllers sets the desired domain controller count.
func (b *InMemoryBackend) UpdateNumberOfDomainControllers(
	ctx context.Context,
	directoryID string,
	desiredNumber int32,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateNumberOfDomainControllers")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	// Count current controllers.
	var current int32
	for _, dc := range b.domainControllersInRegion(region) {
		if dc.DirectoryID == directoryID {
			current++
		}
	}

	// Add controllers if desired > current.
	for i := current; i < desiredNumber; i++ {
		id := fmt.Sprintf("dc-%s", uuid.NewString()[:10])
		b.domainControllerPut(&storedDomainController{
			region:           region,
			ControllerID:     id,
			DirectoryID:      directoryID,
			Status:           "Active",
			AvailabilityZone: "us-east-1a",
			LaunchTime:       time.Now().UTC(),
		})
	}

	// Remove controllers if desired < current.
	if desiredNumber < current {
		var toRemove []string
		for _, dc := range b.domainControllersInRegion(region) {
			if dc.DirectoryID == directoryID {
				toRemove = append(toRemove, dc.ControllerID)
			}
		}
		sort.Strings(toRemove)
		for i := int32(len(toRemove)) - 1; i >= desiredNumber; i-- { //nolint:gosec // existing issue.
			b.domainControllerDelete(region, toRemove[i])
		}
	}

	return nil
}
