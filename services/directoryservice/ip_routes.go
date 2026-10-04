package directoryservice

import (
	"context"
	"sort"
	"time"
)

// AddIpRoutes adds CIDR IP routes to a directory.
func (b *InMemoryBackend) AddIpRoutes( //nolint:revive,staticcheck // existing issue.
	ctx context.Context,
	directoryID string,
	routes []IpRoute,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddIpRoutes")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	ipRoutes := b.ipRoutesStore(region)
	now := time.Now().UTC()
	existing := ipRoutes[directoryID]
	existingSet := make(map[string]bool, len(existing))
	for _, r := range existing {
		existingSet[ipRouteKey(r.CidrIP, r.CidrIPv6)] = true
	}

	for _, r := range routes {
		if k := ipRouteKey(r.CidrIP, r.CidrIPv6); !existingSet[k] {
			existingSet[k] = true
			ipRoutes[directoryID] = append(ipRoutes[directoryID], storedIpRoute{
				DirectoryID:   directoryID,
				CidrIP:        r.CidrIP,
				CidrIPv6:      r.CidrIPv6,
				Description:   r.Description,
				AddedDateTime: now,
				IPRouteStatus: "Added",
			})
		}
	}

	return nil
}

// RemoveIpRoutes removes CIDR IP routes from a directory.
func (b *InMemoryBackend) RemoveIpRoutes( //nolint:revive,staticcheck // existing issue.
	ctx context.Context,
	directoryID string,
	cidrIPs []string,
	cidrIPv6s []string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveIpRoutes")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	remove := make(map[string]bool, len(cidrIPs))
	for _, c := range cidrIPs {
		remove[c] = true
	}

	removeV6 := make(map[string]bool, len(cidrIPv6s))
	for _, c := range cidrIPv6s {
		removeV6[c] = true
	}

	ipRoutes := b.ipRoutesStore(region)
	filtered := ipRoutes[directoryID][:0]
	for _, r := range ipRoutes[directoryID] {
		matched := (r.CidrIP != "" && remove[r.CidrIP]) || (r.CidrIPv6 != "" && removeV6[r.CidrIPv6])
		if !matched {
			filtered = append(filtered, r)
		}
	}
	ipRoutes[directoryID] = filtered

	return nil
}

// ListIpRoutes returns IP routes for a directory.
func (b *InMemoryBackend) ListIpRoutes( //nolint:revive,staticcheck // existing issue.
	ctx context.Context,
	directoryID string,
	limit int32,
	nextToken string,
) ([]IpRoute, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListIpRoutes")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFound
	}

	stored := b.ipRoutesStoreRO(region)[directoryID]
	sorted := make([]storedIpRoute, len(stored))
	copy(sorted, stored)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].key() < sorted[j].key() })

	start := 0
	if nextToken != "" {
		for i, r := range sorted {
			if r.key() == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(sorted))
	result := make([]IpRoute, 0, end-start)
	for _, r := range sorted[start:end] {
		result = append(result, IpRoute{
			DirectoryID: r.DirectoryID,
			CidrIP:      r.CidrIP,
			CidrIPv6:    r.CidrIPv6,
			Description: r.Description,
			AddedTime:   r.AddedDateTime,
			Status:      r.IPRouteStatus,
		})
	}

	var outToken string
	if end < len(sorted) {
		outToken = sorted[end].key()
	}

	return result, outToken, nil
}

func ipRouteKey(cidrIP, cidrIPv6 string) string { return cidrIP + "|" + cidrIPv6 }

func (r storedIpRoute) key() string { return ipRouteKey(r.CidrIP, r.CidrIPv6) }
