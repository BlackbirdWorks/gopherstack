package directoryservice

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// synthesizeDomainControllerDNSIPAddr deterministically derives a plausible
// private DNS server IP for a domain controller from its ID, following the
// same rationale as synthesizeDNSIPAddrs in directories.go.
func synthesizeDomainControllerDNSIPAddr(controllerID string) string {
	sum := sha256.Sum256([]byte(controllerID))

	return fmt.Sprintf("10.0.%d.%d", sum[0], sum[1])
}

// synthesizeDomainControllerDNSIPv6Addr deterministically derives a
// plausible private (ULA) IPv6 DNS server address for a domain controller
// from its ID, following the same rationale as synthesizeDNSIPv6Addrs in
// directories.go. Only used when the parent directory's NetworkType
// indicates IPv6 capability.
func synthesizeDomainControllerDNSIPv6Addr(controllerID string) string {
	sum := sha256.Sum256([]byte(controllerID))

	return fmt.Sprintf("fd00:ec2::%x:%x", sum[0:2], sum[2:4])
}

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
			ControllerID:              dc.ControllerID,
			DirectoryID:               dc.DirectoryID,
			Status:                    dc.Status,
			AvailabilityZone:          dc.AvailabilityZone,
			LaunchTime:                dc.LaunchTime,
			StatusLastUpdatedDateTime: dc.StatusLastUpdatedDateTime,
			DNSIPAddr:                 dc.DNSIPAddr,
			DNSIPv6Addr:               dc.DNSIPv6Addr,
			SubnetID:                  dc.SubnetID,
			VpcID:                     dc.VpcID,
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

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	// Count current controllers.
	var current int32
	for _, dc := range b.domainControllersInRegion(region) {
		if dc.DirectoryID == directoryID {
			current++
		}
	}

	var vpcID string
	var subnetIDs []string
	if d.VpcSettings != nil {
		vpcID = d.VpcSettings.VpcID
		subnetIDs = d.VpcSettings.SubnetIDs
	}

	dirNetworkType := NetworkType(d.NetworkType)
	ipv6Capable := dirNetworkType == NetworkTypeDualStack || dirNetworkType == NetworkTypeIPv6Only

	// Add controllers if desired > current.
	for i := current; i < desiredNumber; i++ {
		id := fmt.Sprintf("dc-%s", uuid.NewString()[:10])

		var subnetID string
		if len(subnetIDs) > 0 {
			subnetID = subnetIDs[int(i)%len(subnetIDs)]
		}

		var dnsIPv6Addr string
		if ipv6Capable {
			dnsIPv6Addr = synthesizeDomainControllerDNSIPv6Addr(id)
		}

		now := time.Now().UTC()
		b.domainControllerPut(&storedDomainController{
			region:                    region,
			ControllerID:              id,
			DirectoryID:               directoryID,
			Status:                    "Active",
			AvailabilityZone:          "us-east-1a",
			LaunchTime:                now,
			StatusLastUpdatedDateTime: now,
			DNSIPAddr:                 synthesizeDomainControllerDNSIPAddr(id),
			DNSIPv6Addr:               dnsIPv6Addr,
			SubnetID:                  subnetID,
			VpcID:                     vpcID,
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

	d.DesiredNumberOfDomainCtrls = desiredNumber

	return nil
}
