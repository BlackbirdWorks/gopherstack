package directoryservice

import (
	"context"
	"sort"
)

// CreateConditionalForwarder creates a conditional forwarder.
func (b *InMemoryBackend) CreateConditionalForwarder(
	ctx context.Context,
	directoryID, remoteDomainName string,
	dnsIPAddrs, dnsIPv6Addrs []string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateConditionalForwarder")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	if _, exists := b.conditionalForwarderGet(region, directoryID, remoteDomainName); exists {
		return ErrAliasAlreadyExists
	}

	b.conditionalForwarderPut(&storedConditionalForwarder{
		region:           region,
		DirectoryID:      directoryID,
		RemoteDomainName: remoteDomainName,
		DNSIPAddrs:       dnsIPAddrs,
		DNSIPv6Addrs:     dnsIPv6Addrs,
		ReplicationScope: "Domain",
	})

	return nil
}

// UpdateConditionalForwarder updates a conditional forwarder.
func (b *InMemoryBackend) UpdateConditionalForwarder(
	ctx context.Context,
	directoryID, remoteDomainName string,
	dnsIPAddrs, dnsIPv6Addrs []string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateConditionalForwarder")
	defer b.mu.Unlock()

	fwd, ok := b.conditionalForwarderGet(region, directoryID, remoteDomainName)
	if !ok {
		return ErrConditionalForwarderNotFound
	}

	fwd.DNSIPAddrs = dnsIPAddrs
	fwd.DNSIPv6Addrs = dnsIPv6Addrs

	return nil
}

// DeleteConditionalForwarder deletes a conditional forwarder.
func (b *InMemoryBackend) DeleteConditionalForwarder(ctx context.Context, directoryID, remoteDomainName string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteConditionalForwarder")
	defer b.mu.Unlock()

	if _, ok := b.conditionalForwarderGet(region, directoryID, remoteDomainName); !ok {
		return ErrConditionalForwarderNotFound
	}

	b.conditionalForwarderDelete(region, directoryID, remoteDomainName)

	return nil
}

// DescribeConditionalForwarders returns conditional forwarders for a directory.
func (b *InMemoryBackend) DescribeConditionalForwarders(
	ctx context.Context,
	directoryID string,
	remoteDomainNames []string,
) ([]ConditionalForwarder, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeConditionalForwarders")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, ErrDirectoryNotFound
	}

	filterSet := make(map[string]bool, len(remoteDomainNames))
	for _, d := range remoteDomainNames {
		filterSet[d] = true
	}

	var result []ConditionalForwarder
	for _, fwd := range b.conditionalForwardersInRegion(region) {
		if fwd.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[fwd.RemoteDomainName] {
			continue
		}
		cp := make([]string, len(fwd.DNSIPAddrs))
		copy(cp, fwd.DNSIPAddrs)
		cpV6 := make([]string, len(fwd.DNSIPv6Addrs))
		copy(cpV6, fwd.DNSIPv6Addrs)
		result = append(result, ConditionalForwarder{
			DirectoryID:      fwd.DirectoryID,
			RemoteDomainName: fwd.RemoteDomainName,
			DNSIPAddrs:       cp,
			DNSIPv6Addrs:     cpV6,
			ReplicationScope: fwd.ReplicationScope,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RemoteDomainName < result[j].RemoteDomainName })

	return result, nil
}
