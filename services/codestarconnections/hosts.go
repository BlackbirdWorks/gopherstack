package codestarconnections

import (
	"context"
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// connectionHasReferenceToHostLocked returns true if any connection in the region references hostArn.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) connectionHasReferenceToHostLocked(region, hostArn string) bool {
	for _, conn := range b.connectionsByRegion.Get(region) {
		if conn.HostArn == hostArn {
			return true
		}
	}

	return false
}

// CreateHost creates a new CodeStar host.
func (b *InMemoryBackend) CreateHost(
	ctx context.Context,
	name, providerType, providerEndpoint string,
	vpcConfig *VpcConfiguration,
	tags map[string]string,
) (*Host, error) {
	if err := validateConnectionName(name); err != nil {
		return nil, err
	}

	if providerEndpoint == "" {
		return nil, fmt.Errorf("%w: ProviderEndpoint is required", ErrValidation)
	}

	if len(providerEndpoint) > maxProviderEndpointLen {
		return nil, fmt.Errorf("%w: ProviderEndpoint must not exceed %d characters",
			ErrValidation, maxProviderEndpointLen)
	}

	if providerType != "" && !validProviderTypes()[providerType] {
		return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateHost")
	defer b.mu.Unlock()

	if len(b.hostsByName.Get(regionKey(region, name))) > 0 {
		return nil, fmt.Errorf("%w: host %q already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	hostArn := arn.Build("codestar-connections", region, b.accountID, "host/"+name+"/"+id[:8])

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	host := &Host{
		Name:             name,
		HostArn:          hostArn,
		ProviderType:     providerType,
		ProviderEndpoint: providerEndpoint,
		Status:           HostStatusPending,
		VpcConfiguration: vpcConfig,
		Tags:             tagsCopy,
	}
	b.hosts.Put(host)

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// GetHost returns a host by ARN.
func (b *InMemoryBackend) GetHost(_ context.Context, hostArn string) (*Host, error) {
	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok {
		return nil, fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// ListHosts returns all hosts sorted by name.
func (b *InMemoryBackend) ListHosts(ctx context.Context) []*Host {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListHosts")
	defer b.mu.RUnlock()

	hs := b.hostsByRegion.Get(region)
	result := make([]*Host, 0, len(hs))

	for _, host := range hs {
		cp := *host
		cp.Tags = make(map[string]string, len(host.Tags))
		maps.Copy(cp.Tags, host.Tags)
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// DeleteHost removes a host by ARN. Returns ErrResourceInUse if any connection references the host.
func (b *InMemoryBackend) DeleteHost(ctx context.Context, hostArn string) error {
	region := regionFromARN(hostArn, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("DeleteHost")
	defer b.mu.Unlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	if b.connectionHasReferenceToHostLocked(region, hostArn) {
		return fmt.Errorf("%w: host %q has active connections; delete them first", ErrResourceInUse, host.Name)
	}

	b.hosts.Delete(hostArn)

	return nil
}

// UpdateHost updates the provider endpoint and optional VPC configuration for a host.
func (b *InMemoryBackend) UpdateHost(
	_ context.Context,
	hostArn, providerEndpoint string,
	vpcConfig *VpcConfiguration,
) error {
	if providerEndpoint != "" && len(providerEndpoint) > maxProviderEndpointLen {
		return fmt.Errorf("%w: ProviderEndpoint must not exceed %d characters", ErrValidation, maxProviderEndpointLen)
	}

	b.mu.Lock("UpdateHost")
	defer b.mu.Unlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	// ProviderEndpoint/VpcConfiguration are not part of any index key
	// (hosts is keyed by HostArn; byRegion/byName derive from HostArn/Name),
	// so mutating the stored *Host in place is safe -- no Delete+Put needed.
	if providerEndpoint != "" {
		host.ProviderEndpoint = providerEndpoint
	}

	if vpcConfig != nil {
		host.VpcConfiguration = vpcConfig
	}

	return nil
}
