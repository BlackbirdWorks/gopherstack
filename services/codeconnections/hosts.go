package codeconnections

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateHost creates a new host.
func (b *InMemoryBackend) CreateHost(
	ctx context.Context,
	name, providerType, providerEndpoint string,
	tags map[string]string,
) (*Host, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if providerEndpoint == "" {
		return nil, fmt.Errorf("%w: ProviderEndpoint is required", ErrValidation)
	}

	if providerType == "" || !validProviderTypes()[providerType] {
		return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateHost")
	defer b.mu.Unlock()

	if len(b.hostsByName.Get(regionKey(region, name))) > 0 {
		return nil, fmt.Errorf("%w: host %q already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	hostArn := arn.Build("codeconnections", region, b.accountID, "host/"+id)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	host := &Host{
		Name:             name,
		HostArn:          hostArn,
		ProviderType:     providerType,
		ProviderEndpoint: providerEndpoint,
		Status:           "AVAILABLE",
		Tags:             tagsCopy,
		CreatedAt:        time.Now().UTC(),
	}

	b.hosts.Put(host)

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// GetHost retrieves a host by ARN, scoped to the caller's request region (see GetConnection).
func (b *InMemoryBackend) GetHost(ctx context.Context, hostArn string) (*Host, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok || regionFromARN(hostArn) != region {
		return nil, ErrNotFound
	}

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// connectionHasReferenceToHostLocked returns true if any connection in region
// references hostArn. Must be called with at least an RLock held.
func (b *InMemoryBackend) connectionHasReferenceToHostLocked(region, hostArn string) bool {
	for _, conn := range b.connectionsByRegion.Get(region) {
		if conn.HostArn == hostArn {
			return true
		}
	}

	return false
}

// DeleteHost removes a host by ARN. The real operation documents that all
// connections associated to a host must be deleted before the host itself
// can be deleted.
func (b *InMemoryBackend) DeleteHost(ctx context.Context, hostArn string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteHost")
	defer b.mu.Unlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok || regionFromARN(hostArn) != region {
		return ErrNotFound
	}

	if b.connectionHasReferenceToHostLocked(region, hostArn) {
		return fmt.Errorf("%w: host %q has active connections; delete them first", ErrResourceInUse, host.Name)
	}

	b.hosts.Delete(hostArn)

	return nil
}

// AddHostInternal seeds a host directly for testing.
func (b *InMemoryBackend) AddHostInternal(_ context.Context, host *Host) {
	b.mu.Lock("AddHostInternal")
	defer b.mu.Unlock()

	b.hosts.Put(host)
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

// UpdateHost updates the provider endpoint for a host.
func (b *InMemoryBackend) UpdateHost(ctx context.Context, hostArn, providerEndpoint string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateHost")
	defer b.mu.Unlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok || regionFromARN(hostArn) != region {
		return ErrNotFound
	}

	// ProviderEndpoint is not part of any index key (hosts is keyed by
	// HostArn; byRegion/byName derive from HostArn/Name), so mutating the
	// stored *Host in place is safe -- no Delete+Put needed.
	if providerEndpoint != "" {
		host.ProviderEndpoint = providerEndpoint
	}

	return nil
}
