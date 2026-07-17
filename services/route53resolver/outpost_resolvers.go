package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateOutpostResolver creates a new Resolver on an Outpost.
func (b *InMemoryBackend) CreateOutpostResolver(
	ctx context.Context,
	name, creatorRequestID, outpostARN, preferredInstanceType string,
	instanceCount int32,
) (*OutpostResolver, error) {
	b.mu.Lock("CreateOutpostResolver")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if instanceCount <= 0 {
		instanceCount = defaultOutpostResolverInstanceCount
	}

	id := "rslvr-op-" + uuid.New().String()[:8]
	resolverARN := arn.Build("route53resolver", region, b.accountID, "outpost-resolver/"+id)
	r := &OutpostResolver{
		ID:                    id,
		ARN:                   resolverARN,
		Name:                  name,
		CreatorRequestID:      creatorRequestID,
		OutpostARN:            outpostARN,
		PreferredInstanceType: preferredInstanceType,
		InstanceCount:         instanceCount,
		Status:                statusOperational,
		Region:                region,
	}
	b.outpostResolvers.Put(r)
	cp := *r

	return &cp, nil
}

// AddOutpostResolverInternal adds an outpost resolver directly to the backend (test seed helper).
func (b *InMemoryBackend) AddOutpostResolverInternal(name, outpostARN string) *OutpostResolver {
	b.mu.Lock("AddOutpostResolverInternal")
	defer b.mu.Unlock()

	id := "rslvr-op-" + uuid.New().String()[:8]
	resolverARN := arn.Build("route53resolver", b.region, b.accountID, "outpost-resolver/"+id)
	r := &OutpostResolver{
		ID:            id,
		ARN:           resolverARN,
		Name:          name,
		OutpostARN:    outpostARN,
		InstanceCount: defaultOutpostResolverInstanceCount,
		Status:        statusOperational,
		Region:        b.region,
	}
	b.outpostResolvers.Put(r)
	cp := *r

	return &cp
}

// DeleteOutpostResolver deletes an outpost resolver.
func (b *InMemoryBackend) DeleteOutpostResolver(ctx context.Context, id string) (*OutpostResolver, error) {
	b.mu.Lock("DeleteOutpostResolver")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	r, ok := b.outpostResolvers.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	cp := *r
	b.outpostResolvers.Delete(regionalKey(region, id))

	return &cp, nil
}

// GetOutpostResolver retrieves an outpost resolver by ID.
func (b *InMemoryBackend) GetOutpostResolver(ctx context.Context, id string) (*OutpostResolver, error) {
	b.mu.RLock("GetOutpostResolver")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	r, ok := b.outpostResolvers.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	cp := *r

	return &cp, nil
}

// ListOutpostResolvers lists all outpost resolvers.
func (b *InMemoryBackend) ListOutpostResolvers(ctx context.Context) []*OutpostResolver {
	b.mu.RLock("ListOutpostResolvers")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionResolvers := b.outpostResolversByRegion.Get(region)
	list := make([]*OutpostResolver, 0, len(regionResolvers))
	for _, r := range regionResolvers {
		cp := *r
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateOutpostResolver updates name, preferred instance type, or instance count.
func (b *InMemoryBackend) UpdateOutpostResolver(
	ctx context.Context,
	id, name, preferredInstanceType string,
	instanceCount int32,
) (*OutpostResolver, error) {
	b.mu.Lock("UpdateOutpostResolver")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	r, ok := b.outpostResolvers.Get(regionalKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	if name != "" {
		r.Name = name
	}
	if preferredInstanceType != "" {
		r.PreferredInstanceType = preferredInstanceType
	}
	if instanceCount > 0 {
		r.InstanceCount = instanceCount
	}
	cp := *r

	return &cp, nil
}

// --- Query Log Config operations ---
