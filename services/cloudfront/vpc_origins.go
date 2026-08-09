package cloudfront

import (
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// vpcOriginARN builds an ARN for a VPC Origin.
func (b *InMemoryBackend) vpcOriginARN(id string) string {
	return arn.Build("cloudfront", "", b.accountID, fmt.Sprintf("vpc-origin/%s", id))
}

// CreateVpcOrigin creates a new CloudFront VPC Origin.
func (b *InMemoryBackend) CreateVpcOrigin(name string, tags map[string]string) (*VpcOrigin, error) {
	b.mu.Lock("CreateVpcOrigin")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	id := generateID()
	origin := &VpcOrigin{
		ID:   id,
		ARN:  b.vpcOriginARN(id),
		Name: name,
		ETag: uuid.NewString(),
	}
	if len(tags) > 0 {
		origin.Tags = maps.Clone(tags)
	}
	b.vpcOrigins.Put(origin)
	cp := *origin

	return &cp, nil
}

// GetVpcOrigin returns a VPC Origin by ID.
func (b *InMemoryBackend) GetVpcOrigin(id string) (*VpcOrigin, error) {
	b.mu.RLock("GetVpcOrigin")
	defer b.mu.RUnlock()

	origin, ok := b.vpcOrigins.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: vpc origin %s not found", ErrVpcOriginNotFound, id)
	}

	cp := *origin

	return &cp, nil
}

// ListVpcOrigins returns all VPC Origins sorted by ID.
func (b *InMemoryBackend) ListVpcOrigins() []*VpcOrigin {
	b.mu.RLock("ListVpcOrigins")
	defer b.mu.RUnlock()

	list := make([]*VpcOrigin, 0, b.vpcOrigins.Len())
	for _, origin := range b.vpcOrigins.All() {
		cp := *origin
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateVpcOrigin updates a VPC Origin.
func (b *InMemoryBackend) UpdateVpcOrigin(id, name string) (*VpcOrigin, error) {
	b.mu.Lock("UpdateVpcOrigin")
	defer b.mu.Unlock()

	origin, ok := b.vpcOrigins.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: vpc origin %s not found", ErrVpcOriginNotFound, id)
	}

	origin.Name = name
	origin.ETag = uuid.NewString()
	cp := *origin

	return &cp, nil
}

// DeleteVpcOrigin deletes a VPC Origin by ID.
func (b *InMemoryBackend) DeleteVpcOrigin(id string) error {
	b.mu.Lock("DeleteVpcOrigin")
	defer b.mu.Unlock()

	if _, ok := b.vpcOrigins.Get(id); !ok {
		return fmt.Errorf("%w: vpc origin %s not found", ErrVpcOriginNotFound, id)
	}

	b.vpcOrigins.Delete(id)

	return nil
}
