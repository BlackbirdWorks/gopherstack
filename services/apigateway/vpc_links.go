package apigateway

import (
	"fmt"
	"sort"
)

// statusAvailable is the shared "AVAILABLE" status literal reused across
// several unrelated AWS enums that all happen to use this same spelling for
// their steady state (VpcLinkStatus, DomainNameStatus, ApiStatus,
// CacheClusterStatus).
const statusAvailable = "AVAILABLE"

// CreateVpcLink creates a new VPC link.
func (b *InMemoryBackend) CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	id := randomID(apiIDLength)

	link := &VpcLink{
		ID:          id,
		Name:        input.Name,
		Description: input.Description,
		Status:      statusAvailable,
		TargetARNs:  input.TargetARNs,
		Tags:        input.Tags,
	}

	b.mu.Lock("CreateVpcLink")
	defer b.mu.Unlock()

	b.vpcLinks.Put(link)

	return link, nil
}

// GetVpcLink retrieves a VPC link by ID.
func (b *InMemoryBackend) GetVpcLink(id string) (*VpcLink, error) {
	b.mu.RLock("GetVpcLink")
	defer b.mu.RUnlock()

	link, ok := b.vpcLinks.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: VPC link %s not found", ErrNotFound, id)
	}

	return link, nil
}

// GetVpcLinks retrieves all VPC links.
func (b *InMemoryBackend) GetVpcLinks() ([]VpcLink, error) {
	b.mu.RLock("GetVpcLinks")
	defer b.mu.RUnlock()

	all := b.vpcLinks.All()
	result := make([]VpcLink, 0, len(all))
	for _, link := range all {
		result = append(result, *link)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// DeleteVpcLink removes a VPC link.
func (b *InMemoryBackend) DeleteVpcLink(id string) error {
	b.mu.Lock("DeleteVpcLink")
	defer b.mu.Unlock()

	if !b.vpcLinks.Delete(id) {
		return fmt.Errorf("%w: VPC link %s not found", ErrNotFound, id)
	}

	return nil
}

// UpdateVpcLink updates the name or description of a VPC link.
func (b *InMemoryBackend) UpdateVpcLink(input UpdateVpcLinkInput) (*VpcLink, error) {
	b.mu.Lock("UpdateVpcLink")
	defer b.mu.Unlock()

	link, ok := b.vpcLinks.Get(input.VpcLinkID)
	if !ok {
		return nil, fmt.Errorf("%w: VPC link %s not found", ErrNotFound, input.VpcLinkID)
	}

	if input.Name != "" {
		link.Name = input.Name
	}

	if input.Description != "" {
		link.Description = input.Description
	}

	return link, nil
}
