package apigatewayv2

import (
	"fmt"
	"sort"
	"time"
)

// CreateVpcLink creates a new VPC link.
func (b *InMemoryBackend) CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	if len(input.SubnetIDs) == 0 {
		return nil, fmt.Errorf("%w: subnetIds is required", ErrBadRequest)
	}

	b.mu.Lock("CreateVpcLink")
	defer b.mu.Unlock()

	securityGroupIDs := input.SecurityGroupIDs
	if securityGroupIDs == nil {
		securityGroupIDs = []string{}
	}

	now := isoTime{time.Now()}
	id := randomID()
	vpcLink := &VpcLink{
		CreatedDate:      now,
		VpcLinkID:        id,
		Name:             input.Name,
		SecurityGroupIDs: securityGroupIDs,
		SubnetIDs:        input.SubnetIDs,
		Tags:             copyTags(input.Tags),
		VpcLinkStatus:    "AVAILABLE",
	}
	b.vpcLinks.Put(vpcLink)

	cp := *vpcLink

	return &cp, nil
}

// GetVpcLink retrieves a VPC link by ID.
func (b *InMemoryBackend) GetVpcLink(vpcLinkID string) (*VpcLink, error) {
	b.mu.RLock("GetVpcLink")
	defer b.mu.RUnlock()

	vpcLink, ok := b.vpcLinks.Get(vpcLinkID)
	if !ok {
		return nil, ErrVpcLinkNotFound
	}

	cp := *vpcLink

	return &cp, nil
}

// GetVpcLinks retrieves all VPC links.
func (b *InMemoryBackend) GetVpcLinks() ([]VpcLink, error) {
	b.mu.RLock("GetVpcLinks")
	defer b.mu.RUnlock()

	all := b.vpcLinks.All()
	out := make([]VpcLink, 0, len(all))

	for _, item := range all {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VpcLinkID < out[j].VpcLinkID })

	return out, nil
}

// UpdateVpcLink updates a VPC link.
func (b *InMemoryBackend) UpdateVpcLink(vpcLinkID string, input UpdateVpcLinkInput) (*VpcLink, error) {
	b.mu.Lock("UpdateVpcLink")
	defer b.mu.Unlock()

	vpcLink, ok := b.vpcLinks.Get(vpcLinkID)
	if !ok {
		return nil, ErrVpcLinkNotFound
	}
	if input.Name != "" {
		vpcLink.Name = input.Name
	}

	cp := *vpcLink

	return &cp, nil
}

// DeleteVpcLink removes a VPC link.
func (b *InMemoryBackend) DeleteVpcLink(vpcLinkID string) error {
	b.mu.Lock("DeleteVpcLink")
	defer b.mu.Unlock()

	if !b.vpcLinks.Delete(vpcLinkID) {
		return ErrVpcLinkNotFound
	}

	return nil
}
