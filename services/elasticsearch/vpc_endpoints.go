package elasticsearch

import (
	"context"
	"fmt"
	"maps"
	"slices"
)

// CreateVpcEndpoint creates a managed VPC endpoint for an Elasticsearch domain.
// The endpoint's region is resolved from the domain ARN, falling back to ctx.
func (b *InMemoryBackend) CreateVpcEndpoint(
	ctx context.Context, domainARN string, vpcOptions map[string]string,
) (*VpcEndpoint, error) {
	if domainARN == "" {
		return nil, fmt.Errorf("%w: DomainArn is required", ErrValidation)
	}

	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.Lock("CreateVpcEndpoint")
	defer b.mu.Unlock()

	// Deep-copy vpcOptions so the stored map is independent of the caller's map.
	optsCopy := make(map[string]string, len(vpcOptions))
	maps.Copy(optsCopy, vpcOptions)

	id := fmt.Sprintf("vpc-endpoint-%010d", b.nextIDLocked())
	endpoint := &VpcEndpoint{
		ID:             id,
		OwnerAccountID: b.accountID,
		DomainARN:      domainARN,
		Endpoint:       fmt.Sprintf("vpc-%s.%s.es.amazonaws.com", id, region),
		Status:         statusActive,
		VpcOptions:     optsCopy,
		region:         region,
	}
	b.vpcEndpointPut(endpoint)

	return vpcEndpointCopy(endpoint), nil
}

// AuthorizeVpcEndpointAccess grants an account or service access to the domain's VPC endpoint.
func (b *InMemoryBackend) AuthorizeVpcEndpointAccess(ctx context.Context, domainName, account string) error {
	if account == "" {
		return fmt.Errorf("%w: account principal is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("AuthorizeVpcEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	access := b.vpcAccessStore(region)
	if !slices.Contains(access[domainName], account) {
		access[domainName] = append(access[domainName], account)
		slices.Sort(access[domainName])
	}

	return nil
}

// DeleteVpcEndpoint removes a VPC endpoint by ID.
func (b *InMemoryBackend) DeleteVpcEndpoint(ctx context.Context, vpcEndpointID string) (*VpcEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteVpcEndpoint")
	defer b.mu.Unlock()

	endpoint, exists := b.vpcEndpointGet(region, vpcEndpointID)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrVpcEndpointNotFound, vpcEndpointID)
	}

	cp := *endpoint
	b.vpcEndpointDelete(region, vpcEndpointID)

	return vpcEndpointCopy(&cp), nil
}

// DescribeVpcEndpoints returns VPC endpoints matching the given IDs, or all endpoints if empty.
func (b *InMemoryBackend) DescribeVpcEndpoints(ctx context.Context, vpcEndpointIDs []string) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeVpcEndpoints")
	defer b.mu.RUnlock()

	if len(vpcEndpointIDs) == 0 {
		endpoints := b.vpcEndpointsInRegion(region)
		result := make([]*VpcEndpoint, 0, len(endpoints))
		for _, ep := range endpoints {
			result = append(result, vpcEndpointCopy(ep))
		}

		return result
	}

	result := make([]*VpcEndpoint, 0, len(vpcEndpointIDs))
	for _, id := range vpcEndpointIDs {
		if ep, exists := b.vpcEndpointGet(region, id); exists {
			result = append(result, vpcEndpointCopy(ep))
		}
	}

	return result
}

// ListVpcEndpointAccess returns authorized account principals for a domain's VPC endpoint access.
func (b *InMemoryBackend) ListVpcEndpointAccess(ctx context.Context, domainName string) ([]string, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListVpcEndpointAccess")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	return slices.Clone(b.vpcAccessStoreRO(region)[domainName]), nil
}

// ListVpcEndpoints returns all VPC endpoints in the request's region.
func (b *InMemoryBackend) ListVpcEndpoints(ctx context.Context) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListVpcEndpoints")
	defer b.mu.RUnlock()

	endpoints := b.vpcEndpointsInRegion(region)
	result := make([]*VpcEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		result = append(result, vpcEndpointCopy(ep))
	}

	return result
}

// ListVpcEndpointsForDomain returns VPC endpoints associated with a specific domain ARN.
func (b *InMemoryBackend) ListVpcEndpointsForDomain(ctx context.Context, domainName string) []*VpcEndpoint {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListVpcEndpointsForDomain")
	defer b.mu.RUnlock()

	d, exists := b.domainGet(region, domainName)
	if !exists {
		return nil
	}

	var result []*VpcEndpoint
	for _, ep := range b.vpcEndpointsInRegion(region) {
		if ep.DomainARN == d.ARN {
			result = append(result, vpcEndpointCopy(ep))
		}
	}

	return result
}

// RevokeVpcEndpointAccess revokes an account's access to a domain's VPC endpoint.
func (b *InMemoryBackend) RevokeVpcEndpointAccess(ctx context.Context, domainName, account string) error {
	if account == "" {
		return fmt.Errorf("%w: account principal is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("RevokeVpcEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	access := b.vpcAccessStore(region)
	accounts := access[domainName]
	for i, authorized := range accounts {
		if authorized == account {
			access[domainName] = append(accounts[:i], accounts[i+1:]...)

			break
		}
	}

	return nil
}

// UpdateVpcEndpoint updates the VPC options of a VPC endpoint.
func (b *InMemoryBackend) UpdateVpcEndpoint(
	ctx context.Context, vpcEndpointID string, vpcOptions map[string]string,
) (*VpcEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpdateVpcEndpoint")
	defer b.mu.Unlock()

	endpoint, exists := b.vpcEndpointGet(region, vpcEndpointID)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrVpcEndpointNotFound, vpcEndpointID)
	}

	newOpts := make(map[string]string, len(vpcOptions))
	maps.Copy(newOpts, vpcOptions)
	endpoint.VpcOptions = newOpts

	return vpcEndpointCopy(endpoint), nil
}
