package opensearch

import (
	"fmt"
)

// AuthorizeVpcEndpointAccess grants VPC endpoint access for an account or service.
func (b *InMemoryBackend) AuthorizeVpcEndpointAccess(
	domainName, account, service string,
) (*AuthorizedPrincipal, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeVpcEndpointAccess")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	principal := account
	principalType := "AWS_ACCOUNT"

	if service != "" {
		principal = service
		principalType = "AWS_SERVICE"
	}

	p := AuthorizedPrincipal{
		Principal:     principal,
		PrincipalType: principalType,
	}
	b.vpcAuthorizations[domainName] = append(b.vpcAuthorizations[domainName], p)

	return &p, nil
}

// CreateVpcEndpoint creates a new VPC endpoint.
func (b *InMemoryBackend) CreateVpcEndpoint(
	domainArn string,
	vpcOptions map[string]any,
) (*VpcEndpoint, error) {
	b.mu.Lock("CreateVpcEndpoint")
	defer b.mu.Unlock()

	b.vpcEndpointCounter++
	id := fmt.Sprintf("vpce-%d", b.vpcEndpointCounter)

	ep := &VpcEndpoint{
		VpcEndpointID:    id,
		VpcEndpointOwner: b.accountID,
		DomainArn:        domainArn,
		Status:           pkgStateActive,
		Endpoint:         fmt.Sprintf("%s.vpc.es.amazonaws.com", id),
		VpcOptions:       vpcOptions,
	}
	b.vpcEndpoints.Put(ep)

	cp := *ep

	return &cp, nil
}

// DescribeVpcEndpoints returns matching VPC endpoints and errors for not-found IDs.
func (b *InMemoryBackend) DescribeVpcEndpoints(ids []string) ([]*VpcEndpoint, []map[string]any) {
	b.mu.RLock("DescribeVpcEndpoints")
	defer b.mu.RUnlock()

	now := b.clock()

	var endpoints []*VpcEndpoint
	var errs []map[string]any

	for _, id := range ids {
		ep, exists := b.vpcEndpoints.Get(id)
		if !exists || statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			errs = append(errs, map[string]any{
				"VpcEndpointId": id,
				"ErrorCode":     "EndpointNotFound",
				"ErrorMessage":  fmt.Sprintf("VPC endpoint %s not found", id),
			})

			continue
		}

		cp := *ep
		endpoints = append(endpoints, &cp)
	}

	if endpoints == nil {
		endpoints = []*VpcEndpoint{}
	}

	if errs == nil {
		errs = []map[string]any{}
	}

	return endpoints, errs
}

// UpdateVpcEndpoint updates the VPC options for a VPC endpoint.
func (b *InMemoryBackend) UpdateVpcEndpoint(
	id string,
	vpcOptions map[string]any,
) (*VpcEndpoint, error) {
	b.mu.Lock("UpdateVpcEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.vpcEndpoints.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrConnectionNotFound, id)
	}

	ep.VpcOptions = vpcOptions
	cp := *ep

	return &cp, nil
}

// DeleteVpcEndpoint removes a VPC endpoint by ID. With a processing delay
// configured the endpoint first enters an observable DELETING window before it
// is finally removed.
func (b *InMemoryBackend) DeleteVpcEndpoint(id string) (*VpcEndpoint, error) {
	b.mu.Lock("DeleteVpcEndpoint")
	defer b.mu.Unlock()

	b.purgeExpiredVpcEndpointsLocked()

	ep, exists := b.vpcEndpoints.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrConnectionNotFound, id)
	}

	if b.processingDelay == 0 {
		cp := *ep
		cp.Status = statusDeleting
		b.vpcEndpoints.Delete(id)

		return &cp, nil
	}

	ep.Status = statusDeleting
	ep.StatusUntil = b.clock().Add(b.processingDelay)
	cp := *ep

	return &cp, nil
}

// purgeExpiredVpcEndpointsLocked removes VPC endpoints past their deleting
// window. The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredVpcEndpointsLocked() {
	now := b.clock()
	for _, ep := range b.vpcEndpoints.All() {
		if statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			b.vpcEndpoints.Delete(ep.VpcEndpointID)
		}
	}
}

// ListVpcEndpoints returns all VPC endpoints, excluding any whose deleting
// window has elapsed.
func (b *InMemoryBackend) ListVpcEndpoints() []*VpcEndpoint {
	b.mu.RLock("ListVpcEndpoints")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]*VpcEndpoint, 0, b.vpcEndpoints.Len())

	for _, ep := range b.vpcEndpoints.All() {
		if statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			continue
		}

		cp := *ep
		out = append(out, &cp)
	}

	return out
}

// ListVpcEndpointsForDomain returns VPC endpoints associated with a domain ARN,
// excluding any whose deleting window has elapsed.
func (b *InMemoryBackend) ListVpcEndpointsForDomain(domainArn string) []*VpcEndpoint {
	b.mu.RLock("ListVpcEndpointsForDomain")
	defer b.mu.RUnlock()

	now := b.clock()

	var out []*VpcEndpoint

	for _, ep := range b.vpcEndpoints.All() {
		if ep.DomainArn == domainArn && !statusWindowElapsed(ep.Status, ep.StatusUntil, now) {
			cp := *ep
			out = append(out, &cp)
		}
	}

	if out == nil {
		out = []*VpcEndpoint{}
	}

	return out
}

// RevokeVpcEndpointAccess removes a principal from the VPC authorizations for a domain.
func (b *InMemoryBackend) RevokeVpcEndpointAccess(domainName, account string) error {
	b.mu.Lock("RevokeVpcEndpointAccess")
	defer b.mu.Unlock()

	principals := b.vpcAuthorizations[domainName]
	filtered := principals[:0]

	for _, p := range principals {
		if p.Principal != account {
			filtered = append(filtered, p)
		}
	}

	b.vpcAuthorizations[domainName] = filtered

	return nil
}

// ListVpcEndpointAccess returns authorized principals for a domain.
func (b *InMemoryBackend) ListVpcEndpointAccess(domainName string) ([]AuthorizedPrincipal, error) {
	b.mu.RLock("ListVpcEndpointAccess")
	defer b.mu.RUnlock()

	principals := b.vpcAuthorizations[domainName]
	out := make([]AuthorizedPrincipal, len(principals))
	copy(out, principals)

	return out, nil
}
