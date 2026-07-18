package ec2

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

// CreateVerifiedAccessEndpoint creates a Verified Access endpoint.
func (b *InMemoryBackend) CreateVerifiedAccessEndpoint(
	groupID, endpointType, description string,
) (*VerifiedAccessEndpoint, error) {
	if groupID == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVerifiedAccessEndpoint")
	defer b.mu.Unlock()

	id := "vae-" + uuid.New().String()[:8]
	ep := &VerifiedAccessEndpoint{
		VerifiedAccessEndpointID: id,
		VerifiedAccessGroupID:    groupID,
		Status:                   stateActive,
		Description:              description,
		EndpointType:             endpointType,
	}
	b.verifiedAccessEndpoints.Put(ep)

	return ep, nil
}

// DeleteVerifiedAccessEndpoint removes a Verified Access endpoint.
func (b *InMemoryBackend) DeleteVerifiedAccessEndpoint(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessEndpoints.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessEndpointNotFound, id)
	}
	b.verifiedAccessEndpoints.Delete(id)

	return nil
}

// DescribeVerifiedAccessEndpoints returns Verified Access endpoints.
func (b *InMemoryBackend) DescribeVerifiedAccessEndpoints(ids []string) []*VerifiedAccessEndpoint {
	b.mu.RLock("DescribeVerifiedAccessEndpoints")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessEndpoint
	for _, ep := range b.verifiedAccessEndpoints.All() {
		if len(filter) > 0 && !filter[ep.VerifiedAccessEndpointID] {
			continue
		}
		cp := *ep
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessEndpointID < out[j].VerifiedAccessEndpointID
	})

	return out
}

// ModifyVerifiedAccessEndpoint modifies a Verified Access endpoint.
func (b *InMemoryBackend) ModifyVerifiedAccessEndpoint(id, description string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.verifiedAccessEndpoints.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessEndpointNotFound, id)
	}
	if description != "" {
		ep.Description = description
	}

	return nil
}

// CreateVerifiedAccessGroup creates a Verified Access group.
func (b *InMemoryBackend) CreateVerifiedAccessGroup(
	instanceID, description string,
) (*VerifiedAccessGroup, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVerifiedAccessGroup")
	defer b.mu.Unlock()

	id := "vagr-" + uuid.New().String()[:8]
	grp := &VerifiedAccessGroup{
		VerifiedAccessGroupID:    id,
		VerifiedAccessInstanceID: instanceID,
		Status:                   stateActive,
		Description:              description,
	}
	b.verifiedAccessGroups.Put(grp)

	return grp, nil
}

// DeleteVerifiedAccessGroup removes a Verified Access group.
func (b *InMemoryBackend) DeleteVerifiedAccessGroup(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessGroup")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessGroups.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessGroupNotFound, id)
	}
	b.verifiedAccessGroups.Delete(id)

	return nil
}

// DescribeVerifiedAccessGroups returns Verified Access groups.
func (b *InMemoryBackend) DescribeVerifiedAccessGroups(ids []string) []*VerifiedAccessGroup {
	b.mu.RLock("DescribeVerifiedAccessGroups")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessGroup
	for _, grp := range b.verifiedAccessGroups.All() {
		if len(filter) > 0 && !filter[grp.VerifiedAccessGroupID] {
			continue
		}
		cp := *grp
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessGroupID < out[j].VerifiedAccessGroupID
	})

	return out
}

// CreateVerifiedAccessInstance creates a Verified Access instance.
func (b *InMemoryBackend) CreateVerifiedAccessInstance(description string) (*VerifiedAccessInstance, error) {
	b.mu.Lock("CreateVerifiedAccessInstance")
	defer b.mu.Unlock()

	id := "vai-" + uuid.New().String()[:8]
	inst := &VerifiedAccessInstance{
		VerifiedAccessInstanceID: id,
		Status:                   stateActive,
		Description:              description,
	}
	b.verifiedAccessInstances.Put(inst)

	return inst, nil
}

// DeleteVerifiedAccessInstance removes a Verified Access instance.
func (b *InMemoryBackend) DeleteVerifiedAccessInstance(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessInstance")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessInstances.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, id)
	}
	b.verifiedAccessInstances.Delete(id)

	return nil
}

// DescribeVerifiedAccessInstances returns Verified Access instances.
func (b *InMemoryBackend) DescribeVerifiedAccessInstances(ids []string) []*VerifiedAccessInstance {
	b.mu.RLock("DescribeVerifiedAccessInstances")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessInstance
	for _, inst := range b.verifiedAccessInstances.All() {
		if len(filter) > 0 && !filter[inst.VerifiedAccessInstanceID] {
			continue
		}
		cp := *inst
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessInstanceID < out[j].VerifiedAccessInstanceID
	})

	return out
}

// CreateVerifiedAccessTrustProvider creates a Verified Access trust provider.
func (b *InMemoryBackend) CreateVerifiedAccessTrustProvider(
	trustProviderType, description string,
) (*VerifiedAccessTrustProvider, error) {
	if trustProviderType == "" {
		return nil, fmt.Errorf("%w: TrustProviderType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	id := "vatp-" + uuid.New().String()[:8]
	tp := &VerifiedAccessTrustProvider{
		VerifiedAccessTrustProviderID: id,
		TrustProviderType:             trustProviderType,
		Status:                        stateActive,
		Description:                   description,
	}
	b.verifiedAccessTrustProviders.Put(tp)

	return tp, nil
}

// DeleteVerifiedAccessTrustProvider removes a Verified Access trust provider.
func (b *InMemoryBackend) DeleteVerifiedAccessTrustProvider(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VerifiedAccessTrustProviderId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessTrustProviders.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessTrustProviderNF, id)
	}
	b.verifiedAccessTrustProviders.Delete(id)

	return nil
}

// DescribeVerifiedAccessTrustProviders returns Verified Access trust providers.
func (b *InMemoryBackend) DescribeVerifiedAccessTrustProviders(
	ids []string,
) []*VerifiedAccessTrustProvider {
	b.mu.RLock("DescribeVerifiedAccessTrustProviders")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessTrustProvider
	for _, tp := range b.verifiedAccessTrustProviders.All() {
		if len(filter) > 0 && !filter[tp.VerifiedAccessTrustProviderID] {
			continue
		}
		cp := *tp
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].VerifiedAccessTrustProviderID < out[j].VerifiedAccessTrustProviderID
	})

	return out
}

// AttachVerifiedAccessTrustProvider attaches a trust provider to a Verified Access instance.
func (b *InMemoryBackend) AttachVerifiedAccessTrustProvider(instanceID, trustProviderID string) error {
	if instanceID == "" || trustProviderID == "" {
		return fmt.Errorf(
			"%w: VerifiedAccessInstanceId and VerifiedAccessTrustProviderId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("AttachVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	inst, ok := b.verifiedAccessInstances.Get(instanceID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
	}
	if _, exists := b.verifiedAccessTrustProviders.Get(trustProviderID); !exists {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessTrustProviderNF, trustProviderID)
	}

	if slices.Contains(inst.AttachedTrustProviderIDs, trustProviderID) {
		return nil // already attached
	}
	inst.AttachedTrustProviderIDs = append(inst.AttachedTrustProviderIDs, trustProviderID)

	return nil
}

// DetachVerifiedAccessTrustProvider detaches a trust provider from a Verified Access instance.
func (b *InMemoryBackend) DetachVerifiedAccessTrustProvider(instanceID, trustProviderID string) error {
	if instanceID == "" || trustProviderID == "" {
		return fmt.Errorf(
			"%w: VerifiedAccessInstanceId and VerifiedAccessTrustProviderId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DetachVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	inst, ok := b.verifiedAccessInstances.Get(instanceID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
	}

	var kept []string
	for _, id := range inst.AttachedTrustProviderIDs {
		if id != trustProviderID {
			kept = append(kept, id)
		}
	}
	inst.AttachedTrustProviderIDs = kept

	return nil
}

// ModifyVerifiedAccessGroup updates a Verified Access group's description
// and/or moves it to a different Verified Access instance, mutating the
// existing group created by CreateVerifiedAccessGroup.
func (b *InMemoryBackend) ModifyVerifiedAccessGroup(
	id, instanceID, description string,
) (*VerifiedAccessGroup, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessGroup")
	defer b.mu.Unlock()

	grp, ok := b.verifiedAccessGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessGroupNotFound, id)
	}

	if instanceID != "" {
		if _, exists := b.verifiedAccessInstances.Get(instanceID); !exists {
			return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
		}

		grp.VerifiedAccessInstanceID = instanceID
	}

	if description != "" {
		grp.Description = description
	}

	cp := *grp

	return &cp, nil
}

// ModifyVerifiedAccessInstance updates a Verified Access instance's
// description, mutating the existing instance created by
// CreateVerifiedAccessInstance.
func (b *InMemoryBackend) ModifyVerifiedAccessInstance(id, description string) (*VerifiedAccessInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessInstance")
	defer b.mu.Unlock()

	inst, ok := b.verifiedAccessInstances.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, id)
	}

	if description != "" {
		inst.Description = description
	}

	cp := *inst

	return &cp, nil
}

// ModifyVerifiedAccessTrustProvider updates a Verified Access trust
// provider's description, mutating the existing trust provider created by
// CreateVerifiedAccessTrustProvider.
func (b *InMemoryBackend) ModifyVerifiedAccessTrustProvider(
	id, description string,
) (*VerifiedAccessTrustProvider, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessTrustProviderId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessTrustProvider")
	defer b.mu.Unlock()

	tp, ok := b.verifiedAccessTrustProviders.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessTrustProviderNF, id)
	}

	if description != "" {
		tp.Description = description
	}

	cp := *tp

	return &cp, nil
}

// ---- Transit Gateway route propagation + unified attachment describe ----
