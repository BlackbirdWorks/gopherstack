package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newMarketplaceEndpointID generates a unique marketplace endpoint ID.
func (b *InMemoryBackend) newMarketplaceEndpointID() string {
	b.marketplaceEndpointCounter++

	return fmt.Sprintf("mme-%07d", b.marketplaceEndpointCounter)
}

// CreateMarketplaceModelEndpoint creates a new marketplace model endpoint.
func (b *InMemoryBackend) CreateMarketplaceModelEndpoint(
	endpointName, modelSourceID string,
	tags []Tag,
) (*MarketplaceModelEndpoint, error) {
	b.mu.Lock("CreateMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	if endpointName == "" {
		return nil, fmt.Errorf("%w: endpointName is required", ErrValidation)
	}

	if _, exists := b.marketplaceEndpointsByName[endpointName]; exists {
		return nil, fmt.Errorf(
			"%w: marketplace endpoint %s already exists",
			ErrAlreadyExists,
			endpointName,
		)
	}

	id := b.newMarketplaceEndpointID()
	endpointARN := arn.Build("bedrock", b.region, b.accountID, "marketplace-model-endpoint/"+id)
	now := time.Now().UTC()

	ep := &MarketplaceModelEndpoint{
		EndpointArn:   endpointARN,
		EndpointName:  endpointName,
		ModelSourceID: modelSourceID,
		Status:        statusCreating,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          copyTags(tags),
	}
	b.marketplaceEndpoints.Put(ep)
	b.marketplaceEndpointsByName[endpointName] = endpointARN
	cp := *ep
	cp.Tags = copyTags(ep.Tags)

	return &cp, nil
}

// findMarketplaceEndpointARN resolves an endpoint ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findMarketplaceEndpointARN(idOrARN string) (string, bool) {
	if _, ok := b.marketplaceEndpoints.Get(idOrARN); ok {
		return idOrARN, true
	}

	if a := b.marketplaceEndpointsByName[idOrARN]; a != "" {
		return a, true
	}

	return "", false
}

// GetMarketplaceModelEndpoint returns a marketplace endpoint by ARN or name.
func (b *InMemoryBackend) GetMarketplaceModelEndpoint(
	idOrARN string,
) (*MarketplaceModelEndpoint, error) {
	b.mu.RLock("GetMarketplaceModelEndpoint")
	defer b.mu.RUnlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep, _ := b.marketplaceEndpoints.Get(epARN)
	cp := *ep
	cp.Tags = copyTags(ep.Tags)

	return &cp, nil
}

// ListMarketplaceModelEndpoints returns all marketplace endpoints with optional pagination.
func (b *InMemoryBackend) ListMarketplaceModelEndpoints(
	nextToken string,
) ([]*MarketplaceModelEndpoint, string) {
	b.mu.RLock("ListMarketplaceModelEndpoints")
	defer b.mu.RUnlock()

	list := make([]*MarketplaceModelEndpoint, 0, b.marketplaceEndpoints.Len())

	for _, ep := range b.marketplaceEndpoints.All() {
		cp := *ep
		cp.Tags = copyTags(ep.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].EndpointArn < list[j].EndpointArn })

	return paginateBedrockSlice(list, nextToken)
}

// DeleteMarketplaceModelEndpoint removes a marketplace endpoint by ARN or name.
func (b *InMemoryBackend) DeleteMarketplaceModelEndpoint(idOrARN string) error {
	b.mu.Lock("DeleteMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep, _ := b.marketplaceEndpoints.Get(epARN)
	delete(b.marketplaceEndpointsByName, ep.EndpointName)
	b.marketplaceEndpoints.Delete(epARN)

	return nil
}

// UpdateMarketplaceModelEndpoint updates a marketplace endpoint status.
func (b *InMemoryBackend) UpdateMarketplaceModelEndpoint(
	idOrARN string,
) (*MarketplaceModelEndpoint, error) {
	b.mu.Lock("UpdateMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep, _ := b.marketplaceEndpoints.Get(epARN)
	ep.UpdatedAt = time.Now().UTC()
	cp := *ep
	cp.Tags = copyTags(ep.Tags)

	return &cp, nil
}

// RegisterMarketplaceModelEndpoint transitions endpoint status to Active.
func (b *InMemoryBackend) RegisterMarketplaceModelEndpoint(idOrARN string) error {
	b.mu.Lock("RegisterMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep, _ := b.marketplaceEndpoints.Get(epARN)
	ep.Status = "Active"

	return nil
}

// DeregisterMarketplaceModelEndpoint transitions endpoint status to Deregistered.
func (b *InMemoryBackend) DeregisterMarketplaceModelEndpoint(idOrARN string) error {
	b.mu.Lock("DeregisterMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep, _ := b.marketplaceEndpoints.Get(epARN)
	ep.Status = "Deregistered"

	return nil
}
