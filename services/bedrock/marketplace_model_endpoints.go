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
// endpointConfig is optional (nil is stored as-is); when the caller supplies one
// it is round-tripped verbatim through Get/List/Update, matching real AWS's
// required EndpointConfig response field.
func (b *InMemoryBackend) CreateMarketplaceModelEndpoint(
	endpointName, modelSourceID string,
	endpointConfig *SageMakerEndpointConfig,
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
		EndpointArn:    endpointARN,
		EndpointName:   endpointName,
		ModelSourceID:  modelSourceID,
		EndpointConfig: copySageMakerEndpointConfig(endpointConfig),
		Status:         statusCreating,
		CreatedAt:      now,
		UpdatedAt:      now,
		Tags:           copyTags(tags),
	}
	b.marketplaceEndpoints.Put(ep)
	b.marketplaceEndpointsByName[endpointName] = endpointARN
	cp := *ep
	cp.Tags = copyTags(ep.Tags)
	cp.EndpointConfig = copySageMakerEndpointConfig(ep.EndpointConfig)

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
	cp.EndpointConfig = copySageMakerEndpointConfig(ep.EndpointConfig)

	return &cp, nil
}

// ListMarketplaceModelEndpoints returns marketplace endpoints matching
// modelSourceEquals (real query param "modelSourceIdentifier", aws-sdk-go-v2
// serializers.go:6822-6824), with optional pagination. An empty
// modelSourceEquals matches every endpoint.
func (b *InMemoryBackend) ListMarketplaceModelEndpoints(
	nextToken, modelSourceEquals string,
) ([]*MarketplaceModelEndpoint, string) {
	b.mu.RLock("ListMarketplaceModelEndpoints")
	defer b.mu.RUnlock()

	list := make([]*MarketplaceModelEndpoint, 0, b.marketplaceEndpoints.Len())

	for _, ep := range b.marketplaceEndpoints.All() {
		if modelSourceEquals != "" && ep.ModelSourceID != modelSourceEquals {
			continue
		}

		cp := *ep
		cp.Tags = copyTags(ep.Tags)
		cp.EndpointConfig = copySageMakerEndpointConfig(ep.EndpointConfig)
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

// UpdateMarketplaceModelEndpoint updates a marketplace endpoint's EndpointConfig
// (real AWS: UpdateMarketplaceModelEndpointInput.EndpointConfig is a required
// field -- gopherstack previously accepted but silently dropped it, only
// bumping UpdatedAt).
func (b *InMemoryBackend) UpdateMarketplaceModelEndpoint(
	idOrARN string,
	endpointConfig *SageMakerEndpointConfig,
) (*MarketplaceModelEndpoint, error) {
	b.mu.Lock("UpdateMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep, _ := b.marketplaceEndpoints.Get(epARN)
	if endpointConfig != nil {
		ep.EndpointConfig = copySageMakerEndpointConfig(endpointConfig)
	}

	ep.UpdatedAt = time.Now().UTC()
	cp := *ep
	cp.Tags = copyTags(ep.Tags)
	cp.EndpointConfig = copySageMakerEndpointConfig(ep.EndpointConfig)

	return &cp, nil
}

// copySageMakerEndpointConfig returns a deep copy of src, or nil if src is nil.
func copySageMakerEndpointConfig(src *SageMakerEndpointConfig) *SageMakerEndpointConfig {
	if src == nil {
		return nil
	}

	cp := *src

	return &cp
}

// RegisterMarketplaceModelEndpoint transitions endpoint status to Active and stores
// the required modelSourceIdentifier from the request
// (aws-sdk-go-v2 api_op_RegisterMarketplaceModelEndpoint.go:37).
func (b *InMemoryBackend) RegisterMarketplaceModelEndpoint(
	idOrARN, modelSourceIdentifier string,
) (*MarketplaceModelEndpoint, error) {
	b.mu.Lock("RegisterMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	if modelSourceIdentifier == "" {
		return nil, fmt.Errorf("%w: modelSourceIdentifier is required", ErrValidation)
	}

	ep, _ := b.marketplaceEndpoints.Get(epARN)
	ep.ModelSourceID = modelSourceIdentifier
	ep.Status = "Active"
	ep.UpdatedAt = time.Now().UTC()

	cp := *ep
	cp.Tags = copyTags(ep.Tags)
	cp.EndpointConfig = copySageMakerEndpointConfig(ep.EndpointConfig)

	return &cp, nil
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
