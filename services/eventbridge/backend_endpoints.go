package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CreateEndpoint creates a new global endpoint.
func (b *InMemoryBackend) CreateEndpoint(ctx context.Context, input CreateEndpointInput) (*Endpoint, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	if b.endpointsTable(region).Has(input.Name) {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	buses := input.EventBuses
	if buses == nil {
		buses = []EndpointEventBus{}
	}

	ep := &Endpoint{
		Arn:               b.endpointARN(input.Name),
		CreationTime:      now,
		Description:       input.Description,
		EndpointID:        input.Name + "-" + b.region,
		EndpointURL:       "https://" + input.Name + ".endpoint.events." + b.region + ".amazonaws.com",
		EventBuses:        buses,
		LastModifiedTime:  now,
		Name:              input.Name,
		ReplicationConfig: input.ReplicationConfig,
		RoleArn:           input.RoleArn,
		RoutingConfig:     input.RoutingConfig,
		State:             stateActive,
	}
	b.endpointsTable(region).Put(ep)

	cp := *ep

	return &cp, nil
}

// DeleteEndpoint deletes an endpoint.
func (b *InMemoryBackend) DeleteEndpoint(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	store := b.endpointsTable(region)
	if !store.Has(name) {
		return fmt.Errorf("%w: endpoint %s not found", ErrNotFound, name)
	}

	store.Delete(name)

	return nil
}

// DescribeEndpoint returns a single endpoint by name.
func (b *InMemoryBackend) DescribeEndpoint(ctx context.Context, name string) (*Endpoint, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeEndpoint")
	defer b.mu.RUnlock()

	ep, exists := b.endpointsTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, name)
	}

	cp := *ep

	return &cp, nil
}

// ListEndpoints returns endpoints optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEndpoints(ctx context.Context, namePrefix, nextToken string) ([]Endpoint, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListEndpoints")
	defer b.mu.RUnlock()

	store := b.endpointsTable(region)
	all := make([]Endpoint, 0, store.Len())
	for _, ep := range store.All() {
		if namePrefix == "" || strings.HasPrefix(ep.Name, namePrefix) {
			all = append(all, *ep)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateEndpoint updates an existing endpoint.
func (b *InMemoryBackend) UpdateEndpoint(ctx context.Context, input UpdateEndpointInput) (*Endpoint, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.endpointsTable(region).Get(input.Name)
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, input.Name)
	}

	if input.Description != "" {
		ep.Description = input.Description
	}
	if input.RoleArn != "" {
		ep.RoleArn = input.RoleArn
	}
	if input.RoutingConfig != nil {
		ep.RoutingConfig = input.RoutingConfig
	}
	if input.ReplicationConfig != nil {
		ep.ReplicationConfig = input.ReplicationConfig
	}
	if len(input.EventBuses) > 0 {
		ep.EventBuses = input.EventBuses
	}
	ep.LastModifiedTime = time.Now()

	cp := *ep

	return &cp, nil
}

// AddEndpointInternal adds an endpoint directly for testing.
func (b *InMemoryBackend) AddEndpointInternal(ep *Endpoint) {
	b.mu.Lock("AddEndpointInternal")
	defer b.mu.Unlock()

	cp := *ep
	b.endpointsTable(b.region).Put(&cp)
}
