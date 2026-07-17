package apigatewayv2

import (
	"fmt"
	"sort"
)

// CreateRouteResponse creates a new route response.
func (b *InMemoryBackend) CreateRouteResponse(
	apiID, routeID string,
	input CreateRouteResponseInput,
) (*RouteResponse, error) {
	if input.RouteResponseKey == "" {
		return nil, fmt.Errorf("%w: routeResponseKey is required", ErrBadRequest)
	}

	b.mu.Lock("CreateRouteResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	for _, existing := range b.routeResponsesByRoute.Get(routeKey(apiID, routeID)) {
		if existing.RouteResponseKey == input.RouteResponseKey {
			return nil, fmt.Errorf("%w: route response key %q already exists", ErrAlreadyExists, input.RouteResponseKey)
		}
	}

	id := randomID()
	rr := &RouteResponse{
		RouteResponseID:          id,
		RouteResponseKey:         input.RouteResponseKey,
		APIID:                    apiID,
		RouteID:                  routeID,
		ModelSelectionExpression: input.ModelSelectionExpression,
		ResponseModels:           input.ResponseModels,
	}

	b.routeResponses.Put(rr)

	cp := *rr

	return &cp, nil
}

// GetRouteResponse retrieves a specific route response.
func (b *InMemoryBackend) GetRouteResponse(apiID, routeID, responseID string) (*RouteResponse, error) {
	b.mu.RLock("GetRouteResponse")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	rr, ok := b.routeResponses.Get(routeResponseKey(apiID, routeID, responseID))
	if !ok {
		return nil, ErrRouteResponseNotFound
	}

	cp := *rr

	return &cp, nil
}

// GetRouteResponses retrieves all route responses for a route.
func (b *InMemoryBackend) GetRouteResponses(apiID, routeID string) ([]RouteResponse, error) {
	b.mu.RLock("GetRouteResponses")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	responses := b.routeResponsesByRoute.Get(routeKey(apiID, routeID))
	result := make([]RouteResponse, 0, len(responses))

	for _, rr := range responses {
		result = append(result, *rr)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RouteResponseID < result[j].RouteResponseID
	})

	return result, nil
}

// DeleteRouteResponse removes a route response.
func (b *InMemoryBackend) DeleteRouteResponse(apiID, routeID, responseID string) error {
	b.mu.Lock("DeleteRouteResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return ErrRouteNotFound
	}

	if !b.routeResponses.Delete(routeResponseKey(apiID, routeID, responseID)) {
		return ErrRouteResponseNotFound
	}

	return nil
}

// UpdateRouteResponse updates fields on an existing route response.
func (b *InMemoryBackend) UpdateRouteResponse(
	apiID, routeID, responseID string,
	input UpdateRouteResponseInput,
) (*RouteResponse, error) {
	b.mu.Lock("UpdateRouteResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	rr, ok := b.routeResponses.Get(routeResponseKey(apiID, routeID, responseID))
	if !ok {
		return nil, ErrRouteResponseNotFound
	}

	if input.RouteResponseKey != "" {
		rr.RouteResponseKey = input.RouteResponseKey
	}

	if input.ModelSelectionExpression != "" {
		rr.ModelSelectionExpression = input.ModelSelectionExpression
	}

	if input.ResponseModels != nil {
		rr.ResponseModels = input.ResponseModels
	}

	cp := *rr

	return &cp, nil
}
