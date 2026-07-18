package apigatewayv2

import (
	"fmt"
	"sort"
)

// CreateIntegrationResponse creates a new integration response.
func (b *InMemoryBackend) CreateIntegrationResponse(
	apiID, integrationID string,
	input CreateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	if input.IntegrationResponseKey == "" {
		return nil, fmt.Errorf("%w: integrationResponseKey is required", ErrBadRequest)
	}

	b.mu.Lock("CreateIntegrationResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	for _, existing := range b.integrationResponsesByIntegration.Get(integrationKey(apiID, integrationID)) {
		if existing.IntegrationResponseKey == input.IntegrationResponseKey {
			return nil, fmt.Errorf(
				"%w: integration response key %q already exists",
				ErrAlreadyExists,
				input.IntegrationResponseKey,
			)
		}
	}

	id := randomID()
	ir := &IntegrationResponse{
		IntegrationResponseID:       id,
		IntegrationResponseKey:      input.IntegrationResponseKey,
		APIID:                       apiID,
		IntegrationID:               integrationID,
		ContentHandlingStrategy:     input.ContentHandlingStrategy,
		TemplateSelectionExpression: input.TemplateSelectionExpression,
		ResponseParameters:          input.ResponseParameters,
		ResponseTemplates:           input.ResponseTemplates,
	}

	b.integrationResponses.Put(ir)

	cp := *ir

	return &cp, nil
}

// GetIntegrationResponse retrieves a specific integration response.
func (b *InMemoryBackend) GetIntegrationResponse(
	apiID, integrationID, responseID string,
) (*IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponse")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	ir, ok := b.integrationResponses.Get(integrationResponseKey(apiID, integrationID, responseID))
	if !ok {
		return nil, ErrIntegrationResponseNotFound
	}

	cp := *ir

	return &cp, nil
}

// GetIntegrationResponses retrieves all integration responses for an integration.
func (b *InMemoryBackend) GetIntegrationResponses(apiID, integrationID string) ([]IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponses")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	responses := b.integrationResponsesByIntegration.Get(integrationKey(apiID, integrationID))
	result := make([]IntegrationResponse, 0, len(responses))

	for _, ir := range responses {
		result = append(result, *ir)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].IntegrationResponseID < result[j].IntegrationResponseID
	})

	return result, nil
}

// DeleteIntegrationResponse removes an integration response.
func (b *InMemoryBackend) DeleteIntegrationResponse(apiID, integrationID, responseID string) error {
	b.mu.Lock("DeleteIntegrationResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return ErrIntegrationNotFound
	}

	if !b.integrationResponses.Delete(integrationResponseKey(apiID, integrationID, responseID)) {
		return ErrIntegrationResponseNotFound
	}

	return nil
}

// UpdateIntegrationResponse updates fields on an existing integration response.
func (b *InMemoryBackend) UpdateIntegrationResponse(
	apiID, integrationID, responseID string,
	input UpdateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("UpdateIntegrationResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	ir, ok := b.integrationResponses.Get(integrationResponseKey(apiID, integrationID, responseID))
	if !ok {
		return nil, ErrIntegrationResponseNotFound
	}

	if input.IntegrationResponseKey != "" {
		ir.IntegrationResponseKey = input.IntegrationResponseKey
	}

	if input.ContentHandlingStrategy != "" {
		ir.ContentHandlingStrategy = input.ContentHandlingStrategy
	}

	if input.TemplateSelectionExpression != "" {
		ir.TemplateSelectionExpression = input.TemplateSelectionExpression
	}

	if input.ResponseParameters != nil {
		ir.ResponseParameters = input.ResponseParameters
	}

	if input.ResponseTemplates != nil {
		ir.ResponseTemplates = input.ResponseTemplates
	}

	cp := *ir

	return &cp, nil
}
