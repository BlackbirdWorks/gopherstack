package apigateway

import (
	"fmt"
)

// PutIntegration creates or replaces an integration on a method.
func (b *InMemoryBackend) PutIntegration(
	restAPIID, resourceID, httpMethod string,
	input PutIntegrationInput,
) (*Integration, error) {
	b.mu.Lock("PutIntegration")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}

	timeout := input.TimeoutInMillis
	if timeout == 0 {
		timeout = defaultIntegrationTimeoutMs
	}

	integ := &Integration{
		Type:                 input.Type,
		HTTPMethod:           input.HTTPMethod,
		URI:                  input.URI,
		PassthroughBehavior:  input.PassthroughBehavior,
		RequestTemplates:     input.RequestTemplates,
		RequestParameters:    input.RequestParameters,
		CacheKeyParameters:   input.CacheKeyParameters,
		ConnectionType:       input.ConnectionType,
		ConnectionID:         input.ConnectionID,
		ContentHandling:      input.ContentHandling,
		Credentials:          input.Credentials,
		CacheNamespace:       input.CacheNamespace,
		TimeoutInMillis:      timeout,
		IntegrationResponses: make(map[string]*IntegrationResponse),
	}
	m.MethodIntegration = integ

	cp := *integ

	return &cp, nil
}

// GetIntegration retrieves the integration for a method.
func (b *InMemoryBackend) GetIntegration(restAPIID, resourceID, httpMethod string) (*Integration, error) {
	b.mu.RLock("GetIntegration")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	cp := *m.MethodIntegration

	return &cp, nil
}

// DeleteIntegration removes the integration from a method.
func (b *InMemoryBackend) DeleteIntegration(restAPIID, resourceID, httpMethod string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	m.MethodIntegration = nil

	return nil
}

// PutIntegrationResponse creates or replaces an integration response.
func (b *InMemoryBackend) PutIntegrationResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
	input PutIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("PutIntegrationResponse")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}

	ir := &IntegrationResponse{
		StatusCode:         statusCode,
		ResponseTemplates:  input.ResponseTemplates,
		ResponseParameters: input.ResponseParameters,
		SelectionPattern:   input.SelectionPattern,
		ContentHandling:    input.ContentHandling,
	}
	if m.MethodIntegration.IntegrationResponses == nil {
		m.MethodIntegration.IntegrationResponses = make(map[string]*IntegrationResponse)
	}
	m.MethodIntegration.IntegrationResponses[statusCode] = ir

	cp := *ir

	return &cp, nil
}

// GetIntegrationResponse retrieves an integration response for a given status code.
func (b *InMemoryBackend) GetIntegrationResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
) (*IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponse")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	ir, ok := m.MethodIntegration.IntegrationResponses[statusCode]
	if !ok {
		return nil, fmt.Errorf("%w: integration response %s not found", ErrIntegrationResponseNotFound, statusCode)
	}
	cp := *ir

	return &cp, nil
}

// DeleteIntegrationResponse removes an integration response from a method integration.
func (b *InMemoryBackend) DeleteIntegrationResponse(restAPIID, resourceID, httpMethod, statusCode string) error {
	b.mu.Lock("DeleteIntegrationResponse")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	if _, exists := m.MethodIntegration.IntegrationResponses[statusCode]; !exists {
		return fmt.Errorf("%w: integration response %s not found", ErrIntegrationResponseNotFound, statusCode)
	}
	delete(m.MethodIntegration.IntegrationResponses, statusCode)

	return nil
}

// UpdateIntegration updates an integration's URI or type.
func (b *InMemoryBackend) UpdateIntegration(input UpdateIntegrationInput) (*Integration, error) {
	b.mu.Lock("UpdateIntegration")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, input.HTTPMethod)
	}

	if m.MethodIntegration == nil {
		m.MethodIntegration = &Integration{}
	}

	applyIntegrationFields(m.MethodIntegration, input)

	return m.MethodIntegration, nil
}

func applyIntegrationFields(intg *Integration, input UpdateIntegrationInput) {
	if input.URI != "" {
		intg.URI = input.URI
	}
	if input.IntegrationType != "" {
		intg.Type = input.IntegrationType
	}
	if input.IntegrationHTTPMethod != "" {
		intg.HTTPMethod = input.IntegrationHTTPMethod
	}
	if input.RequestTemplates != nil {
		intg.RequestTemplates = input.RequestTemplates
	}
	if input.RequestParameters != nil {
		intg.RequestParameters = input.RequestParameters
	}
	if input.CacheKeyParameters != nil {
		intg.CacheKeyParameters = input.CacheKeyParameters
	}
	if input.PassthroughBehavior != "" {
		intg.PassthroughBehavior = input.PassthroughBehavior
	}
	if input.ConnectionType != "" {
		intg.ConnectionType = input.ConnectionType
	}
	if input.ConnectionID != "" {
		intg.ConnectionID = input.ConnectionID
	}
	if input.ContentHandling != "" {
		intg.ContentHandling = input.ContentHandling
	}
	if input.Credentials != "" {
		intg.Credentials = input.Credentials
	}
	if input.CacheNamespace != "" {
		intg.CacheNamespace = input.CacheNamespace
	}
	if input.TimeoutInMillis > 0 {
		intg.TimeoutInMillis = input.TimeoutInMillis
	}
}

// UpdateIntegrationResponse updates an integration response's templates or selection pattern.
func (b *InMemoryBackend) UpdateIntegrationResponse(
	input UpdateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("UpdateIntegrationResponse")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, input.HTTPMethod)
	}

	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: no integration on method %s", ErrNotFound, input.HTTPMethod)
	}

	ir, ok := m.MethodIntegration.IntegrationResponses[input.StatusCode]
	if !ok {
		return nil, fmt.Errorf("%w: integration response %s not found", ErrNotFound, input.StatusCode)
	}

	if input.SelectionPattern != "" {
		ir.SelectionPattern = input.SelectionPattern
	}

	if input.ResponseTemplates != nil {
		ir.ResponseTemplates = input.ResponseTemplates
	}

	if input.ResponseParameters != nil {
		ir.ResponseParameters = input.ResponseParameters
	}

	if input.ContentHandling != "" {
		ir.ContentHandling = input.ContentHandling
	}

	m.MethodIntegration.IntegrationResponses[input.StatusCode] = ir

	return ir, nil
}
