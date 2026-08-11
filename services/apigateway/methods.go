package apigateway

import (
	"fmt"
	"net/http"
)

// PutMethod creates or replaces a method on a resource.
func (b *InMemoryBackend) PutMethod(input PutMethodInput) (*Method, error) {
	b.mu.Lock("PutMethod")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}
	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	reqParams := input.RequestParameters
	if reqParams == nil {
		reqParams = make(map[string]bool)
	}

	m := &Method{
		HTTPMethod:         input.HTTPMethod,
		AuthorizationType:  input.AuthorizationType,
		AuthorizerID:       input.AuthorizerID,
		RequestValidatorID: input.RequestValidatorID,
		APIKeyRequired:     input.APIKeyRequired,
		RequestParameters:  reqParams,
		RequestModels:      input.RequestModels,
		MethodResponses:    make(map[string]*MethodResponse),
		OperationName:      input.OperationName,
	}
	r.ResourceMethods[input.HTTPMethod] = m

	cp := *m

	return &cp, nil
}

// GetMethod retrieves a method on a resource.
func (b *InMemoryBackend) GetMethod(restAPIID, resourceID, httpMethod string) (*Method, error) {
	b.mu.RLock("GetMethod")
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
	cp := *m

	return &cp, nil
}

// DeleteMethod removes a method from a resource.
func (b *InMemoryBackend) DeleteMethod(restAPIID, resourceID, httpMethod string) error {
	b.mu.Lock("DeleteMethod")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	if _, exists := r.ResourceMethods[httpMethod]; !exists {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	delete(r.ResourceMethods, httpMethod)

	return nil
}

// PutMethodResponse creates or replaces a method response on a method.
func (b *InMemoryBackend) PutMethodResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
	input PutMethodResponseInput,
) (*MethodResponse, error) {
	b.mu.Lock("PutMethodResponse")
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

	mr := &MethodResponse{
		StatusCode:         statusCode,
		ResponseModels:     input.ResponseModels,
		ResponseParameters: input.ResponseParameters,
	}
	if m.MethodResponses == nil {
		m.MethodResponses = make(map[string]*MethodResponse)
	}
	m.MethodResponses[statusCode] = mr

	cp := *mr

	return &cp, nil
}

// GetMethodResponse retrieves a method response for a given status code.
func (b *InMemoryBackend) GetMethodResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
) (*MethodResponse, error) {
	b.mu.RLock("GetMethodResponse")
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
	mr, ok := m.MethodResponses[statusCode]
	if !ok {
		return nil, fmt.Errorf("%w: method response %s not found", ErrMethodResponseNotFound, statusCode)
	}
	cp := *mr

	return &cp, nil
}

// DeleteMethodResponse removes a method response from a method.
func (b *InMemoryBackend) DeleteMethodResponse(restAPIID, resourceID, httpMethod, statusCode string) error {
	b.mu.Lock("DeleteMethodResponse")
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
	if _, exists := m.MethodResponses[statusCode]; !exists {
		return fmt.Errorf("%w: method response %s not found", ErrMethodResponseNotFound, statusCode)
	}
	delete(m.MethodResponses, statusCode)

	return nil
}

// TestInvokeMethod performs a test invocation of a method, returning a mock 200 response.
func (b *InMemoryBackend) TestInvokeMethod(input TestInvokeMethodInput) (*TestInvokeMethodOutput, error) {
	b.mu.RLock("TestInvokeMethod")
	defer b.mu.RUnlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf(
			"%w: method %s not found on resource %s",
			ErrMethodNotFound,
			input.HTTPMethod,
			input.ResourceID,
		)
	}

	body := "{}"
	if m.MethodIntegration != nil && m.MethodIntegration.Type == IntegrationTypeMock {
		body = `{"statusCode": 200}`
	}

	return &TestInvokeMethodOutput{
		Status:  http.StatusOK,
		Body:    body,
		Latency: 1,
		Log:     "Test invocation (mock)",
		Headers: map[string]string{"Content-Type": contentTypeJSON},
	}, nil
}

// UpdateMethod updates method settings (authorization, API key requirement, etc.)
func (b *InMemoryBackend) UpdateMethod(input UpdateMethodInput) (*Method, error) {
	b.mu.Lock("UpdateMethod")
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

	if input.AuthorizationType != "" {
		m.AuthorizationType = input.AuthorizationType
	}

	if input.AuthorizerID != "" {
		m.AuthorizerID = input.AuthorizerID
	}

	if input.APIKeyRequired != nil {
		m.APIKeyRequired = *input.APIKeyRequired
	}

	if input.OperationName != "" {
		m.OperationName = input.OperationName
	}

	if input.RequestValidatorID != "" {
		m.RequestValidatorID = input.RequestValidatorID
	}

	if input.RequestModels != nil {
		m.RequestModels = input.RequestModels
	}

	if input.RequestParameters != nil {
		m.RequestParameters = input.RequestParameters
	}

	cp := *m

	return &cp, nil
}

// UpdateMethodResponse updates a method response's models or parameters.
func (b *InMemoryBackend) UpdateMethodResponse(input UpdateMethodResponseInput) (*MethodResponse, error) {
	b.mu.Lock("UpdateMethodResponse")
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

	mr, ok := m.MethodResponses[input.StatusCode]
	if !ok {
		return nil, fmt.Errorf("%w: method response %s not found", ErrNotFound, input.StatusCode)
	}

	if input.ResponseModels != nil {
		mr.ResponseModels = input.ResponseModels
	}

	if input.ResponseParameters != nil {
		mr.ResponseParameters = input.ResponseParameters
	}

	m.MethodResponses[input.StatusCode] = mr

	cp := *mr

	return &cp, nil
}
