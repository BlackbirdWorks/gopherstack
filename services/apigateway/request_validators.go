package apigateway

import (
	"fmt"
	"sort"
)

// CreateRequestValidator creates a new request validator for a REST API.
func (b *InMemoryBackend) CreateRequestValidator(
	restAPIID string,
	input CreateRequestValidatorInput,
) (*RequestValidator, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRequestValidator")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	id := randomID(resourceIDLength)
	rv := &RequestValidator{
		ID:                        id,
		RestAPIID:                 restAPIID,
		Name:                      input.Name,
		ValidateRequestBody:       input.ValidateRequestBody,
		ValidateRequestParameters: input.ValidateRequestParameters,
	}
	b.requestValidators.Put(rv)

	cp := *rv

	return &cp, nil
}

// GetRequestValidator retrieves a request validator by ID.
func (b *InMemoryBackend) GetRequestValidator(restAPIID, validatorID string) (*RequestValidator, error) {
	b.mu.RLock("GetRequestValidator")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	rv, ok := b.requestValidators.Get(requestValidatorKey(restAPIID, validatorID))
	if !ok {
		return nil, fmt.Errorf("%w: request validator %s not found", ErrValidatorNotFound, validatorID)
	}
	cp := *rv

	return &cp, nil
}

// GetRequestValidators returns all request validators for a REST API.
func (b *InMemoryBackend) GetRequestValidators(restAPIID string) ([]RequestValidator, error) {
	b.mu.RLock("GetRequestValidators")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.requestValidatorsByAPI.Get(restAPIID)
	all := make([]RequestValidator, 0, len(group))
	for _, rv := range group {
		all = append(all, *rv)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// UpdateRequestValidator updates fields on an existing request validator.
func (b *InMemoryBackend) UpdateRequestValidator(
	restAPIID, validatorID string,
	input UpdateRequestValidatorInput,
) (*RequestValidator, error) {
	b.mu.Lock("UpdateRequestValidator")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	rv, ok := b.requestValidators.Get(requestValidatorKey(restAPIID, validatorID))
	if !ok {
		return nil, fmt.Errorf("%w: request validator %s not found", ErrValidatorNotFound, validatorID)
	}

	if input.Name != "" {
		rv.Name = input.Name
	}
	if input.ValidateRequestBody != nil {
		rv.ValidateRequestBody = *input.ValidateRequestBody
	}
	if input.ValidateRequestParameters != nil {
		rv.ValidateRequestParameters = *input.ValidateRequestParameters
	}

	cp := *rv

	return &cp, nil
}

// DeleteRequestValidator removes a request validator from a REST API.
func (b *InMemoryBackend) DeleteRequestValidator(restAPIID, validatorID string) error {
	b.mu.Lock("DeleteRequestValidator")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if !b.requestValidators.Delete(requestValidatorKey(restAPIID, validatorID)) {
		return fmt.Errorf("%w: request validator %s not found", ErrValidatorNotFound, validatorID)
	}

	return nil
}
