package apigateway

import (
	"fmt"
	"net/http"
	"sort"
)

// CreateAuthorizer creates a new authorizer for a REST API.
func (b *InMemoryBackend) CreateAuthorizer(restAPIID string, input CreateAuthorizerInput) (*Authorizer, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}
	if input.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrInvalidParameter)
	}

	// AWS rejects AuthorizerResultTtlInSeconds outside [0, 3600].
	const maxAuthorizerTTL = 3600
	if input.AuthorizerResultTTLInSeconds < 0 || input.AuthorizerResultTTLInSeconds > maxAuthorizerTTL {
		return nil, fmt.Errorf("%w: authorizerResultTtlInSeconds must be in [0, %d]",
			ErrInvalidParameter, maxAuthorizerTTL)
	}

	b.mu.Lock("CreateAuthorizer")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	id := randomID(resourceIDLength)
	auth := &Authorizer{
		ID:                           id,
		RestAPIID:                    restAPIID,
		Name:                         input.Name,
		Type:                         input.Type,
		AuthorizerURI:                input.AuthorizerURI,
		AuthorizerCredentials:        input.AuthorizerCredentials,
		IdentitySource:               input.IdentitySource,
		IdentityValidationExpression: input.IdentityValidationExpression,
		AuthType:                     input.AuthType,
		AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
		ProviderARNs:                 input.ProviderARNs,
	}
	b.authorizers.Put(auth)

	cp := *auth

	return &cp, nil
}

// GetAuthorizer retrieves an authorizer by ID.
func (b *InMemoryBackend) GetAuthorizer(restAPIID, authorizerID string) (*Authorizer, error) {
	b.mu.RLock("GetAuthorizer")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	auth, ok := b.authorizers.Get(authorizerKey(restAPIID, authorizerID))
	if !ok {
		return nil, fmt.Errorf("%w: authorizer %s not found", ErrAuthorizerNotFound, authorizerID)
	}
	cp := *auth

	return &cp, nil
}

// GetAuthorizers returns all authorizers for a REST API.
func (b *InMemoryBackend) GetAuthorizers(restAPIID string) ([]Authorizer, error) {
	b.mu.RLock("GetAuthorizers")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.authorizersByAPI.Get(restAPIID)
	all := make([]Authorizer, 0, len(group))
	for _, auth := range group {
		all = append(all, *auth)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// UpdateAuthorizer updates fields on an existing authorizer.
func (b *InMemoryBackend) UpdateAuthorizer(
	restAPIID, authorizerID string,
	input UpdateAuthorizerInput,
) (*Authorizer, error) {
	b.mu.Lock("UpdateAuthorizer")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	auth, ok := b.authorizers.Get(authorizerKey(restAPIID, authorizerID))
	if !ok {
		return nil, fmt.Errorf("%w: authorizer %s not found", ErrAuthorizerNotFound, authorizerID)
	}

	if input.Name != "" {
		auth.Name = input.Name
	}
	if input.Type != "" {
		auth.Type = input.Type
	}
	if input.AuthorizerURI != "" {
		auth.AuthorizerURI = input.AuthorizerURI
	}
	if input.AuthorizerCredentials != "" {
		auth.AuthorizerCredentials = input.AuthorizerCredentials
	}
	if input.AuthType != "" {
		auth.AuthType = input.AuthType
	}
	// IdentitySource is a *string so an explicit PATCH "remove" (a pointer to
	// "") is distinguishable from the field being absent from this PATCH.
	if input.IdentitySource != nil {
		auth.IdentitySource = *input.IdentitySource
	}
	if input.IdentityValidationExpression != "" {
		auth.IdentityValidationExpression = input.IdentityValidationExpression
	}
	if input.AuthorizerResultTTLInSeconds != 0 {
		const maxAuthorizerTTL = 3600
		if input.AuthorizerResultTTLInSeconds < 0 || input.AuthorizerResultTTLInSeconds > maxAuthorizerTTL {
			return nil, fmt.Errorf("%w: authorizerResultTtlInSeconds must be in [0, %d]",
				ErrInvalidParameter, maxAuthorizerTTL)
		}
		auth.AuthorizerResultTTLInSeconds = input.AuthorizerResultTTLInSeconds
	}
	// nil-checked rather than len-checked so patching the last ARN away via
	// "/providerARNs" remove actually clears it (see applyAuthorizerPatchOp).
	if input.ProviderARNs != nil {
		auth.ProviderARNs = input.ProviderARNs
	}

	cp := *auth

	return &cp, nil
}

// DeleteAuthorizer removes an authorizer from a REST API.
func (b *InMemoryBackend) DeleteAuthorizer(restAPIID, authorizerID string) error {
	b.mu.Lock("DeleteAuthorizer")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if !b.authorizers.Delete(authorizerKey(restAPIID, authorizerID)) {
		return fmt.Errorf("%w: authorizer %s not found", ErrAuthorizerNotFound, authorizerID)
	}

	return nil
}

// TestInvokeAuthorizer performs a mock test invocation of an authorizer.
func (b *InMemoryBackend) TestInvokeAuthorizer(input TestInvokeAuthorizerInput) (*TestInvokeAuthorizerOutput, error) {
	b.mu.RLock("TestInvokeAuthorizer")
	defer b.mu.RUnlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if !b.authorizers.Has(authorizerKey(input.RestAPIID, input.AuthorizerID)) {
		return nil, fmt.Errorf("%w: authorizer %s not found", ErrNotFound, input.AuthorizerID)
	}

	return &TestInvokeAuthorizerOutput{
		PrincipalID:  "test-principal",
		ClientStatus: http.StatusOK,
		Latency:      1,
		Log:          "Test authorizer invocation (mock)",
	}, nil
}
