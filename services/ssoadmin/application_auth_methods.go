package ssoadmin

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// DeleteApplicationAuthenticationMethod removes an authentication method from an application.
func (b *InMemoryBackend) DeleteApplicationAuthenticationMethod(applicationArn, authMethodType string) error {
	b.mu.Lock("DeleteApplicationAuthenticationMethod")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	methods := b.applicationAuthMethods[applicationArn]
	if methods == nil {
		return ErrAuthMethodNotFound
	}
	if _, exists := methods[authMethodType]; !exists {
		return ErrAuthMethodNotFound
	}
	delete(methods, authMethodType)

	return nil
}

// PutApplicationAuthenticationMethod stores a typed authentication method with its body.
// authMethodType must be IAM; body is the full AuthenticationMethod JSON object.
func (b *InMemoryBackend) PutApplicationAuthenticationMethod(
	applicationArn, authMethodType string,
	body json.RawMessage,
) error {
	b.mu.Lock("PutApplicationAuthenticationMethod")
	defer b.mu.Unlock()

	if authMethodType != authMethodTypeIAM {
		return fmt.Errorf("%w: AuthenticationMethodType must be IAM", awserr.ErrInvalidParameter)
	}

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	if b.applicationAuthMethods[applicationArn] == nil {
		b.applicationAuthMethods[applicationArn] = make(map[string]json.RawMessage)
	}
	if len(body) == 0 {
		body = json.RawMessage("null")
	}
	b.applicationAuthMethods[applicationArn][authMethodType] = body

	return nil
}

// ListApplicationAuthenticationMethods lists authentication methods with their bodies.
func (b *InMemoryBackend) ListApplicationAuthenticationMethods(applicationArn string) ([]AuthMethod, error) {
	b.mu.RLock("ListApplicationAuthenticationMethods")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}
	methods := b.applicationAuthMethods[applicationArn]
	result := make([]AuthMethod, 0, len(methods))
	for mType, body := range methods {
		bodyCopy := make(json.RawMessage, len(body))
		copy(bodyCopy, body)
		result = append(result, AuthMethod{AuthMethodType: mType, Body: bodyCopy})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AuthMethodType < result[j].AuthMethodType })

	return result, nil
}

// GetApplicationAuthenticationMethod returns a specific auth method body.
func (b *InMemoryBackend) GetApplicationAuthenticationMethod(
	applicationArn, authMethodType string,
) (json.RawMessage, error) {
	b.mu.RLock("GetApplicationAuthenticationMethod")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}
	methods := b.applicationAuthMethods[applicationArn]
	if methods == nil {
		return nil, ErrAuthMethodNotFound
	}
	body, exists := methods[authMethodType]
	if !exists {
		return nil, ErrAuthMethodNotFound
	}
	result := make(json.RawMessage, len(body))
	copy(result, body)

	return result, nil
}
