package ssoadmin

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// isValidGrantType reports whether grantType is an AWS-specified grant type enum value.
func isValidGrantType(grantType string) bool {
	switch grantType {
	case "authorization_code",
		"refresh_token",
		"urn:ietf:params:oauth:grant-type:jwt-bearer",
		"urn:ietf:params:oauth:grant-type:token-exchange":
		return true
	default:
		return false
	}
}

// PutApplicationGrant stores a typed grant with its full body.
func (b *InMemoryBackend) PutApplicationGrant(applicationArn, grantType string, body json.RawMessage) error {
	b.mu.Lock("PutApplicationGrant")
	defer b.mu.Unlock()

	if !isValidGrantType(grantType) {
		return fmt.Errorf("%w: GrantType must be one of authorization_code, refresh_token, jwt-bearer, token-exchange",
			awserr.ErrInvalidParameter)
	}

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	if b.applicationGrants[applicationArn] == nil {
		b.applicationGrants[applicationArn] = make(map[string]json.RawMessage)
	}
	if len(body) == 0 {
		body = json.RawMessage("null")
	}
	b.applicationGrants[applicationArn][grantType] = body

	return nil
}

// DeleteApplicationGrant removes a grant from an application.
func (b *InMemoryBackend) DeleteApplicationGrant(applicationArn, grantType string) error {
	b.mu.Lock("DeleteApplicationGrant")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	grants := b.applicationGrants[applicationArn]
	if grants == nil {
		return ErrGrantNotFound
	}
	if _, exists := grants[grantType]; !exists {
		return ErrGrantNotFound
	}
	delete(grants, grantType)

	return nil
}

// ListApplicationGrants lists grants on an application with their bodies.
func (b *InMemoryBackend) ListApplicationGrants(applicationArn string) ([]ApplicationGrant, error) {
	b.mu.RLock("ListApplicationGrants")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}
	grants := b.applicationGrants[applicationArn]
	result := make([]ApplicationGrant, 0, len(grants))
	for gType, body := range grants {
		bodyCopy := make(json.RawMessage, len(body))
		copy(bodyCopy, body)
		result = append(result, ApplicationGrant{GrantType: gType, Grant: bodyCopy})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].GrantType < result[j].GrantType })

	return result, nil
}

// GetApplicationGrant returns a specific grant body.
func (b *InMemoryBackend) GetApplicationGrant(applicationArn, grantType string) (json.RawMessage, error) {
	b.mu.RLock("GetApplicationGrant")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}
	grants := b.applicationGrants[applicationArn]
	if grants == nil {
		return nil, ErrGrantNotFound
	}
	body, exists := grants[grantType]
	if !exists {
		return nil, ErrGrantNotFound
	}
	result := make(json.RawMessage, len(body))
	copy(result, body)

	return result, nil
}
