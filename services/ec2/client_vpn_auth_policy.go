package ec2

import (
	"encoding/json"
	"fmt"
)

const (
	cvpnPolicyStatusCreating = "creating"
	cvpnPolicyStatusUpdating = "updating"
	cvpnPolicyStatusActive   = "active"
	cvpnPolicyStatusDeleting = "deleting"

	cvpnShadowEnabled  = "enabled"
	cvpnShadowDisabled = "disabled"
)

// GetClientVpnEndpointAuthorizationPolicy returns a copy of the endpoint's authorization policy.
func (b *InMemoryBackend) GetClientVpnEndpointAuthorizationPolicy(endpointID string) (*ClientVpnAuthPolicy, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetClientVpnEndpointAuthorizationPolicy")
	defer b.mu.RUnlock()

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	if ep.AuthPolicy == nil {
		return nil, fmt.Errorf("%w: %s", ErrClientVpnAuthPolicyNotFound, endpointID)
	}

	cp := *ep.AuthPolicy

	return &cp, nil
}

// ModifyClientVpnEndpointAuthorizationPolicy creates the endpoint's policy or merges the supplied values into it;
// created reports whether this call created it.
func (b *InMemoryBackend) ModifyClientVpnEndpointAuthorizationPolicy(
	endpointID string, description, policyDocument, shadowMode *string,
) (bool, error) {
	if endpointID == "" {
		return false, fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	if policyDocument != nil {
		var doc map[string]any
		if err := json.Unmarshal([]byte(*policyDocument), &doc); err != nil || doc == nil {
			return false, fmt.Errorf("%w: PolicyDocument must be a JSON object", ErrInvalidParameter)
		}
	}

	if shadowMode != nil && *shadowMode != cvpnShadowEnabled && *shadowMode != cvpnShadowDisabled {
		return false, fmt.Errorf("%w: ShadowMode must be enabled or disabled", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClientVpnEndpointAuthorizationPolicy")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	created := ep.AuthPolicy == nil

	next := ClientVpnAuthPolicy{ShadowMode: cvpnShadowDisabled}
	if !created {
		next = *ep.AuthPolicy
	}

	if description != nil {
		next.Description = *description
	}

	if policyDocument != nil {
		next.PolicyDocument = *policyDocument
	}

	if shadowMode != nil {
		next.ShadowMode = *shadowMode
	}

	ep.AuthPolicy = &next

	return created, nil
}

// DeleteClientVpnEndpointAuthorizationPolicy removes the endpoint's authorization policy.
func (b *InMemoryBackend) DeleteClientVpnEndpointAuthorizationPolicy(endpointID string) error {
	if endpointID == "" {
		return fmt.Errorf("%w: ClientVpnEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteClientVpnEndpointAuthorizationPolicy")
	defer b.mu.Unlock()

	ep, ok := b.clientVpnEndpoints.Get(endpointID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrClientVpnEndpointNotFound, endpointID)
	}

	if ep.AuthPolicy == nil {
		return fmt.Errorf("%w: %s", ErrClientVpnAuthPolicyNotFound, endpointID)
	}

	ep.AuthPolicy = nil

	return nil
}
