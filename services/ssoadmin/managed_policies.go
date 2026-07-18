package ssoadmin

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// AttachManagedPolicyToPermissionSet attaches a managed policy to a permission set.
func (b *InMemoryBackend) AttachManagedPolicyToPermissionSet(
	instanceArn, permissionSetArn, managedPolicyArn, name string,
) error {
	b.mu.Lock("AttachManagedPolicyToPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	for _, mp := range ps.ManagedPolicies {
		if mp.Arn == managedPolicyArn {
			return nil
		}
	}
	ps.ManagedPolicies = append(ps.ManagedPolicies, ManagedPolicy{Arn: managedPolicyArn, Name: name})

	return nil
}

// DetachManagedPolicyFromPermissionSet detaches a managed policy from a permission set.
// Returns ResourceNotFoundException if the policy is not attached.
func (b *InMemoryBackend) DetachManagedPolicyFromPermissionSet(
	instanceArn, permissionSetArn, managedPolicyArn string,
) error {
	b.mu.Lock("DetachManagedPolicyFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	found := false
	remaining := make([]ManagedPolicy, 0, len(ps.ManagedPolicies))
	for _, mp := range ps.ManagedPolicies {
		if mp.Arn == managedPolicyArn {
			found = true
		} else {
			remaining = append(remaining, mp)
		}
	}
	if !found {
		return ErrRequestNotFound
	}
	ps.ManagedPolicies = remaining

	return nil
}

// ListManagedPoliciesInPermissionSet lists managed policies attached to a permission set.
func (b *InMemoryBackend) ListManagedPoliciesInPermissionSet(
	instanceArn, permissionSetArn string,
) ([]ManagedPolicy, error) {
	b.mu.RLock("ListManagedPoliciesInPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}
	result := make([]ManagedPolicy, len(ps.ManagedPolicies))
	copy(result, ps.ManagedPolicies)

	return result, nil
}

// validateCustomerManagedPolicyReference checks Name and Path per AWS spec.
func validateCustomerManagedPolicyReference(name, path string) error {
	if name == "" {
		return fmt.Errorf("%w: CustomerManagedPolicyReference.Name is required", awserr.ErrInvalidParameter)
	}
	if len(name) > maxCMPRNameLen {
		return fmt.Errorf("%w: CustomerManagedPolicyReference.Name exceeds maximum length of %d",
			awserr.ErrInvalidParameter, maxCMPRNameLen)
	}
	if !cmprNameRe.MatchString(name) {
		return fmt.Errorf("%w: CustomerManagedPolicyReference.Name contains invalid characters",
			awserr.ErrInvalidParameter)
	}
	if path != "" {
		if len(path) > maxCMPRPathLen {
			return fmt.Errorf("%w: CustomerManagedPolicyReference.Path exceeds maximum length of %d",
				awserr.ErrInvalidParameter, maxCMPRPathLen)
		}
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("%w: CustomerManagedPolicyReference.Path must begin with '/'",
				awserr.ErrInvalidParameter)
		}
	}

	return nil
}

// AttachCustomerManagedPolicyReferenceToPermissionSet attaches a customer-managed policy reference to a permission set.
func (b *InMemoryBackend) AttachCustomerManagedPolicyReferenceToPermissionSet(
	instanceArn, permissionSetArn, name, path string,
) error {
	b.mu.Lock("AttachCustomerManagedPolicyReferenceToPermissionSet")
	defer b.mu.Unlock()

	if err := validateCustomerManagedPolicyReference(name, path); err != nil {
		return err
	}

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	for _, ref := range b.customerManagedPolicies[permissionSetArn] {
		if ref.Name == name && ref.Path == path {
			return nil
		}
	}
	b.customerManagedPolicies[permissionSetArn] = append(
		b.customerManagedPolicies[permissionSetArn],
		CustomerManagedPolicyReference{Name: name, Path: path},
	)

	return nil
}

// ListCustomerManagedPolicyReferencesInPermissionSet lists customer-managed policy references in a permission set.
func (b *InMemoryBackend) ListCustomerManagedPolicyReferencesInPermissionSet(
	instanceArn,
	permissionSetArn string,
) ([]CustomerManagedPolicyReference, error) {
	b.mu.RLock("ListCustomerManagedPolicyReferencesInPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}
	refs := b.customerManagedPolicies[permissionSetArn]
	result := make([]CustomerManagedPolicyReference, len(refs))
	copy(result, refs)

	return result, nil
}

// DetachCustomerManagedPolicyReferenceFromPermissionSet detaches a customer-managed policy reference.
func (b *InMemoryBackend) DetachCustomerManagedPolicyReferenceFromPermissionSet(
	instanceArn, permissionSetArn, name, path string,
) error {
	b.mu.Lock("DetachCustomerManagedPolicyReferenceFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	all := b.customerManagedPolicies[permissionSetArn]
	found := false
	remaining := make([]CustomerManagedPolicyReference, 0, len(all))
	for _, ref := range all {
		if ref.Name == name && ref.Path == path {
			found = true
		} else {
			remaining = append(remaining, ref)
		}
	}
	if !found {
		return ErrRequestNotFound
	}
	b.customerManagedPolicies[permissionSetArn] = remaining

	return nil
}
