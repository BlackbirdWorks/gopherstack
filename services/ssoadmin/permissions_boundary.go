package ssoadmin

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// PutPermissionsBoundaryToPermissionSet sets the permissions boundary on a permission set.
// boundary must have exactly one of ManagedPolicyArn or CustomerManagedPolicyReference set.
func (b *InMemoryBackend) PutPermissionsBoundaryToPermissionSet(
	instanceArn, permissionSetArn string, boundary *PermissionsBoundary,
) error {
	b.mu.Lock("PutPermissionsBoundaryToPermissionSet")
	defer b.mu.Unlock()

	if boundary == nil {
		return fmt.Errorf("%w: PermissionsBoundary is required", awserr.ErrInvalidParameter)
	}
	hasManaged := boundary.ManagedPolicyArn != ""
	hasCMPR := boundary.CustomerManagedPolicyReference != nil
	if hasManaged == hasCMPR {
		return fmt.Errorf(
			"%w: PermissionsBoundary must have exactly one of ManagedPolicyArn or CustomerManagedPolicyReference",
			awserr.ErrInvalidParameter,
		)
	}
	if hasCMPR {
		if err := validateCustomerManagedPolicyReference(
			boundary.CustomerManagedPolicyReference.Name,
			boundary.CustomerManagedPolicyReference.Path,
		); err != nil {
			return err
		}
	}

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	// boundary is stored by the same pointer the caller supplied, matching the
	// pre-store.Table aliasing semantics of the raw map this replaces. The
	// identity field is set here rather than by the caller since it is
	// internal bookkeeping for the permissionBoundaries Table's keyFn (see
	// store_setup.go) and is excluded from JSON via its json:"-" tag.
	boundary.PermissionSetArn = permissionSetArn
	b.permissionBoundaries.Put(boundary)
	bumpModified(ps)

	return nil
}

// GetPermissionsBoundaryForPermissionSet returns the permissions boundary for a permission set.
func (b *InMemoryBackend) GetPermissionsBoundaryForPermissionSet(
	instanceArn,
	permissionSetArn string,
) (*PermissionsBoundary, error) {
	b.mu.RLock("GetPermissionsBoundaryForPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}

	boundary, ok := b.permissionBoundaries.Get(permissionSetArn)
	if !ok {
		return nil, ErrRequestNotFound
	}

	return boundary, nil
}

// DeletePermissionsBoundaryFromPermissionSet removes the permissions boundary from a permission set.
func (b *InMemoryBackend) DeletePermissionsBoundaryFromPermissionSet(instanceArn, permissionSetArn string) error {
	b.mu.Lock("DeletePermissionsBoundaryFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	if !b.permissionBoundaries.Has(permissionSetArn) {
		return ErrRequestNotFound
	}
	b.permissionBoundaries.Delete(permissionSetArn)
	bumpModified(ps)

	return nil
}
