package s3control

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// AssociateAccessGrantsIdentityCenter associates an IAM Identity Center instance with an S3 Access Grants instance.
func (b *InMemoryBackend) AssociateAccessGrantsIdentityCenter(accountID, identityCenterArn string) {
	b.mu.Lock("AssociateAccessGrantsIdentityCenter")
	defer b.mu.Unlock()

	inst, ok := b.accessGrantsInstances.Get(accountID)
	if !ok {
		inst = &AccessGrantsInstance{
			AccountID:               accountID,
			AccessGrantsInstanceID:  defaultAccessGrantsInstanceID,
			AccessGrantsInstanceArn: fmt.Sprintf(arnFmtAccessGrantsInstance, b.region, accountID),
			CreatedAt:               nowRFC3339(),
		}
		b.accessGrantsInstances.Put(inst)
	}

	inst.IdentityCenterArn = identityCenterArn
	inst.IdentityCenterInstanceArn = identityCenterArn
}

// CreateAccessGrantsInstance creates an S3 Access Grants instance for an account.
func (b *InMemoryBackend) CreateAccessGrantsInstance(accountID, identityCenterArn string) *AccessGrantsInstance {
	b.mu.Lock("CreateAccessGrantsInstance")
	defer b.mu.Unlock()

	inst := &AccessGrantsInstance{
		AccountID:                 accountID,
		AccessGrantsInstanceID:    defaultAccessGrantsInstanceID,
		AccessGrantsInstanceArn:   fmt.Sprintf(arnFmtAccessGrantsInstance, b.region, accountID),
		IdentityCenterArn:         identityCenterArn,
		IdentityCenterInstanceArn: identityCenterArn,
		CreatedAt:                 nowRFC3339(),
	}
	b.accessGrantsInstances.Put(inst)

	cp := *inst

	return &cp
}

// CreateAccessGrant creates an access grant for an account.
// Returns ErrValidation if permission is empty.
func (b *InMemoryBackend) CreateAccessGrant(
	accountID, locationID, granteeType, granteeIdentifier, permission, applicationArn string,
) (*AccessGrant, error) {
	if permission == "" {
		return nil, fmt.Errorf("permission is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateAccessGrant")
	defer b.mu.Unlock()

	id := b.newID("grant")
	arn := fmt.Sprintf(arnFmtAccessGrant, b.region, accountID, id)

	grant := &AccessGrant{
		AccountID:              accountID,
		AccessGrantID:          id,
		AccessGrantArn:         arn,
		AccessGrantsLocationID: locationID,
		GrantScope:             fmt.Sprintf("s3://%s/*", locationID),
		Permission:             permission,
		GranteeType:            granteeType,
		GranteeIdentifier:      granteeIdentifier,
		ApplicationArn:         applicationArn,
		CreatedAt:              nowRFC3339(),
	}
	b.accessGrants.Put(grant)

	cp := *grant

	return &cp, nil
}

// CreateAccessGrantsLocation creates an Access Grants location.
func (b *InMemoryBackend) CreateAccessGrantsLocation(
	accountID, locationScope, iamRoleArn string,
) *AccessGrantsLocation {
	b.mu.Lock("CreateAccessGrantsLocation")
	defer b.mu.Unlock()

	id := b.newID("location")
	arn := fmt.Sprintf(arnFmtAccessGrantsLocation, b.region, accountID, id)

	loc := &AccessGrantsLocation{
		AccountID:               accountID,
		AccessGrantsLocationID:  id,
		AccessGrantsLocationArn: arn,
		LocationScope:           locationScope,
		IAMRoleArn:              iamRoleArn,
		CreatedAt:               nowRFC3339(),
	}
	b.accessGrantsLocations.Put(loc)

	cp := *loc

	return &cp
}

// ---- Access Grants Instance ----

// ListAccessGrantsInstances returns all Access Grants instances for the account.
func (b *InMemoryBackend) ListAccessGrantsInstances(accountID string) []*AccessGrantsInstance {
	b.mu.RLock("ListAccessGrantsInstances")
	defer b.mu.RUnlock()

	inst, ok := b.accessGrantsInstances.Get(accountID)
	if !ok {
		return nil
	}

	return []*AccessGrantsInstance{inst}
}

func (b *InMemoryBackend) GetAccessGrantsInstance(accountID string) (*AccessGrantsInstance, error) {
	b.mu.RLock("GetAccessGrantsInstance")
	defer b.mu.RUnlock()

	inst, ok := b.accessGrantsInstances.Get(accountID)
	if !ok {
		return nil, awserr.New("AccessGrantsInstanceNotExistsError", awserr.ErrNotFound)
	}

	return inst, nil
}

// DeleteAccessGrantsInstance removes the Access Grants instance and
// cascade-cleans its resource policy and generic resource tags. Per the real
// API's documented behavior, this does NOT cascade-delete AccessGrants or
// AccessGrantsLocations -- AWS requires those to be deleted individually
// first (DeleteAccessGrantsInstance's doc comment: "You must first delete
// the access grants and locations before S3 Access Grants can delete the
// instance").
func (b *InMemoryBackend) DeleteAccessGrantsInstance(accountID string) error {
	b.mu.Lock("DeleteAccessGrantsInstance")
	defer b.mu.Unlock()

	// arn is "" if no instance exists; deleting resourceTags[""] is a
	// harmless no-op, so no separate not-found branch is needed here --
	// DeleteAccessGrantsInstance is idempotent in the real API too.
	var arn string
	if inst, ok := b.accessGrantsInstances.Get(accountID); ok {
		arn = inst.AccessGrantsInstanceArn
	}

	b.accessGrantsInstances.Delete(accountID)
	delete(b.accessGrantsInstancePolicies, accountID)
	delete(b.resourceTags, arn)

	return nil
}

// GetAccessGrantsInstanceResourcePolicy returns the resource policy for an AGI.
func (b *InMemoryBackend) GetAccessGrantsInstanceResourcePolicy(accountID string) (string, error) {
	b.mu.RLock("GetAccessGrantsInstanceResourcePolicy")
	defer b.mu.RUnlock()

	policy := b.accessGrantsInstancePolicies[accountID]

	return policy, nil
}

// PutAccessGrantsInstanceResourcePolicy sets the resource policy for an AGI.
func (b *InMemoryBackend) PutAccessGrantsInstanceResourcePolicy(accountID, policy string) {
	b.mu.Lock("PutAccessGrantsInstanceResourcePolicy")
	defer b.mu.Unlock()

	b.accessGrantsInstancePolicies[accountID] = policy
}

// DeleteAccessGrantsInstanceResourcePolicy removes the resource policy.
func (b *InMemoryBackend) DeleteAccessGrantsInstanceResourcePolicy(accountID string) {
	b.mu.Lock("DeleteAccessGrantsInstanceResourcePolicy")
	defer b.mu.Unlock()

	delete(b.accessGrantsInstancePolicies, accountID)
}

// DissociateAccessGrantsIdentityCenter removes the identity center association.
func (b *InMemoryBackend) DissociateAccessGrantsIdentityCenter(accountID string) {
	b.mu.Lock("DissociateAccessGrantsIdentityCenter")
	defer b.mu.Unlock()

	if inst, ok := b.accessGrantsInstances.Get(accountID); ok {
		inst.IdentityCenterArn = ""
	}
}

// GetAccessGrantsInstanceForPrefix returns the AGI for a given S3 prefix.
func (b *InMemoryBackend) GetAccessGrantsInstanceForPrefix(
	accountID, prefix string,
) (*AccessGrantsInstance, error) {
	b.mu.RLock("GetAccessGrantsInstanceForPrefix")
	defer b.mu.RUnlock()

	inst, ok := b.accessGrantsInstances.Get(accountID)
	if !ok {
		return nil, awserr.New("AccessGrantsInstanceNotExistsError", awserr.ErrNotFound)
	}
	_ = prefix

	return inst, nil
}

// ---- Access Grants CRUD ----

// GetAccessGrant returns an access grant by ID.
func (b *InMemoryBackend) GetAccessGrant(accountID, grantID string) (*AccessGrant, error) {
	b.mu.RLock("GetAccessGrant")
	defer b.mu.RUnlock()

	key := accountID + ":" + grantID
	grant, ok := b.accessGrants.Get(key)
	if !ok {
		return nil, awserr.New("NoSuchAccessGrant", awserr.ErrNotFound)
	}

	return grant, nil
}

// DeleteAccessGrant removes an access grant and cascade-cleans its generic
// resource tags.
func (b *InMemoryBackend) DeleteAccessGrant(accountID, grantID string) error {
	b.mu.Lock("DeleteAccessGrant")
	defer b.mu.Unlock()

	key := accountID + ":" + grantID

	grant, ok := b.accessGrants.Get(key)
	if !ok {
		return awserr.New("NoSuchAccessGrant", awserr.ErrNotFound)
	}

	arn := grant.AccessGrantArn

	b.accessGrants.Delete(key)
	delete(b.resourceTags, arn)

	return nil
}

// ListAccessGrants returns all access grants for an account, optionally filtered by locationScope.
func (b *InMemoryBackend) ListAccessGrants(accountID, locationScope string) []*AccessGrant {
	b.mu.RLock("ListAccessGrants")
	defer b.mu.RUnlock()

	var out []*AccessGrant
	for _, g := range b.accessGrants.All() {
		if g.AccountID != accountID {
			continue
		}
		if locationScope != "" && g.GrantScope != locationScope {
			continue
		}
		cp := *g
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccessGrantID < out[j].AccessGrantID })

	return out
}

// ListCallerAccessGrants returns access grants visible to the caller.
func (b *InMemoryBackend) ListCallerAccessGrants(accountID string) []*AccessGrant {
	return b.ListAccessGrants(accountID, "")
}

// GetAccessGrantsLocation returns an access grants location by ID.
func (b *InMemoryBackend) GetAccessGrantsLocation(
	accountID, locationID string,
) (*AccessGrantsLocation, error) {
	b.mu.RLock("GetAccessGrantsLocation")
	defer b.mu.RUnlock()

	key := accountID + ":" + locationID
	loc, ok := b.accessGrantsLocations.Get(key)
	if !ok {
		return nil, awserr.New("NoSuchAccessGrantsLocation", awserr.ErrNotFound)
	}

	return loc, nil
}

// DeleteAccessGrantsLocation removes an access grants location and
// cascade-cleans its generic resource tags.
func (b *InMemoryBackend) DeleteAccessGrantsLocation(accountID, locationID string) error {
	b.mu.Lock("DeleteAccessGrantsLocation")
	defer b.mu.Unlock()

	key := accountID + ":" + locationID

	loc, ok := b.accessGrantsLocations.Get(key)
	if !ok {
		return awserr.New("NoSuchAccessGrantsLocation", awserr.ErrNotFound)
	}

	arn := loc.AccessGrantsLocationArn

	b.accessGrantsLocations.Delete(key)
	delete(b.resourceTags, arn)

	return nil
}

// UpdateAccessGrantsLocation updates the IAM role ARN for a location.
func (b *InMemoryBackend) UpdateAccessGrantsLocation(
	accountID, locationID, iamRoleArn string,
) (*AccessGrantsLocation, error) {
	b.mu.Lock("UpdateAccessGrantsLocation")
	defer b.mu.Unlock()

	key := accountID + ":" + locationID
	loc, ok := b.accessGrantsLocations.Get(key)
	if !ok {
		return nil, awserr.New("NoSuchAccessGrantsLocation", awserr.ErrNotFound)
	}
	loc.IAMRoleArn = iamRoleArn

	return loc, nil
}

// ListAccessGrantsLocations returns all locations for an account.
func (b *InMemoryBackend) ListAccessGrantsLocations(accountID string) []*AccessGrantsLocation {
	b.mu.RLock("ListAccessGrantsLocations")
	defer b.mu.RUnlock()

	var out []*AccessGrantsLocation
	for _, loc := range b.accessGrantsLocations.All() {
		if loc.AccountID == accountID {
			cp := *loc
			out = append(out, &cp)
		}
	}
	sort.Slice(
		out,
		func(i, j int) bool { return out[i].AccessGrantsLocationID < out[j].AccessGrantsLocationID },
	)

	return out
}

// GetDataAccess returns a presigned URL for accessing data via access grants.
func (b *InMemoryBackend) GetDataAccess(accountID, target, permission string) (string, error) {
	b.mu.RLock("GetDataAccess")
	defer b.mu.RUnlock()

	// Verify the instance exists
	if !b.accessGrantsInstances.Has(accountID) {
		return "", awserr.New("AccessGrantsInstanceNotExistsError", awserr.ErrNotFound)
	}
	_ = target
	_ = permission

	return "https://s3.amazonaws.com/presigned-mock-url", nil
}

// ---- Seed helpers for testing ----

// AddAccessGrantsInstanceInternal creates an access grants instance directly, for seeding test data.
func (b *InMemoryBackend) AddAccessGrantsInstanceInternal(accountID, identityCenterArn string) *AccessGrantsInstance {
	return b.CreateAccessGrantsInstance(accountID, identityCenterArn)
}

// AddAccessGrantInternal creates an access grant directly, for seeding test data.
func (b *InMemoryBackend) AddAccessGrantInternal(
	accountID, locationID, granteeType, granteeIdentifier, permission string,
) *AccessGrant {
	grant, _ := b.CreateAccessGrant(accountID, locationID, granteeType, granteeIdentifier, permission, "")

	return grant
}
