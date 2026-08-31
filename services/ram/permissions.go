package ram

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// AddPermissionInternal inserts a permission directly, bypassing validation.
// Useful for seeding test state.
func (b *InMemoryBackend) AddPermissionInternal(p *Permission) {
	b.mu.Lock("AddPermissionInternal")
	defer b.mu.Unlock()

	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	if p.Versions == nil {
		p.Versions = make(map[int32]*PermissionVersion)
	}

	b.permissions.Put(p)
}

// permissionARN builds an ARN for a customer-managed RAM permission.
func (b *InMemoryBackend) permissionARN(name string) string {
	return arn.Build("ram", b.region, b.accountID, "permission/"+name)
}

// CreatePermission creates a new customer-managed RAM permission.
func (b *InMemoryBackend) CreatePermission(
	name, resourceType, policyTemplate string,
	tags map[string]string,
) (*Permission, error) {
	b.mu.Lock("CreatePermission")
	defer b.mu.Unlock()

	permARN := b.permissionARN(name)

	if p, ok := b.permissions.Get(permARN); ok && !p.Deleted {
		return nil, fmt.Errorf("%w: permission %s already exists", ErrPermissionAlreadyExists, name)
	}

	now := time.Now()
	version := int32(1)
	pv := &PermissionVersion{
		Version:         version,
		PolicyTemplate:  policyTemplate,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	p := &Permission{
		ARN:                 permARN,
		Name:                name,
		ResourceType:        resourceType,
		PermissionType:      permissionTypeCustomer,
		ResourceRegionScope: resourceRegionScopeRegional,
		Tags:                mergeTags(nil, tags),
		CreationTime:        now,
		LastUpdatedTime:     now,
		DefaultVersion:      version,
		LatestVersion:       version,
		Versions:            map[int32]*PermissionVersion{version: pv},
	}
	b.permissions.Put(p)

	return clonePermission(p), nil
}

// DeletePermission soft-deletes a customer-managed RAM permission and removes it from all shares.
// AWS-managed permissions cannot be deleted.
func (b *InMemoryBackend) DeletePermission(permissionARN string) error {
	b.mu.Lock("DeletePermission")
	defer b.mu.Unlock()

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	if p.PermissionType == permissionTypeAWSManaged {
		return fmt.Errorf(
			"%w: cannot delete AWS-managed permission %s",
			ErrOperationNotPermitted,
			permissionARN,
		)
	}

	// Refuse deletion if permission is associated with any active resource share.
	for _, perms := range b.sharePermissions {
		if _, inUse := perms[permissionARN]; inUse {
			return fmt.Errorf(
				"%w: permission %s is associated with one or more resource shares",
				ErrPermissionInUse,
				permissionARN,
			)
		}
	}

	p.Deleted = true

	return nil
}

// GetPermission returns the details of a RAM permission, optionally at a specific version.
func (b *InMemoryBackend) GetPermission(
	permissionARN string,
	permissionVersion *int32,
) (*Permission, *PermissionVersion, error) {
	b.mu.RLock("GetPermission")
	defer b.mu.RUnlock()

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return nil, nil, fmt.Errorf(
			"%w: permission %s not found",
			ErrPermissionNotFound,
			permissionARN,
		)
	}

	version := p.DefaultVersion
	if permissionVersion != nil {
		version = *permissionVersion
	}

	pv, exists := p.Versions[version]
	if !exists {
		return nil, nil, fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			version,
			permissionARN,
		)
	}

	pvCopy := *pv

	return clonePermission(p), &pvCopy, nil
}

// ListPermissions returns all non-deleted customer-managed permissions,
// optionally filtered by resource type, sorted by ARN.
func (b *InMemoryBackend) ListPermissions(resourceType string) []*Permission {
	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	result := make([]*Permission, 0, b.permissions.Len())

	for _, p := range b.permissions.All() {
		if p.Deleted {
			continue
		}

		if resourceType != "" && !strs.Equal(p.ResourceType, resourceType) {
			continue
		}

		result = append(result, clonePermission(p))
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ARN < result[j].ARN })

	return result
}

// PromotePermissionCreatedFromPolicy promotes a CREATED_FROM_POLICY permission
// to a CUSTOMER_MANAGED permission with the given name.
func (b *InMemoryBackend) PromotePermissionCreatedFromPolicy(
	permissionARN string, name string,
) (*Permission, error) {
	b.mu.Lock("PromotePermissionCreatedFromPolicy")
	defer b.mu.Unlock()

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	if p.PermissionType != permissionTypeCreatedFromPolicy {
		return nil, fmt.Errorf(
			"%w: permission %s is not of type CREATED_FROM_POLICY",
			ErrInvalidParameter, permissionARN,
		)
	}

	p.PermissionType = permissionTypeCustomer
	if name != "" {
		p.Name = name
	}

	return clonePermission(p), nil
}
