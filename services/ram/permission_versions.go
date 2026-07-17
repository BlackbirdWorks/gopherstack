package ram

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// CreatePermissionVersion creates a new version of an existing customer-managed RAM permission.
func (b *InMemoryBackend) CreatePermissionVersion(
	permissionARN, policyTemplate string,
) (*Permission, error) {
	b.mu.Lock("CreatePermissionVersion")
	defer b.mu.Unlock()

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	now := time.Now()
	newVersion := p.LatestVersion + 1
	p.Versions[newVersion] = &PermissionVersion{
		Version:         newVersion,
		PolicyTemplate:  policyTemplate,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	p.LatestVersion = newVersion
	p.LastUpdatedTime = now

	return clonePermission(p), nil
}

// DeletePermissionVersion deletes a specific version of a customer-managed RAM permission.
// The default version cannot be deleted. If the latest version is deleted, LatestVersion
// is updated to the next-highest remaining version.
func (b *InMemoryBackend) DeletePermissionVersion(
	permissionARN string,
	permissionVersion int32,
) error {
	b.mu.Lock("DeletePermissionVersion")
	defer b.mu.Unlock()

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	if _, exists := p.Versions[permissionVersion]; !exists {
		return fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			permissionVersion,
			permissionARN,
		)
	}

	if p.DefaultVersion == permissionVersion {
		return fmt.Errorf("%w: cannot delete the default version of a permission", ErrValidation)
	}

	delete(p.Versions, permissionVersion)
	p.LastUpdatedTime = time.Now()

	// If we deleted the latest version, recalculate it.
	if p.LatestVersion == permissionVersion && len(p.Versions) > 0 {
		versions := make([]int32, 0, len(p.Versions))

		for v := range p.Versions {
			versions = append(versions, v)
		}

		slices.Sort(versions)
		p.LatestVersion = versions[len(versions)-1]
	}

	// Cascade: remove share-permission associations pointing at the deleted version.
	for shareARN, perms := range b.sharePermissions {
		if ver, exists := perms[permissionARN]; exists && ver == permissionVersion {
			delete(perms, permissionARN)

			if len(perms) == 0 {
				delete(b.sharePermissions, shareARN)
			}
		}
	}

	return nil
}

// ListPermissionVersions returns all versions of a permission, sorted ascending by version number.
func (b *InMemoryBackend) ListPermissionVersions(
	permissionARN string,
) ([]*PermissionVersion, error) {
	b.mu.RLock("ListPermissionVersions")
	defer b.mu.RUnlock()

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	result := make([]*PermissionVersion, 0, len(p.Versions))

	for _, pv := range p.Versions {
		pvCopy := *pv
		result = append(result, &pvCopy)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Version < result[j].Version })

	return result, nil
}

// SetDefaultPermissionVersion updates the default version of a customer-managed permission.
func (b *InMemoryBackend) SetDefaultPermissionVersion(
	permissionARN string,
	version int32,
) (*Permission, error) {
	b.mu.Lock("SetDefaultPermissionVersion")
	defer b.mu.Unlock()

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	if _, exists := p.Versions[version]; !exists {
		return nil, fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			version,
			permissionARN,
		)
	}

	p.DefaultVersion = version
	p.LastUpdatedTime = time.Now()

	return clonePermission(p), nil
}
