package ram

import (
	"fmt"
	"sort"
)

// AssociateResourceSharePermission associates a managed permission with a resource share.
func (b *InMemoryBackend) AssociateResourceSharePermission(
	shareARN, permissionARN string,
	replace bool,
	permissionVersion *int32,
) error {
	b.mu.Lock("AssociateResourceSharePermission")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	p, ok := b.permissions.Get(permissionARN)
	if !ok || p.Deleted {
		return fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	version := p.DefaultVersion
	if permissionVersion != nil {
		version = *permissionVersion
	}

	if _, exists := p.Versions[version]; !exists {
		return fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			version,
			permissionARN,
		)
	}

	if b.sharePermissions[shareARN] == nil {
		b.sharePermissions[shareARN] = make(map[string]int32)
	}

	if replace {
		b.sharePermissions[shareARN] = map[string]int32{permissionARN: version}
	} else {
		b.sharePermissions[shareARN][permissionARN] = version
	}

	return nil
}

// DisassociateResourceSharePermission removes a managed permission from a resource share.
func (b *InMemoryBackend) DisassociateResourceSharePermission(
	shareARN, permissionARN string,
) error {
	b.mu.Lock("DisassociateResourceSharePermission")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	perms := b.sharePermissions[shareARN]
	if perms == nil {
		return nil
	}

	delete(perms, permissionARN)

	return nil
}

// ListResourceSharePermissions returns the permissions associated with a resource share,
// each paired with the version actually associated with that share (which may differ
// from the permission's current default version), sorted by ARN for deterministic output.
func (b *InMemoryBackend) ListResourceSharePermissions(shareARN string) []*ResourceSharePermissionDetail {
	b.mu.RLock("ListResourceSharePermissions")
	defer b.mu.RUnlock()

	permARNs := b.sharePermissions[shareARN]
	result := make([]*ResourceSharePermissionDetail, 0, len(permARNs))

	for pARN, version := range permARNs {
		p, ok := b.permissions.Get(pARN)
		if !ok || p.Deleted {
			continue
		}

		result = append(result, &ResourceSharePermissionDetail{
			Permission: clonePermission(p),
			Version:    version,
		})
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].Permission.ARN < result[j].Permission.ARN },
	)

	return result
}

// ListPermissionAssociations returns all share-permission associations filtered optionally
// by permissionARN, sorted by share ARN + permission ARN.
func (b *InMemoryBackend) ListPermissionAssociations(
	permissionARN string,
) []SharePermissionAssociation {
	b.mu.RLock("ListPermissionAssociations")
	defer b.mu.RUnlock()

	result := make([]SharePermissionAssociation, 0, len(b.sharePermissions))

	for shareARN, perms := range b.sharePermissions {
		for pARN, ver := range perms {
			if permissionARN != "" && pARN != permissionARN {
				continue
			}

			result = append(result, SharePermissionAssociation{
				ShareARN:      shareARN,
				PermissionARN: pARN,
				Version:       ver,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].ShareARN != result[j].ShareARN {
			return result[i].ShareARN < result[j].ShareARN
		}

		return result[i].PermissionARN < result[j].PermissionARN
	})

	return result
}

// ReplacePermissionAssociations replaces all associations using fromPermissionARN
// with toPermissionARN across all resource shares.
// Returns a work ID for ListReplacePermissionAssociationsWork.
func (b *InMemoryBackend) ReplacePermissionAssociations(
	fromPermissionARN, toPermissionARN string,
) (string, error) {
	b.mu.Lock("ReplacePermissionAssociations")
	defer b.mu.Unlock()

	_, fromOK := b.permissions.Get(fromPermissionARN)
	if !fromOK {
		return "", fmt.Errorf(
			"%w: permission %s not found",
			ErrPermissionNotFound,
			fromPermissionARN,
		)
	}

	toP, toOK := b.permissions.Get(toPermissionARN)
	if !toOK || toP.Deleted {
		return "", fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, toPermissionARN)
	}

	for shareARN, perms := range b.sharePermissions {
		if _, has := perms[fromPermissionARN]; has {
			ver := toP.DefaultVersion
			delete(perms, fromPermissionARN)
			perms[toPermissionARN] = ver
			b.sharePermissions[shareARN] = perms
		}
	}

	return "replace-work-" + fromPermissionARN, nil
}
