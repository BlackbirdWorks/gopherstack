package ram

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
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

// AutoAssociateDefaultPermissions attaches the AWS-managed default permission for every
// resource type present in the share's active resource associations that does not
// already have an associated permission. Mirrors real AWS: CreateResourceShare and
// AssociateResourceShare automatically associate the default managed permission for
// each resource type included in the share when no explicit permission covers that
// type yet (CreateResourceShare's caller-supplied permissionArns skip this; the handler
// only calls this when none were supplied. AssociateResourceShare never accepts
// permissionArns at all, so real AWS always runs this step for it). Idempotent: calling
// it again after nothing changed is a no-op.
func (b *InMemoryBackend) AutoAssociateDefaultPermissions(shareARN string) error {
	b.mu.Lock("AutoAssociateDefaultPermissions")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	covered := make(map[string]struct{})

	for permARN := range b.sharePermissions[shareARN] {
		if p, pok := b.permissions.Get(permARN); pok && !p.Deleted {
			covered[p.ResourceType] = struct{}{}
		}
	}

	for _, a := range b.associations {
		if a.ResourceShareARN != shareARN || a.AssociationType != associationTypeResource ||
			a.Status != associationStatusAssociated {
			continue
		}

		resType := resourceTypeFromARN(a.AssociatedEntity)
		if _, done := covered[resType]; done {
			continue
		}

		permARN, version, found := b.defaultPermissionForTypeLocked(resType)
		if !found {
			// No built-in default permission is known for this resource type;
			// real AWS only auto-attaches for types it has a default for.
			continue
		}

		if b.sharePermissions[shareARN] == nil {
			b.sharePermissions[shareARN] = make(map[string]int32)
		}

		b.sharePermissions[shareARN][permARN] = version
		covered[resType] = struct{}{}
	}

	return nil
}

// defaultPermissionForTypeLocked returns the ARN and default version of the AWS-managed
// default permission for resourceType, if one is seeded. Caller must hold at least a
// read lock.
func (b *InMemoryBackend) defaultPermissionForTypeLocked(resourceType string) (string, int32, bool) {
	for _, p := range b.permissions.All() {
		if !p.Deleted && p.PermissionType == permissionTypeAWSManaged &&
			p.IsResourceTypeDefault && p.ResourceType == resourceType {
			return p.ARN, p.DefaultVersion, true
		}
	}

	return "", 0, false
}

// DisassociateResourceSharePermission removes a managed permission from a resource share.
// Real AWS refuses the request while any resource of the permission's resource type is
// still actively attached to the share, per the documented rule: you can remove a
// managed permission from a resource share only if there are currently no resources of
// the relevant resource type currently attached to the resource share.
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

	if _, associated := perms[permissionARN]; !associated {
		return nil
	}

	if p, pok := b.permissions.Get(permissionARN); pok {
		if b.shareHasActiveResourceOfTypeLocked(shareARN, p.ResourceType) {
			return fmt.Errorf(
				"%w: cannot disassociate permission %s: resource share %s still has "+
					"resources of type %s attached",
				ErrOperationNotPermitted, permissionARN, shareARN, p.ResourceType,
			)
		}
	}

	delete(perms, permissionARN)

	if len(perms) == 0 {
		delete(b.sharePermissions, shareARN)
	}

	return nil
}

// shareHasActiveResourceOfTypeLocked reports whether shareARN has any actively
// (ASSOCIATED) attached resource whose derived resource type matches resourceType.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) shareHasActiveResourceOfTypeLocked(shareARN, resourceType string) bool {
	for _, a := range b.associations {
		if a.ResourceShareARN != shareARN || a.AssociationType != associationTypeResource ||
			a.Status != associationStatusAssociated {
			continue
		}

		if resourceTypeFromARN(a.AssociatedEntity) == resourceType {
			return true
		}
	}

	return false
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
// with toPermissionARN across all resource shares. If fromPermissionVersion is
// non-nil, only shares currently pinned to that specific version are replaced
// (matching AWS's documented per-version filtering); otherwise every share using
// fromPermissionARN at any version is replaced. The replacement always associates
// toPermissionARN's current default version, matching real AWS.
//
// This mock performs the swap synchronously, so the returned work item's Status
// is always the terminal COMPLETED state -- there is no separate async
// completion step to model. Returns the work item for
// ListReplacePermissionAssociationsWork lookups.
func (b *InMemoryBackend) ReplacePermissionAssociations(
	fromPermissionARN, toPermissionARN string,
	fromPermissionVersion *int32,
) (*ReplacePermissionAssociationsWork, error) {
	b.mu.Lock("ReplacePermissionAssociations")
	defer b.mu.Unlock()

	fromP, fromOK := b.permissions.Get(fromPermissionARN)
	if !fromOK {
		return nil, fmt.Errorf(
			"%w: permission %s not found",
			ErrPermissionNotFound,
			fromPermissionARN,
		)
	}

	toP, toOK := b.permissions.Get(toPermissionARN)
	if !toOK || toP.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, toPermissionARN)
	}

	reportedFromVersion := fromP.DefaultVersion
	if fromPermissionVersion != nil {
		reportedFromVersion = *fromPermissionVersion
	}

	for shareARN, perms := range b.sharePermissions {
		ver, has := perms[fromPermissionARN]
		if !has {
			continue
		}

		if fromPermissionVersion != nil && ver != *fromPermissionVersion {
			continue
		}

		delete(perms, fromPermissionARN)
		perms[toPermissionARN] = toP.DefaultVersion
		b.sharePermissions[shareARN] = perms
	}

	now := time.Now()
	work := &ReplacePermissionAssociationsWork{
		ID:                    uuid.NewString(),
		FromPermissionARN:     fromPermissionARN,
		FromPermissionVersion: reportedFromVersion,
		ToPermissionARN:       toPermissionARN,
		ToPermissionVersion:   toP.DefaultVersion,
		Status:                replaceWorkStatusCompleted,
		CreationTime:          now,
		LastUpdatedTime:       now,
	}
	b.replaceWorks.Put(work)

	return cloneReplaceWork(work), nil
}

// ListReplacePermissionAssociationsWork returns recorded ReplacePermissionAssociations
// background work items, optionally filtered by work ID and/or status, sorted
// newest-first.
func (b *InMemoryBackend) ListReplacePermissionAssociationsWork(
	workIDs []string, status string,
) []*ReplacePermissionAssociationsWork {
	b.mu.RLock("ListReplacePermissionAssociationsWork")
	defer b.mu.RUnlock()

	idSet := make(map[string]struct{}, len(workIDs))
	for _, id := range workIDs {
		idSet[id] = struct{}{}
	}

	result := make([]*ReplacePermissionAssociationsWork, 0, b.replaceWorks.Len())

	for _, w := range b.replaceWorks.All() {
		if len(idSet) > 0 {
			if _, ok := idSet[w.ID]; !ok {
				continue
			}
		}

		if status != "" && w.Status != status {
			continue
		}

		result = append(result, cloneReplaceWork(w))
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].CreationTime.After(result[j].CreationTime) },
	)

	return result
}
