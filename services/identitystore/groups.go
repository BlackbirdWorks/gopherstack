package identitystore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// maxDisplayNameLength is the maximum length allowed for a group's DisplayName.
const maxDisplayNameLength = 1024

// ----------------------------------------
// Group operations
// ----------------------------------------

// CreateGroupRequest holds the parameters for creating a group.
//
// The real CreateGroupRequest smithy shape has no ExternalIds member --
// ExternalIds is populated only by external-identity-provider provisioning
// and is exposed as an attribute Group carries, but it is not settable at
// creation time by any real API caller. A previous revision of this struct
// (and the wire-facing createGroupRequest in handler_groups.go) accepted and
// applied an ExternalIds field at CreateGroup time; that was a
// gopherstack-invented capability with no real-AWS counterpart and has been
// removed. ExternalIds is instead settable the same way DisplayName and
// Description are: via UpdateGroup's AttributeOperations (see
// applyGroupAttributes below), mirroring how User.ExternalIDs is only
// reachable through UpdateUser, never CreateUser.
type CreateGroupRequest struct {
	DisplayName string `json:"DisplayName"`
	Description string `json:"Description"`
}

// CreateGroup creates a new group in the identity store.
func (b *InMemoryBackend) CreateGroup(ctx context.Context, storeID string, req *CreateGroupRequest) (*Group, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	// Check uniqueness by DisplayName using index.
	if req.DisplayName != "" {
		if existing := b.groupsByDisplayName.Get(storeKey(region, storeID) + "#" + req.DisplayName); len(existing) > 0 {
			return nil, fmt.Errorf("%w: group with DisplayName %q already exists", ErrConflict, req.DisplayName)
		}
	}

	if len(req.DisplayName) > maxDisplayNameLength {
		return nil, fmt.Errorf("%w: DisplayName must not exceed 1024 characters", ErrValidation)
	}

	groupID := b.generateID()
	now := epochTime(time.Now().UTC())
	callerARN := b.simulatedCallerARN()
	group := &Group{
		GroupID:         groupID,
		IdentityStoreID: storeID,
		DisplayName:     req.DisplayName,
		Description:     req.Description,
		region:          region,
		CreatedAt:       now,
		CreatedBy:       callerARN,
		UpdatedAt:       now,
		UpdatedBy:       callerARN,
	}

	b.groups.Put(group)

	return copyGroup(group), nil
}

// DescribeGroup returns a group by ID.
func (b *InMemoryBackend) DescribeGroup(ctx context.Context, storeID, groupID string) (*Group, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	group, ok := b.groups.Get(regionKey(region, groupID))
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	return copyGroup(group), nil
}

// ListGroups lists all groups for the given identity store, sorted by GroupID.
func (b *InMemoryBackend) ListGroups(ctx context.Context, storeID string) []*Group {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	matches := b.groupsByStore.Get(storeKey(region, storeID))
	result := make([]*Group, 0, len(matches))

	for _, g := range matches {
		result = append(result, copyGroup(g))
	}

	slices.SortFunc(result, func(a, b *Group) int { return strings.Compare(a.GroupID, b.GroupID) })

	return result
}

// UpdateGroup applies attribute operations to a group.
func (b *InMemoryBackend) UpdateGroup(ctx context.Context, storeID, groupID string, ops []attributeOperation) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	id := regionKey(region, groupID)

	group, ok := b.groups.Get(id)
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	if err := b.validateGroupOps(region, storeID, group.DisplayName, ops); err != nil {
		return err
	}

	// Delete before mutating the indexed DisplayName field -- see the same
	// hazard note in UpdateUser above.
	b.groups.Delete(id)

	applyGroupAttributes(group, ops)

	group.UpdatedAt = epochTime(time.Now().UTC())
	group.UpdatedBy = b.simulatedCallerARN()

	b.groups.Put(group)

	return nil
}

// validateGroupOps checks for display-name conflicts before applying group updates.
func (b *InMemoryBackend) validateGroupOps(region, storeID, currentDisplayName string, ops []attributeOperation) error {
	for _, op := range ops {
		if strings.ToLower(op.AttributePath) != attrDisplayName {
			continue
		}

		newName, _ := op.AttributeValue.(string)
		if newName == "" || newName == currentDisplayName {
			continue
		}

		if existing := b.groupsByDisplayName.Get(storeKey(region, storeID) + "#" + newName); len(existing) > 0 {
			return fmt.Errorf("%w: group with DisplayName %q already exists", ErrConflict, newName)
		}
	}

	return nil
}

// applyGroupAttributes applies each attribute operation to the group in place.
func applyGroupAttributes(group *Group, ops []attributeOperation) {
	for _, op := range ops {
		switch strings.ToLower(op.AttributePath) {
		case attrDisplayName:
			if s, isStr := op.AttributeValue.(string); isStr {
				group.DisplayName = s
			}
		case "description":
			if s, isStr := op.AttributeValue.(string); isStr {
				group.Description = s
			}
		case "externalids":
			group.ExternalIDs = parseExternalIDs(op.AttributeValue)
		}
	}
}

// applyGroupFilters returns only the groups matching all provided filters.
func applyGroupFilters(groups []*Group, filters []ListFilter) []*Group {
	if len(filters) == 0 {
		return groups
	}

	result := make([]*Group, 0, len(groups))

	for _, g := range groups {
		if groupMatchesFilters(g, filters) {
			result = append(result, g)
		}
	}

	return result
}

// groupMatchesFilters reports whether g satisfies every filter in the slice.
func groupMatchesFilters(g *Group, filters []ListFilter) bool {
	for _, f := range filters {
		switch strings.ToLower(f.AttributePath) {
		case attrDisplayName:
			if g.DisplayName != f.AttributeValue {
				return false
			}
		case "description":
			if g.Description != f.AttributeValue {
				return false
			}
		}
	}

	return true
}

// DeleteGroup removes a group from the identity store.
func (b *InMemoryBackend) DeleteGroup(ctx context.Context, storeID, groupID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	id := regionKey(region, groupID)

	group, ok := b.groups.Get(id)
	if !ok || group.IdentityStoreID != storeID {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	b.groups.Delete(id)

	// Remove associated memberships via the byGroup index. Clone the
	// index-owned slice before the delete loop, since Table.Delete mutates
	// the same index's backing storage as it removes each entry.
	groupKey := storeKey(region, storeID) + "#" + groupID

	for _, m := range slices.Clone(b.membershipsByGroup.Get(groupKey)) {
		b.memberships.Delete(regionKey(region, m.MembershipID))
	}

	return nil
}

// GetGroupID looks up a group ID by alternate identifier (DisplayName or ExternalId).
func (b *InMemoryBackend) GetGroupID(ctx context.Context, storeID, attrPath, attrValue string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetGroupID")
	defer b.mu.RUnlock()

	switch {
	case strings.EqualFold(attrPath, "displayName"):
		if matches := b.groupsByDisplayName.Get(storeKey(region, storeID) + "#" + attrValue); len(matches) > 0 {
			return matches[0].GroupID, nil
		}
	case strings.EqualFold(attrPath, "ExternalId"):
		if gid, ok := b.resolveGroupByExternalID(region, storeID, attrValue); ok {
			return gid, nil
		}
	}

	return "", fmt.Errorf("%w: no group found with %s=%q", ErrGroupNotFound, attrPath, attrValue)
}

// resolveGroupByExternalID returns the group ID whose ExternalIDs contain both the given Issuer and Id.
// The compound argument is Issuer+externalIDSep+Id as encoded by extractAlternateIdentifier.
func (b *InMemoryBackend) resolveGroupByExternalID(region, storeID, compound string) (string, bool) {
	issuer, extID := splitExternalIDCompound(compound)

	for _, g := range b.groupsByStore.Get(storeKey(region, storeID)) {
		for _, ext := range g.ExternalIDs {
			if ext.Issuer == issuer && ext.ID == extID {
				return g.GroupID, true
			}
		}
	}

	return "", false
}

func copyGroup(g *Group) *Group {
	if g == nil {
		return nil
	}
	cp := *g
	cp.ExternalIDs = append([]ExternalID(nil), g.ExternalIDs...)

	return &cp
}
