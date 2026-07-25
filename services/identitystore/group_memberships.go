package identitystore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ----------------------------------------
// Membership operations
// ----------------------------------------

// CreateGroupMembership creates a membership between a user and a group.
func (b *InMemoryBackend) CreateGroupMembership(
	ctx context.Context, storeID, groupID string, memberID MemberID,
) (*GroupMembership, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateGroupMembership")
	defer b.mu.Unlock()

	// Validate group exists.
	group, ok := b.groups.Get(regionKey(region, groupID))
	if !ok || group.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupID)
	}

	// Validate user exists.
	if memberID.UserID != "" {
		user, userOK := b.users.Get(regionKey(region, memberID.UserID))
		if !userOK || user.IdentityStoreID != storeID {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, memberID.UserID)
		}
	}

	// Check for duplicate membership using the byGroupMember index.
	groupMemberKey := storeKey(region, storeID) + "#" + groupID + "#" + memberID.UserID
	if existing := b.membershipsByGroupMember.Get(groupMemberKey); len(existing) > 0 {
		return nil, fmt.Errorf("%w: membership already exists", ErrConflict)
	}

	membershipID := b.generateID()
	now := epochTime(time.Now().UTC())
	callerARN := b.simulatedCallerARN()
	// There is no UpdateGroupMembership API in real AWS -- a membership's
	// CreatedAt/CreatedBy and UpdatedAt/UpdatedBy are therefore always equal
	// for the lifetime of the resource (it can only be created or deleted).
	membership := &GroupMembership{
		MembershipID:    membershipID,
		IdentityStoreID: storeID,
		GroupID:         groupID,
		MemberID:        memberID,
		region:          region,
		CreatedAt:       now,
		CreatedBy:       callerARN,
		UpdatedAt:       now,
		UpdatedBy:       callerARN,
	}

	b.memberships.Put(membership)

	return copyMembership(membership), nil
}

// DescribeGroupMembership returns a membership by ID.
func (b *InMemoryBackend) DescribeGroupMembership(
	ctx context.Context, storeID, membershipID string,
) (*GroupMembership, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeGroupMembership")
	defer b.mu.RUnlock()

	m, ok := b.memberships.Get(regionKey(region, membershipID))
	if !ok || m.IdentityStoreID != storeID {
		return nil, fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	return copyMembership(m), nil
}

// ListGroupMemberships lists all memberships for a group, sorted by MembershipID.
func (b *InMemoryBackend) ListGroupMemberships(ctx context.Context, storeID, groupID string) []*GroupMembership {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListGroupMemberships")
	defer b.mu.RUnlock()

	matches := b.membershipsByGroup.Get(storeKey(region, storeID) + "#" + groupID)
	result := make([]*GroupMembership, 0, len(matches))

	for _, m := range matches {
		result = append(result, copyMembership(m))
	}

	slices.SortFunc(result, func(a, b *GroupMembership) int {
		return strings.Compare(a.MembershipID, b.MembershipID)
	})

	return result
}

// DeleteGroupMembership removes a membership.
func (b *InMemoryBackend) DeleteGroupMembership(ctx context.Context, storeID, membershipID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteGroupMembership")
	defer b.mu.Unlock()

	id := regionKey(region, membershipID)

	m, ok := b.memberships.Get(id)
	if !ok || m.IdentityStoreID != storeID {
		return fmt.Errorf("%w: membership %q not found", ErrMembershipNotFound, membershipID)
	}

	b.memberships.Delete(id)

	return nil
}

// GetGroupMembershipID looks up a membership ID by group and member.
func (b *InMemoryBackend) GetGroupMembershipID(
	ctx context.Context, storeID, groupID string, memberID MemberID,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetGroupMembershipID")
	defer b.mu.RUnlock()

	key := storeKey(region, storeID) + "#" + groupID + "#" + memberID.UserID
	if matches := b.membershipsByGroupMember.Get(key); len(matches) > 0 {
		return matches[0].MembershipID, nil
	}

	return "", fmt.Errorf(
		"%w: membership not found for group=%q member=%q",
		ErrMembershipNotFound,
		groupID,
		memberID.UserID,
	)
}

// ListGroupMembershipsForMember lists all group memberships for a given member, sorted by MembershipID.
func (b *InMemoryBackend) ListGroupMembershipsForMember(
	ctx context.Context, storeID string, memberID MemberID,
) []*GroupMembership {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListGroupMembershipsForMember")
	defer b.mu.RUnlock()

	if memberID.UserID == "" {
		return nil
	}

	matches := b.membershipsByMember.Get(storeKey(region, storeID) + "#" + memberID.UserID)
	result := make([]*GroupMembership, 0, len(matches))

	for _, m := range matches {
		result = append(result, copyMembership(m))
	}

	slices.SortFunc(result, func(a, b *GroupMembership) int {
		return strings.Compare(a.MembershipID, b.MembershipID)
	})

	return result
}

// IsMemberInGroups checks which of the given groups contain the specified member.
// Uses the O(1) byGroupMember index instead of scanning all memberships.
func (b *InMemoryBackend) IsMemberInGroups(
	ctx context.Context,
	storeID string,
	memberID MemberID,
	groupIDs []string,
) []GroupMembershipExistence {
	region := getRegion(ctx, b.region)

	b.mu.RLock("IsMemberInGroups")
	defer b.mu.RUnlock()

	result := make([]GroupMembershipExistence, 0, len(groupIDs))

	for _, id := range groupIDs {
		key := storeKey(region, storeID) + "#" + id + "#" + memberID.UserID
		exists := len(b.membershipsByGroupMember.Get(key)) > 0
		result = append(result, GroupMembershipExistence{
			GroupID:          id,
			MemberID:         memberID,
			MembershipExists: exists,
		})
	}

	return result
}

func copyMembership(m *GroupMembership) *GroupMembership {
	if m == nil {
		return nil
	}
	cp := *m

	return &cp
}
