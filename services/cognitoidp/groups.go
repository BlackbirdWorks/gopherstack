package cognitoidp

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"
)

// CreateGroup creates a group in a user pool.
func (b *InMemoryBackend) CreateGroup(userPoolID, groupName, description string, precedence int32) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.groups.Get(groupKey(userPoolID, groupName)); exists {
		return nil, fmt.Errorf("%w: group %q already exists in pool %q", ErrAlreadyExists, groupName, userPoolID)
	}

	g := &Group{
		GroupName:   groupName,
		UserPoolID:  userPoolID,
		Description: description,
		Precedence:  precedence,
		CreatedAt:   time.Now().UTC(),
	}
	b.groups.Put(g)

	cp := *g

	return &cp, nil
}

// DeleteGroup removes a group from a user pool.
func (b *InMemoryBackend) DeleteGroup(userPoolID, groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return fmt.Errorf("%w: group %q not found in pool %q", ErrGroupNotFound, groupName, userPoolID)
	}

	b.groups.Delete(groupKey(userPoolID, groupName))

	if b.groupMembers[userPoolID] != nil {
		delete(b.groupMembers[userPoolID], groupName)
	}

	return nil
}

// ListGroups returns all groups in a user pool sorted by group name.
func (b *InMemoryBackend) ListGroups(userPoolID string) ([]*Group, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolGroups := b.groupsByPool.Get(userPoolID)
	out := make([]*Group, 0, len(poolGroups))

	for _, g := range poolGroups {
		cp := *g
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].GroupName < out[j].GroupName })

	return out, nil
}

// AdminAddUserToGroup adds a user to a group.
func (b *InMemoryBackend) AdminAddUserToGroup(userPoolID, username, groupName string) error {
	b.mu.Lock("AdminAddUserToGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if b.groupMembers[userPoolID] == nil {
		b.groupMembers[userPoolID] = make(map[string]map[string]struct{})
	}

	if b.groupMembers[userPoolID][groupName] == nil {
		b.groupMembers[userPoolID][groupName] = make(map[string]struct{})
	}

	b.groupMembers[userPoolID][groupName][username] = struct{}{}

	return nil
}

// AdminRemoveUserFromGroup removes a user from a group.
func (b *InMemoryBackend) AdminRemoveUserFromGroup(userPoolID, username, groupName string) error {
	b.mu.Lock("AdminRemoveUserFromGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if b.groupMembers[userPoolID] != nil && b.groupMembers[userPoolID][groupName] != nil {
		delete(b.groupMembers[userPoolID][groupName], username)
	}

	return nil
}

// AdminListGroupsForUser returns the groups a user belongs to, sorted by group name.
func (b *InMemoryBackend) AdminListGroupsForUser(userPoolID, username string) ([]*Group, error) {
	b.mu.RLock("AdminListGroupsForUser")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	out := make([]*Group, 0, len(b.groupMembers[userPoolID]))

	for groupName, members := range b.groupMembers[userPoolID] {
		if _, isMember := members[username]; !isMember {
			continue
		}

		if g, ok := b.groups.Get(groupKey(userPoolID, groupName)); ok {
			cp := *g
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].GroupName < out[j].GroupName })

	return out, nil
}

// ListUsersInGroup returns all users belonging to a group, sorted by username.
func (b *InMemoryBackend) ListUsersInGroup(userPoolID, groupName string) ([]*User, error) {
	b.mu.RLock("ListUsersInGroup")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	members := b.groupMembers[userPoolID][groupName]
	out := make([]*User, 0, len(members))

	for username := range members {
		u, ok := b.users.Get(userKey(userPoolID, username))
		if !ok {
			continue
		}
		cp := *u
		cp.Attributes = maps.Clone(u.Attributes)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })

	return out, nil
}

// UpdateGroup updates a group's description and precedence.
func (b *InMemoryBackend) UpdateGroup(userPoolID, groupName, description string, precedence int32) (*Group, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	g, ok := b.groups.Get(groupKey(userPoolID, groupName))
	if !ok {
		return nil, fmt.Errorf("%w: group %q not found in pool %q", ErrGroupNotFound, groupName, userPoolID)
	}

	g.Description = description
	g.Precedence = precedence

	cp := *g

	return &cp, nil
}

// GetGroup returns a single group from a user pool by name.
func (b *InMemoryBackend) GetGroup(userPoolID, groupName string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	g, ok := b.groups.Get(groupKey(userPoolID, groupName))
	if !ok {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	cp := *g

	return &cp, nil
}

// userGroupsLocked returns the group names for a user in a pool, sorted by group precedence ascending
// (lowest precedence value = highest priority), then alphabetically. Caller must hold at least a read lock.
func (b *InMemoryBackend) userGroupsLocked(poolID, username string) []string {
	type gp struct {
		name       string
		precedence int32
	}

	var matched []gp

	for groupName, members := range b.groupMembers[poolID] {
		if _, isMember := members[username]; !isMember {
			continue
		}
		if g, ok := b.groups.Get(groupKey(poolID, groupName)); ok {
			matched = append(matched, gp{name: groupName, precedence: g.Precedence})
		}
	}

	slices.SortFunc(matched, func(a, b gp) int {
		if a.precedence != b.precedence {
			if a.precedence < b.precedence {
				return -1
			}

			return 1
		}

		if a.name < b.name {
			return -1
		}

		if a.name > b.name {
			return 1
		}

		return 0
	})

	out := make([]string, len(matched))
	for i, g := range matched {
		out[i] = g.name
	}

	return out
}

// CreateGroupFull creates a group with a RoleArn.
func (b *InMemoryBackend) CreateGroupFull(
	userPoolID, groupName, description, roleArn string,
	precedence int32,
) (*Group, error) {
	b.mu.Lock("CreateGroupFull")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.groups.Get(groupKey(userPoolID, groupName)); exists {
		return nil, fmt.Errorf("%w: group %q already exists in pool %q", ErrAlreadyExists, groupName, userPoolID)
	}

	now := time.Now()
	g := &Group{
		GroupName:      groupName,
		UserPoolID:     userPoolID,
		Description:    description,
		RoleArn:        roleArn,
		Precedence:     precedence,
		CreatedAt:      now,
		LastModifiedAt: now,
	}

	b.groups.Put(g)

	if b.groupMembers[userPoolID] == nil {
		b.groupMembers[userPoolID] = make(map[string]map[string]struct{})
	}

	b.groupMembers[userPoolID][groupName] = make(map[string]struct{})

	cp := *g

	return &cp, nil
}

// UpdateGroupFull updates group with description, roleArn, and precedence.
func (b *InMemoryBackend) UpdateGroupFull(
	userPoolID, groupName, description, roleArn string,
	precedence int32,
) (*Group, error) {
	b.mu.Lock("UpdateGroupFull")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	g, ok := b.groups.Get(groupKey(userPoolID, groupName))
	if !ok {
		return nil, fmt.Errorf("%w: group %q not found in pool %q", ErrGroupNotFound, groupName, userPoolID)
	}

	if description != "" {
		g.Description = description
	}

	if roleArn != "" {
		g.RoleArn = roleArn
	}

	g.Precedence = precedence
	g.LastModifiedAt = time.Now()

	cp := *g

	return &cp, nil
}

// ListGroupsPage returns a page of groups with optional NextToken pagination.
func (b *InMemoryBackend) ListGroupsPage(userPoolID string, limit int, nextToken string) ([]*Group, string, error) {
	b.mu.RLock("ListGroupsPage")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolGroups := b.groupsByPool.Get(userPoolID)
	all := make([]*Group, 0, len(poolGroups))

	for _, g := range poolGroups {
		cp := *g
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].GroupName < all[j].GroupName })

	// Apply token-based pagination. A miss (the group the token named was
	// deleted) defaults startIdx to the end of the collection: leaving it at
	// 0 would resume a stale cursor at page one, forever.
	startIdx := 0

	if nextToken != "" {
		startIdx = len(all)

		for i, g := range all {
			if g.GroupName == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit >= len(all) {
		return all, "", nil
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].GroupName
	}

	return page, newToken, nil
}

// ListUsersInGroupPage returns a page of users in a group with optional NextToken pagination.
func (b *InMemoryBackend) ListUsersInGroupPage(
	userPoolID, groupName string,
	limit int,
	nextToken string,
) ([]*User, string, error) {
	b.mu.RLock("ListUsersInGroupPage")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.groups.Get(groupKey(userPoolID, groupName)); !ok {
		return nil, "", fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	members := b.groupMembers[userPoolID][groupName]
	all := make([]*User, 0, len(members))

	for username := range members {
		if u, ok := b.users.Get(userKey(userPoolID, username)); ok {
			cp := *u
			all = append(all, &cp)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Username < all[j].Username })

	// A miss (the user the token named left the group) defaults startIdx to
	// the end of the collection: leaving it at 0 would resume a stale cursor
	// at page one, forever.
	startIdx := 0

	if nextToken != "" {
		startIdx = len(all)

		for i, u := range all {
			if u.Username == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit >= len(all) {
		return all, "", nil
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].Username
	}

	return page, newToken, nil
}
