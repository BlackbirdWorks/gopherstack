package workmail

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// --- Groups ---

func (b *InMemoryBackend) findGroup(orgID, entityID string) *Group {
	if g, ok := b.groups.Get(orgKey(orgID, entityID)); ok {
		return g
	}
	for _, g := range b.groupsByOrg.Get(orgID) {
		if g.Name == entityID {
			return g
		}
	}

	return nil
}

// CreateGroup creates a new WorkMail group.
func (b *InMemoryBackend) CreateGroup(orgID, name string, hidden bool) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	for _, g := range b.groupsByOrg.Get(orgID) {
		if g.Name == name {
			return nil, fmt.Errorf("%w: group %q already exists", ErrConflict, name)
		}
	}

	groupID := newID()
	now := time.Now().UTC()

	g := &Group{
		CreatedAt: now,
		GroupID:   groupID,
		Name:      name,
		State:     stateDisabled,
		ARN:       b.entityARN(orgID, "group", groupID),
		Hidden:    hidden,
		orgID:     orgID,
	}

	b.groups.Put(g)
	b.groupMembers[orgID][groupID] = make(map[string]bool)

	return g, nil
}

// DescribeGroup returns group details.
func (b *InMemoryBackend) DescribeGroup(orgID, entityID string) (*Group, error) {
	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, entityID)
	if g == nil {
		return nil, fmt.Errorf("%w: group %q not found", ErrNotFound, entityID)
	}

	return g, nil
}

// UpdateGroup updates a group.
func (b *InMemoryBackend) UpdateGroup(orgID, entityID string, hidden bool) error {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, entityID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, entityID)
	}
	g.Hidden = hidden

	return nil
}

// DeleteGroup deletes a group. Both DeleteGroup and DeleteResource are
// org-check + find + state-guard + email/globalAliases cleanup +
// cascadeCleanEntity (the actual shared cascade logic is already deduped
// into cascadeCleanEntity in store.go) + own-table delete; the remaining
// shape differs only in field/type names (Group/GroupID/groupsByEmail/
// groupMembers vs Resource/ResourceID/resourcesByEmail/delegates), which a
// generic helper isn't worth introducing for.
//
//nolint:dupl // structurally-identical CRUD pair with DeleteResource; see doc comment above.
func (b *InMemoryBackend) DeleteGroup(orgID, entityID string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, entityID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, entityID)
	}

	if g.State == stateEnabled {
		return fmt.Errorf(
			"%w: group %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first",
			ErrEntityState,
			entityID,
		)
	}

	if g.Email != "" {
		delete(b.groupsByEmail[orgID], g.Email)
		b.globalAliases.Delete(g.Email)
	}
	b.cascadeCleanEntity(orgID, g.GroupID, g.ARN)
	b.groups.Delete(orgKey(orgID, g.GroupID))
	delete(b.groupMembers[orgID], g.GroupID)

	return nil
}

// ListGroups returns a paginated list of groups, optionally narrowed by
// filter (see GroupFilter -- mirrors ListGroupsInput.Filters).
func (b *InMemoryBackend) ListGroups(
	orgID string,
	filter *GroupFilter,
	maxResults int32,
	nextToken string,
) ([]*GroupSummary, string, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	gs := make([]*GroupSummary, 0, len(b.groupsByOrg.Get(orgID)))
	for _, g := range b.groupsByOrg.Get(orgID) {
		if !groupMatchesFilter(g, filter) {
			continue
		}
		gs = append(gs, &GroupSummary{
			GroupID:      g.GroupID,
			Name:         g.Name,
			Email:        g.Email,
			State:        g.State,
			EnabledDate:  g.EnabledDate,
			DisabledDate: g.DisabledDate,
		})
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].Name < gs[j].Name })

	items, next := paginate(gs, maxResults, nextToken)

	return items, next, nil
}

// groupMatchesFilter reports whether g satisfies every non-empty dimension
// of filter. A nil filter matches everything.
func groupMatchesFilter(g *Group, filter *GroupFilter) bool {
	if filter == nil {
		return true
	}
	if filter.NamePrefix != "" && !strings.HasPrefix(g.Name, filter.NamePrefix) {
		return false
	}
	if filter.PrimaryEmailPrefix != "" && !strings.HasPrefix(g.Email, filter.PrimaryEmailPrefix) {
		return false
	}
	if filter.State != "" && g.State != filter.State {
		return false
	}

	return true
}

// AssociateMemberToGroup adds a member to a group.
func (b *InMemoryBackend) AssociateMemberToGroup(orgID, groupID, memberID string) error {
	b.mu.Lock("AssociateMemberToGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, groupID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, groupID)
	}

	// member must exist as user or group
	if b.findUser(orgID, memberID) == nil && b.findGroup(orgID, memberID) == nil {
		return fmt.Errorf("%w: member entity %q not found", ErrNotFound, memberID)
	}

	if b.groupMembers[orgID][g.GroupID] == nil {
		b.groupMembers[orgID][g.GroupID] = make(map[string]bool)
	}
	b.groupMembers[orgID][g.GroupID][memberID] = true

	return nil
}

// DisassociateMemberFromGroup removes a member from a group.
func (b *InMemoryBackend) DisassociateMemberFromGroup(orgID, groupID, memberID string) error {
	b.mu.Lock("DisassociateMemberFromGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, groupID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, groupID)
	}

	members := b.groupMembers[orgID][g.GroupID]
	if members == nil || !members[memberID] {
		return fmt.Errorf("%w: member %q not in group", ErrNotFound, memberID)
	}
	delete(members, memberID)

	return nil
}

// ListGroupMembers returns members of a group.
func (b *InMemoryBackend) ListGroupMembers(
	orgID, groupID string,
	maxResults int32,
	nextToken string,
) ([]*Member, string, error) {
	b.mu.RLock("ListGroupMembers")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, groupID)
	if g == nil {
		return nil, "", fmt.Errorf("%w: group %q not found", ErrNotFound, groupID)
	}

	members := make([]*Member, 0)
	for memberID := range b.groupMembers[orgID][g.GroupID] {
		m := &Member{MemberID: memberID, State: stateEnabled}
		if u := b.findUser(orgID, memberID); u != nil {
			m.Name = u.Name
			m.MemberType = memberTypeUser
		} else if grp := b.findGroup(orgID, memberID); grp != nil {
			m.Name = grp.Name
			m.MemberType = memberTypeGroup
		}
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].MemberID < members[j].MemberID })

	items, next := paginate(members, maxResults, nextToken)

	return items, next, nil
}

// ListGroupsForEntity returns groups containing the given entity, optionally
// narrowed to group names starting with groupNamePrefix (mirrors
// ListGroupsForEntityInput.Filters.GroupNamePrefix, the ListGroupsForEntity
// operation's single filter dimension).
func (b *InMemoryBackend) ListGroupsForEntity(
	orgID, entityID, groupNamePrefix string,
	maxResults int32,
	nextToken string,
) ([]*GroupSummary, string, error) {
	b.mu.RLock("ListGroupsForEntity")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	gs := make([]*GroupSummary, 0)
	for _, g := range b.groupsByOrg.Get(orgID) {
		if !b.groupMembers[orgID][g.GroupID][entityID] {
			continue
		}
		if groupNamePrefix != "" && !strings.HasPrefix(g.Name, groupNamePrefix) {
			continue
		}
		gs = append(
			gs,
			&GroupSummary{GroupID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State},
		)
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].Name < gs[j].Name })

	items, next := paginate(gs, maxResults, nextToken)

	return items, next, nil
}
