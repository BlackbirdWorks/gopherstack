package quicksight

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// ---- Groups ----

func (b *InMemoryBackend) CreateGroup(accountID, namespace, groupName, description string) (*Group, error) {
	if groupName == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if !b.namespaces.Has(nsKey(accountID, namespace)) {
		return nil, ErrNamespaceNotFound
	}

	key := groupKey(accountID, namespace, groupName)
	if b.groups.Has(key) {
		return nil, ErrGroupAlreadyExists
	}

	g := &storedGroup{
		GroupName:   groupName,
		Arn:         arn.Build("quicksight", b.region, accountID, fmt.Sprintf("group/%s/%s", namespace, groupName)),
		Description: description,
		Namespace:   namespace,
		PrincipalID: uuid.New().String(),
	}
	b.groups.Put(g)

	return g.toGroup(), nil
}

func (b *InMemoryBackend) DescribeGroup(accountID, namespace, groupName string) (*Group, error) {
	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups.Get(groupKey(accountID, namespace, groupName))
	if !ok {
		return nil, ErrGroupNotFound
	}

	return g.toGroup(), nil
}

func (b *InMemoryBackend) UpdateGroup(accountID, namespace, groupName, description string) (*Group, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	key := groupKey(accountID, namespace, groupName)
	g, ok := b.groups.Get(key)
	if !ok {
		return nil, ErrGroupNotFound
	}

	g.Description = description

	return g.toGroup(), nil
}

func (b *InMemoryBackend) DeleteGroup(accountID, namespace, groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	key := groupKey(accountID, namespace, groupName)
	if !b.groups.Delete(key) {
		return ErrGroupNotFound
	}

	// Remove all memberships for this group.
	prefix := groupKey(accountID, namespace, groupName) + "/"
	for k := range b.groupMembers {
		if strings.HasPrefix(k, prefix) {
			delete(b.groupMembers, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListGroups(
	_, namespace string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	var all []*storedGroup
	for _, g := range b.groups.All() {
		if g.Namespace == namespace {
			all = append(all, g)
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchGroups(
	_, namespace, query string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("SearchGroups")
	defer b.mu.RUnlock()

	var all []*storedGroup
	for _, g := range b.groups.All() {
		if g.Namespace == namespace &&
			(query == "" || strings.Contains(strings.ToLower(g.GroupName), strings.ToLower(query))) {
			all = append(all, g)
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

func paginateGroups(all []*storedGroup, maxResults int32, nextToken string) ([]*Group, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, g := range all {
			if g.GroupName == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].GroupName
	} else {
		end = len(all)
	}

	result := make([]*Group, 0, end-start)
	for _, g := range all[start:end] {
		result = append(result, g.toGroup())
	}

	return result, next
}

// ---- Group Memberships ----

func (b *InMemoryBackend) CreateGroupMembership(
	accountID, namespace, groupName, memberName string,
) (*GroupMember, error) {
	b.mu.Lock("CreateGroupMembership")
	defer b.mu.Unlock()

	if !b.groups.Has(groupKey(accountID, namespace, groupName)) {
		return nil, ErrGroupNotFound
	}

	key := groupMemberKey(accountID, namespace, groupName, memberName)
	if b.groupMembers[key] {
		return nil, ErrGroupMemberAlreadyExists
	}

	b.groupMembers[key] = true

	return &GroupMember{
		MemberName: memberName,
		Arn:        arn.Build("quicksight", b.region, accountID, fmt.Sprintf("user/%s/%s", namespace, memberName)),
	}, nil
}

func (b *InMemoryBackend) DescribeGroupMembership(
	accountID, namespace, groupName, memberName string,
) (*GroupMember, error) {
	b.mu.RLock("DescribeGroupMembership")
	defer b.mu.RUnlock()

	if !b.groupMembers[groupMemberKey(accountID, namespace, groupName, memberName)] {
		return nil, ErrGroupMemberNotFound
	}

	return &GroupMember{
		MemberName: memberName,
		Arn:        arn.Build("quicksight", b.region, accountID, fmt.Sprintf("user/%s/%s", namespace, memberName)),
	}, nil
}

func (b *InMemoryBackend) DeleteGroupMembership(accountID, namespace, groupName, memberName string) error {
	b.mu.Lock("DeleteGroupMembership")
	defer b.mu.Unlock()

	key := groupMemberKey(accountID, namespace, groupName, memberName)
	if !b.groupMembers[key] {
		return ErrGroupMemberNotFound
	}

	delete(b.groupMembers, key)

	return nil
}

func (b *InMemoryBackend) ListGroupMemberships(
	accountID, namespace, groupName string,
	maxResults int32,
	nextToken string,
) ([]*GroupMember, string, error) {
	b.mu.RLock("ListGroupMemberships")
	defer b.mu.RUnlock()

	if !b.groups.Has(groupKey(accountID, namespace, groupName)) {
		return nil, "", ErrGroupNotFound
	}

	prefix := groupMemberKey(accountID, namespace, groupName, "") + "/"
	_ = prefix
	fullPrefix := accountID + "/" + namespace + "/" + groupName + "/"
	var members []string
	for k := range b.groupMembers {
		if member, ok := strings.CutPrefix(k, fullPrefix); ok {
			members = append(members, member)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, m := range members {
			if m == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(members) {
		next = members[end]
	} else {
		end = len(members)
	}

	result := make([]*GroupMember, 0, end-start)
	for _, m := range members[start:end] {
		result = append(result, &GroupMember{
			MemberName: m,
			Arn:        arn.Build("quicksight", b.region, accountID, fmt.Sprintf("user/%s/%s", namespace, m)),
		})
	}

	return result, next, nil
}
