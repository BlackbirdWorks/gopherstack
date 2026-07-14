package iam

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateGroup creates a new IAM group.
func (b *InMemoryBackend) CreateGroup(groupName, path string) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); exists {
		return nil, fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, groupName)
	}

	p := normPath(path)
	g := Group{
		GroupName:  groupName,
		GroupID:    newID("AGPA"),
		Arn:        arn.Build("iam", "", b.accountID, "group"+p+groupName),
		Path:       p,
		CreateDate: time.Now().UTC(),
	}
	b.groups.Put(&g)
	b.sortedGroupNames = insertSorted(b.sortedGroupNames, groupName)

	return &g, nil
}

// DeleteGroup deletes an IAM group by name.
func (b *InMemoryBackend) DeleteGroup(groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if len(b.groupPolicies[groupName]) > 0 {
		return fmt.Errorf("%w: group %q has attached policies", ErrDeleteConflict, groupName)
	}

	if len(b.groupInlinePolicies[groupName]) > 0 {
		return fmt.Errorf("%w: group %q has inline policies", ErrDeleteConflict, groupName)
	}

	b.groups.Delete(groupName)
	b.sortedGroupNames = deleteSorted(b.sortedGroupNames, groupName)

	// Clean up group membership tracking.
	delete(b.groupMembers, groupName)

	return nil
}

// AddUserToGroup adds a user to an IAM group, tracking the membership.
func (b *InMemoryBackend) AddUserToGroup(groupName, userName string) error {
	b.mu.Lock("AddUserToGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.users.Get(userName); !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if slices.Contains(b.groupMembers[groupName], userName) {
		return nil // already a member
	}

	b.groupMembers[groupName] = append(b.groupMembers[groupName], userName)

	return nil
}

// RemoveUserFromGroup removes a user from an IAM group.
func (b *InMemoryBackend) RemoveUserFromGroup(groupName, userName string) error {
	b.mu.Lock("RemoveUserFromGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.users.Get(userName); !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	members := b.groupMembers[groupName]
	for i, m := range members {
		if m == userName {
			b.groupMembers[groupName] = append(members[:i], members[i+1:]...)

			return nil
		}
	}

	return nil
}

// GetGroup retrieves a single IAM group by name.
func (b *InMemoryBackend) GetGroup(groupName string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, exists := b.groups.Get(groupName)
	if !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	return g, nil
}

// GetGroupUsers returns the users that are members of the given group.
func (b *InMemoryBackend) GetGroupUsers(groupName string) ([]User, error) {
	b.mu.RLock("GetGroupUsers")
	defer b.mu.RUnlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	members := b.groupMembers[groupName]
	out := make([]User, 0, len(members))

	for _, userName := range members {
		if u, ok := b.users.Get(userName); ok {
			out = append(out, *u)
		}
	}

	return out, nil
}

// AttachGroupPolicy attaches a policy to a group.
func (b *InMemoryBackend) AttachGroupPolicy(groupName, policyArn string) error {
	b.mu.Lock("AttachGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if slices.Contains(b.groupPolicies[groupName], policyArn) {
		return nil // already attached
	}

	b.groupPolicies[groupName] = append(b.groupPolicies[groupName], policyArn)
	b.addPolicyAttachmentLocked(policyArn, groupName, "group")

	return nil
}

// DetachGroupPolicy detaches a policy from a group.
func (b *InMemoryBackend) DetachGroupPolicy(groupName, policyArn string) error {
	b.mu.Lock("DetachGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	policies := b.groupPolicies[groupName]
	for i, p := range policies {
		if p == policyArn {
			b.groupPolicies[groupName] = append(policies[:i], policies[i+1:]...)
			b.removePolicyAttachmentLocked(policyArn, groupName, "group")

			return nil
		}
	}

	return nil
}

// ListAttachedGroupPolicies returns all policy ARNs attached to the named group.
func (b *InMemoryBackend) ListAttachedGroupPolicies(groupName string) ([]AttachedPolicy, error) {
	b.mu.RLock("ListAttachedGroupPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	arns := b.groupPolicies[groupName]
	result := make([]AttachedPolicy, 0, len(arns))

	for _, a := range arns {
		name := policyNameFromARN(a)
		result = append(result, AttachedPolicy{PolicyName: name, PolicyArn: a})
	}

	return result, nil
}

// ListGroups returns a paginated list of IAM groups sorted by name.
func (b *InMemoryBackend) ListGroups(marker string, maxItems int) (page.Page[Group], error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	return pageFromSortedNames(
		b.sortedGroupNames,
		b.groups.Get,
		marker,
		maxItems,
		iamDefaultMaxItems,
	), nil
}

// ListAllGroups returns all groups (for dashboard).
func (b *InMemoryBackend) ListAllGroups() []Group {
	b.mu.RLock("ListAllGroups")
	defer b.mu.RUnlock()

	groups := make([]Group, 0, b.groups.Len())
	for _, g := range b.groups.All() {
		groups = append(groups, *g)
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupName < groups[j].GroupName })

	return groups
}

// PutGroupPolicy creates or replaces an inline policy on a group.
func (b *InMemoryBackend) PutGroupPolicy(groupName, policyName, policyDocument string) error {
	b.mu.Lock("PutGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if err := validateIdentityPolicyDocument(policyDocument); err != nil {
		return err
	}

	if len(policyDocument) > maxGroupPolicySize {
		return fmt.Errorf("%w: inline policy for group %q exceeds %d bytes",
			ErrLimitExceeded, groupName, maxGroupPolicySize)
	}

	if b.groupInlinePolicies[groupName] == nil {
		b.groupInlinePolicies[groupName] = make(map[string]string)
	}

	b.groupInlinePolicies[groupName][policyName] = policyDocument

	return nil
}

// GetGroupPolicy retrieves an inline policy document from a group.
func (b *InMemoryBackend) GetGroupPolicy(groupName, policyName string) (string, error) {
	b.mu.RLock("GetGroupPolicy")
	defer b.mu.RUnlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return "", fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	doc, exists := b.groupInlinePolicies[groupName][policyName]
	if !exists {
		return "", fmt.Errorf(
			"%w: inline policy %q not found on group %q",
			ErrInlinePolicyNotFound,
			policyName,
			groupName,
		)
	}

	return doc, nil
}

// DeleteGroupPolicy removes an inline policy from a group.
func (b *InMemoryBackend) DeleteGroupPolicy(groupName, policyName string) error {
	b.mu.Lock("DeleteGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.groupInlinePolicies[groupName][policyName]; !exists {
		return fmt.Errorf(
			"%w: inline policy %q not found on group %q",
			ErrInlinePolicyNotFound,
			policyName,
			groupName,
		)
	}

	delete(b.groupInlinePolicies[groupName], policyName)

	return nil
}

// ListGroupPolicies returns sorted inline policy names for a group.
func (b *InMemoryBackend) ListGroupPolicies(groupName string) ([]string, error) {
	b.mu.RLock("ListGroupPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.groups.Get(groupName); !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	names := collections.SortedKeys(b.groupInlinePolicies[groupName])

	return names, nil
}

// purgeGroupsLocked removes groups created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeGroupsLocked(cutoff time.Time) {
	for _, g := range b.groups.All() {
		if g.CreateDate.Before(cutoff) {
			name := g.GroupName
			b.groups.Delete(name)
			delete(b.groupPolicies, name)
			delete(b.groupInlinePolicies, name)
			delete(b.groupMembers, name)
		}
	}
}

// ListGroupsForUser returns all groups that the specified user belongs to.
func (b *InMemoryBackend) ListGroupsForUser(userName string) ([]Group, error) {
	b.mu.RLock("ListGroupsForUser")
	defer b.mu.RUnlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	result := make([]Group, 0, len(b.groupMembers))
	for groupName, members := range b.groupMembers {
		if slices.Contains(members, userName) {
			if g, exists := b.groups.Get(groupName); exists {
				result = append(result, *g)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].GroupName < result[j].GroupName })

	return result, nil
}

// UpdateGroup renames a group and/or updates its path.
func (b *InMemoryBackend) UpdateGroup(groupName, newPath, newGroupName string) error {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if newGroupName != "" && newGroupName != groupName {
		if _, taken := b.groups.Get(newGroupName); taken {
			return fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, newGroupName)
		}
	}

	if newPath != "" {
		g.Path = normPath(newPath)
	}

	if newGroupName != "" && newGroupName != groupName {
		b.migrateGroupData(groupName, newGroupName)
		g.GroupName = newGroupName
		b.groups.Delete(groupName)
	}

	b.groups.Put(g)

	return nil
}

// migrateGroupData moves per-group maps to the new name (called with lock held).
func (b *InMemoryBackend) migrateGroupData(oldName, newName string) {
	if policies, found := b.groupPolicies[oldName]; found {
		b.groupPolicies[newName] = policies
		delete(b.groupPolicies, oldName)
	}

	if members, found := b.groupMembers[oldName]; found {
		b.groupMembers[newName] = members
		delete(b.groupMembers, oldName)
	}

	if inline, found := b.groupInlinePolicies[oldName]; found {
		b.groupInlinePolicies[newName] = inline
		delete(b.groupInlinePolicies, oldName)
	}
}

// TagGroup merges the given key-value pairs into the group's Tags field.
func (b *InMemoryBackend) TagGroup(groupName string, tags map[string]string) error {
	b.mu.Lock("TagGroup")
	defer b.mu.Unlock()

	g, exists := b.groups.Get(groupName)
	if !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if g.Tags == nil {
		g.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(g.Tags, tags)
	b.groups.Put(g)

	return nil
}

// UntagGroup removes the given keys from the group's Tags field.
func (b *InMemoryBackend) UntagGroup(groupName string, keys []string) error {
	b.mu.Lock("UntagGroup")
	defer b.mu.Unlock()

	g, exists := b.groups.Get(groupName)
	if !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	for _, k := range keys {
		delete(g.Tags, k)
	}

	b.groups.Put(g)

	return nil
}
