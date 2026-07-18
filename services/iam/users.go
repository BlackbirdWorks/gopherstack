package iam

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateUser creates a new IAM user.
func (b *InMemoryBackend) CreateUser(userName, path, permissionsBoundary string) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, userName)
	}

	p := normPath(path)
	u := User{
		UserName:            userName,
		UserID:              newID("AIDA"),
		Arn:                 arn.Build("iam", "", b.accountID, "user"+p+userName),
		Path:                p,
		CreateDate:          time.Now().UTC(),
		PermissionsBoundary: permissionsBoundary,
	}
	b.users.Put(&u)
	b.sortedUserNames = insertSorted(b.sortedUserNames, userName)

	return &u, nil
}

// DeleteUser deletes an IAM user by name, removing all associated access keys and login profile.
func (b *InMemoryBackend) DeleteUser(userName string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if len(b.userPolicies[userName]) > 0 {
		return fmt.Errorf("%w: user %q has attached policies", ErrDeleteConflict, userName)
	}

	if len(b.userInlinePolicies[userName]) > 0 {
		return fmt.Errorf("%w: user %q has inline policies", ErrDeleteConflict, userName)
	}

	// Clean up access keys belonging to the user.
	for _, id := range b.userAccessKeys[userName] {
		b.accessKeys.Delete(id)
	}
	delete(b.userAccessKeys, userName)

	// Clean up login profile.
	b.loginProfiles.Delete(userName)

	// Remove user from all group memberships.
	for groupName, members := range b.groupMembers {
		for i, m := range members {
			if m == userName {
				b.groupMembers[groupName] = append(members[:i], members[i+1:]...)

				break
			}
		}
	}

	b.users.Delete(userName)
	b.sortedUserNames = deleteSorted(b.sortedUserNames, userName)

	return nil
}

// ListUsers returns a paginated list of IAM users sorted by name.
func (b *InMemoryBackend) ListUsers(marker string, maxItems int) (page.Page[User], error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	return pageFromSortedNames(
		b.sortedUserNames,
		b.users.Get,
		marker,
		maxItems,
		iamDefaultMaxItems,
	), nil
}

// GetUser retrieves a single IAM user by name.
func (b *InMemoryBackend) GetUser(userName string) (*User, error) {
	b.mu.RLock("GetUser")
	defer b.mu.RUnlock()

	u, exists := b.users.Get(userName)
	if !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	return u, nil
}

// ListAllUsers returns all users (for dashboard).
func (b *InMemoryBackend) ListAllUsers() []User {
	b.mu.RLock("ListAllUsers")
	defer b.mu.RUnlock()

	return sortedUsers(b.users)
}

// ListAttachedUserPolicies returns all policy ARNs attached to the named user.
func (b *InMemoryBackend) ListAttachedUserPolicies(userName string) ([]AttachedPolicy, error) {
	b.mu.RLock("ListAttachedUserPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	arns := b.userPolicies[userName]
	result := make([]AttachedPolicy, 0, len(arns))

	for _, arn := range arns {
		name := policyNameFromARN(arn)
		result = append(result, AttachedPolicy{PolicyName: name, PolicyArn: arn})
	}

	return result, nil
}

// GetUserByAccessKeyID returns the User associated with the given access key ID.
// Returns ErrAccessKeyNotFound if no key with that ID exists.
func (b *InMemoryBackend) GetUserByAccessKeyID(accessKeyID string) (*User, error) {
	b.mu.RLock("GetUserByAccessKeyID")
	defer b.mu.RUnlock()

	ak, exists := b.accessKeys.Get(accessKeyID)
	if !exists {
		return nil, fmt.Errorf("%w: access key %q not found", ErrAccessKeyNotFound, accessKeyID)
	}

	u, exists := b.users.Get(ak.UserName)
	if !exists {
		return nil, fmt.Errorf("%w: user %q not found for access key", ErrUserNotFound, ak.UserName)
	}

	return u, nil
}

// GetPoliciesForUser returns the policy documents for all policies attached to the named user.
// Policies that are referenced but not found in the backend are silently skipped.
func (b *InMemoryBackend) GetPoliciesForUser(userName string) ([]string, error) {
	b.mu.RLock("GetPoliciesForUser")
	defer b.mu.RUnlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	arns := b.userPolicies[userName]
	docs := make([]string, 0, len(arns))

	for _, policyArn := range arns {
		policy, exists := b.getPolicyByARNLocked(policyArn)
		if !exists || policy.PolicyDocument == "" {
			continue
		}

		docs = append(docs, policy.PolicyDocument)
	}

	return docs, nil
}

// PutUserPolicy creates or replaces an inline policy on a user.
func (b *InMemoryBackend) PutUserPolicy(userName, policyName, policyDocument string) error {
	b.mu.Lock("PutUserPolicy")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if err := validateIdentityPolicyDocument(policyDocument); err != nil {
		return err
	}

	if len(policyDocument) > maxUserPolicySize {
		return fmt.Errorf("%w: inline policy for user %q exceeds %d bytes",
			ErrLimitExceeded, userName, maxUserPolicySize)
	}

	if b.userInlinePolicies[userName] == nil {
		b.userInlinePolicies[userName] = make(map[string]string)
	}

	b.userInlinePolicies[userName][policyName] = policyDocument

	return nil
}

// GetUserPolicy retrieves an inline policy document from a user.
func (b *InMemoryBackend) GetUserPolicy(userName, policyName string) (string, error) {
	b.mu.RLock("GetUserPolicy")
	defer b.mu.RUnlock()

	if _, exists := b.users.Get(userName); !exists {
		return "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	doc, exists := b.userInlinePolicies[userName][policyName]
	if !exists {
		return "", fmt.Errorf(
			"%w: inline policy %q not found on user %q",
			ErrInlinePolicyNotFound,
			policyName,
			userName,
		)
	}

	return doc, nil
}

// DeleteUserPolicy removes an inline policy from a user.
func (b *InMemoryBackend) DeleteUserPolicy(userName, policyName string) error {
	b.mu.Lock("DeleteUserPolicy")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if _, exists := b.userInlinePolicies[userName][policyName]; !exists {
		return fmt.Errorf(
			"%w: inline policy %q not found on user %q",
			ErrInlinePolicyNotFound,
			policyName,
			userName,
		)
	}

	delete(b.userInlinePolicies[userName], policyName)

	return nil
}

// ListUserPolicies returns sorted inline policy names for a user.
func (b *InMemoryBackend) ListUserPolicies(userName string) ([]string, error) {
	b.mu.RLock("ListUserPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	names := collections.SortedKeys(b.userInlinePolicies[userName])

	return names, nil
}

// PutUserPermissionsBoundary sets the permissions boundary on a user.
func (b *InMemoryBackend) PutUserPermissionsBoundary(userName, policyArn string) error {
	b.mu.Lock("PutUserPermissionsBoundary")
	defer b.mu.Unlock()

	u, exists := b.users.Get(userName)
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	u.PermissionsBoundary = policyArn
	b.users.Put(u)

	return nil
}

// DeleteUserPermissionsBoundary clears the permissions boundary on a user.
func (b *InMemoryBackend) DeleteUserPermissionsBoundary(userName string) error {
	b.mu.Lock("DeleteUserPermissionsBoundary")
	defer b.mu.Unlock()

	u, exists := b.users.Get(userName)
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	u.PermissionsBoundary = ""
	b.users.Put(u)

	return nil
}

// purgeUsersLocked removes users created before cutoff and cleans up associated data.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeUsersLocked(cutoff time.Time) {
	for _, u := range b.users.All() {
		if !u.CreateDate.Before(cutoff) {
			continue
		}
		name := u.UserName
		b.users.Delete(name)
		b.loginProfiles.Delete(name)
		delete(b.userPolicies, name)
		delete(b.userInlinePolicies, name)
		b.removeUserFromGroupsLocked(name)
	}
}

// removeUserFromGroupsLocked removes a user from all group membership lists.
// Caller must hold b.mu.
func (b *InMemoryBackend) removeUserFromGroupsLocked(userName string) {
	for g, members := range b.groupMembers {
		for i, m := range members {
			if m == userName {
				b.groupMembers[g] = append(members[:i], members[i+1:]...)

				break
			}
		}
	}
}

// UpdateUser renames a user and/or updates their path.
// If newUserName is non-empty the user is renamed; if newPath is non-empty the path is updated.
func (b *InMemoryBackend) UpdateUser(userName, newPath, newUserName string) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	u, ok := b.users.Get(userName)
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if newUserName != "" && newUserName != userName {
		if _, taken := b.users.Get(newUserName); taken {
			return fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, newUserName)
		}
	}

	if newPath != "" {
		u.Path = normPath(newPath)
	}

	if newUserName != "" && newUserName != userName {
		b.migrateUserData(userName, newUserName)
		u.UserName = newUserName
		b.users.Delete(userName)
	}

	b.users.Put(u)

	return nil
}

// migrateUserData moves per-user maps to the new name (called with lock held).
func (b *InMemoryBackend) migrateUserData(oldName, newName string) {
	for _, ak := range b.accessKeys.All() {
		if ak.UserName == oldName {
			ak.UserName = newName
			b.accessKeys.Put(ak)
		}
	}

	// loginProfiles is keyed by UserName (see loginProfilesKeyFn in
	// store_setup.go), so the rename must update the value's own UserName
	// field before re-inserting -- store.Table always derives the key from
	// the value, unlike the raw map this replaces which allowed the map key
	// and the value's UserName field to diverge after a rename.
	if lp, found := b.loginProfiles.Get(oldName); found {
		b.loginProfiles.Delete(oldName)
		lp.UserName = newName
		b.loginProfiles.Put(lp)
	}

	if policies, found := b.userPolicies[oldName]; found {
		b.userPolicies[newName] = policies
		delete(b.userPolicies, oldName)
	}

	if inline, found := b.userInlinePolicies[oldName]; found {
		b.userInlinePolicies[newName] = inline
		delete(b.userInlinePolicies, oldName)
	}

	for groupName, members := range b.groupMembers {
		for i, m := range members {
			if m == oldName {
				b.groupMembers[groupName][i] = newName
			}
		}
	}
}

// TagUser merges the given key-value pairs into the user's Tags field.
func (b *InMemoryBackend) TagUser(userName string, tags map[string]string) error {
	b.mu.Lock("TagUser")
	defer b.mu.Unlock()

	u, exists := b.users.Get(userName)
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if u.Tags == nil {
		u.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(u.Tags, tags)
	b.users.Put(u)

	return nil
}

// UntagUser removes the given keys from the user's Tags field.
func (b *InMemoryBackend) UntagUser(userName string, keys []string) error {
	b.mu.Lock("UntagUser")
	defer b.mu.Unlock()

	u, exists := b.users.Get(userName)
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	for _, k := range keys {
		delete(u.Tags, k)
	}

	b.users.Put(u)

	return nil
}
