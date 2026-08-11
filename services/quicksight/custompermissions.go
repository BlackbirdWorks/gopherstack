package quicksight

import (
	"maps"
	"sort"
	"strings"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
)

// storedCustomPermissions is the persisted representation of one QuickSight
// custom permissions profile.
type storedCustomPermissions struct {
	Capabilities map[string]any `json:"capabilities,omitempty"`
	Name         string         `json:"name"`
	Arn          string         `json:"arn"`
}

func (c *storedCustomPermissions) toCustomPermissions() *CustomPermissions {
	return &CustomPermissions{
		Name:         c.Name,
		Arn:          c.Arn,
		Capabilities: c.Capabilities,
	}
}

// isValidRole derives its answer from types.Role.Values() so it cannot drift
// from the real enum -- the previous hand-copied list invented two roles
// (RESTRICTED_AUTHOR, RESTRICTED_READER) that don't exist in the real API,
// the more-permissive-than-AWS class.
func isValidRole(role string) bool {
	for _, v := range sdktypes.Role("").Values() {
		if string(v) == role {
			return true
		}
	}

	return false
}

// ---- Custom permissions ----

func (b *InMemoryBackend) CreateCustomPermissions(
	accountID, name string,
	capabilities map[string]any,
	tags map[string]string,
) (*CustomPermissions, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateCustomPermissions")
	defer b.mu.Unlock()

	key := customPermissionsKey(accountID, name)
	if b.customPermissions.Has(key) {
		return nil, ErrCustomPermissionsAlreadyExists
	}

	cp := &storedCustomPermissions{
		Name:         name,
		Arn:          b.buildARN("custom-permissions", name),
		Capabilities: capabilities,
	}
	b.customPermissions.Put(cp)

	if len(tags) > 0 {
		b.tags[cp.Arn] = maps.Clone(tags)
	}

	return cp.toCustomPermissions(), nil
}

func (b *InMemoryBackend) DescribeCustomPermissions(accountID, name string) (*CustomPermissions, error) {
	b.mu.RLock("DescribeCustomPermissions")
	defer b.mu.RUnlock()

	cp, ok := b.customPermissions.Get(customPermissionsKey(accountID, name))
	if !ok {
		return nil, ErrCustomPermissionsNotFound
	}

	return cp.toCustomPermissions(), nil
}

func (b *InMemoryBackend) UpdateCustomPermissions(
	accountID, name string,
	capabilities map[string]any,
) (*CustomPermissions, error) {
	b.mu.Lock("UpdateCustomPermissions")
	defer b.mu.Unlock()

	cp, ok := b.customPermissions.Get(customPermissionsKey(accountID, name))
	if !ok {
		return nil, ErrCustomPermissionsNotFound
	}

	if capabilities != nil {
		cp.Capabilities = capabilities
	}

	return cp.toCustomPermissions(), nil
}

func (b *InMemoryBackend) DeleteCustomPermissions(accountID, name string) error {
	b.mu.Lock("DeleteCustomPermissions")
	defer b.mu.Unlock()

	key := customPermissionsKey(accountID, name)
	if !b.customPermissions.Has(key) {
		return ErrCustomPermissionsNotFound
	}

	prefix := accountID + "/"
	for k, v := range b.roleCustomPermissions {
		if strings.HasPrefix(k, prefix) && v == name {
			return ErrCustomPermissionsInUse
		}
	}
	for k, v := range b.userCustomPermissions {
		if strings.HasPrefix(k, prefix) && v == name {
			return ErrCustomPermissionsInUse
		}
	}

	b.customPermissions.Delete(key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListCustomPermissions(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*CustomPermissions, string, error) {
	b.mu.RLock("ListCustomPermissions")
	defer b.mu.RUnlock()

	all := b.customPermissions.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, cp := range all {
			if cp.Name == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].Name
	} else {
		end = len(all)
	}

	result := make([]*CustomPermissions, 0, end-start)
	for _, cp := range all[start:end] {
		result = append(result, cp.toCustomPermissions())
	}

	return result, next, nil
}

// ---- Role custom permission ----

func (b *InMemoryBackend) UpdateRoleCustomPermission(accountID, namespace, role, customPermissionsName string) error {
	if !isValidRole(role) || customPermissionsName == "" {
		return ErrValidation
	}

	b.mu.Lock("UpdateRoleCustomPermission")
	defer b.mu.Unlock()

	if !b.customPermissions.Has(customPermissionsKey(accountID, customPermissionsName)) {
		return ErrCustomPermissionsNotFound
	}

	b.roleCustomPermissions[roleCustomPermissionKey(accountID, namespace, role)] = customPermissionsName

	return nil
}

func (b *InMemoryBackend) DescribeRoleCustomPermission(accountID, namespace, role string) (string, error) {
	b.mu.RLock("DescribeRoleCustomPermission")
	defer b.mu.RUnlock()

	name, ok := b.roleCustomPermissions[roleCustomPermissionKey(accountID, namespace, role)]
	if !ok {
		return "", ErrRoleCustomPermissionNotFound
	}

	return name, nil
}

func (b *InMemoryBackend) DeleteRoleCustomPermission(accountID, namespace, role string) error {
	b.mu.Lock("DeleteRoleCustomPermission")
	defer b.mu.Unlock()

	key := roleCustomPermissionKey(accountID, namespace, role)
	if _, ok := b.roleCustomPermissions[key]; !ok {
		return ErrRoleCustomPermissionNotFound
	}
	delete(b.roleCustomPermissions, key)

	return nil
}

// ---- Role memberships ----

func (b *InMemoryBackend) CreateRoleMembership(accountID, namespace, role, memberName string) error {
	if !isValidRole(role) || memberName == "" {
		return ErrValidation
	}

	b.mu.Lock("CreateRoleMembership")
	defer b.mu.Unlock()

	key := roleMembershipKey(accountID, namespace, role, memberName)
	if b.roleMemberships[key] {
		return ErrRoleMembershipAlreadyExists
	}
	b.roleMemberships[key] = true

	return nil
}

func (b *InMemoryBackend) DeleteRoleMembership(accountID, namespace, role, memberName string) error {
	b.mu.Lock("DeleteRoleMembership")
	defer b.mu.Unlock()

	key := roleMembershipKey(accountID, namespace, role, memberName)
	if !b.roleMemberships[key] {
		return ErrRoleMembershipNotFound
	}
	delete(b.roleMemberships, key)

	return nil
}

func (b *InMemoryBackend) ListRoleMemberships(
	accountID, namespace, role string,
	maxResults int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListRoleMemberships")
	defer b.mu.RUnlock()

	prefix := roleCustomPermissionKey(accountID, namespace, role) + "/"
	var members []string
	for k := range b.roleMemberships {
		if member, ok := strings.CutPrefix(k, prefix); ok {
			members = append(members, member)
		}
	}
	sort.Strings(members)

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

	return members[start:end], next, nil
}

// ---- User custom permission ----

func (b *InMemoryBackend) UpdateUserCustomPermission(
	accountID, namespace, userName, customPermissionsName string,
) error {
	if userName == "" || customPermissionsName == "" {
		return ErrValidation
	}

	b.mu.Lock("UpdateUserCustomPermission")
	defer b.mu.Unlock()

	if !b.users.Has(userKey(accountID, namespace, userName)) {
		return ErrUserNotFound
	}
	if !b.customPermissions.Has(customPermissionsKey(accountID, customPermissionsName)) {
		return ErrCustomPermissionsNotFound
	}

	b.userCustomPermissions[userCustomPermissionKey(accountID, namespace, userName)] = customPermissionsName

	return nil
}

func (b *InMemoryBackend) DeleteUserCustomPermission(accountID, namespace, userName string) error {
	b.mu.Lock("DeleteUserCustomPermission")
	defer b.mu.Unlock()

	key := userCustomPermissionKey(accountID, namespace, userName)
	if _, ok := b.userCustomPermissions[key]; !ok {
		return ErrUserCustomPermissionNotFound
	}
	delete(b.userCustomPermissions, key)

	return nil
}
