package workmail

import (
	"fmt"
	"sort"
	"time"
)

// --- Users ---

// CreateUser creates a new WorkMail user.
func (b *InMemoryBackend) CreateUser(
	orgID, name, displayName, password, role string,
) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	validRoles := map[string]bool{roleUser: true, roleResource: true, roleSystemUser: true}
	if role != "" && !validRoles[role] {
		return nil, fmt.Errorf(
			"%w: invalid Role %q, must be USER, RESOURCE, or SYSTEM_USER",
			ErrValidation,
			role,
		)
	}

	for _, u := range b.usersByOrg.Get(orgID) {
		if u.Name == name {
			return nil, fmt.Errorf("%w: user %q already exists", ErrConflict, name)
		}
	}

	userID := newID()
	now := time.Now().UTC()
	_ = password // stored in real AWS but not needed for simulation

	u := &User{
		CreatedAt:   now,
		UserID:      userID,
		Name:        name,
		DisplayName: displayName,
		Role:        role,
		State:       stateDisabled,
		ARN:         b.entityARN(orgID, "user", userID),
		orgID:       orgID,
	}

	b.users.Put(u)

	return u, nil
}

// DescribeUser returns details about a user.
func (b *InMemoryBackend) DescribeUser(orgID, entityID string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, entityID)
	if u == nil {
		return nil, fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	return u, nil
}

func (b *InMemoryBackend) findUser(orgID, entityID string) *User {
	if u, ok := b.users.Get(orgKey(orgID, entityID)); ok {
		return u
	}
	// search by name
	for _, u := range b.usersByOrg.Get(orgID) {
		if u.Name == entityID {
			return u
		}
	}

	return nil
}

// UpdateUser updates display name and name fields.
func (b *InMemoryBackend) UpdateUser(
	orgID, entityID, displayName, firstName, lastName string,
) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, entityID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	if displayName != "" {
		u.DisplayName = displayName
	}
	if firstName != "" {
		u.FirstName = firstName
	}
	if lastName != "" {
		u.LastName = lastName
	}

	return nil
}

// DeleteUser removes a user.
func (b *InMemoryBackend) DeleteUser(orgID, entityID string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, entityID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	if u.State == stateEnabled {
		return fmt.Errorf(
			"%w: user %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first",
			ErrEntityState,
			entityID,
		)
	}

	actualID := u.UserID
	if u.Email != "" {
		delete(b.usersByEmail[orgID], u.Email)
	}
	b.users.Delete(orgKey(orgID, actualID))

	return nil
}

// ListUsers returns a paginated list of users.
func (b *InMemoryBackend) ListUsers(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*UserSummary, string, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	users := make([]*UserSummary, 0)
	for _, u := range b.usersByOrg.Get(orgID) {
		users = append(users, &UserSummary{
			UserID:      u.UserID,
			Name:        u.Name,
			Email:       u.Email,
			DisplayName: u.DisplayName,
			State:       u.State,
			Role:        u.Role,
		})
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })

	items, next := paginate(users, maxResults, nextToken)

	return items, next, nil
}

// RegisterToWorkMail assigns an email address to a user/group/resource.
func (b *InMemoryBackend) RegisterToWorkMail(orgID, entityID, email string) error {
	b.mu.Lock("RegisterToWorkMail")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if ta, exists := b.globalAliases.Get(email); exists && ta.OrgID == orgID {
		return fmt.Errorf("%w: email %q already in use", ErrConflict, email)
	}

	now := time.Now().UTC()

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			b.globalAliases.Delete(u.Email)
		}
		u.Email = email
		u.State = stateEnabled
		u.EnabledDate = now
		b.usersByEmail[orgID][email] = u.UserID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: u.UserID})

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			b.globalAliases.Delete(g.Email)
		}
		g.Email = email
		g.State = stateEnabled
		g.EnabledDate = now
		b.groupsByEmail[orgID][email] = g.GroupID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: g.GroupID})

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			b.globalAliases.Delete(r.Email)
		}
		r.Email = email
		r.State = stateEnabled
		r.EnabledDate = now
		b.resourcesByEmail[orgID][email] = r.ResourceID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: r.ResourceID})

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// DeregisterFromWorkMail removes an email address assignment.
func (b *InMemoryBackend) DeregisterFromWorkMail(orgID, entityID string) error {
	b.mu.Lock("DeregisterFromWorkMail")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	now := time.Now().UTC()

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			b.globalAliases.Delete(u.Email)
		}
		u.Email = ""
		u.State = stateDisabled
		u.DisabledDate = now

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			b.globalAliases.Delete(g.Email)
		}
		g.Email = ""
		g.State = stateDisabled
		g.DisabledDate = now

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			b.globalAliases.Delete(r.Email)
		}
		r.Email = ""
		r.State = stateDisabled
		r.DisabledDate = now

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// ResetPassword updates the user's password (simulated — no-op).
func (b *InMemoryBackend) ResetPassword(orgID, userID, _ string) error {
	b.mu.RLock("ResetPassword")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, userID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, userID)
	}

	return nil
}

// UpdatePrimaryEmailAddress updates the primary email of an entity.
func (b *InMemoryBackend) UpdatePrimaryEmailAddress(orgID, entityID, email string) error {
	b.mu.Lock("UpdatePrimaryEmailAddress")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			b.globalAliases.Delete(u.Email)
		}
		u.Email = email
		b.usersByEmail[orgID][email] = u.UserID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: u.UserID})

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			b.globalAliases.Delete(g.Email)
		}
		g.Email = email
		b.groupsByEmail[orgID][email] = g.GroupID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: g.GroupID})

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			b.globalAliases.Delete(r.Email)
		}
		r.Email = email
		b.resourcesByEmail[orgID][email] = r.ResourceID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: r.ResourceID})

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}
