package workmail

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// --- Impersonation Roles ---

// CreateImpersonationRole creates a new impersonation role.
func (b *InMemoryBackend) CreateImpersonationRole(
	orgID, name, roleType, description string,
	rules []ImpersonationRule,
) (*ImpersonationRole, error) {
	b.mu.Lock("CreateImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	for _, r := range b.impersonationByOrg.Get(orgID) {
		if r.Name == name {
			return nil, fmt.Errorf("%w: impersonation role %q already exists", ErrConflict, name)
		}
	}

	roleID := newID()
	now := time.Now().UTC()

	role := &ImpersonationRole{
		DateCreated:  now,
		DateModified: now,
		RoleID:       roleID,
		Name:         name,
		RoleType:     roleType,
		Description:  description,
		Rules:        rules,
		orgID:        orgID,
	}

	b.impersonation.Put(role)

	return role, nil
}

// GetImpersonationRole returns an impersonation role.
func (b *InMemoryBackend) GetImpersonationRole(orgID, roleID string) (*ImpersonationRole, error) {
	b.mu.RLock("GetImpersonationRole")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}

	role, ok := b.impersonation.Get(orgKey(orgID, roleID))
	if !ok {
		return nil, fmt.Errorf("%w: impersonation role %q not found", ErrResourceNotFound, roleID)
	}

	return role, nil
}

// UpdateImpersonationRole updates an impersonation role.
func (b *InMemoryBackend) UpdateImpersonationRole(
	orgID, roleID, name, roleType, description string,
	rules []ImpersonationRule,
) error {
	b.mu.Lock("UpdateImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	role, ok := b.impersonation.Get(orgKey(orgID, roleID))
	if !ok {
		return fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}

	if name != "" {
		role.Name = name
	}
	if roleType != "" {
		role.RoleType = roleType
	}
	if description != "" {
		role.Description = description
	}
	if rules != nil {
		role.Rules = rules
	}
	role.DateModified = time.Now().UTC()

	return nil
}

// DeleteImpersonationRole removes an impersonation role.
func (b *InMemoryBackend) DeleteImpersonationRole(orgID, roleID string) error {
	b.mu.Lock("DeleteImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	if !b.impersonation.Delete(orgKey(orgID, roleID)) {
		// DeleteImpersonationRole's own error model declares no not-found type
		// for the role itself (only Organization*); no correct code exists to
		// send here (gopherstack-6flj/uox6 error-envelope sweep).
		return fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}

	return nil
}

// ListImpersonationRoles returns impersonation roles.
func (b *InMemoryBackend) ListImpersonationRoles(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*ImpersonationRole, string, error) {
	b.mu.RLock("ListImpersonationRoles")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}

	byOrg := b.impersonationByOrg.Get(orgID)
	roles := make([]*ImpersonationRole, 0, len(byOrg))
	roles = append(roles, byOrg...)
	sort.Slice(roles, func(i, j int) bool { return roles[i].Name < roles[j].Name })

	items, next := paginate(roles, maxResults, nextToken)

	return items, next, nil
}

// --- Impersonation Role Effect ---

// GetImpersonationRoleEffect evaluates impersonation rules for a target user.
func (b *InMemoryBackend) GetImpersonationRoleEffect(
	orgID, roleID, targetUser string,
) (string, string, []*ImpersonationMatchedRule, error) {
	b.mu.RLock("GetImpersonationRoleEffect")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return "", "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	role, ok := b.impersonation.Get(orgKey(orgID, roleID))
	if !ok {
		return "", "", nil, fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}
	effect := "DENY"
	matched := []*ImpersonationMatchedRule{}
	for _, rule := range role.Rules {
		inTarget := slices.Contains(rule.TargetUsers, targetUser)
		inNotTarget := slices.Contains(rule.NotTargetUsers, targetUser)
		if len(rule.TargetUsers) > 0 && !inTarget {
			continue
		}
		if inNotTarget {
			continue
		}
		effect = rule.Effect
		matched = append(matched, &ImpersonationMatchedRule{RuleID: rule.RuleID, Name: rule.Name})
	}

	return effect, role.RoleType, matched, nil
}

// --- Assume Impersonation Role ---

// AssumeImpersonationRole issues a validated token for an impersonation role.
func (b *InMemoryBackend) AssumeImpersonationRole(orgID, roleID string) (string, int64, error) {
	b.mu.Lock("AssumeImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return "", 0, fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	if !b.impersonation.Has(orgKey(orgID, roleID)) {
		return "", 0, fmt.Errorf("%w: impersonation role %q not found", ErrResourceNotFound, roleID)
	}

	const ttl = int64(3600)
	token := newID()
	b.issuedTokens.Put(&issuedImpersonationToken{
		Token:     token,
		OrgID:     orgID,
		RoleID:    roleID,
		ExpiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
	})

	return token, ttl, nil
}

// paginate returns a page of items and a next token using index-based paging.
