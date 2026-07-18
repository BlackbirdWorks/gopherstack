package workmail

import (
	"fmt"
)

// --- DescribeEntity ---

// DescribeEntity describes an entity looked up by its primary email address
// (DescribeEntityInput.Email is the only lookup field the real API accepts --
// see aws-sdk-go-v2/service/workmail/api_op_DescribeEntity.go). The plain
// findUser/findGroup/findResource ID-or-name lookup never matches an email
// (they only check the composite-key ID and the Name field), so email lookup
// is checked first via the byEmail reverse-index maps; ID/name is kept as a
// fallback for backward compatibility with any caller that already has the
// entity's own ID.
func (b *InMemoryBackend) DescribeEntity(orgID, identifier string) (*EntityDescription, error) {
	b.mu.RLock("DescribeEntity")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if desc := b.describeEntityByEmail(orgID, identifier); desc != nil {
		return desc, nil
	}

	if u := b.findUser(orgID, identifier); u != nil {
		return &EntityDescription{EntityID: u.UserID, Name: u.Name, Type: "USER", State: u.State}, nil
	}
	if g := b.findGroup(orgID, identifier); g != nil {
		return &EntityDescription{EntityID: g.GroupID, Name: g.Name, Type: "GROUP", State: g.State}, nil
	}
	if r := b.findResource(orgID, identifier); r != nil {
		return &EntityDescription{EntityID: r.ResourceID, Name: r.Name, Type: "RESOURCE", State: r.State}, nil
	}

	return nil, fmt.Errorf("%w: entity %q not found", ErrNotFound, identifier)
}

// describeEntityByEmail resolves email against the byEmail reverse-index
// maps. Callers must hold at least b.mu.RLock. Returns nil (not an error)
// when email doesn't match any entity, so DescribeEntity can fall through to
// its ID/name lookup.
func (b *InMemoryBackend) describeEntityByEmail(orgID, email string) *EntityDescription {
	if userID, found := b.usersByEmail[orgID][email]; found {
		if u, exists := b.users.Get(orgKey(orgID, userID)); exists {
			return &EntityDescription{EntityID: u.UserID, Name: u.Name, Type: "USER", State: u.State}
		}
	}
	if groupID, found := b.groupsByEmail[orgID][email]; found {
		if g, exists := b.groups.Get(orgKey(orgID, groupID)); exists {
			return &EntityDescription{EntityID: g.GroupID, Name: g.Name, Type: "GROUP", State: g.State}
		}
	}
	if resourceID, found := b.resourcesByEmail[orgID][email]; found {
		if r, exists := b.resources.Get(orgKey(orgID, resourceID)); exists {
			return &EntityDescription{EntityID: r.ResourceID, Name: r.Name, Type: "RESOURCE", State: r.State}
		}
	}

	return nil
}
