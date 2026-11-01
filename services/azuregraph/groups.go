package azuregraph

import (
	"encoding/json"
	"slices"
	"strings"
)

const groupTypeUnified = "Unified"

func (b *Backend) findGroup(r Ref) (*Group, error) {
	if r.Field != "" {
		return nil, badRequest("The alternate key '%s' is not supported on groups.", r.Field)
	}

	if !isGUID(r.ID) {
		return nil, badRequest("Invalid object identifier '%s'.", r.ID)
	}

	if g, ok := b.groups.get(lc(r.ID)); ok {
		return g, nil
	}

	return nil, notFound(r.ID)
}

func validateGroup(g *Group) error {
	if strings.TrimSpace(g.DisplayName) == "" {
		return badRequest("A value is required for property 'displayName' of resource 'Group'.")
	}

	unified := slices.Contains(g.GroupTypes, groupTypeUnified)

	for _, t := range g.GroupTypes {
		if t != groupTypeUnified && t != "DynamicMembership" {
			return badRequest("Invalid value specified for property 'groupTypes' of resource 'Group'.")
		}
	}

	if !g.SecurityEnabled && !unified && !g.MailEnabled {
		return badRequest("A group must be security enabled or mail enabled.")
	}

	if g.MailEnabled && !unified {
		return badRequest(
			"Mail-enabled groups other than Microsoft 365 groups cannot be created through Microsoft Graph.",
		)
	}

	if g.IsAssignableToRole != nil && *g.IsAssignableToRole && !g.SecurityEnabled {
		return badRequest("Role-assignable groups must be security enabled.")
	}

	return nil
}

// CreateGroup creates a group; members@odata.bind and owners@odata.bind add references atomically.
func (b *Backend) CreateGroup(body map[string]json.RawMessage) (Group, error) {
	if err := requireProps("Group", body, "displayName", "mailEnabled", "mailNickname", "securityEnabled"); err != nil {
		return Group{}, err
	}

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	seed := &Group{ID: newID(), CreatedDateTime: b.stamp()}
	seed.normalize()

	g, err := applyBody(seed, body, groupReadOnly()...)
	if err != nil {
		return Group{}, err
	}

	if err = validateGroup(g); err != nil {
		return Group{}, err
	}

	owners, err := b.ownerBinds(body)
	if err != nil {
		return Group{}, err
	}

	members, err := b.bindTargets(body["members@odata.bind"])
	if err != nil {
		return Group{}, err
	}

	if slices.Contains(g.GroupTypes, groupTypeUnified) {
		g.Mail = lc(g.MailNickname) + "@" + b.domains[0].Name
		g.Visibility = orDefault(g.Visibility, "Public")
		g.ResourceProvisioningOptions = nonNil(g.ResourceProvisioningOptions)
	}

	b.groups.put(g.ID, g)
	b.owners[g.ID], b.members[g.ID] = owners, members

	return *g, nil
}

// GetGroup returns a group.
func (b *Backend) GetGroup(r Ref) (Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, err := b.findGroup(r)
	if err != nil {
		return Group{}, err
	}

	return *g, nil
}

// ListGroups returns every group in creation order.
func (b *Backend) ListGroups() []Group {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	return b.groups.slice()
}

// UpdateGroup applies a PATCH body. isAssignableToRole can only be set at creation.
func (b *Backend) UpdateGroup(r Ref, body map[string]json.RawMessage) error {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	cur, err := b.findGroup(r)
	if err != nil {
		return err
	}

	next, err := applyBody(cur, body, append([]string{"isAssignableToRole"}, groupReadOnly()...)...)
	if err != nil {
		return err
	}

	if err = validateGroup(next); err != nil {
		return err
	}

	b.groups.put(cur.ID, next)

	return nil
}

// DeleteGroup removes a group.
func (b *Backend) DeleteGroup(r Ref) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	g, err := b.findGroup(r)
	if err != nil {
		return err
	}

	b.removeObject(g.ID)

	return nil
}
