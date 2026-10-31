package azuregraph

import (
	"encoding/json"
	"slices"
	"strings"
)

// Object is a directory object of any kind with its entity (Application, ServicePrincipal, User or Group).
type Object struct {
	Entity any
	Kind   string
}

func (b *Backend) object(id string) (Object, bool) {
	o, ok := b.lookup(id)
	if !ok {
		return Object{}, false
	}

	switch e := o.(type) {
	case *Application:
		return Object{Kind: kindApplication, Entity: *e}, true
	case *ServicePrincipal:
		return Object{Kind: kindServicePrincipal, Entity: *e}, true
	case *User:
		return Object{Kind: kindUser, Entity: *e}, true
	case *Group:
		return Object{Kind: kindGroup, Entity: *e}, true
	}

	return Object{}, false
}

func (b *Backend) objects(ids []string) []Object {
	out := make([]Object, 0, len(ids))

	for _, id := range ids {
		if o, ok := b.object(id); ok {
			out = append(out, o)
		}
	}

	return out
}

// idFromRef extracts the object ID from an @odata.id URL such as https://graph.microsoft.com/v1.0/users/{id}.
func idFromRef(s string) string {
	s = strings.TrimSuffix(strings.TrimSpace(s), "/")
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		s = s[i+1:]
	}

	return lc(s)
}

// bindTargets resolves a list of @odata.bind references, failing if any target does not exist.
func (b *Backend) bindTargets(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return []string{}, nil
	}

	var refs []string
	if err := json.Unmarshal(raw, &refs); err != nil {
		return nil, badRequest("Invalid value specified for an @odata.bind property.")
	}

	out := make([]string, 0, len(refs))

	for _, r := range refs {
		id := idFromRef(r)
		if _, ok := b.lookup(id); !ok {
			return nil, notFound(id)
		}

		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}

	return out, nil
}

func ownable(kind string) bool {
	return kind == kindApplication || kind == kindServicePrincipal || kind == kindGroup
}

func (b *Backend) edit(op, ownerID, refID, relation string, relations map[string][]string, add bool) error {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	parent, ok := b.lookup(ownerID)
	if !ok {
		return notFound(ownerID)
	}

	ownerID, refID = lc(ownerID), lc(refID)

	child, ok := b.lookup(refID)
	if !ok {
		return notFound(refID)
	}

	if relation == "owners" && (!ownable(parent.objKind()) || (child.objKind() != kindUser && child.objKind() != kindServicePrincipal)) {
		return badRequest("Invalid object type for the owners relationship.")
	}

	if relation == "members" && (parent.objKind() != kindGroup || ownerID == refID) {
		return badRequest("Invalid object type for the members relationship.")
	}

	cur := relations[ownerID]
	if add {
		if slices.Contains(cur, refID) {
			return badRequest("One or more added object references already exist for the following modified properties: '%s'.", relation)
		}

		relations[ownerID] = append(slices.Clone(cur), refID)

		return nil
	}

	if !slices.Contains(cur, refID) {
		return notFound(refID)
	}

	relations[ownerID] = without(cur, refID)

	return nil
}

// AddOwner adds an owner reference.
func (b *Backend) AddOwner(id, ownerID string) error {
	return b.edit("AddOwner", id, ownerID, "owners", b.owners, true)
}

// RemoveOwner removes an owner reference.
func (b *Backend) RemoveOwner(id, ownerID string) error {
	return b.edit("RemoveOwner", id, ownerID, "owners", b.owners, false)
}

// AddMember adds a group member reference.
func (b *Backend) AddMember(groupID, memberID string) error {
	return b.edit("AddMember", groupID, memberID, "members", b.members, true)
}

// RemoveMember removes a group member reference.
func (b *Backend) RemoveMember(groupID, memberID string) error {
	return b.edit("RemoveMember", groupID, memberID, "members", b.members, false)
}

// Owners lists the owners of an application, service principal or group.
func (b *Backend) Owners(id string) ([]Object, error) {
	b.mu.RLock("Owners")
	defer b.mu.RUnlock()

	o, ok := b.lookup(id)
	if !ok || !ownable(o.objKind()) {
		return nil, notFound(id)
	}

	return b.objects(b.owners[lc(id)]), nil
}

// Members lists the direct members of a group.
func (b *Backend) Members(id string) ([]Object, error) {
	b.mu.RLock("Members")
	defer b.mu.RUnlock()

	if _, ok := b.groups.get(lc(id)); !ok {
		return nil, notFound(id)
	}

	return b.objects(b.members[lc(id)]), nil
}

// TransitiveMembers lists a group's members including those of nested groups.
func (b *Backend) TransitiveMembers(id string) ([]Object, error) {
	b.mu.RLock("TransitiveMembers")
	defer b.mu.RUnlock()

	if _, ok := b.groups.get(lc(id)); !ok {
		return nil, notFound(id)
	}

	seen := map[string]bool{lc(id): true}

	var out []string

	var walk func(string)

	walk = func(g string) {
		for _, m := range b.members[g] {
			if seen[m] {
				continue
			}

			seen[m] = true
			out = append(out, m)

			if _, isGroup := b.groups.get(m); isGroup {
				walk(m)
			}
		}
	}

	walk(lc(id))

	return b.objects(out), nil
}

// MemberOf lists the groups id belongs to, directly or (transitive) through nesting.
func (b *Backend) MemberOf(id string, transitive bool) ([]Object, error) {
	b.mu.RLock("MemberOf")
	defer b.mu.RUnlock()

	if _, ok := b.lookup(id); !ok {
		return nil, notFound(id)
	}

	seen := map[string]bool{}
	frontier := []string{lc(id)}

	var out []string

	for len(frontier) > 0 {
		cur := frontier[0]
		frontier = frontier[1:]

		for _, g := range b.groups.order {
			if seen[g] || !slices.Contains(b.members[g], cur) {
				continue
			}

			seen[g] = true
			out = append(out, g)

			if transitive {
				frontier = append(frontier, g)
			}
		}
	}

	return b.objects(out), nil
}

// ByIDs resolves object IDs, optionally restricted to kinds ("user", "group", ... or "directoryObject").
func (b *Backend) ByIDs(ids, types []string) ([]Object, error) {
	for _, id := range ids {
		if !isGUID(id) {
			return nil, badRequest("Invalid object identifier '%s'.", id)
		}
	}

	b.mu.RLock("ByIDs")
	defer b.mu.RUnlock()

	var out []Object

	for _, o := range b.objects(ids) {
		if len(types) == 0 || slices.ContainsFunc(types, func(t string) bool {
			return strings.EqualFold(t, o.Kind) || strings.EqualFold(t, "directoryObject")
		}) {
			out = append(out, o)
		}
	}

	return out, nil
}

// ObjectKind reports whether id names a directory object and its kind.
func (b *Backend) ObjectKind(id string) (string, bool) {
	b.mu.RLock("ObjectKind")
	defer b.mu.RUnlock()

	o, ok := b.lookup(id)
	if !ok {
		return "", false
	}

	return o.objKind(), true
}

// GetObject returns any directory object by ID.
func (b *Backend) GetObject(id string) (Object, error) {
	if !isGUID(id) {
		return Object{}, badRequest("Invalid object identifier '%s'.", id)
	}

	b.mu.RLock("GetObject")
	defer b.mu.RUnlock()

	if o, ok := b.object(id); ok {
		return o, nil
	}

	return Object{}, notFound(id)
}

// PrincipalType reports whether id is a user, group or service principal and returns its role-assignment
// principal type (User, Group or ServicePrincipal).
func (b *Backend) PrincipalType(id string) (string, bool) {
	b.mu.RLock("PrincipalType")
	defer b.mu.RUnlock()

	o, ok := b.lookup(id)
	if !ok || o.objKind() == kindApplication {
		return "", false
	}

	return principalType(o.objKind()), true
}
