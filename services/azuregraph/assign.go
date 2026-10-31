package azuregraph

import (
	"encoding/json"
	"slices"
	"strings"
)

const defaultAccessRole = "00000000-0000-0000-0000-000000000000"

func principalType(kind string) string {
	switch kind {
	case kindUser:
		return typeUser
	case kindGroup:
		return "Group"
	case kindServicePrincipal:
		return "ServicePrincipal"
	}

	return ""
}

// CreateAppRoleAssignment grants an app role. ownerID is the path service principal, user or group;
// asResource selects appRoleAssignedTo (owner is the resource) over appRoleAssignments (owner is the principal).
func (b *Backend) CreateAppRoleAssignment(
	ownerID string, asResource bool, body map[string]json.RawMessage,
) (AppRoleAssignment, error) {
	if err := requireProps("AppRoleAssignment", body, "principalId", "resourceId", "appRoleId"); err != nil {
		return AppRoleAssignment{}, err
	}

	b.mu.Lock("CreateAppRoleAssignment")
	defer b.mu.Unlock()

	in, err := applyBody(&AppRoleAssignment{}, body, assignReadOnly()...)
	if err != nil {
		return AppRoleAssignment{}, err
	}

	pathID := lc(ownerID)
	if (asResource && lc(in.ResourceID) != pathID) || (!asResource && lc(in.PrincipalID) != pathID) {
		return AppRoleAssignment{}, badRequest("The identifier in the request path does not match the request body.")
	}

	p, ok := b.lookup(in.PrincipalID)
	if !ok || p.objKind() == kindApplication {
		return AppRoleAssignment{}, notFound(in.PrincipalID)
	}

	res, ok := b.sps.get(lc(in.ResourceID))
	if !ok {
		return AppRoleAssignment{}, notFound(in.ResourceID)
	}

	if err = checkRole(res, p.objKind(), in.AppRoleID); err != nil {
		return AppRoleAssignment{}, err
	}

	for _, a := range b.assigns.all() {
		if lc(a.PrincipalID) == lc(in.PrincipalID) && lc(a.ResourceID) == lc(in.ResourceID) &&
			lc(a.AppRoleID) == lc(in.AppRoleID) {
			return AppRoleAssignment{}, badRequest("Permission being assigned already exists on the object.")
		}
	}

	in.ID, in.CreatedDateTime = newID(), b.stamp()
	in.PrincipalID, in.ResourceID, in.AppRoleID = lc(in.PrincipalID), lc(in.ResourceID), lc(in.AppRoleID)
	in.PrincipalType, in.PrincipalDisplayName = principalType(p.objKind()), p.objName()
	in.ResourceDisplayName = res.DisplayName
	b.assigns.put(in.ID, in)

	return *in, nil
}

func checkRole(res *ServicePrincipal, principalKind, roleID string) error {
	if lc(roleID) == defaultAccessRole {
		if principalKind == kindServicePrincipal {
			return badRequest("Invalid appRoleId '%s' for a service principal assignment.", roleID)
		}

		return nil
	}

	i := slices.IndexFunc(res.AppRoles, func(r AppRole) bool { return strings.EqualFold(r.ID, roleID) && r.IsEnabled })
	if i < 0 {
		return badRequest("Permission being assigned was not found on application '%s'.", res.AppID)
	}

	want := typeUser
	if principalKind == kindServicePrincipal {
		want = typeApplication
	}

	if !slices.Contains(res.AppRoles[i].AllowedMemberTypes, want) {
		return badRequest("The app role '%s' does not allow members of type '%s'.", roleID, want)
	}

	return nil
}

// ListAppRoleAssignments lists assignments where id is the principal, or (asResource) the resource.
func (b *Backend) ListAppRoleAssignments(id string, asResource bool) ([]AppRoleAssignment, error) {
	b.mu.RLock("ListAppRoleAssignments")
	defer b.mu.RUnlock()

	if _, ok := b.lookup(id); !ok {
		return nil, notFound(id)
	}

	out := []AppRoleAssignment{}

	for _, a := range b.assigns.all() {
		if (asResource && lc(a.ResourceID) == lc(id)) || (!asResource && lc(a.PrincipalID) == lc(id)) {
			out = append(out, *a)
		}
	}

	return out, nil
}

// DeleteAppRoleAssignment removes an assignment owned by id.
func (b *Backend) DeleteAppRoleAssignment(id, assignmentID string, asResource bool) error {
	b.mu.Lock("DeleteAppRoleAssignment")
	defer b.mu.Unlock()

	a, ok := b.assigns.get(lc(assignmentID))
	if !ok || (asResource && lc(a.ResourceID) != lc(id)) || (!asResource && lc(a.PrincipalID) != lc(id)) {
		return notFound(assignmentID)
	}

	b.assigns.del(lc(a.ID))

	return nil
}

// CreateGrant records a delegated permission grant.
func (b *Backend) CreateGrant(body map[string]json.RawMessage) (OAuth2PermissionGrant, error) {
	if err := requireProps("OAuth2PermissionGrant", body, "clientId", "consentType", "resourceId"); err != nil {
		return OAuth2PermissionGrant{}, err
	}

	b.mu.Lock("CreateGrant")
	defer b.mu.Unlock()

	g, err := applyBody(&OAuth2PermissionGrant{}, body, grantReadOnly()...)
	if err != nil {
		return OAuth2PermissionGrant{}, err
	}

	if g.ConsentType != "AllPrincipals" && g.ConsentType != "Principal" {
		return OAuth2PermissionGrant{}, badRequest(
			"Invalid value specified for property 'consentType' of resource 'OAuth2PermissionGrant'.",
		)
	}

	if g.ConsentType == "Principal" && g.PrincipalID == "" {
		return OAuth2PermissionGrant{}, badRequest(
			"A value is required for property 'principalId' of resource 'OAuth2PermissionGrant'.",
		)
	}

	for _, id := range []string{g.ClientID, g.ResourceID} {
		if _, ok := b.sps.get(lc(id)); !ok {
			return OAuth2PermissionGrant{}, notFound(id)
		}
	}

	g.ID = newID()
	b.grants.put(g.ID, g)

	return *g, nil
}

// ListGrants returns every delegated permission grant.
func (b *Backend) ListGrants() []OAuth2PermissionGrant {
	b.mu.RLock("ListGrants")
	defer b.mu.RUnlock()

	return b.grants.slice()
}

// UpdateGrant changes the scope of a grant.
func (b *Backend) UpdateGrant(id string, body map[string]json.RawMessage) error {
	b.mu.Lock("UpdateGrant")
	defer b.mu.Unlock()

	cur, ok := b.grants.get(lc(id))
	if !ok {
		return notFound(id)
	}

	next, err := applyBody(cur, body, "id", "clientId", "consentType", "principalId", "resourceId")
	if err != nil {
		return err
	}

	b.grants.put(cur.ID, next)

	return nil
}

// DeleteGrant removes a grant.
func (b *Backend) DeleteGrant(id string) error {
	b.mu.Lock("DeleteGrant")
	defer b.mu.Unlock()

	if _, ok := b.grants.get(lc(id)); !ok {
		return notFound(id)
	}

	b.grants.del(lc(id))

	return nil
}
