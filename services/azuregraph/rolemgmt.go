package azuregraph

import (
	"encoding/json/jsontext"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

// RoleAssignment is a unifiedRoleAssignment of the Entra directory role management API.
type RoleAssignment struct {
	AppScopeID       *string `json:"appScopeId"`
	Condition        *string `json:"condition"`
	ID               string  `json:"id"`
	PrincipalID      string  `json:"principalId"`
	RoleDefinitionID string  `json:"roleDefinitionId"`
	DirectoryScopeID string  `json:"directoryScopeId"`
}

func (*RoleAssignment) normalize() {}

type builtinRole struct {
	id, name, description string
	actions               []string
}

// builtinRoles is a subset of the Entra built-in roles; the template IDs are recalled from the Learn "Microsoft
// Entra built-in roles" page and rolePermissions carries representative actions only.
func builtinRoles() []builtinRole {
	return []builtinRole{
		{"62e90394-69f5-4237-9190-012177145e10", "Global Administrator",
			"Can manage all aspects of Microsoft Entra ID and Microsoft services that use Microsoft Entra identities.",
			[]string{"microsoft.directory/*"}},
		{"9b895d92-2cd3-44c7-9d02-a6ac2d5ea5c3", "Application Administrator",
			"Can create and manage all aspects of app registrations and enterprise apps.",
			[]string{
				"microsoft.directory/applications/allProperties/allTasks",
				"microsoft.directory/servicePrincipals/allProperties/allTasks",
			}},
		{"cf1c38e5-3621-4004-a7cb-879624dced7c", "Application Developer",
			"Can create application registrations independent of the 'Users can register applications' setting.",
			[]string{"microsoft.directory/applications/createAsOwner"}},
		{"158c047a-c907-4556-b7ef-446551a6b5f7", "Cloud Application Administrator",
			"Can create and manage all aspects of app registrations and enterprise apps except App Proxy.",
			[]string{"microsoft.directory/applications/allProperties/allTasks"}},
		{"fe930be7-5e62-47db-91af-98c3a49a38b1", "User Administrator",
			"Can manage all aspects of users and groups, including resetting passwords for limited admins.",
			[]string{"microsoft.directory/users/allProperties/allTasks", "microsoft.directory/groups/create"}},
		{"fdd7a751-b60b-444a-984c-02652fe8fa1c", "Groups Administrator",
			"Members of this role can create/manage groups, create/manage groups settings like naming and expiration " +
				"policies, and view groups activity and audit reports.",
			[]string{"microsoft.directory/groups/allProperties/allTasks"}},
		{
			"88d8e3e3-8f55-4a1e-953a-9b9898b8876b",
			"Directory Readers",
			"Can read basic directory information. Commonly used to grant directory read access to applications and guests.",
			[]string{"microsoft.directory/users/standard/read", "microsoft.directory/groups/standard/read"},
		},
		{"f2ef992c-3afb-46b9-b7cf-a126ee74c451", "Global Reader",
			"Can read everything that a Global Administrator can, but not update anything.",
			[]string{"microsoft.directory/*/read"}},
		{"194ae4cb-b126-40b2-bd5b-6091b380977d", "Security Administrator",
			"Can read security information and reports, and manage configuration in Microsoft Entra ID and Office 365.",
			[]string{"microsoft.directory/conditionalAccessPolicies/standard/read"}},
		{"e8611ab8-c189-46e8-94e1-60213ab1f814", "Privileged Role Administrator",
			"Can manage role assignments in Microsoft Entra ID, and all aspects of Privileged Identity Management.",
			[]string{"microsoft.directory/roleAssignments/allProperties/allTasks"}},
	}
}

func roleDefinitionMaps() []map[string]any {
	roles := builtinRoles()
	out := make([]map[string]any, 0, len(roles))

	for _, r := range roles {
		out = append(out, map[string]any{
			"id": r.id, "templateId": r.id, propDisplayName: r.name, propDescription: r.description,
			"isBuiltIn": true, "isEnabled": true, "resourceScopes": []string{"/"}, "version": "1",
			"rolePermissions": []map[string]any{{"allowedResourceActions": r.actions}},
		})
	}

	return out
}

func roleDefinitionExists(id string) bool {
	return slices.ContainsFunc(builtinRoles(), func(r builtinRole) bool { return strings.EqualFold(r.id, id) })
}

func roleAssignReadOnly() []string { return []string{"id"} }

// CreateRoleAssignment assigns a built-in directory role to a user, role-assignable group or service principal.
func (b *Backend) CreateRoleAssignment(body map[string]jsontext.Value) (RoleAssignment, error) {
	b.mu.Lock("CreateRoleAssignment")
	defer b.mu.Unlock()

	if err := requireProps("UnifiedRoleAssignment", body, "principalId", "roleDefinitionId"); err != nil {
		return RoleAssignment{}, err
	}

	a, err := applyBody(&RoleAssignment{ID: newID()}, body, roleAssignReadOnly()...)
	if err != nil {
		return RoleAssignment{}, err
	}

	if a.DirectoryScopeID == "" && a.AppScopeID == nil {
		return RoleAssignment{}, badRequest(
			"A value is required for property 'directoryScopeId' of resource 'UnifiedRoleAssignment'.")
	}

	if err = b.validateRoleAssignment(a); err != nil {
		return RoleAssignment{}, err
	}

	a.PrincipalID, a.RoleDefinitionID = lc(a.PrincipalID), lc(a.RoleDefinitionID)
	b.roleAssigns.put(lc(a.ID), a)
	b.activateRole(a.RoleDefinitionID)

	return *a, nil
}

func (b *Backend) validateRoleAssignment(a *RoleAssignment) error {
	if !roleDefinitionExists(a.RoleDefinitionID) {
		return badRequest("The roleDefinitionId '%s' does not reference a valid role definition.", a.RoleDefinitionID)
	}

	p, ok := b.lookup(a.PrincipalID)
	if !ok || p.objKind() == kindApplication {
		return badRequest("The principalId '%s' does not reference a valid principal.", a.PrincipalID)
	}

	if g, isGroup := p.(*Group); isGroup && (g.IsAssignableToRole == nil || !*g.IsAssignableToRole) {
		return badRequest("The group '%s' is not role-assignable (isAssignableToRole must be true).", a.PrincipalID)
	}

	if err := b.validateRoleScope(a.DirectoryScopeID); err != nil {
		return err
	}

	for _, o := range b.roleAssigns.all() {
		if lc(o.PrincipalID) == lc(a.PrincipalID) && lc(o.RoleDefinitionID) == lc(a.RoleDefinitionID) &&
			o.DirectoryScopeID == a.DirectoryScopeID {
			return conflict("The Role assignment already exists.")
		}
	}

	return nil
}

func (b *Backend) validateRoleScope(scope string) error {
	if scope == "" || scope == "/" {
		return nil
	}

	target, found := b.lookup(strings.TrimPrefix(scope, "/"))
	if !found || (target.objKind() != kindApplication && target.objKind() != kindServicePrincipal) {
		return badRequest("The directoryScopeId '%s' does not reference a valid scope.", scope)
	}

	return nil
}

// ListRoleAssignments returns every directory role assignment.
func (b *Backend) ListRoleAssignments() []RoleAssignment {
	b.mu.RLock("ListRoleAssignments")
	defer b.mu.RUnlock()

	return b.roleAssigns.slice()
}

// GetRoleAssignment returns one role assignment.
func (b *Backend) GetRoleAssignment(id string) (RoleAssignment, error) {
	b.mu.RLock("GetRoleAssignment")
	defer b.mu.RUnlock()

	if a, ok := b.roleAssigns.get(lc(id)); ok {
		return *a, nil
	}

	return RoleAssignment{}, notFound(id)
}

// DeleteRoleAssignment removes a role assignment.
func (b *Backend) DeleteRoleAssignment(id string) error {
	b.mu.Lock("DeleteRoleAssignment")
	defer b.mu.Unlock()

	if _, ok := b.roleAssigns.get(lc(id)); !ok {
		return notFound(id)
	}

	b.roleAssigns.del(lc(id))

	return nil
}

// handleRoleManagement serves /roleManagement/directory/{roleDefinitions,roleAssignments}.
func (h *Handler) handleRoleManagement(c *echo.Context, segs []string) error {
	if len(segs) < pathWithID+1 || !strings.EqualFold(segs[1], "directory") {
		return h.writeErr(c, segmentErr(segs[len(segs)-1]))
	}

	ctx := "roleManagement/directory/" + segs[2]

	switch strings.ToLower(segs[2]) {
	case "roledefinitions":
		return h.roleDefinitions(c, ctx, segs[3:])
	case "roleassignments":
		return h.roleAssignments(c, ctx, segs[3:])
	}

	return h.writeErr(c, segmentErr(segs[2]))
}

func (h *Handler) roleDefinitions(c *echo.Context, ctx string, tail []string) error {
	if c.Request().Method != http.MethodGet {
		return h.methodNotAllowed(c)
	}

	defs := roleDefinitionMaps()

	switch len(tail) {
	case 0:
		return h.list(c, ctx, "UnifiedRoleDefinition", nil, defs)
	case 1:
		for _, d := range defs {
			if id, _ := d["id"].(string); strings.EqualFold(id, tail[0]) {
				return h.entity(c, http.StatusOK, ctx, d)
			}
		}

		return h.writeErr(c, notFound(tail[0]))
	}

	return h.writeErr(c, segmentErr(strings.Join(tail, "/")))
}

func (h *Handler) roleAssignments(c *echo.Context, ctx string, tail []string) error {
	r, b := c.Request(), h.Backend

	switch {
	case len(tail) == 0 && r.Method == http.MethodGet:
		return h.list(c, ctx, "UnifiedRoleAssignment", fieldsOf[RoleAssignment](), mapAll(b.ListRoleAssignments()))
	case len(tail) == 0 && r.Method == http.MethodPost:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		a, err := b.CreateRoleAssignment(body)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusCreated, ctx, toMap(a))
	case len(tail) == 1 && r.Method == http.MethodGet:
		a, err := b.GetRoleAssignment(tail[0])
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusOK, ctx, toMap(a))
	case len(tail) == 1 && r.Method == http.MethodDelete:
		return h.noContent(c, b.DeleteRoleAssignment(tail[0]))
	}

	return h.methodNotAllowed(c)
}
