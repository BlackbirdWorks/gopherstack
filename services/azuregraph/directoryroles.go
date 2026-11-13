package azuregraph

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	collDirectoryRoles         = "directoryRoles"
	collDirectoryRoleTemplates = "directoryRoleTemplates"
	keyRoleTemplateID          = "roleTemplateId"
	rootScope                  = "/"
)

func (r builtinRole) templateMap() map[string]any {
	return map[string]any{"id": r.id, "deletedDateTime": nil, propDisplayName: r.name, propDescription: r.description}
}

func findTemplate(id string) (builtinRole, bool) {
	for _, r := range builtinRoles() {
		if strings.EqualFold(r.id, id) {
			return r, true
		}
	}

	return builtinRole{}, false
}

func (b *Backend) roleMap(templateID, id string) map[string]any {
	t, _ := findTemplate(templateID)

	return map[string]any{
		"id": id, "deletedDateTime": nil, propDescription: t.description, propDisplayName: t.name,
		keyRoleTemplateID: t.id,
	}
}

// activateRole records the directoryRole for a template and returns its ID. Callers hold b.mu.
func (b *Backend) activateRole(templateID string) string {
	templateID = lc(templateID)
	if id, ok := b.dirRoles[templateID]; ok {
		return id
	}

	id := newID()
	b.dirRoles[templateID] = id

	return id
}

// ActivateDirectoryRole activates a directory role from a built-in template.
func (b *Backend) ActivateDirectoryRole(body map[string]jsontext.Value) (map[string]any, error) {
	if err := requireProps("DirectoryRole", body, keyRoleTemplateID); err != nil {
		return nil, err
	}

	for k := range body {
		if k != keyRoleTemplateID && !strings.Contains(k, "@") {
			return nil, badRequest("Property '%s' cannot be specified when activating a directory role.", k)
		}
	}

	var tid string
	if err := json.Unmarshal(body[keyRoleTemplateID], &tid, jsonx.V1()); err != nil {
		return nil, badRequest("Invalid value specified for property 'roleTemplateId' of resource 'DirectoryRole'.")
	}

	b.mu.Lock("ActivateDirectoryRole")
	defer b.mu.Unlock()

	t, ok := findTemplate(tid)
	if !ok {
		return nil, badRequest("The role template '%s' does not exist.", tid)
	}

	if _, dup := b.dirRoles[lc(t.id)]; dup {
		return nil, badRequest("A conflicting object with one or more of the specified property values is " +
			"present in the directory.")
	}

	return b.roleMap(t.id, b.activateRole(t.id)), nil
}

// DirectoryRoles lists the activated directory roles.
func (b *Backend) DirectoryRoles() []map[string]any {
	b.mu.RLock("DirectoryRoles")
	defer b.mu.RUnlock()

	var out []map[string]any

	for _, t := range builtinRoles() {
		if id, ok := b.dirRoles[lc(t.id)]; ok {
			out = append(out, b.roleMap(t.id, id))
		}
	}

	return out
}

// directoryRoleTemplate resolves an activated role by object ID or roleTemplateId. Callers hold b.mu.
func (b *Backend) directoryRoleTemplate(ref Ref) (string, error) {
	key := ref.ID
	if ref.Field != "" {
		if ref.Field != keyRoleTemplateID {
			return "", badRequest("The alternate key '%s' is not supported on directoryRoles.", ref.Field)
		}

		key = ref.Value
	}

	for tid, id := range b.dirRoles {
		if (ref.Field == "" && strings.EqualFold(id, key)) || (ref.Field != "" && strings.EqualFold(tid, key)) {
			return tid, nil
		}
	}

	return "", notFound(key)
}

// GetDirectoryRole returns an activated role by ID or roleTemplateId.
func (b *Backend) GetDirectoryRole(ref Ref) (map[string]any, error) {
	b.mu.RLock("GetDirectoryRole")
	defer b.mu.RUnlock()

	tid, err := b.directoryRoleTemplate(ref)
	if err != nil {
		return nil, err
	}

	return b.roleMap(tid, b.dirRoles[tid]), nil
}

func (b *Backend) roleMemberAssignments(tid string) []*RoleAssignment {
	var out []*RoleAssignment

	for _, a := range b.roleAssigns.all() {
		if lc(a.RoleDefinitionID) == tid && a.DirectoryScopeID == rootScope {
			out = append(out, a)
		}
	}

	return out
}

// DirectoryRoleMembers lists the principals holding the role at tenant scope.
func (b *Backend) DirectoryRoleMembers(ref Ref) ([]Object, error) {
	b.mu.RLock("DirectoryRoleMembers")
	defer b.mu.RUnlock()

	tid, err := b.directoryRoleTemplate(ref)
	if err != nil {
		return nil, err
	}

	var ids []string
	for _, a := range b.roleMemberAssignments(tid) {
		ids = append(ids, a.PrincipalID)
	}

	return b.objects(ids), nil
}

// AddDirectoryRoleMember assigns the role to a principal at tenant scope.
func (b *Backend) AddDirectoryRoleMember(ref Ref, memberID string) error {
	b.mu.Lock("AddDirectoryRoleMember")
	defer b.mu.Unlock()

	tid, err := b.directoryRoleTemplate(ref)
	if err != nil {
		return err
	}

	memberID = lc(memberID)
	if _, ok := b.lookup(memberID); !ok {
		return notFound(memberID)
	}

	for _, a := range b.roleMemberAssignments(tid) {
		if lc(a.PrincipalID) == memberID {
			return badRequest("One or more added object references already exist for the following modified "+
				"properties: '%s'.", propMembers)
		}
	}

	a := &RoleAssignment{ID: newID(), PrincipalID: memberID, RoleDefinitionID: tid, DirectoryScopeID: rootScope}
	if err = b.validateRoleAssignment(a); err != nil {
		return err
	}

	b.roleAssigns.put(lc(a.ID), a)

	return nil
}

// RemoveDirectoryRoleMember removes a principal's tenant-scope assignment of the role.
func (b *Backend) RemoveDirectoryRoleMember(ref Ref, memberID string) error {
	b.mu.Lock("RemoveDirectoryRoleMember")
	defer b.mu.Unlock()

	tid, err := b.directoryRoleTemplate(ref)
	if err != nil {
		return err
	}

	for _, a := range b.roleMemberAssignments(tid) {
		if lc(a.PrincipalID) == lc(memberID) {
			b.roleAssigns.del(lc(a.ID))

			return nil
		}
	}

	return notFound(memberID)
}

func (h *Handler) handleDirectoryRoleTemplates(c *echo.Context, segs []string) error {
	if c.Request().Method != http.MethodGet {
		return h.methodNotAllowed(c)
	}

	if len(segs) == 1 {
		items := make([]map[string]any, 0)
		for _, t := range builtinRoles() {
			items = append(items, t.templateMap())
		}

		return h.list(c, collDirectoryRoleTemplates, "DirectoryRoleTemplate", nil, items)
	}

	if t, ok := findTemplate(segs[1]); ok && len(segs) == pathWithID {
		return h.entity(c, http.StatusOK, collDirectoryRoleTemplates, t.templateMap())
	}

	if len(segs) == pathWithID {
		return h.writeErr(c, notFound(segs[1]))
	}

	return h.writeErr(c, segmentErr(segs[len(segs)-1]))
}

// roleRef resolves the role addressed by directoryRoles(roleTemplateId='..'), directoryRoles/{id} or
// directoryRoles/roleTemplateId={id}; the returned tail is what follows it.
func roleRef(segs []string) (Ref, []string) {
	_, ref := splitKey(segs[0])
	rest := segs[1:]

	if ref.Field != "" || len(rest) == 0 {
		return ref, rest
	}

	if v, ok := strings.CutPrefix(rest[0], keyRoleTemplateID+"="); ok {
		return Ref{Field: keyRoleTemplateID, Value: v}, rest[1:]
	}

	return Ref{ID: rest[0]}, rest[1:]
}

func (h *Handler) handleDirectoryRoles(c *echo.Context, segs []string) error {
	ref, rest := roleRef(segs)
	if ref.Field == "" && ref.ID == "" {
		return h.directoryRolesRoot(c)
	}

	r, b := c.Request(), h.Backend

	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		role, err := b.GetDirectoryRole(ref)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusOK, collDirectoryRoles, role)
	case len(rest) > 0 && strings.EqualFold(rest[0], propMembers):
		return h.refs(c, "", rest[1:], refOps{
			add:    func(_, m string) error { return b.AddDirectoryRoleMember(ref, m) },
			remove: func(_, m string) error { return b.RemoveDirectoryRoleMember(ref, m) },
			load:   func(string) ([]Object, error) { return b.DirectoryRoleMembers(ref) },
		})
	}

	return h.methodNotAllowed(c)
}

func (h *Handler) directoryRolesRoot(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.list(c, collDirectoryRoles, "DirectoryRole", nil, h.Backend.DirectoryRoles())
	case http.MethodPost:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		role, err := h.Backend.ActivateDirectoryRole(body)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusCreated, collDirectoryRoles, role)
	}

	return h.methodNotAllowed(c)
}
