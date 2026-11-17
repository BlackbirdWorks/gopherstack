package azuregraph

import (
	"encoding/json/jsontext"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	typeRoleDefinition   = "UnifiedRoleDefinition"
	maxCustomRoles       = 1000
	maxRolePermissions   = 100
	maxRoleActions       = 3000
	maxRoleNameLen       = 256
	maxRoleActionLen     = 500
	maxRoleDescriptorLen = 1024
	roleActionPrefix     = "microsoft."
)

// RolePermission is a unifiedRolePermission.
type RolePermission struct {
	Condition              *string  `json:"condition"`
	AllowedResourceActions []string `json:"allowedResourceActions"`
}

// RoleDefinition is a unifiedRoleDefinition; built-in roles are derived from the template catalog.
type RoleDefinition struct {
	DeletedDateTime *time.Time       `json:"deletedDateTime"`
	Version         *string          `json:"version"`
	ID              string           `json:"id"`
	TemplateID      string           `json:"templateId"`
	DisplayName     string           `json:"displayName"`
	Description     string           `json:"description"`
	ResourceScopes  []string         `json:"resourceScopes"`
	RolePermissions []RolePermission `json:"rolePermissions"`
	IsBuiltIn       bool             `json:"isBuiltIn"`
	IsEnabled       bool             `json:"isEnabled"`
}

func (d *RoleDefinition) normalize() {
	d.ResourceScopes, d.RolePermissions = nonNil(d.ResourceScopes), nonNil(d.RolePermissions)

	for i := range d.RolePermissions {
		d.RolePermissions[i].AllowedResourceActions = nonNil(d.RolePermissions[i].AllowedResourceActions)
	}
}

func roleDefReadOnly() []string { return []string{"id", "isBuiltIn", propDeletedDateTime} }

func builtinDefinition(r builtinRole) RoleDefinition {
	one := "1"

	return RoleDefinition{
		ID: r.id, TemplateID: r.id, DisplayName: r.name, Description: r.description, IsBuiltIn: true, IsEnabled: true,
		ResourceScopes: []string{rootScope}, Version: &one,
		RolePermissions: []RolePermission{{AllowedResourceActions: r.actions}},
	}
}

func cloneRoleDef(d RoleDefinition) RoleDefinition {
	d.ResourceScopes = slices.Clone(d.ResourceScopes)
	d.RolePermissions = slices.Clone(d.RolePermissions)

	for i := range d.RolePermissions {
		d.RolePermissions[i].AllowedResourceActions = slices.Clone(d.RolePermissions[i].AllowedResourceActions)
	}

	return d
}

func validRoleAction(a string) bool {
	if len(a) > maxRoleActionLen || !strings.HasPrefix(a, roleActionPrefix) || strings.ContainsAny(a, " \t\r\n") {
		return false
	}

	_, tail, ok := strings.Cut(a, "/")

	return ok && tail != "" && !strings.HasSuffix(a, "/")
}

func validateRoleDefinition(d *RoleDefinition) error {
	if strings.TrimSpace(d.DisplayName) == "" || len(d.DisplayName) > maxRoleNameLen {
		return badRequest("Invalid value specified for property 'displayName' of resource '%s'.", typeRoleDefinition)
	}

	if len(d.Description) > maxRoleDescriptorLen {
		return badRequest("Invalid value specified for property 'description' of resource '%s'.", typeRoleDefinition)
	}

	if !isGUID(d.TemplateID) {
		return badRequest("Invalid value specified for property 'templateId' of resource '%s'.", typeRoleDefinition)
	}

	if len(d.ResourceScopes) != 1 || d.ResourceScopes[0] != rootScope {
		return badRequest("Invalid value specified for property 'resourceScopes' of resource '%s'.", typeRoleDefinition)
	}

	return validateRolePermissions(d.RolePermissions)
}

func validateRolePermissions(perms []RolePermission) error {
	if len(perms) == 0 || len(perms) > maxRolePermissions {
		return badRequest("A value is required for property 'rolePermissions' of resource '%s'.", typeRoleDefinition)
	}

	total := 0

	for _, p := range perms {
		if len(p.AllowedResourceActions) == 0 {
			return badRequest(
				"A value is required for property 'allowedResourceActions' of resource 'UnifiedRolePermission'.",
			)
		}

		for _, a := range p.AllowedResourceActions {
			if total++; total > maxRoleActions || !validRoleAction(a) {
				return badRequest("The resource action '%s' is not valid.", a)
			}
		}
	}

	return nil
}

func (b *Backend) customRoleCount() int {
	n := 0

	for _, d := range b.roleDefs.all() {
		if d.DeletedDateTime == nil {
			n++
		}
	}

	return n
}

func (b *Backend) roleNameTaken(name, self string) bool {
	for _, r := range builtinRoles() {
		if strings.EqualFold(r.name, name) {
			return true
		}
	}

	for _, d := range b.roleDefs.all() {
		if d.DeletedDateTime == nil && d.ID != self && strings.EqualFold(d.DisplayName, name) {
			return true
		}
	}

	return false
}

func (b *Backend) templateTaken(tid, self string) bool {
	if _, builtin := findTemplate(tid); builtin {
		return true
	}

	for _, d := range b.roleDefs.all() {
		if d.ID != self && strings.EqualFold(d.TemplateID, tid) {
			return true
		}
	}

	return false
}

func (b *Backend) checkRoleUniqueness(d *RoleDefinition) error {
	if b.roleNameTaken(d.DisplayName, d.ID) {
		return badRequest("A role definition with the display name '%s' already exists.", d.DisplayName)
	}

	if b.templateTaken(d.TemplateID, d.ID) {
		return badRequest("A role definition with the templateId '%s' already exists.", d.TemplateID)
	}

	return nil
}

// CreateRoleDefinition creates a custom role definition.
func (b *Backend) CreateRoleDefinition(body map[string]jsontext.Value) (RoleDefinition, error) {
	if err := requireProps(typeRoleDefinition, body, "displayName", "isEnabled", "rolePermissions"); err != nil {
		return RoleDefinition{}, err
	}

	b.mu.Lock("CreateRoleDefinition")
	defer b.mu.Unlock()

	b.purgeExpired()

	id := newID()

	seed := &RoleDefinition{ID: id, TemplateID: id, ResourceScopes: []string{rootScope}}
	seed.normalize()

	d, err := applyBody(seed, body, roleDefReadOnly()...)
	if err != nil {
		return RoleDefinition{}, err
	}

	d.ID = id

	if err = validateRoleDefinition(d); err != nil {
		return RoleDefinition{}, err
	}

	if b.customRoleCount() >= maxCustomRoles {
		return RoleDefinition{}, badRequest("The maximum number of custom role definitions (%d) has been reached.",
			maxCustomRoles)
	}

	if err = b.checkRoleUniqueness(d); err != nil {
		return RoleDefinition{}, err
	}

	d.TemplateID = lc(d.TemplateID)
	b.roleDefs.put(lc(id), d)

	return cloneRoleDef(*d), nil
}

// activeRoleDef returns an active (not soft-deleted) custom role definition. Callers hold b.mu.
func (b *Backend) activeRoleDef(id string) (*RoleDefinition, bool) {
	d, ok := b.roleDefs.get(lc(id))
	if !ok || d.DeletedDateTime != nil {
		return nil, false
	}

	return d, true
}

// ListRoleDefinitions returns the built-in and active custom role definitions.
func (b *Backend) ListRoleDefinitions() []RoleDefinition {
	b.mu.RLock("ListRoleDefinitions")
	defer b.mu.RUnlock()

	roles := builtinRoles()
	out := make([]RoleDefinition, 0, len(roles)+len(b.roleDefs.order))

	for _, r := range roles {
		out = append(out, builtinDefinition(r))
	}

	for _, d := range b.roleDefs.all() {
		if d.DeletedDateTime == nil {
			out = append(out, cloneRoleDef(*d))
		}
	}

	return out
}

// GetRoleDefinition returns one active role definition.
func (b *Backend) GetRoleDefinition(id string) (RoleDefinition, error) {
	if r, ok := findTemplate(id); ok {
		return builtinDefinition(r), nil
	}

	b.mu.RLock("GetRoleDefinition")
	defer b.mu.RUnlock()

	if d, ok := b.activeRoleDef(id); ok {
		return cloneRoleDef(*d), nil
	}

	return RoleDefinition{}, notFound(id)
}

// UpdateRoleDefinition applies a PATCH body to a custom role definition.
func (b *Backend) UpdateRoleDefinition(id string, body map[string]jsontext.Value) error {
	if _, builtin := findTemplate(id); builtin {
		return badRequest("Built-in role definitions cannot be updated.")
	}

	b.mu.Lock("UpdateRoleDefinition")
	defer b.mu.Unlock()

	cur, ok := b.activeRoleDef(id)
	if !ok {
		return notFound(id)
	}

	next, err := applyBody(cloneRolePtr(cur), body, roleDefReadOnly()...)
	if err != nil {
		return err
	}

	next.TemplateID = lc(next.TemplateID)

	if err = validateRoleDefinition(next); err != nil {
		return err
	}

	if err = b.checkRoleUniqueness(next); err != nil {
		return err
	}

	b.roleDefs.put(lc(cur.ID), next)

	return nil
}

func cloneRolePtr(d *RoleDefinition) *RoleDefinition {
	c := cloneRoleDef(*d)

	return &c
}

func (b *Backend) roleInUse(id string) bool {
	return slices.ContainsFunc(
		b.roleAssigns.all(),
		func(a *RoleAssignment) bool { return lc(a.RoleDefinitionID) == lc(id) },
	)
}

// DeleteRoleDefinition soft-deletes a custom role definition that has no assignments.
func (b *Backend) DeleteRoleDefinition(id string) error {
	if _, builtin := findTemplate(id); builtin {
		return badRequest("Built-in role definitions cannot be deleted.")
	}

	b.mu.Lock("DeleteRoleDefinition")
	defer b.mu.Unlock()

	cur, ok := b.activeRoleDef(id)
	if !ok {
		return notFound(id)
	}

	if b.roleInUse(cur.ID) {
		return badRequest("The role definition '%s' has role assignments and cannot be deleted.", cur.ID)
	}

	next := cloneRoleDef(*cur)
	at := b.stamp()
	next.DeletedDateTime = &at
	b.roleDefs.put(lc(cur.ID), &next)

	return nil
}

func (b *Backend) deletedRoleDef(id string) (*RoleDefinition, error) {
	b.purgeExpired()

	d, ok := b.roleDefs.get(lc(id))
	if !ok || d.DeletedDateTime == nil {
		return nil, notFound(id)
	}

	return d, nil
}

// GetDeletedRoleDefinition returns a soft-deleted custom role definition.
func (b *Backend) GetDeletedRoleDefinition(id string) (RoleDefinition, error) {
	b.mu.Lock("GetDeletedRoleDefinition")
	defer b.mu.Unlock()

	d, err := b.deletedRoleDef(id)
	if err != nil {
		return RoleDefinition{}, err
	}

	return cloneRoleDef(*d), nil
}

// PermanentDeleteRoleDefinition removes a soft-deleted custom role definition for good.
func (b *Backend) PermanentDeleteRoleDefinition(id string) error {
	b.mu.Lock("PermanentDeleteRoleDefinition")
	defer b.mu.Unlock()

	d, err := b.deletedRoleDef(id)
	if err != nil {
		return err
	}

	b.roleDefs.del(lc(d.ID))

	return nil
}

// RestoreRoleDefinition moves a soft-deleted custom role definition back to the active set.
func (b *Backend) RestoreRoleDefinition(id string) (RoleDefinition, error) {
	b.mu.Lock("RestoreRoleDefinition")
	defer b.mu.Unlock()

	d, err := b.deletedRoleDef(id)
	if err != nil {
		return RoleDefinition{}, err
	}

	next := cloneRoleDef(*d)
	next.DeletedDateTime = nil

	if err = b.checkRoleUniqueness(&next); err != nil {
		return RoleDefinition{}, err
	}

	if b.customRoleCount() >= maxCustomRoles {
		return RoleDefinition{}, badRequest("The maximum number of custom role definitions (%d) has been reached.",
			maxCustomRoles)
	}

	b.roleDefs.put(lc(next.ID), &next)

	return cloneRoleDef(next), nil
}

// assignableRole checks that id names an existing, enabled role definition. Callers hold b.mu.
func (b *Backend) assignableRole(id string) error {
	if _, ok := findTemplate(id); ok {
		return nil
	}

	d, ok := b.activeRoleDef(id)
	if !ok {
		return badRequest("The roleDefinitionId '%s' does not reference a valid role definition.", id)
	}

	if !d.IsEnabled {
		return badRequest("The role definition '%s' is disabled and cannot be assigned.", id)
	}

	return nil
}

// purgeRoleDefs drops soft-deleted role definitions past retention. Callers hold the write lock.
func (b *Backend) purgeRoleDefs(cutoff time.Time) {
	for _, d := range b.roleDefs.all() {
		if d.DeletedDateTime != nil && !d.DeletedDateTime.After(cutoff) {
			b.roleDefs.del(lc(d.ID))
		}
	}
}

func (h *Handler) roleDefinitions(c *echo.Context, ctx string, tail []string) error {
	r, b := c.Request(), h.Backend
	fields := fieldsOf[RoleDefinition]()

	switch {
	case len(tail) == 0 && r.Method == http.MethodGet:
		return h.list(c, ctx, typeRoleDefinition, fields, mapAll(b.ListRoleDefinitions()))
	case len(tail) == 0 && r.Method == http.MethodPost:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		d, err := b.CreateRoleDefinition(body)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusCreated, ctx, toMap(d))
	case len(tail) == 1:
		return h.roleDefinition(c, ctx, tail[0])
	case len(tail) == 0:
		return h.methodNotAllowed(c)
	}

	return h.writeErr(c, segmentErr(strings.Join(tail, "/")))
}

func (h *Handler) roleDefinition(c *echo.Context, ctx, id string) error {
	b := h.Backend

	switch c.Request().Method {
	case http.MethodGet:
		d, err := b.GetRoleDefinition(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.roleDefEntity(c, ctx, d)
	case http.MethodPatch:
		body, err := readJSON(c)
		if err == nil {
			err = b.UpdateRoleDefinition(id, body)
		}

		return h.noContent(c, err)
	case http.MethodDelete:
		return h.noContent(c, b.DeleteRoleDefinition(id))
	}

	return h.methodNotAllowed(c)
}

func (h *Handler) roleDefEntity(c *echo.Context, ctx string, d RoleDefinition) error {
	q, err := parseListQuery(c.Request(), typeRoleDefinition, fieldsOf[RoleDefinition]())
	if err != nil {
		return h.writeErr(c, err)
	}

	q.single = true

	return h.entity(c, http.StatusOK, ctx, q.project(toMap(d)))
}

// deletedRoleDefinitions serves roleManagement/directory/deletedItems/roleDefinitions/{id}[/restore].
func (h *Handler) deletedRoleDefinitions(c *echo.Context, tail []string) error {
	if len(tail) < pathWithID || !strings.EqualFold(tail[0], "roleDefinitions") || len(tail) > pathWithID+1 {
		return h.writeErr(c, segmentErr(strings.Join(tail, "/")))
	}

	r, b, id := c.Request(), h.Backend, tail[1]
	ctx := "roleManagement/directory/deletedItems/roleDefinitions"

	switch {
	case len(tail) == pathWithID && r.Method == http.MethodGet:
		d, err := b.GetDeletedRoleDefinition(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusOK, ctx, toMap(d))
	case len(tail) == pathWithID && r.Method == http.MethodDelete:
		return h.noContent(c, b.PermanentDeleteRoleDefinition(id))
	case len(tail) == pathWithID+1 && strings.EqualFold(tail[2], "restore") && r.Method == http.MethodPost:
		d, err := b.RestoreRoleDefinition(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusOK, "roleManagement/directory/roleDefinitions", toMap(d))
	}

	return h.methodNotAllowed(c)
}
