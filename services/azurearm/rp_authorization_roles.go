package azurearm

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
)

const (
	roleCodeNotFound   = "RoleDefinitionDoesNotExist"
	roleCodeSameName   = "RoleDefinitionWithSameNameExists"
	roleCodeHasAssigns = "RoleDefinitionHasAssignments"
)

func (b *builtinRole) permissions() []authPermission { return nonNilPermissions(b.Permissions) }

func nonNilPermissions(in []authPermission) []authPermission {
	out := make([]authPermission, 0, len(in))

	for _, p := range in {
		out = append(out, authPermission{
			Actions: nonNilStrings(p.Actions), NotActions: nonNilStrings(p.NotActions),
			DataActions: nonNilStrings(p.DataActions), NotDataActions: nonNilStrings(p.NotDataActions),
		})
	}

	return out
}

func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}

	return in
}

func (p *AuthorizationProvider) builtinRoleBody(scope string, b *builtinRole) map[string]any {
	return map[string]any{
		authFieldID: authID(scope, authRoleDefinitions, b.Name), fieldName: b.Name,
		fieldType: namespaceMicrosoftAuthorization + "/" + authRoleDefinitions,
		fieldProperties: map[string]any{
			authFieldRoleName: b.RoleName, fieldType: authTypeBuiltIn, authFieldDescription: b.Description,
			"assignableScopes": []string{"/"}, "permissions": b.permissions(),
		},
	}
}

func (p *AuthorizationProvider) customRoleBody(d *storedRoleDef) map[string]any {
	return map[string]any{
		authFieldID: authID(d.Scope, authRoleDefinitions, d.Name), fieldName: d.Name,
		fieldType: namespaceMicrosoftAuthorization + "/" + authRoleDefinitions,
		fieldProperties: map[string]any{
			authFieldRoleName: d.RoleName, fieldType: authTypeCustom, authFieldDescription: d.Description,
			"assignableScopes": nonNilStrings(d.AssignableScopes), "permissions": nonNilPermissions(d.Permissions),
			authFieldCreatedOn: authStamp(d.CreatedOn), authFieldUpdatedOn: authStamp(d.UpdatedOn),
		},
	}
}

// customRoleVisible reports whether a custom role is addressable from scope: defined there, assignable there or
// assignable on an ancestor or descendant.
func customRoleVisible(d *storedRoleDef, scope string) bool {
	if scopeWithin(scope, d.Scope) {
		return true
	}

	return slices.ContainsFunc(d.AssignableScopes, func(a string) bool {
		return scopeWithin(scope, a) || scopeWithin(a, scope)
	})
}

func (p *AuthorizationProvider) doRoleDefinition(req authReq) (authResp, error) {
	switch {
	case req.Name == "" && req.Method == http.MethodGet:
		return p.listRoleDefinitions(req)
	case req.Name == "":
		return authResp{}, ErrResourceNotFound
	}

	switch req.Method {
	case http.MethodGet:
		return p.getRoleDefinition(req)
	case http.MethodPut:
		return p.putRoleDefinition(req)
	case http.MethodDelete:
		return p.deleteRoleDefinition(req)
	}

	return authResp{}, ErrResourceNotFound
}

func roleDefNotFound(id string) error {
	return authErr(http.StatusNotFound, roleCodeNotFound,
		"The specified role definition with ID '%s' does not exist.", id)
}

func (p *AuthorizationProvider) getRoleDefinition(req authReq) (authResp, error) {
	p.mu.RLock("getRoleDefinition")
	defer p.mu.RUnlock()

	if b, ok := p.builtinRoles[strings.ToLower(req.Name)]; ok {
		return authResp{Status: http.StatusOK, Body: p.builtinRoleBody(req.Scope.id, b)}, nil
	}

	if d, ok := p.roleDefs[strings.ToLower(req.Name)]; ok && customRoleVisible(d, req.Scope.id) {
		return authResp{Status: http.StatusOK, Body: p.customRoleBody(d)}, nil
	}

	return authResp{}, roleDefNotFound(req.Name)
}

func (p *AuthorizationProvider) listRoleDefinitions(req authReq) (authResp, error) {
	f, err := parseAuthFilter(req.Query.Get("$filter"),
		[]string{authFieldRoleName, fieldType}, []string{authFnAtScopeAndBelow})
	if err != nil {
		return authResp{}, err
	}

	wantType := f.eq[strings.ToLower(fieldType)]
	if wantType != "" && wantType != authTypeBuiltIn && wantType != authTypeCustom {
		return authResp{}, authErr(http.StatusBadRequest, authCodeFilter,
			"The role type '%s' is not valid. Use BuiltInRole or CustomRole.", wantType)
	}

	nameEq, below := f.eq[strings.ToLower(authFieldRoleName)], f.hasFn(authFnAtScopeAndBelow)
	match := func(roleName string) bool { return nameEq == "" || strings.EqualFold(roleName, nameEq) }

	p.mu.RLock("listRoleDefinitions")
	defer p.mu.RUnlock()

	var out []map[string]any

	if wantType != authTypeCustom {
		for _, b := range p.builtinRoles {
			if match(b.RoleName) {
				out = append(out, p.builtinRoleBody(req.Scope.id, b))
			}
		}
	}

	if wantType != authTypeBuiltIn {
		for _, d := range p.roleDefs {
			assignable := slices.ContainsFunc(d.AssignableScopes, func(a string) bool {
				return scopeWithin(req.Scope.id, a) || (below && scopeWithin(a, req.Scope.id))
			})
			if assignable && match(d.RoleName) {
				out = append(out, p.customRoleBody(d))
			}
		}
	}

	return listBody(out), nil
}

func (p *AuthorizationProvider) putRoleDefinition(req authReq) (authResp, error) {
	if !isGUID(req.Name) {
		return authResp{}, authBad("The role definition name '%s' must be a GUID.", req.Name)
	}

	if req.Scope.kind == authScopeTenant {
		return authResp{}, authBad("A custom role definition cannot be created at the tenant root scope.")
	}

	spec, err := parseRoleDefinitionBody(req.Body)
	if err != nil {
		return authResp{}, err
	}

	key := strings.ToLower(req.Name)

	p.mu.Lock("putRoleDefinition")
	defer p.mu.Unlock()

	if _, builtin := p.builtinRoles[key]; builtin {
		return authResp{}, authBad("The role definition '%s' is built in and cannot be modified.", req.Name)
	}

	if err = p.checkRoleNameFree(spec.RoleName, key); err != nil {
		return authResp{}, err
	}

	now := p.now()
	rec := &storedRoleDef{
		Name: req.Name, Scope: req.Scope.id, RoleName: spec.RoleName, Description: spec.Description,
		AssignableScopes: spec.AssignableScopes, Permissions: spec.Permissions, CreatedOn: now, UpdatedOn: now,
	}

	if cur, ok := p.roleDefs[key]; ok {
		rec.CreatedOn, rec.Scope = cur.CreatedOn, cur.Scope
	}

	p.roleDefs[key] = rec

	return authResp{Status: http.StatusCreated, Body: p.customRoleBody(rec)}, nil
}

func (p *AuthorizationProvider) checkRoleNameFree(roleName, selfKey string) error {
	for _, b := range p.builtinRoles {
		if strings.EqualFold(b.RoleName, roleName) {
			return authErr(http.StatusConflict, roleCodeSameName,
				"A role definition with the name '%s' already exists.", roleName)
		}
	}

	for k, d := range p.roleDefs {
		if k != selfKey && strings.EqualFold(d.RoleName, roleName) {
			return authErr(http.StatusConflict, roleCodeSameName,
				"A role definition with the name '%s' already exists.", roleName)
		}
	}

	return nil
}

type roleDefSpec struct {
	RoleName         string
	Description      string
	AssignableScopes []string
	Permissions      []authPermission
}

func stringList(v any, field string) ([]string, error) {
	if v == nil {
		return []string{}, nil
	}

	arr, ok := v.([]any)
	if !ok {
		return nil, authBad("The property '%s' must be an array of strings.", field)
	}

	out := make([]string, 0, len(arr))

	for _, e := range arr {
		s, isStr := e.(string)
		if !isStr || strings.TrimSpace(s) == "" {
			return nil, authBad("The property '%s' must contain non-empty strings.", field)
		}

		out = append(out, s)
	}

	return out, nil
}

func parseRoleDefinitionBody(body map[string]any) (roleDefSpec, error) {
	props := mapOf(body[fieldProperties])
	spec := roleDefSpec{RoleName: strOf(props, authFieldRoleName), Description: strOf(props, authFieldDescription)}

	if strings.TrimSpace(spec.RoleName) == "" {
		return spec, authBad("The property 'properties.roleName' is required.")
	}

	if t := strOf(props, fieldType); t != "" && !strings.EqualFold(t, authTypeCustom) {
		return spec, authBad("The role definition type '%s' is not valid; only CustomRole can be created.", t)
	}

	scopes, err := stringList(props["assignableScopes"], "properties.assignableScopes")
	if err != nil {
		return spec, err
	}

	if len(scopes) == 0 {
		return spec, authBad("The property 'properties.assignableScopes' must contain at least one scope.")
	}

	mgmt, err := checkAssignableScopes(scopes)
	if err != nil {
		return spec, err
	}

	spec.AssignableScopes = scopes

	rawPerms, _ := props["permissions"].([]any)
	if len(rawPerms) == 0 {
		return spec, authBad("The property 'properties.permissions' must contain at least one permission block.")
	}

	for i, rp := range rawPerms {
		perm, permErr := parsePermission(mapOf(rp), i)
		if permErr != nil {
			return spec, permErr
		}

		if mgmt && (len(perm.DataActions) > 0 || len(perm.NotDataActions) > 0) {
			return spec, authBad("You cannot add data action permissions when you have a management group as an " +
				"assignable scope.")
		}

		spec.Permissions = append(spec.Permissions, perm)
	}

	return spec, nil
}

func checkAssignableScopes(scopes []string) (bool, error) {
	mgmt := 0

	for _, s := range scopes {
		sc, ok := parseAuthScope(splitARMPath(s))
		if !ok || sc.kind == authScopeTenant {
			return false, authBad("The assignable scope '%s' is not a valid subscription, resource group, "+
				"resource or management group scope.", s)
		}

		if sc.kind == authScopeManagementGroup {
			mgmt++
		}
	}

	if mgmt > 1 {
		return false, authBad("Only one management group can be defined in assignableScopes.")
	}

	return mgmt == 1, nil
}

func parsePermission(m map[string]any, idx int) (authPermission, error) {
	var out authPermission

	for _, f := range []struct {
		dst  *[]string
		name string
	}{
		{&out.Actions, "actions"}, {&out.NotActions, "notActions"},
		{&out.DataActions, "dataActions"}, {&out.NotDataActions, "notDataActions"},
	} {
		list, err := stringList(m[f.name], "properties.permissions["+strconv.Itoa(idx)+"]."+f.name)
		if err != nil {
			return out, err
		}

		*f.dst = list
	}

	return out, nil
}

func (p *AuthorizationProvider) deleteRoleDefinition(req authReq) (authResp, error) {
	key := strings.ToLower(req.Name)

	p.mu.Lock("deleteRoleDefinition")
	defer p.mu.Unlock()

	if _, builtin := p.builtinRoles[key]; builtin {
		return authResp{}, authBad("The role definition '%s' is built in and cannot be deleted.", req.Name)
	}

	d, ok := p.roleDefs[key]
	if !ok {
		return authResp{Status: http.StatusNoContent}, nil
	}

	for _, a := range p.roleAssigns {
		if strings.EqualFold(a.RoleGUID, d.Name) {
			return authResp{}, authErr(http.StatusConflict, roleCodeHasAssigns,
				"There are existing role assignments referencing role (code: RoleDefinitionHasAssignments)")
		}
	}

	delete(p.roleDefs, key)

	return authResp{Status: http.StatusOK, Body: p.customRoleBody(d)}, nil
}
