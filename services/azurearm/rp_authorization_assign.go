package azurearm

import (
	"net/http"
	"slices"
	"strings"
)

const (
	assignCodeExists      = "RoleAssignmentExists"
	assignCodeNotFound    = "RoleAssignmentNotFound"
	assignCodeNoUpdate    = "RoleAssignmentUpdateNotPermitted"
	assignCodeBadName     = "InvalidRoleAssignmentId"
	assignCodePrincipal   = "PrincipalNotFound"
	assignCodeNotAssignee = "RoleAssignmentScopeNotAssignableToRoleDefinition"
	assignCodeLimit       = "RoleAssignmentLimitExceeded"
	principalUser         = "User"
	principalSP           = "ServicePrincipal"
)

// Limits from learn.microsoft.com/azure/role-based-access-control/troubleshoot-limits.
const (
	assignLimitSubscription    = 5000
	assignLimitManagementGroup = 500
	assignCondVersion          = "2.0"
)

func principalTypes() []string {
	return []string{principalUser, "Group", principalSP, "ForeignGroup", "Device"}
}

// roleRef is a role definition resolved for an assignment.
type roleRef struct {
	guid       string
	assignable []string
}

func (p *AuthorizationProvider) doRoleAssignment(req authReq) (authResp, error) {
	if req.Name == "" {
		if req.Method == http.MethodGet {
			return p.listRoleAssignments(req)
		}

		return authResp{}, ErrResourceNotFound
	}

	switch req.Method {
	case http.MethodGet:
		return p.getRoleAssignment(req)
	case http.MethodPut:
		return p.putRoleAssignment(req)
	case http.MethodDelete:
		return p.deleteRoleAssignment(req)
	}

	return authResp{}, ErrResourceNotFound
}

func (p *AuthorizationProvider) roleAssignmentBody(a *storedRoleAssign) map[string]any {
	props := map[string]any{
		"roleDefinitionId": a.RoleDefinitionID, authFieldPrincipalID: a.PrincipalID, authFieldScope: a.Scope,
		authFieldCreatedOn: authStamp(a.CreatedOn), authFieldUpdatedOn: authStamp(a.UpdatedOn),
	}

	for k, v := range map[string]string{
		"principalType": a.PrincipalType, "condition": a.Condition, "conditionVersion": a.ConditionVersion,
		authFieldDescription: a.Description, "delegatedManagedIdentityResourceId": a.DelegatedMI,
	} {
		if v != "" {
			props[k] = v
		}
	}

	return map[string]any{
		authFieldID: authID(a.Scope, authRoleAssignments, a.Name), fieldName: a.Name,
		fieldType: namespaceMicrosoftAuthorization + "/" + authRoleAssignments, fieldProperties: props,
	}
}

func (p *AuthorizationProvider) getRoleAssignment(req authReq) (authResp, error) {
	p.mu.RLock("getRoleAssignment")
	defer p.mu.RUnlock()

	a, ok := p.roleAssigns[authKey(req.Scope.id, req.Name)]
	if !ok {
		return authResp{}, authErr(http.StatusNotFound, assignCodeNotFound,
			"The role assignment '%s' was not found.", req.Name)
	}

	return authResp{Status: http.StatusOK, Body: p.roleAssignmentBody(a)}, nil
}

func (p *AuthorizationProvider) deleteRoleAssignment(req authReq) (authResp, error) {
	p.mu.Lock("deleteRoleAssignment")
	defer p.mu.Unlock()

	key := authKey(req.Scope.id, req.Name)

	a, ok := p.roleAssigns[key]
	if !ok {
		return authResp{Status: http.StatusNoContent}, nil
	}

	delete(p.roleAssigns, key)

	return authResp{Status: http.StatusOK, Body: p.roleAssignmentBody(a)}, nil
}

func (p *AuthorizationProvider) listRoleAssignments(req authReq) (authResp, error) {
	f, err := parseAuthFilter(req.Query.Get("$filter"), []string{authFieldPrincipalID},
		[]string{authFnAtScope, authFnAssignedTo})
	if err != nil {
		return authResp{}, err
	}

	principal, hasPrincipal := f.eq[strings.ToLower(authFieldPrincipalID)]
	if assignee, ok := f.fns[authFnAssignedTo]; ok {
		principal, hasPrincipal = assignee, true
	}

	p.mu.RLock("listRoleAssignments")
	defer p.mu.RUnlock()

	var out []map[string]any

	for _, a := range p.roleAssigns {
		at := strings.EqualFold(strings.TrimSuffix(a.Scope, "/"), strings.TrimSuffix(req.Scope.id, "/"))
		above := scopeWithin(req.Scope.id, a.Scope)
		below := scopeWithin(a.Scope, req.Scope.id)

		// atScope() and assignedTo() are effective views (this scope and its ancestors); principalId eq alone, and
		// the unfiltered list, also include descendants.
		visible := above || below
		if f.hasFn(authFnAtScope) || f.hasFn(authFnAssignedTo) {
			visible = above
		} else if !hasPrincipal {
			visible = at || below
		}

		if visible && (!hasPrincipal || strings.EqualFold(a.PrincipalID, principal)) {
			out = append(out, p.roleAssignmentBody(a))
		}
	}

	return listBody(out), nil
}

// resolveRole finds the role a roleDefinitionId refers to; ok is false when it does not exist.
func (p *AuthorizationProvider) resolveRole(roleDefID string) (roleRef, bool) {
	segs := splitARMPath(roleDefID)
	if len(segs) < authPathListTail+1 {
		return roleRef{}, false
	}

	n := len(segs)
	if !strings.EqualFold(segs[n-2], authRoleDefinitions) ||
		!strings.EqualFold(segs[n-3], namespaceMicrosoftAuthorization) ||
		!isGUID(segs[n-1]) {
		return roleRef{}, false
	}

	key := strings.ToLower(segs[n-1])
	if b, ok := p.builtinRoles[key]; ok {
		return roleRef{guid: b.Name, assignable: []string{"/"}}, true
	}

	if d, ok := p.roleDefs[key]; ok {
		return roleRef{guid: d.Name, assignable: d.AssignableScopes}, true
	}

	return roleRef{}, false
}

func (p *AuthorizationProvider) canonicalRoleID(scope authScope, guid string) string {
	key := strings.ToLower(guid)
	if d, ok := p.roleDefs[key]; ok {
		return authID(d.Scope, authRoleDefinitions, d.Name)
	}

	base := ""
	if scope.sub != "" {
		base = "/subscriptions/" + scope.sub
	}

	return authID(base, authRoleDefinitions, guid)
}

func (p *AuthorizationProvider) principalTypeFor(given, principalID string) string {
	if given != "" {
		return given
	}

	if t, ok := p.directoryType(principalID); ok {
		return t
	}

	if p.msi != nil && p.msi.hasPrincipal(principalID) {
		return principalSP
	}

	return principalUser
}

func (p *AuthorizationProvider) putRoleAssignment(req authReq) (authResp, error) {
	if !isGUID(req.Name) {
		return authResp{}, authErr(http.StatusBadRequest, assignCodeBadName,
			"The role assignment name '%s' is not valid. It must be a GUID.", req.Name)
	}

	props := mapOf(req.Body[fieldProperties])
	roleDefID, principalID := strOf(props, "roleDefinitionId"), strOf(props, authFieldPrincipalID)

	if roleDefID == "" || principalID == "" {
		return authResp{}, authBad("The properties 'roleDefinitionId' and 'principalId' are required.")
	}

	ptype, condition, condVersion, err := p.checkAssignmentProps(props, principalID)
	if err != nil {
		return authResp{}, err
	}

	p.mu.Lock("putRoleAssignment")
	defer p.mu.Unlock()

	if p.validatePrincipals && !p.principalKnown(principalID) {
		return authResp{}, authErr(http.StatusBadRequest, assignCodePrincipal,
			"Principal %s does not exist in the directory %s. Check that you have the correct principal ID.",
			principalID, p.tenantID)
	}

	role, ok := p.resolveRole(roleDefID)
	if !ok {
		return authResp{}, authErr(http.StatusBadRequest, roleCodeNotFound,
			"The specified role definition with ID '%s' does not exist.", roleDefID)
	}

	if !slices.ContainsFunc(role.assignable, func(a string) bool { return scopeWithin(req.Scope.id, a) }) {
		return authResp{}, authErr(http.StatusBadRequest, assignCodeNotAssignee,
			"The role definition '%s' is not assignable at the scope '%s'.", role.guid, req.Scope.id)
	}

	key := authKey(req.Scope.id, req.Name)
	cur, exists := p.roleAssigns[key]

	if uerr := p.checkAssignmentUnique(req, key, role.guid, principalID, exists, cur); uerr != nil {
		return authResp{}, uerr
	}

	if !exists {
		if lerr := p.checkAssignmentLimit(req.Scope); lerr != nil {
			return authResp{}, lerr
		}
	}

	now := p.now()
	rec := &storedRoleAssign{
		Name: req.Name, Scope: req.Scope.id, RoleDefinitionID: p.canonicalRoleID(req.Scope, role.guid),
		RoleGUID: role.guid, PrincipalID: strings.ToLower(principalID), CreatedOn: now, UpdatedOn: now,
		PrincipalType: p.principalTypeFor(ptype, principalID), Condition: condition,
		ConditionVersion: condVersion, Description: strOf(props, authFieldDescription),
		DelegatedMI: strOf(props, "delegatedManagedIdentityResourceId"),
	}

	status := http.StatusCreated

	if exists {
		rec.CreatedOn, status = cur.CreatedOn, http.StatusOK
	}

	p.roleAssigns[key] = rec

	return authResp{Status: status, Body: p.roleAssignmentBody(rec)}, nil
}

// checkAssignmentLimit enforces 5000 assignments per subscription (management group scope excluded) and 500 per
// management group.
func (p *AuthorizationProvider) checkAssignmentLimit(scope authScope) error {
	var (
		limit int
		root  string
	)

	switch scope.kind {
	case authScopeManagementGroup:
		limit, root = assignLimitManagementGroup, scope.id
	case authScopeSubscription, authScopeResourceGroup, authScopeResource:
		limit, root = assignLimitSubscription, "/subscriptions/"+scope.sub
	default:
		return nil
	}

	n := 0

	for _, a := range p.roleAssigns {
		if scopeWithin(a.Scope, root) {
			n++
		}
	}

	if n >= limit {
		return authErr(http.StatusBadRequest, assignCodeLimit,
			"No more role assignments can be created (code: %s)", assignCodeLimit)
	}

	return nil
}

// checkAssignmentUnique rejects an update that changes identity, a name already used elsewhere and a duplicate
// role/principal/scope triple.
func (p *AuthorizationProvider) checkAssignmentUnique(
	req authReq, key, roleGUID, principalID string, exists bool, cur *storedRoleAssign,
) error {
	if exists {
		if !strings.EqualFold(cur.RoleGUID, roleGUID) || !strings.EqualFold(cur.PrincipalID, principalID) {
			return authErr(http.StatusBadRequest, assignCodeNoUpdate,
				"Tenant ID, application ID, principal ID, and scope are not allowed to be updated.")
		}

		return nil
	}

	for k, a := range p.roleAssigns {
		sameName := strings.EqualFold(a.Name, req.Name)
		sameTriple := k != key && strings.EqualFold(a.Scope, req.Scope.id) &&
			strings.EqualFold(a.RoleGUID, roleGUID) && strings.EqualFold(a.PrincipalID, principalID)

		if sameName || sameTriple {
			return authErr(http.StatusConflict, assignCodeExists, "The role assignment already exists.")
		}
	}

	return nil
}

func (p *ManagedIdentityProvider) hasPrincipal(principalID string) bool {
	p.mu.RLock("hasPrincipal")
	defer p.mu.RUnlock()

	for _, i := range p.identities {
		if strings.EqualFold(i.PrincipalID, principalID) {
			return true
		}
	}

	return false
}

// checkAssignmentCondition validates an ABAC condition: conditionVersion is 2.0 (the only accepted value, defaulted
// when a condition is set; learn.microsoft.com/rest/api/authorization/role-assignments/create) and parentheses and
// brackets must balance. Conditions are stored, not evaluated.
func checkAssignmentCondition(condition, version string) (string, string, error) {
	if condition == "" {
		if version != "" {
			return "", "", authBad("The property 'conditionVersion' requires a 'condition'.")
		}

		return "", "", nil
	}

	if version == "" {
		version = assignCondVersion
	}

	if version != assignCondVersion {
		return "", "", authBad("The conditionVersion '%s' is not supported; the only accepted value is '2.0'.", version)
	}

	if !balancedCondition(condition) {
		return "", "", authBad("The condition is not valid: parentheses and brackets must be balanced.")
	}

	return condition, version, nil
}

func balancedCondition(s string) bool {
	var stack []rune

	pairs := map[rune]rune{')': '(', ']': '[', '}': '{'}
	quoted := false

	for _, r := range s {
		switch {
		case r == '\'':
			quoted = !quoted
		case quoted:
		case r == '(' || r == '[' || r == '{':
			stack = append(stack, r)
		case r == ')' || r == ']' || r == '}':
			if len(stack) == 0 || stack[len(stack)-1] != pairs[r] {
				return false
			}

			stack = stack[:len(stack)-1]
		}
	}

	return len(stack) == 0 && !quoted
}

func (p *AuthorizationProvider) checkAssignmentProps(
	props map[string]any,
	principalID string,
) (string, string, string, error) {
	if !isGUID(principalID) {
		return "", "", "", authErr(http.StatusBadRequest, assignCodePrincipal,
			"Principal %s does not exist in the directory %s. Check that you have the correct principal ID.",
			principalID, p.tenantID)
	}

	ptype := strOf(props, "principalType")
	if ptype != "" &&
		!slices.ContainsFunc(principalTypes(), func(t string) bool { return strings.EqualFold(t, ptype) }) {
		return "", "", "", authBad("The principalType '%s' is not valid. Allowed values are: %s.", ptype,
			strings.Join(principalTypes(), ", "))
	}

	condition, version, err := checkAssignmentCondition(strOf(props, "condition"), strOf(props, "conditionVersion"))

	return ptype, condition, version, err
}
