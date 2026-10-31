package azurearm

import (
	"maps"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	policyCodeDefNotFound  = "PolicyDefinitionNotFound"
	policyCodeSetNotFound  = "PolicySetDefinitionNotFound"
	policyCodeAssignNotFnd = "PolicyAssignmentNotFound"
	policyCodeRule         = "InvalidPolicyRule"
	policyBadNameChars     = `<>*%&:\?.+/`
	policyMaxName          = 128
	policyModeIndexed      = "Indexed"
	policyEnforceDefault   = "Default"
	policyEnforceNone      = "DoNotEnforce"
	identitySystemAssigned = "SystemAssigned"
)

type authPolKind int

const (
	authPolKindDef authPolKind = iota
	authPolKindSet
)

func (k authPolKind) typeName() string {
	if k == authPolKindSet {
		return authPolicySets
	}

	return authPolicyDefinitions
}

func (p *AuthorizationProvider) storeFor(k authPolKind) map[string]*storedPolicy {
	if k == authPolKindSet {
		return p.policySets
	}

	return p.policyDefs
}

func (p *AuthorizationProvider) builtinFor(k authPolKind) map[string]*builtinPolicy {
	if k == authPolKindSet {
		return p.builtinSets
	}

	return p.builtinPols
}

func (k authPolKind) notFound(name string) error {
	if k == authPolKindSet {
		return authErr(http.StatusNotFound, policyCodeSetNotFound,
			"The policy set definition '%s' could not be found.", name)
	}

	return authErr(http.StatusNotFound, policyCodeDefNotFound, "The policy definition '%s' could not be found.", name)
}

func builtinPolicyBody(k authPolKind, b *builtinPolicy) map[string]any {
	return map[string]any{
		authFieldID: b.ID, fieldName: b.Name, fieldType: namespaceMicrosoftAuthorization + "/" + k.typeName(),
		fieldProperties: b.Properties,
	}
}

func customPolicyBody(k authPolKind, s *storedPolicy) map[string]any {
	props := maps.Clone(s.Props)
	props[authFieldPolicyType] = authPolicyCustom
	props[authFieldMetadata] = mergeMeta(props[authFieldMetadata], s)

	return map[string]any{
		authFieldID: authID(s.Scope, k.typeName(), s.Name), fieldName: s.Name,
		fieldType: namespaceMicrosoftAuthorization + "/" + k.typeName(), fieldProperties: props,
	}
}

func mergeMeta(raw any, s *storedPolicy) map[string]any {
	out := map[string]any{}
	maps.Copy(out, mapOf(raw))
	out[authFieldCreatedOn] = authStamp(s.CreatedOn)
	out[authFieldUpdatedOn] = authStamp(s.UpdatedOn)

	return out
}

func (p *AuthorizationProvider) doPolicy(req authReq, k authPolKind) (authResp, error) {
	if req.Scope.kind == authScopeResourceGroup || req.Scope.kind == authScopeResource {
		return authResp{}, authErr(http.StatusBadRequest, authCodeScope,
			"Policy definitions can only be defined at the tenant, management group or subscription scope.")
	}

	if req.Name == "" {
		if req.Method == http.MethodGet {
			return p.listPolicies(req, k)
		}

		return authResp{}, ErrResourceNotFound
	}

	switch req.Method {
	case http.MethodGet:
		return p.getPolicy(req, k)
	case http.MethodPut:
		return p.putPolicy(req, k)
	case http.MethodDelete:
		return p.deletePolicy(req, k)
	}

	return authResp{}, ErrResourceNotFound
}

func (p *AuthorizationProvider) getPolicy(req authReq, k authPolKind) (authResp, error) {
	p.mu.RLock("getPolicy")
	defer p.mu.RUnlock()

	if req.Scope.kind == authScopeTenant {
		if b, ok := p.builtinFor(k)[strings.ToLower(req.Name)]; ok {
			return authResp{Status: http.StatusOK, Body: builtinPolicyBody(k, b)}, nil
		}

		return authResp{}, k.notFound(req.Name)
	}

	if s, ok := p.storeFor(k)[authKey(req.Scope.id, req.Name)]; ok {
		return authResp{Status: http.StatusOK, Body: customPolicyBody(k, s)}, nil
	}

	return authResp{}, k.notFound(req.Name)
}

func (p *AuthorizationProvider) listPolicies(req authReq, k authPolKind) (authResp, error) {
	f, err := parseAuthFilter(req.Query.Get("$filter"), []string{authFieldPolicyType}, []string{authFnAtExactScope})
	if err != nil {
		return authResp{}, err
	}

	want := f.eq[strings.ToLower(authFieldPolicyType)]

	top, err := parseTop(req)
	if err != nil {
		return authResp{}, err
	}

	p.mu.RLock("listPolicies")
	defer p.mu.RUnlock()

	var out []map[string]any

	if (want == "" || strings.EqualFold(want, authPolicyBuiltIn)) &&
		(req.Scope.kind == authScopeTenant || !f.hasFn(authFnAtExactScope)) {
		for _, b := range p.builtinFor(k) {
			out = append(out, builtinPolicyBody(k, b))
		}
	}

	if req.Scope.kind != authScopeTenant && (want == "" || strings.EqualFold(want, authPolicyCustom)) {
		for _, s := range p.storeFor(k) {
			if strings.EqualFold(strings.TrimSuffix(s.Scope, "/"), req.Scope.id) {
				out = append(out, customPolicyBody(k, s))
			}
		}
	}

	out = sortedBodies(out)
	if top > 0 && len(out) > top {
		out = out[:top]
	}

	return authResp{Status: http.StatusOK, Body: map[string]any{fieldValue: out}}, nil
}

func parseTop(req authReq) (int, error) {
	raw := req.Query.Get("$top")
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, authBad("The $top value '%s' is not a positive integer.", raw)
	}

	return n, nil
}

func validPolicyName(name string) bool {
	return name != "" && len(name) <= policyMaxName && !strings.ContainsAny(name, policyBadNameChars) &&
		!strings.HasSuffix(name, " ")
}

func (p *AuthorizationProvider) putPolicy(req authReq, k authPolKind) (authResp, error) {
	if req.Scope.kind == authScopeTenant {
		return authResp{}, authErr(http.StatusBadRequest, authCodeScope,
			"Custom policy definitions cannot be created at the tenant root scope.")
	}

	if !validPolicyName(req.Name) {
		return authResp{}, authBad("The policy name '%s' is invalid.", req.Name)
	}

	props := mapOf(req.Body[fieldProperties])
	if t := strOf(props, authFieldPolicyType); t != "" && !strings.EqualFold(t, authPolicyCustom) {
		return authResp{}, authBad("The policyType '%s' is not valid; only Custom can be created.", t)
	}

	clean := maps.Clone(props)
	delete(clean, authFieldPolicyType)

	p.mu.Lock("putPolicy")
	defer p.mu.Unlock()

	if err := p.validatePolicy(k, clean, req.Scope.id); err != nil {
		return authResp{}, err
	}

	now := p.now()
	rec := &storedPolicy{Name: req.Name, Scope: req.Scope.id, Props: clean, CreatedOn: now, UpdatedOn: now}
	key := authKey(req.Scope.id, req.Name)

	status := http.StatusCreated

	if cur, ok := p.storeFor(k)[key]; ok {
		rec.CreatedOn = cur.CreatedOn

		if k == authPolKindSet {
			status = http.StatusOK
		}
	}

	p.storeFor(k)[key] = rec

	return authResp{Status: status, Body: customPolicyBody(k, rec)}, nil
}

func (p *AuthorizationProvider) validatePolicy(k authPolKind, props map[string]any, scope string) error {
	if raw, ok := props[authFieldParameters]; ok && raw != nil {
		if _, isMap := raw.(map[string]any); !isMap {
			return authBad("The property 'properties.parameters' must be an object.")
		}
	}

	if k == authPolKindSet {
		return p.validatePolicySet(props, scope)
	}

	rule, ok := props["policyRule"].(map[string]any)
	if !ok || rule["if"] == nil || strOf(mapOf(rule["then"]), "effect") == "" {
		return authErr(http.StatusBadRequest, policyCodeRule,
			"The policy rule must contain an 'if' condition and a 'then' block with an 'effect'.")
	}

	if _, has := props["mode"]; !has {
		props["mode"] = policyModeIndexed
	}

	return nil
}

func (p *AuthorizationProvider) validatePolicySet(props map[string]any, scope string) error {
	refs, _ := props["policyDefinitions"].([]any)
	if len(refs) == 0 {
		return authBad("The property 'properties.policyDefinitions' must contain at least one policy definition.")
	}

	seen := map[string]bool{}

	for _, r := range refs {
		ref := mapOf(r)
		id := strOf(ref, authFieldPolicyDefinitionID)

		if kind, ok := p.resolvePolicyRef(id, scope); !ok || kind != authPolKindDef {
			return authErr(http.StatusBadRequest, policyCodeDefNotFound,
				"The policy definition '%s' could not be found.", id)
		}

		if refID := strOf(ref, "policyDefinitionReferenceId"); refID != "" {
			if seen[strings.ToLower(refID)] {
				return authBad("The policyDefinitionReferenceId '%s' is used more than once.", refID)
			}

			seen[strings.ToLower(refID)] = true
		}
	}

	return nil
}

// resolvePolicyRef finds the policy or policy set definition id names, as seen from scope.
func (p *AuthorizationProvider) resolvePolicyRef(id, from string) (authPolKind, bool) {
	segs := splitARMPath(id)
	n := len(segs)

	if n < authPathListTail+1 || !strings.EqualFold(segs[n-3], namespaceMicrosoftAuthorization) {
		return 0, false
	}

	var k authPolKind

	switch {
	case strings.EqualFold(segs[n-2], authPolicyDefinitions):
		k = authPolKindDef
	case strings.EqualFold(segs[n-2], authPolicySets):
		k = authPolKindSet
	default:
		return 0, false
	}

	name, scope := segs[n-1], ""
	if n > authPathListTail+1 {
		scope = "/" + joinSegs(segs[:n-authPathListTail-1])
	}

	if scope == "" {
		_, ok := p.builtinFor(k)[strings.ToLower(name)]

		return k, ok
	}

	_, ok := p.storeFor(k)[authKey(scope, name)]

	return k, ok && scopeWithin(from, scope)
}

func (p *AuthorizationProvider) deletePolicy(req authReq, k authPolKind) (authResp, error) {
	if req.Scope.kind == authScopeTenant {
		return authResp{}, authBad("Built-in policy definitions cannot be deleted.")
	}

	p.mu.Lock("deletePolicy")
	defer p.mu.Unlock()

	key := authKey(req.Scope.id, req.Name)

	s, ok := p.storeFor(k)[key]
	if !ok {
		return authResp{Status: http.StatusNoContent}, nil
	}

	delete(p.storeFor(k), key)

	return authResp{Status: http.StatusOK, Body: customPolicyBody(k, s)}, nil
}

func (p *AuthorizationProvider) policyAssignmentBody(a *storedPolicyAssign) map[string]any {
	props := maps.Clone(a.Props)
	props[authFieldScope] = a.Scope

	if strOf(props, "enforcementMode") == "" {
		props["enforcementMode"] = policyEnforceDefault
	}

	if _, ok := props["notScopes"]; !ok {
		props["notScopes"] = []any{}
	}

	out := map[string]any{
		authFieldID: authID(a.Scope, authPolicyAssignments, a.Name), fieldName: a.Name,
		fieldType: namespaceMicrosoftAuthorization + "/" + authPolicyAssignments, fieldProperties: props,
	}

	if a.Location != "" {
		out[fieldLocation] = a.Location
	}

	if ident := mapOf(a.Extra[authFieldIdentity]); ident != nil {
		ident = maps.Clone(ident)
		if a.PrincipalID != "" {
			ident[authFieldPrincipalID], ident["tenantId"] = a.PrincipalID, p.tenantID
		}

		out[authFieldIdentity] = ident
	}

	return out
}

func (p *AuthorizationProvider) doPolicyAssignment(req authReq) (authResp, error) {
	if req.Name == "" {
		if req.Method == http.MethodGet {
			return p.listPolicyAssignments(req)
		}

		return authResp{}, ErrResourceNotFound
	}

	switch req.Method {
	case http.MethodGet:
		return p.getPolicyAssignment(req)
	case http.MethodPut:
		return p.putPolicyAssignment(req)
	case http.MethodPatch:
		return p.patchPolicyAssignment(req)
	case http.MethodDelete:
		return p.deletePolicyAssignment(req)
	}

	return authResp{}, ErrResourceNotFound
}

func assignmentNotFound(name string) error {
	return authErr(http.StatusNotFound, policyCodeAssignNotFnd, "The policy assignment '%s' could not be found.", name)
}

func (p *AuthorizationProvider) getPolicyAssignment(req authReq) (authResp, error) {
	p.mu.RLock("getPolicyAssignment")
	defer p.mu.RUnlock()

	a, ok := p.policyAssigns[authKey(req.Scope.id, req.Name)]
	if !ok {
		return authResp{}, assignmentNotFound(req.Name)
	}

	return authResp{Status: http.StatusOK, Body: p.policyAssignmentBody(a)}, nil
}

func (p *AuthorizationProvider) deletePolicyAssignment(req authReq) (authResp, error) {
	p.mu.Lock("deletePolicyAssignment")
	defer p.mu.Unlock()

	key := authKey(req.Scope.id, req.Name)

	a, ok := p.policyAssigns[key]
	if !ok {
		return authResp{Status: http.StatusNoContent}, nil
	}

	delete(p.policyAssigns, key)

	return authResp{Status: http.StatusOK, Body: p.policyAssignmentBody(a)}, nil
}

func (p *AuthorizationProvider) listPolicyAssignments(req authReq) (authResp, error) {
	f, err := parseAuthFilter(req.Query.Get("$filter"), []string{authFieldPolicyDefinitionID},
		[]string{authFnAtScope, authFnAtExactScope})
	if err != nil {
		return authResp{}, err
	}

	top, err := parseTop(req)
	if err != nil {
		return authResp{}, err
	}

	defID := f.eq[strings.ToLower(authFieldPolicyDefinitionID)]

	p.mu.RLock("listPolicyAssignments")
	defer p.mu.RUnlock()

	var out []map[string]any

	for _, a := range p.policyAssigns {
		visible := scopeWithin(a.Scope, req.Scope.id) || scopeWithin(req.Scope.id, a.Scope)

		switch {
		case f.hasFn(authFnAtExactScope):
			visible = strings.EqualFold(strings.TrimSuffix(a.Scope, "/"), req.Scope.id)
		case f.hasFn(authFnAtScope):
			visible = scopeWithin(req.Scope.id, a.Scope)
		}

		if visible && (defID == "" || strings.EqualFold(strOf(a.Props, authFieldPolicyDefinitionID), defID)) {
			out = append(out, p.policyAssignmentBody(a))
		}
	}

	out = sortedBodies(out)
	if top > 0 && len(out) > top {
		out = out[:top]
	}

	return authResp{Status: http.StatusOK, Body: map[string]any{fieldValue: out}}, nil
}

func (p *AuthorizationProvider) putPolicyAssignment(req authReq) (authResp, error) {
	if req.Name == "" || strings.ContainsAny(req.Name, policyBadNameChars) || len(req.Name) > policyMaxName {
		return authResp{}, authBad("The policy assignment name '%s' is invalid.", req.Name)
	}

	props := maps.Clone(mapOf(req.Body[fieldProperties]))
	if props == nil {
		props = map[string]any{}
	}

	defID := strOf(props, authFieldPolicyDefinitionID)
	if defID == "" {
		return authResp{}, authBad("The property 'properties.policyDefinitionId' is required.")
	}

	if mode := strOf(props, "enforcementMode"); mode != "" && mode != policyEnforceDefault && mode != policyEnforceNone {
		return authResp{}, authBad("The enforcementMode '%s' is invalid. Allowed values are Default and DoNotEnforce.",
			mode)
	}

	delete(props, authFieldScope)

	p.mu.Lock("putPolicyAssignment")
	defer p.mu.Unlock()

	if _, ok := p.resolvePolicyRef(defID, req.Scope.id); !ok {
		return authResp{}, authErr(http.StatusBadRequest, policyCodeDefNotFound,
			"The policy definition '%s' could not be found.", defID)
	}

	now := p.now()
	rec := &storedPolicyAssign{
		Name: req.Name, Scope: req.Scope.id, Props: props, CreatedOn: now, UpdatedOn: now,
		Location: strOf(req.Body, fieldLocation),
	}

	key := authKey(req.Scope.id, req.Name)
	cur := p.policyAssigns[key]

	if cur != nil {
		rec.CreatedOn = cur.CreatedOn
	}

	if err := p.applyAssignmentIdentity(rec, cur, req.Body); err != nil {
		return authResp{}, err
	}

	p.policyAssigns[key] = rec

	return authResp{Status: http.StatusCreated, Body: p.policyAssignmentBody(rec)}, nil
}

// applyAssignmentIdentity records the assignment's managed identity; a system-assigned principal survives updates.
func (p *AuthorizationProvider) applyAssignmentIdentity(rec, prev *storedPolicyAssign, body map[string]any) error {
	raw, ok := body[authFieldIdentity]
	if !ok || raw == nil {
		return nil
	}

	ident := mapOf(raw)
	typ := strOf(ident, fieldType)

	switch {
	case strings.EqualFold(typ, identitySystemAssigned):
		typ = identitySystemAssigned
	case strings.EqualFold(typ, identityUser), strings.EqualFold(typ, identityNone):
	default:
		return authBad("The identity type '%s' is invalid. Allowed values are SystemAssigned, UserAssigned and None.",
			typ)
	}

	out := map[string]any{fieldType: typ}
	if uai, has := ident[fieldUserAssigned]; has {
		out[fieldUserAssigned] = uai
	}

	rec.IdentityTyp, rec.Extra = typ, map[string]any{authFieldIdentity: out}

	if typ == identitySystemAssigned {
		rec.PrincipalID = uuid.NewString()
		if prev != nil && prev.PrincipalID != "" {
			rec.PrincipalID = prev.PrincipalID
		}
	}

	return nil
}

func (p *AuthorizationProvider) patchPolicyAssignment(req authReq) (authResp, error) {
	p.mu.Lock("patchPolicyAssignment")
	defer p.mu.Unlock()

	cur, ok := p.policyAssigns[authKey(req.Scope.id, req.Name)]
	if !ok {
		return authResp{}, assignmentNotFound(req.Name)
	}

	rec := *cur
	rec.Props = maps.Clone(cur.Props)
	rec.UpdatedOn = p.now()

	if loc := strOf(req.Body, fieldLocation); loc != "" {
		rec.Location = loc
	}

	for _, f := range []string{"resourceSelectors", "overrides"} {
		if v, has := mapOf(req.Body[fieldProperties])[f]; has {
			rec.Props[f] = v
		}
	}

	if err := p.applyAssignmentIdentity(&rec, cur, req.Body); err != nil {
		return authResp{}, err
	}

	p.policyAssigns[authKey(req.Scope.id, req.Name)] = &rec

	return authResp{Status: http.StatusOK, Body: p.policyAssignmentBody(&rec)}, nil
}
