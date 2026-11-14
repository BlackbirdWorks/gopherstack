package azurearm

import (
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
)

const (
	authPolicyExemptions    = "policyExemptions"
	policyCodeExemptNotFnd  = "PolicyExemptionNotFound"
	authFnExcludeExpired    = "excludeexpired"
	authFieldPolicyAssignID = "policyAssignmentId"
	authFieldExemptCategory = "exemptionCategory"
	authFieldExpiresOn      = "expiresOn"
	authFieldRefIDs         = "policyDefinitionReferenceIds"
	authFieldScopeValidate  = "assignmentScopeValidation"
	authFieldResourceSelect = "resourceSelectors"
	exemptWaiver            = "Waiver"
	exemptMitigated         = "Mitigated"
	exemptValidateDefault   = "Default"
	exemptValidateSkip      = "DoNotValidate"
)

type storedPolicyExempt struct {
	CreatedOn time.Time      `json:"createdOn"`
	UpdatedOn time.Time      `json:"updatedOn"`
	Props     map[string]any `json:"props"`
	Name      string         `json:"name"`
	Scope     string         `json:"scope"`
}

func exemptionNotFound(name string) error {
	return authErr(http.StatusNotFound, policyCodeExemptNotFnd, "The policy exemption '%s' could not be found.", name)
}

func policyExemptionBody(e *storedPolicyExempt) map[string]any {
	props := maps.Clone(e.Props)
	if props == nil {
		props = map[string]any{}
	}

	return map[string]any{
		authFieldID: authID(e.Scope, authPolicyExemptions, e.Name), fieldName: e.Name,
		fieldType: namespaceMicrosoftAuthorization + "/" + authPolicyExemptions, fieldProperties: props,
		"systemData": map[string]any{
			ehPropCreatedAt: authStamp(e.CreatedOn), "lastModifiedAt": authStamp(e.UpdatedOn),
		},
	}
}

func (p *AuthorizationProvider) doPolicyExemption(req authReq) (authResp, error) {
	if req.Name == "" {
		if req.Method == http.MethodGet {
			return p.listPolicyExemptions(req)
		}

		return authResp{}, ErrResourceNotFound
	}

	switch req.Method {
	case http.MethodGet:
		return p.getPolicyExemption(req)
	case http.MethodPut:
		return p.putPolicyExemption(req)
	case http.MethodPatch:
		return p.patchPolicyExemption(req)
	case http.MethodDelete:
		return p.deletePolicyExemption(req)
	}

	return authResp{}, ErrResourceNotFound
}

func (p *AuthorizationProvider) getPolicyExemption(req authReq) (authResp, error) {
	p.mu.RLock("getPolicyExemption")
	defer p.mu.RUnlock()

	e, ok := p.policyExempts[authKey(req.Scope.id, req.Name)]
	if !ok {
		return authResp{}, exemptionNotFound(req.Name)
	}

	return authResp{Status: http.StatusOK, Body: policyExemptionBody(e)}, nil
}

func (p *AuthorizationProvider) deletePolicyExemption(req authReq) (authResp, error) {
	p.mu.Lock("deletePolicyExemption")
	defer p.mu.Unlock()

	key := authKey(req.Scope.id, req.Name)
	if _, ok := p.policyExempts[key]; !ok {
		return authResp{Status: http.StatusNoContent}, nil
	}

	delete(p.policyExempts, key)

	return authResp{Status: http.StatusOK}, nil
}

func (p *AuthorizationProvider) listPolicyExemptions(req authReq) (authResp, error) {
	f, err := parseAuthFilter(req.Query.Get("$filter"), []string{authFieldPolicyAssignID},
		[]string{authFnAtScope, authFnAtExactScope, authFnExcludeExpired})
	if err != nil {
		return authResp{}, err
	}

	top, err := parseTop(req)
	if err != nil {
		return authResp{}, err
	}

	assignID := f.eq[strings.ToLower(authFieldPolicyAssignID)]
	now := p.now()

	p.mu.RLock("listPolicyExemptions")
	defer p.mu.RUnlock()

	var out []map[string]any

	for _, e := range p.policyExempts {
		visible := scopeWithin(e.Scope, req.Scope.id) || scopeWithin(req.Scope.id, e.Scope)

		switch {
		case f.hasFn(authFnAtExactScope):
			visible = strings.EqualFold(strings.TrimSuffix(e.Scope, "/"), req.Scope.id)
		case f.hasFn(authFnAtScope):
			visible = scopeWithin(req.Scope.id, e.Scope)
		}

		if !visible || (assignID != "" && !strings.EqualFold(strOf(e.Props, authFieldPolicyAssignID), assignID)) ||
			(f.hasFn(authFnExcludeExpired) && exemptionExpired(e, now)) {
			continue
		}

		out = append(out, policyExemptionBody(e))
	}

	out = sortedBodies(out)
	if top > 0 && len(out) > top {
		out = out[:top]
	}

	return authResp{Status: http.StatusOK, Body: map[string]any{fieldValue: out}}, nil
}

func exemptionExpired(e *storedPolicyExempt, now time.Time) bool {
	raw := strOf(e.Props, authFieldExpiresOn)
	if raw == "" {
		return false
	}

	t, err := time.Parse(time.RFC3339, raw)

	return err == nil && !t.After(now)
}

// assignmentByID resolves a policy assignment ID to its stored record. Callers hold p.mu.
func (p *AuthorizationProvider) assignmentByID(id string) (*storedPolicyAssign, bool) {
	segs := splitARMPath(id)
	n := len(segs)

	if n < authPathListTail+1 || !strings.EqualFold(segs[n-3], namespaceMicrosoftAuthorization) ||
		!strings.EqualFold(segs[n-2], authPolicyAssignments) {
		return nil, false
	}

	scope := ""
	if n > authPathListTail+1 {
		scope = "/" + joinSegs(segs[:n-authPathListTail-1])
	}

	a, ok := p.policyAssigns[authKey(scope, segs[n-1])]

	return a, ok
}

func (p *AuthorizationProvider) validateExemption(scope string, props map[string]any) error {
	assignID := strOf(props, authFieldPolicyAssignID)
	if assignID == "" {
		return authBad("The property 'properties.policyAssignmentId' is required.")
	}

	cat := strOf(props, authFieldExemptCategory)
	if cat != exemptWaiver && cat != exemptMitigated {
		return authBad("The exemptionCategory '%s' is invalid. Allowed values are Waiver and Mitigated.", cat)
	}

	if v := strOf(props, authFieldScopeValidate); v != "" && v != exemptValidateDefault && v != exemptValidateSkip {
		return authBad(
			"The assignmentScopeValidation '%s' is invalid. Allowed values are Default and DoNotValidate.",
			v,
		)
	}

	if raw := strOf(props, authFieldExpiresOn); raw != "" {
		if _, err := time.Parse(time.RFC3339, raw); err != nil {
			return authBad("The expiresOn '%s' is not a valid UTC ISO 8601 date-time.", raw)
		}
	}

	a, ok := p.assignmentByID(assignID)
	if !ok {
		return authErr(http.StatusBadRequest, policyCodeAssignNotFnd,
			"The policy assignment '%s' could not be found.", assignID)
	}

	if strOf(props, authFieldScopeValidate) != exemptValidateSkip && !scopeWithin(scope, a.Scope) {
		return authBad("The policy exemption scope '%s' must be at or under the policy assignment scope '%s'.",
			scope, a.Scope)
	}

	return p.validateExemptionRefs(a, props, scope)
}

func (p *AuthorizationProvider) validateExemptionRefs(
	a *storedPolicyAssign, props map[string]any, scope string,
) error {
	raw, present := props[authFieldRefIDs]
	if !present {
		return nil
	}

	refs, _ := raw.([]any)

	k, ok := p.resolvePolicyRef(strOf(a.Props, authFieldPolicyDefinitionID), scope)
	if !ok || k != authPolKindSet {
		return authBad("The property 'policyDefinitionReferenceIds' applies only to assignments of a policy set " +
			"definition.")
	}

	known := p.setReferenceIDs(strOf(a.Props, authFieldPolicyDefinitionID))

	for _, r := range refs {
		id, _ := r.(string)
		if known != nil && !slices.Contains(known, id) {
			return authBad("The policy definition reference ID '%s' is not part of the assigned policy set.", id)
		}
	}

	return nil
}

// setReferenceIDs lists the policyDefinitionReferenceId values of a policy set, or nil when it cannot be read.
func (p *AuthorizationProvider) setReferenceIDs(setID string) []string {
	segs := splitARMPath(setID)
	n := len(segs)

	if n < authPathListTail+1 {
		return nil
	}

	var props map[string]any

	if n == authPathListTail+1 {
		if b, ok := p.builtinSets[strings.ToLower(segs[n-1])]; ok {
			props = b.Properties
		}
	} else if s, ok := p.policySets[authKey("/"+joinSegs(segs[:n-authPathListTail-1]), segs[n-1])]; ok {
		props = s.Props
	}

	defs, _ := props["policyDefinitions"].([]any)
	out := make([]string, 0, len(defs))

	for _, d := range defs {
		out = append(out, strOf(mapOf(d), "policyDefinitionReferenceId"))
	}

	return out
}

func (p *AuthorizationProvider) putPolicyExemption(req authReq) (authResp, error) {
	if req.Name == "" || strings.ContainsAny(req.Name, policyBadNameChars) || len(req.Name) > policyMaxName {
		return authResp{}, authBad("The policy exemption name '%s' is invalid.", req.Name)
	}

	props := maps.Clone(mapOf(req.Body[fieldProperties]))
	if props == nil {
		props = map[string]any{}
	}

	p.mu.Lock("putPolicyExemption")
	defer p.mu.Unlock()

	if err := p.validateExemption(req.Scope.id, props); err != nil {
		return authResp{}, err
	}

	now := p.now()
	key := authKey(req.Scope.id, req.Name)
	rec := &storedPolicyExempt{Name: req.Name, Scope: req.Scope.id, Props: props, CreatedOn: now, UpdatedOn: now}
	status := http.StatusCreated

	if cur, ok := p.policyExempts[key]; ok {
		rec.CreatedOn, status = cur.CreatedOn, http.StatusOK
	}

	p.policyExempts[key] = rec

	return authResp{Status: status, Body: policyExemptionBody(rec)}, nil
}

// patchPolicyExemption updates the only mutable properties: assignmentScopeValidation and resourceSelectors.
func (p *AuthorizationProvider) patchPolicyExemption(req authReq) (authResp, error) {
	in := mapOf(req.Body[fieldProperties])

	p.mu.Lock("patchPolicyExemption")
	defer p.mu.Unlock()

	cur, ok := p.policyExempts[authKey(req.Scope.id, req.Name)]
	if !ok {
		return authResp{}, exemptionNotFound(req.Name)
	}

	next := maps.Clone(cur.Props)

	for _, k := range []string{authFieldScopeValidate, authFieldResourceSelect} {
		if v, present := in[k]; present {
			next[k] = v
		}
	}

	if err := p.validateExemption(cur.Scope, next); err != nil {
		return authResp{}, err
	}

	rec := *cur
	rec.Props, rec.UpdatedOn = next, p.now()
	p.policyExempts[authKey(req.Scope.id, req.Name)] = &rec

	return authResp{Status: http.StatusOK, Body: policyExemptionBody(&rec)}, nil
}
