package azurearm

import (
	"context"
	_ "embed" // role and policy catalogs
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	namespaceMicrosoftAuthorization = "Microsoft.Authorization"
	authRoleDefinitions             = "roleDefinitions"
	authRoleAssignments             = "roleAssignments"
	authLocks                       = "locks"
	authPolicyDefinitions           = "policyDefinitions"
	authPolicySets                  = "policySetDefinitions"
	authPolicyAssignments           = "policyAssignments"
	authCodeBad                     = "InvalidRequestContent"
	authCodeFilter                  = "InvalidFilter"
	authCodeScope                   = "InvalidScope"
	authCodeNotFound                = armErrorCodeResourceNotFound
	authCodeConflict                = "Conflict"
	authPathProviders               = "/providers/" + namespaceMicrosoftAuthorization + "/"
	authFieldID                     = "id"
	authFieldLevel                  = "level"
	authFieldScope                  = "scope"
	authFieldDescription            = "description"
	authFieldDisplayName            = "displayName"
	authFieldIdentity               = "identity"
	authFieldMetadata               = "metadata"
	authFieldParameters             = "parameters"
	authFieldPolicyType             = "policyType"
	authFieldRoleName               = "roleName"
	authFieldRoleType               = "roleType"
	authFieldCreatedOn              = "createdOn"
	authFieldUpdatedOn              = "updatedOn"
	authFieldPrincipalID            = "principalId"
	authFieldPermissions            = "permissions"
	authVerbAction                  = "action"
	authFieldPolicyDefinitionID     = "policyDefinitionId"
	authTypeBuiltIn                 = "BuiltInRole"
	authTypeCustom                  = "CustomRole"
	authPolicyBuiltIn               = "BuiltIn"
	authPolicyCustom                = "Custom"
	authFnAtScope                   = "atscope"
	authFnAtExactScope              = "atexactscope"
	authFnAtScopeAndBelow           = "atscopeandbelow"
	authFnAssignedTo                = "assignedto"
	authGUIDLen                     = 36
)

//go:embed authcatalog/roles.json
var authRolesJSON []byte

//go:embed authcatalog/policies.json
var authPoliciesJSON []byte

func authAPIVersions() []string {
	return []string{"2022-04-01", "2020-05-01", "2016-09-01", "2025-03-01", "2021-06-01"}
}

// authPermission is one permissions block of a role definition.
type authPermission struct {
	Actions        []string `json:"actions"`
	NotActions     []string `json:"notActions"`
	DataActions    []string `json:"dataActions"`
	NotDataActions []string `json:"notDataActions"`
}

type builtinRole struct {
	Name        string           `json:"name"`
	RoleName    string           `json:"roleName"`
	Description string           `json:"description"`
	Permissions []authPermission `json:"permissions"`
}

type builtinPolicyCatalog struct {
	Definitions []builtinPolicy `json:"definitions"`
	Sets        []builtinPolicy `json:"sets"`
}

type builtinPolicy struct {
	Properties map[string]any `json:"properties"`
	ID         string         `json:"id"`
	Name       string         `json:"name"`
}

type storedRoleDef struct {
	CreatedOn        time.Time        `json:"createdOn"`
	UpdatedOn        time.Time        `json:"updatedOn"`
	Name             string           `json:"name"`
	RoleName         string           `json:"roleName"`
	Description      string           `json:"description"`
	Scope            string           `json:"scope"`
	AssignableScopes []string         `json:"assignableScopes"`
	Permissions      []authPermission `json:"permissions"`
}

type storedRoleAssign struct {
	CreatedOn        time.Time `json:"createdOn"`
	UpdatedOn        time.Time `json:"updatedOn"`
	Name             string    `json:"name"`
	Scope            string    `json:"scope"`
	RoleDefinitionID string    `json:"roleDefinitionId"`
	RoleGUID         string    `json:"roleGuid"`
	PrincipalID      string    `json:"principalId"`
	PrincipalType    string    `json:"principalType,omitempty"`
	Condition        string    `json:"condition,omitempty"`
	ConditionVersion string    `json:"conditionVersion,omitempty"`
	Description      string    `json:"description,omitempty"`
	DelegatedMI      string    `json:"delegatedManagedIdentityResourceId,omitempty"`
}

type storedLock struct {
	Name   string   `json:"name"`
	Scope  string   `json:"scope"`
	Level  string   `json:"level"`
	Notes  string   `json:"notes,omitempty"`
	Owners []string `json:"owners,omitempty"`
}

type storedPolicy struct {
	CreatedOn time.Time      `json:"createdOn"`
	UpdatedOn time.Time      `json:"updatedOn"`
	Props     map[string]any `json:"props"`
	Name      string         `json:"name"`
	Scope     string         `json:"scope"`
}

type storedPolicyAssign struct {
	CreatedOn   time.Time      `json:"createdOn"`
	UpdatedOn   time.Time      `json:"updatedOn"`
	Props       map[string]any `json:"props"`
	Extra       map[string]any `json:"extra,omitempty"`
	Name        string         `json:"name"`
	Scope       string         `json:"scope"`
	Location    string         `json:"location,omitempty"`
	IdentityTyp string         `json:"identityType,omitempty"`
	PrincipalID string         `json:"principalId,omitempty"`
}

// AuthorizationProvider implements ResourceProvider for Microsoft.Authorization: role definitions and assignments,
// management locks and policy definitions, set definitions and assignments. Requests are served from the scope-aware
// routes in rp_authorization_routes.go; the generic ResourceProvider methods exist for the provider registry only.
type AuthorizationProvider struct {
	mu            *lockmetrics.RWMutex
	roleDefs      map[string]*storedRoleDef
	roleAssigns   map[string]*storedRoleAssign
	locks         map[string]*storedLock
	policyDefs    map[string]*storedPolicy
	policySets    map[string]*storedPolicy
	policyAssigns map[string]*storedPolicyAssign
	builtinRoles  map[string]*builtinRole
	builtinPols   map[string]*builtinPolicy
	builtinSets   map[string]*builtinPolicy
	msi           *ManagedIdentityProvider
	now           func() time.Time
	tenantID      string
}

var _ ResourceProvider = (*AuthorizationProvider)(nil)

// NewAuthorizationProvider creates a provider with the built-in role and policy catalogs loaded. msi, when set, lets
// role assignments recognise managed identity principals.
func NewAuthorizationProvider(tenantID string, msi *ManagedIdentityProvider) *AuthorizationProvider {
	if tenantID == "" {
		tenantID = DefaultTenantID
	}

	p := &AuthorizationProvider{
		mu: lockmetrics.New("azurearm.authorization"), msi: msi, tenantID: tenantID, now: time.Now,
		builtinRoles: map[string]*builtinRole{}, builtinPols: map[string]*builtinPolicy{},
		builtinSets: map[string]*builtinPolicy{},
	}
	p.resetMaps()

	var roles []*builtinRole
	if err := json.Unmarshal(authRolesJSON, &roles); err == nil {
		for _, r := range roles {
			p.builtinRoles[strings.ToLower(r.Name)] = r
		}
	}

	var cat builtinPolicyCatalog
	if err := json.Unmarshal(authPoliciesJSON, &cat); err == nil {
		for i := range cat.Definitions {
			p.builtinPols[strings.ToLower(cat.Definitions[i].Name)] = &cat.Definitions[i]
		}

		for i := range cat.Sets {
			p.builtinSets[strings.ToLower(cat.Sets[i].Name)] = &cat.Sets[i]
		}
	}

	return p
}

func (p *AuthorizationProvider) resetMaps() {
	p.roleDefs = map[string]*storedRoleDef{}
	p.roleAssigns = map[string]*storedRoleAssign{}
	p.locks = map[string]*storedLock{}
	p.policyDefs = map[string]*storedPolicy{}
	p.policySets = map[string]*storedPolicy{}
	p.policyAssigns = map[string]*storedPolicyAssign{}
}

// Namespace implements ResourceProvider.
func (p *AuthorizationProvider) Namespace() string { return namespaceMicrosoftAuthorization }

// ResourceTypes implements ResourceProvider.
func (p *AuthorizationProvider) ResourceTypes() []ResourceTypeDef {
	v := authAPIVersions()

	return []ResourceTypeDef{
		{Type: authRoleDefinitions, APIVersions: v},
		{Type: authRoleAssignments, APIVersions: v},
		{Type: authLocks, APIVersions: v},
		{Type: authPolicyDefinitions, APIVersions: v},
		{Type: authPolicySets, APIVersions: v},
		{Type: authPolicyAssignments, APIVersions: v},
	}
}

// Put implements ResourceProvider; Microsoft.Authorization is served by the scope routes.
func (p *AuthorizationProvider) Put(context.Context, ResourceID, map[string]any) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Get implements ResourceProvider.
func (p *AuthorizationProvider) Get(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Delete implements ResourceProvider.
func (p *AuthorizationProvider) Delete(context.Context, ResourceID) error { return ErrResourceNotFound }

// List implements ResourceProvider.
func (p *AuthorizationProvider) List(context.Context, ResourceID) ([]map[string]any, error) {
	return nil, ErrResourceNotFound
}

// ListKeys implements ResourceProvider.
func (p *AuthorizationProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *AuthorizationProvider) Reset() {
	p.mu.Lock("Reset")
	p.resetMaps()
	p.mu.Unlock()
}

// DeleteResourcesInGroup implements ResourceProvider: locks, role assignments and policy assignments scoped to the
// group or to its resources go with it.
func (p *AuthorizationProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.removeWhere(func(scope string) bool {
		segs := splitARMPath(scope)

		return len(segs) > rgPathSegs-1 && strings.EqualFold(segs[2], resourceGroupsSegment) &&
			strings.EqualFold(segs[3], resourceGroup)
	})
}

// RemoveUnder drops locks, role assignments and policy assignments scoped to path or beneath it, after the resource
// at path was deleted.
func (p *AuthorizationProvider) RemoveUnder(path string) {
	p.removeWhere(func(scope string) bool { return scopeWithin(scope, path) })
}

func (p *AuthorizationProvider) removeWhere(match func(scope string) bool) {
	p.mu.Lock("removeWhere")
	defer p.mu.Unlock()

	for k, l := range p.locks {
		if match(l.Scope) {
			delete(p.locks, k)
		}
	}

	for k, a := range p.roleAssigns {
		if match(a.Scope) {
			delete(p.roleAssigns, k)
		}
	}

	for k, a := range p.policyAssigns {
		if match(a.Scope) {
			delete(p.policyAssigns, k)
		}
	}
}

// authReq is one Microsoft.Authorization request at a scope.
type authReq struct {
	Query  url.Values
	Body   map[string]any
	Type   string
	Name   string
	Method string
	Scope  authScope
}

// authResp is a provider reply; a nil Body means an empty response.
type authResp struct {
	Body   any
	Status int
}

// Do serves one request.
func (p *AuthorizationProvider) Do(req authReq) (authResp, error) {
	switch req.Type {
	case authRoleDefinitions:
		return p.doRoleDefinition(req)
	case authRoleAssignments:
		return p.doRoleAssignment(req)
	case authLocks:
		return p.doLock(req)
	case authPolicyDefinitions:
		return p.doPolicy(req, authPolKindDef)
	case authPolicySets:
		return p.doPolicy(req, authPolKindSet)
	case authPolicyAssignments:
		return p.doPolicyAssignment(req)
	}

	return authResp{}, ErrResourceNotFound
}

func authErr(status int, code, format string, args ...any) error {
	return netErr(status, code, format, args...)
}

func authBad(format string, args ...any) error {
	return authErr(http.StatusBadRequest, authCodeBad, format, args...)
}

// scopeWithin reports whether child is parent or lies beneath it; the tenant root contains every scope.
func scopeWithin(child, parent string) bool {
	parent = strings.TrimSuffix(strings.ToLower(parent), "/")
	child = strings.TrimSuffix(strings.ToLower(child), "/")

	return parent == "" || child == parent || strings.HasPrefix(child, parent+"/")
}

func isGUID(s string) bool {
	if len(s) != authGUIDLen {
		return false
	}

	_, err := uuid.Parse(s)

	return err == nil
}

func authStamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func authID(scope, typ, name string) string {
	return strings.TrimSuffix(scope, "/") + authPathProviders + typ + "/" + name
}

func authKey(scope, name string) string {
	return strings.ToLower(strings.TrimSuffix(scope, "/")) + "|" + strings.ToLower(name)
}

func sortedBodies(items []map[string]any) []map[string]any {
	sort.Slice(items, func(i, j int) bool {
		return strings.ToLower(strOf(items[i], authFieldID)) < strings.ToLower(strOf(items[j], authFieldID))
	})

	if items == nil {
		return []map[string]any{}
	}

	return items
}

func listBody(items []map[string]any) authResp {
	return authResp{Status: http.StatusOK, Body: map[string]any{fieldValue: sortedBodies(items)}}
}

var (
	authTermEq = regexp.MustCompile(`^(\w+)\s+eq\s+'([^']*)'$`)
	authTermFn = regexp.MustCompile(`^(\w+)\(\s*(?:'([^']*)')?\s*\)$`)
	authAnd    = regexp.MustCompile(`(?i)\s+and\s+`)
)

// authFilter is a parsed $filter: lower-cased property equalities and function calls.
type authFilter struct {
	eq  map[string]string
	fns map[string]string
}

func (f authFilter) hasFn(name string) bool {
	_, ok := f.fns[name]

	return ok
}

func parseAuthFilter(raw string, eqProps, fns []string) (authFilter, error) {
	f := authFilter{eq: map[string]string{}, fns: map[string]string{}}
	if strings.TrimSpace(raw) == "" {
		return f, nil
	}

	for _, term := range authAnd.Split(strings.TrimSpace(raw), -1) {
		term = strings.TrimSpace(term)

		if m := authTermEq.FindStringSubmatch(term); m != nil && strs.ContainsFold(eqProps, m[1]) {
			f.eq[strings.ToLower(m[1])] = m[2]

			continue
		}

		if m := authTermFn.FindStringSubmatch(term); m != nil && strs.ContainsFold(fns, m[1]) {
			f.fns[strings.ToLower(m[1])] = m[2]

			continue
		}

		return f, authErr(http.StatusBadRequest, authCodeFilter, "The filter '%s' is not supported.", raw)
	}

	return f, nil
}
