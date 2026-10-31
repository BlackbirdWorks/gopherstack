package azurearm

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type authScopeKind int

const (
	authScopeTenant authScopeKind = iota
	authScopeManagementGroup
	authScopeSubscription
	authScopeResourceGroup
	authScopeResource
)

const (
	authMgmtGroupSegs  = 4
	authSubResourceMin = 6
	authRGResourceMin  = 8
	authPathItemTail   = 4
	authPathListTail   = 3
)

// authScope is the scope an authorization resource lives at: the tenant root, a management group, a subscription,
// a resource group or a resource.
type authScope struct {
	id   string
	sub  string
	rg   string
	kind authScopeKind
}

type authPath struct {
	typ   string
	name  string
	scope []string
}

func authKnownType(t string) bool {
	switch strings.ToLower(t) {
	case strings.ToLower(authRoleDefinitions), strings.ToLower(authRoleAssignments), strings.ToLower(authLocks),
		strings.ToLower(authPolicyDefinitions), strings.ToLower(authPolicySets), strings.ToLower(authPolicyAssignments):
		return true
	}

	return false
}

func canonicalAuthType(t string) string {
	for _, c := range []string{
		authRoleDefinitions, authRoleAssignments, authLocks, authPolicyDefinitions, authPolicySets,
		authPolicyAssignments,
	} {
		if strings.EqualFold(c, t) {
			return c
		}
	}

	return t
}

// parseAuthPath recognises {scope}/providers/Microsoft.Authorization/{type}[/{name}].
func parseAuthPath(segs []string) (authPath, bool) {
	for _, tail := range []int{authPathItemTail, authPathListTail} {
		i := len(segs) - tail
		if i < 0 || !strings.EqualFold(segs[i], providersSegment) ||
			!strings.EqualFold(segs[i+1], namespaceMicrosoftAuthorization) || !authKnownType(segs[i+2]) {
			continue
		}

		ap := authPath{scope: segs[:i], typ: canonicalAuthType(segs[i+2])}
		if tail == authPathItemTail {
			ap.name = segs[i+3]
		}

		return ap, true
	}

	return authPath{}, false
}

// parseAuthScope classifies scope segments; ok is false for a shape that is no ARM scope.
func parseAuthScope(segs []string) (authScope, bool) {
	out := authScope{id: ""}
	if len(segs) > 0 {
		out.id = "/" + joinSegs(segs)
	}

	switch {
	case len(segs) == 0:
		return out, true
	case len(segs) == authMgmtGroupSegs && strings.EqualFold(segs[0], providersSegment) &&
		strings.EqualFold(segs[1], "Microsoft.Management") && strings.EqualFold(segs[2], "managementGroups"):
		out.kind = authScopeManagementGroup

		return out, true
	case !strings.EqualFold(segs[0], subscriptionsSegment) || len(segs) < minResourceSegs:
		return out, false
	}

	out.sub = segs[1]

	switch {
	case len(segs) == minResourceSegs:
		out.kind = authScopeSubscription
	case len(segs) == rgPathSegs && strings.EqualFold(segs[2], resourceGroupsSegment):
		out.kind, out.rg = authScopeResourceGroup, segs[3]
	case len(segs) >= authSubResourceMin && strings.EqualFold(segs[2], providersSegment) &&
		(len(segs)-rgPathSegs)%typeNamePairWidth == 0:
		out.kind = authScopeResource
	case len(segs) >= authRGResourceMin && strings.EqualFold(segs[2], resourceGroupsSegment) &&
		strings.EqualFold(segs[4], providersSegment) && (len(segs)-authSubResourceMin)%typeNamePairWidth == 0:
		out.kind, out.rg = authScopeResource, segs[3]
	default:
		return out, false
	}

	return out, true
}

//nolint:gochecknoglobals // static route table, read-only after init
var authorizationRoutes = []armRoute{
	{
		match: func(segs []string, _ *http.Request) bool {
			_, ok := parseAuthPath(segs)

			return ok
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAuthorization(c, segs) },
	},
}

func (h *Handler) authorizationProvider() (*AuthorizationProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftAuthorization)
	if !ok {
		return nil, false
	}

	ap, ok := p.(*AuthorizationProvider)

	return ap, ok
}

func (h *Handler) handleAuthorization(c *echo.Context, segs []string) error {
	p, ok := h.authorizationProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	ap, _ := parseAuthPath(segs)
	r := c.Request()

	scope, ok := parseAuthScope(ap.scope)
	if !ok {
		return h.writeAPIError(c, authErr(http.StatusBadRequest, authCodeScope,
			"The scope '/%s' is invalid.", joinSegs(ap.scope)))
	}

	req := authReq{Method: r.Method, Scope: scope, Type: ap.typ, Name: ap.name, Query: r.URL.Query()}

	if r.Method == http.MethodPut || r.Method == http.MethodPatch {
		body, err := decodeJSONBody(r)
		if err != nil {
			return h.writeAPIError(c, err)
		}

		req.Body = body

		if r.Method == http.MethodPut && ap.name != "" && ap.typ != authRoleDefinitions {
			if err = h.authScopeExists(c, scope); err != nil {
				return h.writeAPIError(c, err)
			}
		}
	}

	resp, err := p.Do(req)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if resp.Body == nil || r.Method == http.MethodHead {
		return c.NoContent(resp.Status)
	}

	return h.writeJSON(c, resp.Status, resp.Body)
}

// authScopeExists fails when the resource group or resource the scope names does not exist.
func (h *Handler) authScopeExists(c *echo.Context, scope authScope) error {
	if scope.rg != "" && !h.Backend.ResourceGroupExists(scope.rg) {
		return fmt.Errorf("%w: %s", ErrResourceGroupNotFound, scope.rg)
	}

	if scope.kind != authScopeResource || scope.rg == "" {
		return nil
	}

	id, err := ParseGenericResourcePath(scope.id)
	if err != nil {
		return err
	}

	if _, err = h.Registry.Get(c.Request().Context(), id); err != nil {
		return authErr(http.StatusNotFound, authCodeNotFound,
			"The Resource '%s/%s' under resource group '%s' was not found.",
			id.ResourceType(), joinSegs(id.Names), id.ResourceGroup)
	}

	return nil
}

// enforceLocks rejects a write, delete or action that a management lock forbids. It reports whether it answered.
func (h *Handler) enforceLocks(c *echo.Context, segs []string) (bool, error) {
	r := c.Request()

	switch r.Method {
	case http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete:
	default:
		return false, nil
	}

	p, ok := h.authorizationProvider()
	if !ok || len(segs) < minResourceSegs || !strings.EqualFold(segs[0], subscriptionsSegment) {
		return false, nil
	}

	if ap, isAuth := parseAuthPath(segs); isAuth && ap.typ == authLocks {
		return false, nil
	}

	target, verb := segs, "write"

	switch r.Method {
	case http.MethodDelete:
		verb = "delete"
	case http.MethodPost:
		verb = "action"
		if len(segs) <= rgPathSegs || !strings.EqualFold(segs[2], resourceGroupsSegment) {
			return false, nil
		}

		target = segs[:len(segs)-1]
	}

	id := "/" + joinSegs(target)

	locked := p.lockedScopes(id, r.Method == http.MethodDelete)
	if len(locked) == 0 {
		return false, nil
	}

	return true, h.writeError(c, http.StatusConflict, "ScopeLocked", fmt.Sprintf(
		"The scope '%s' cannot perform %s operation because following scope(s) are locked: '%s'. "+
			"Please remove the lock and try again.", id, verb, strings.Join(locked, "','")))
}

// cleanupAuthorization drops locks and assignments scoped to a resource once its DELETE succeeded.
func (h *Handler) cleanupAuthorization(c *echo.Context, segs []string) {
	if c.Request().Method != http.MethodDelete || len(segs) < minResourceSegs {
		return
	}

	if _, isAuth := parseAuthPath(segs); isAuth {
		return
	}

	resp, err := echo.UnwrapResponse(c.Response())
	if err != nil || resp.Status < http.StatusOK || resp.Status >= http.StatusMultipleChoices {
		return
	}

	if p, ok := h.authorizationProvider(); ok {
		p.RemoveUnder("/" + joinSegs(segs))
	}
}
