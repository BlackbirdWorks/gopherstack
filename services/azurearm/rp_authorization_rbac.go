package azurearm

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const rbacPermissionsType = "permissions"

// rbacAction derives the ARM operation name and the scope it applies to from a request, following the
// {namespace}/{type}[/{childType}...]/{read|write|delete|action} naming of Azure role actions. ok is false when the
// request is not a resource operation RBAC applies to here.
func rbacAction(method string, segs []string) (string, string, bool) {
	if len(segs) < rgPathSegs || !strings.EqualFold(segs[0], subscriptionsSegment) ||
		!strings.EqualFold(segs[2], resourceGroupsSegment) {
		return "", "", false
	}

	target, verb, suffix := segs, rbacVerb(method), ""

	if verb == authVerbAction {
		if len(segs) <= rgPathSegs+1 {
			return "", "", false
		}

		target, suffix = segs[:len(segs)-1], "/"+segs[len(segs)-1]
	}

	if len(target) == rgPathSegs {
		return "Microsoft.Resources/subscriptions/resourceGroups/" + verb, "/" + joinSegs(target), true
	}

	if len(target) < authSubResourceMin+1 || !strings.EqualFold(target[4], providersSegment) {
		return "", "", false
	}

	ns := target[5]
	rest := target[6:]
	types := []string{ns}

	for i := 0; i < len(rest); i += typeNamePairWidth {
		types = append(types, rest[i])
	}

	if len(rest)%typeNamePairWidth == 1 {
		verb = "read"
	}

	return strings.Join(types, "/") + suffix + "/" + verb, "/" + joinSegs(target), true
}

func rbacVerb(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead:
		return "read"
	case http.MethodDelete:
		return strings.ToLower(http.MethodDelete)
	case http.MethodPost:
		return authVerbAction
	}

	return "write"
}

func (h *Handler) rbacSuperuser(oid string) bool {
	for s := range strings.SplitSeq(h.Settings.RBACSuperusers, ",") {
		if strings.EqualFold(strings.TrimSpace(s), oid) && oid != "" {
			return true
		}
	}

	return false
}

// callerOID returns the object id of the bearer token's principal.
func (h *Handler) callerOID(c *echo.Context) (string, bool) {
	token, ok := strings.CutPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return "", false
	}

	claims, err := h.Issuer.Parse(token)
	if err != nil {
		return "", false
	}

	oid, _ := claims["oid"].(string)

	return oid, oid != ""
}

// rbacDenied answers 401 or 403 when --azure-arm-enforce-rbac is set and the caller may not perform the request.
func (h *Handler) rbacDenied(c *echo.Context, segs []string) (bool, error) {
	if !h.Settings.EnforceRBAC {
		return false, nil
	}

	oid, ok := h.callerOID(c)
	if !ok {
		return true, h.writeUnauthorized(c, "The request is missing a valid bearer token.")
	}

	action, scope, applies := rbacAction(c.Request().Method, segs)
	if !applies || h.rbacSuperuser(oid) {
		return false, nil
	}

	p, found := h.authorizationProvider()
	if !found || p.Allowed(oid, scope, action, false) {
		return false, nil
	}

	return true, h.writeError(c, http.StatusForbidden, "AuthorizationFailed", fmt.Sprintf(
		"The client '%s' with object id '%s' does not have authorization to perform action '%s' over scope '%s' "+
			"or the scope is invalid. If access was recently granted, please refresh your credentials.",
		oid, oid, action, scope))
}

// servePermissions answers GET .../providers/Microsoft.Authorization/permissions at resource group and resource
// scope with the permissions of the caller's role assignments at or above the scope (armauthorization v2.2.0
// PermissionsClient, api 2022-04-01).
func (h *Handler) servePermissions(c *echo.Context, segs []string) (bool, error) {
	n := len(segs)
	if c.Request().Method != http.MethodGet || n < rgPathSegs+2 || !strings.EqualFold(segs[n-1], rbacPermissionsType) ||
		!strings.EqualFold(
			segs[n-2],
			namespaceMicrosoftAuthorization,
		) || !strings.EqualFold(segs[n-3], providersSegment) {
		return false, nil
	}

	p, ok := h.authorizationProvider()
	if !ok {
		return false, nil
	}

	oid, hasOID := h.callerOID(c)
	if !hasOID {
		return true, h.writeUnauthorized(c, "The request is missing a valid bearer token.")
	}

	scope := "/" + joinSegs(segs[:n-3])

	return true, h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: p.PermissionsFor(oid, scope)})
}

// PermissionsFor lists the permissions of the roles principalID holds at or above scope, one entry per distinct role.
func (p *AuthorizationProvider) PermissionsFor(principalID, scope string) []any {
	p.mu.RLock("PermissionsFor")
	defer p.mu.RUnlock()

	var seen []string

	out := []any{}

	for _, a := range p.roleAssigns {
		if !strings.EqualFold(a.PrincipalID, principalID) || !scopeWithin(scope, a.Scope) ||
			slices.ContainsFunc(seen, func(g string) bool { return strings.EqualFold(g, a.RoleGUID) }) {
			continue
		}

		seen = append(seen, a.RoleGUID)

		for _, perm := range nonNilPermissions(p.permissionsOf(a.RoleGUID)) {
			out = append(out, map[string]any{
				"actions": perm.Actions, "notActions": perm.NotActions,
				"dataActions": perm.DataActions, "notDataActions": perm.NotDataActions,
			})
		}
	}

	return out
}
