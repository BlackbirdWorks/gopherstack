package dashboard

import (
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

const (
	azureAuthzPrefix  = "/dashboard/api/v1/azure/authz"
	authzPxSubScope   = 2
	authzPxRGScope    = 4
	authzPxNS         = "microsoft.authorization"
	authzPxRoleDefs   = "roledefinitions"
	authzPxRoleAssign = "roleassignments"
	authzPxLocks      = "locks"
	authzPxMaxNotes   = 512
)

//nolint:gochecknoglobals // immutable allow-lists and validators
var (
	authzPxReadOnly = []string{"policydefinitions", "policysetdefinitions", "policyassignments", authzPxRoleDefs}
	authzPxLevels   = []string{"CanNotDelete", "ReadOnly"}
	authzPxTypes    = []string{"User", "Group", "ServicePrincipal"}
	authzPxLockName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,89}$`)
	authzPxRoleDef  = regexp.MustCompile(
		`^/subscriptions/[^/]+/providers/microsoft\.authorization/roledefinitions/[0-9a-f-]{36}$`)
)

// registerAzureAuthzRoutes serves the access half of the Entra page: role definitions and assignments, locks and
// policy at subscription or resource-group scope, by dispatching in-process to the ARM handler. Role assignment and
// lock writes are rebuilt from fixed fields; policy and role definitions are read-only.
func (h *DashboardHandler) registerAzureAuthzRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure Authorization", prefix: azureAuthzPrefix,
		queryKeys: []string{armQueryVersion, "$filter"},
		methods:   []string{http.MethodGet, http.MethodPut, http.MethodDelete},
		allowed:   authzProxyAllowed,
		body:      authzProxyBody,
	})
}

// authzSplit splits a lower-cased path ({scope}/providers/microsoft.authorization/{type}[/{name}]) where the scope
// is a subscription or a resource group.
func authzSplit(p []string) (string, string, bool) {
	scope := authzPxSubScope
	if len(p) > authzPxSubScope && p[0] == armPxSubsSeg && p[2] == armPxRGsSeg {
		scope = authzPxRGScope
	}

	if len(p) < scope+3 || len(p) > scope+4 || p[0] != armPxSubsSeg || p[scope] != armPxProvSeg ||
		p[scope+1] != authzPxNS {
		return "", "", false
	}

	name := ""
	if len(p) == scope+4 {
		name = p[scope+3]
	}

	return p[scope+2], name, true
}

func authzProxyAllowed(method string, p []string) bool {
	typ, name, ok := authzSplit(p)
	if !ok {
		return false
	}

	switch {
	case method == http.MethodGet:
		return typ == authzPxRoleAssign || typ == authzPxLocks || slices.Contains(authzPxReadOnly, typ)
	case name == "":
		return false
	case typ == authzPxRoleAssign:
		return authzPxGUIDName(name)
	case typ == authzPxLocks:
		return authzPxLockName.MatchString(name)
	}

	return false
}

func authzPxGUIDName(n string) bool { return graphPxGUID.MatchString(n) }

func authzProxyBody(method string, p []string, in []byte) ([]byte, bool) {
	if method != http.MethodPut {
		return in, true
	}

	typ, _, _ := authzSplit(p)

	var props map[string]any

	switch typ {
	case authzPxRoleAssign:
		props = authzAssignmentProps(in)
	case authzPxLocks:
		props = authzLockProps(in)
	}

	if props == nil {
		return nil, false
	}

	b, err := json.Marshal(map[string]any{"properties": props})

	return b, err == nil
}

type authzPropsIn struct {
	Properties map[string]any `json:"properties"`
}

func (r authzPropsIn) text(key string) string {
	v, _ := r.Properties[key].(string)

	return v
}

func authzAssignmentProps(in []byte) map[string]any {
	var req authzPropsIn
	if json.Unmarshal(in, &req) != nil {
		return nil
	}

	def, pid, typ := req.text("roleDefinitionId"), req.text("principalId"), req.text("principalType")

	if !authzPxRoleDef.MatchString(strings.ToLower(def)) || !graphPxGUID.MatchString(strings.ToLower(pid)) {
		return nil
	}

	out := map[string]any{"roleDefinitionId": def, "principalId": pid}

	if typ != "" {
		if !slices.Contains(authzPxTypes, typ) {
			return nil
		}

		out["principalType"] = typ
	}

	return out
}

func authzLockProps(in []byte) map[string]any {
	var req authzPropsIn
	if json.Unmarshal(in, &req) != nil {
		return nil
	}

	level, notes := req.text("level"), req.text("notes")
	if !slices.Contains(authzPxLevels, level) || len(notes) > authzPxMaxNotes {
		return nil
	}

	out := map[string]any{"level": level}
	if notes != "" {
		out["notes"] = notes
	}

	return out
}
