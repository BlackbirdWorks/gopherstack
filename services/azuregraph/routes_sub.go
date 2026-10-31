package azuregraph

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

func supports(kind string, allowed ...string) bool { return slices.Contains(allowed, kind) }

// subRoute serves one sub-resource segment of an application, service principal, user or group.
type subRoute struct {
	serve func(h *Handler, c *echo.Context, id string, tail []string) error
	kinds []string
}

func appOrSPKinds() []string { return []string{kindApplication, kindServicePrincipal} }

func memberOfKinds() []string { return []string{kindUser, kindGroup, kindServicePrincipal} }

func leaf(fn func(h *Handler, c *echo.Context, id string) error) func(*Handler, *echo.Context, string, []string) error {
	return func(h *Handler, c *echo.Context, id string, tail []string) error {
		if len(tail) != 0 {
			return h.writeErr(c, segmentErr(strings.Join(tail, "/")))
		}

		return fn(h, c, id)
	}
}

func (h *Handler) relationLoader(load func(id string) ([]Object, error)) func(*Handler, *echo.Context, string) error {
	return func(h *Handler, c *echo.Context, id string) error {
		return h.relation(c, func() ([]Object, error) { return load(id) })
	}
}

// subRoutes maps lower-cased sub-resource segments to their handlers.
func (h *Handler) subRoutes() map[string]subRoute {
	b := h.Backend

	return map[string]subRoute{
		"addpassword": {kinds: appOrSPKinds(), serve: leaf((*Handler).addPassword)},
		"removepassword": {kinds: appOrSPKinds(), serve: leaf(func(h *Handler, c *echo.Context, id string) error {
			return h.removeCred(c, id, b.RemovePassword)
		})},
		"addkey": {kinds: appOrSPKinds(), serve: leaf((*Handler).addKey)},
		"removekey": {kinds: appOrSPKinds(), serve: leaf(func(h *Handler, c *echo.Context, id string) error {
			return h.removeCred(c, id, b.RemoveKey)
		})},
		"owners": {
			kinds: []string{kindApplication, kindServicePrincipal, kindGroup},
			serve: func(h *Handler, c *echo.Context, id string, tail []string) error {
				return h.refs(c, id, tail, refOps{add: b.AddOwner, remove: b.RemoveOwner, load: b.Owners})
			},
		},
		"members": {
			kinds: []string{kindGroup},
			serve: func(h *Handler, c *echo.Context, id string, tail []string) error {
				return h.refs(c, id, tail, refOps{add: b.AddMember, remove: b.RemoveMember, load: b.Members})
			},
		},
		"transitivemembers": {kinds: []string{kindGroup}, serve: leaf(h.relationLoader(b.TransitiveMembers))},
		"memberof": {kinds: memberOfKinds(), serve: leaf(h.relationLoader(func(id string) ([]Object, error) {
			return b.MemberOf(id, false)
		}))},
		"transitivememberof": {kinds: memberOfKinds(), serve: leaf(h.relationLoader(func(id string) ([]Object, error) {
			return b.MemberOf(id, true)
		}))},
		"approleassignments": {
			kinds: []string{kindServicePrincipal, kindUser, kindGroup},
			serve: func(h *Handler, c *echo.Context, id string, tail []string) error {
				return h.assignments(c, id, false, tail)
			},
		},
		"approleassignedto": {
			kinds: []string{kindServicePrincipal},
			serve: func(h *Handler, c *echo.Context, id string, tail []string) error {
				return h.assignments(c, id, true, tail)
			},
		},
		"oauth2permissiongrants": {kinds: []string{kindServicePrincipal}, serve: leaf((*Handler).clientGrants)},
	}
}

func (h *Handler) handleSub(c *echo.Context, coll *collection, ref Ref, rest []string) error {
	m, err := coll.get(h.Backend, ref)
	if err != nil {
		return h.writeErr(c, err)
	}

	id, _ := m["id"].(string)

	route, ok := h.subs[strings.ToLower(rest[0])]
	if !ok || !supports(coll.kind, route.kinds...) {
		return h.writeErr(c, segmentErr(rest[0]))
	}

	return route.serve(h, c, id, rest[1:])
}

func (h *Handler) addPassword(c *echo.Context, id string) error {
	if c.Request().Method != http.MethodPost {
		return h.methodNotAllowed(c)
	}

	body, err := readJSON(c)
	if err != nil {
		return h.writeErr(c, err)
	}

	var in PasswordCredential
	if raw := body["passwordCredential"]; len(raw) > 0 && string(raw) != jsonNull {
		if err = json.Unmarshal(raw, &in); err != nil {
			return h.writeErr(c, badRequest("Invalid value specified for property 'passwordCredential'."))
		}
	}

	cred, err := h.Backend.AddPassword(id, in)
	if err != nil {
		return h.writeErr(c, err)
	}

	return h.json(c, http.StatusOK, withContext(c, "microsoft.graph.passwordCredential", toMap(cred)))
}

func withContext(c *echo.Context, typeName string, m map[string]any) map[string]any {
	m["@odata.context"] = odataBase(c.Request()) + "/$metadata#" + typeName

	return m
}

func (h *Handler) addKey(c *echo.Context, id string) error {
	if c.Request().Method != http.MethodPost {
		return h.methodNotAllowed(c)
	}

	body, err := readJSON(c)
	if err != nil {
		return h.writeErr(c, err)
	}

	var in KeyCredential
	if err = json.Unmarshal(body["keyCredential"], &in); err != nil {
		return h.writeErr(c, badRequest("A value is required for property 'keyCredential' of resource 'Application'."))
	}

	cred, err := h.Backend.AddKey(id, in)
	if err != nil {
		return h.writeErr(c, err)
	}

	return h.json(c, http.StatusOK, withContext(c, "microsoft.graph.keyCredential", toMap(cred)))
}

func (h *Handler) removeCred(c *echo.Context, id string, remove func(id, keyID string) error) error {
	if c.Request().Method != http.MethodPost {
		return h.methodNotAllowed(c)
	}

	body, err := readJSON(c)
	if err != nil {
		return h.writeErr(c, err)
	}

	var keyID string
	if json.Unmarshal(body["keyId"], &keyID) != nil || keyID == "" {
		return h.writeErr(c, badRequest("A value is required for property 'keyId'."))
	}

	if err = remove(id, keyID); err != nil {
		return h.writeErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) relation(c *echo.Context, load func() ([]Object, error)) error {
	if c.Request().Method != http.MethodGet {
		return h.methodNotAllowed(c)
	}

	objs, err := load()
	if err != nil {
		return h.writeErr(c, err)
	}

	return h.list(c, "directoryObjects", "DirectoryObject", nil, typedObjects(objs))
}

func odataRefTarget(body map[string]json.RawMessage) (string, bool) {
	var ref string
	if json.Unmarshal(body["@odata.id"], &ref) != nil || ref == "" {
		return "", false
	}

	return idFromRef(ref), true
}

// refOps are the backend operations behind an owners or members relationship.
type refOps struct {
	add    func(id, ref string) error
	remove func(id, ref string) error
	load   func(id string) ([]Object, error)
}

// refs serves owners and members: GET list, POST /$ref add, GET /$ref, DELETE /{id}/$ref remove, and type casts.
func (h *Handler) refs(c *echo.Context, id string, tail []string, ops refOps) error {
	r := c.Request()
	load := func() ([]Object, error) { return ops.load(id) }

	switch {
	case len(tail) == 0 && r.Method == http.MethodGet:
		return h.relation(c, load)
	case len(tail) == 1 && tail[0] == segRef:
		return h.refCollection(c, id, ops)
	case len(tail) == 2 && tail[1] == segRef && r.Method == http.MethodDelete:
		if err := ops.remove(id, tail[0]); err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case len(tail) == 1 && strings.HasPrefix(tail[0], "microsoft.graph.") && r.Method == http.MethodGet:
		want := strings.TrimPrefix(tail[0], "microsoft.graph.")

		return h.relation(c, func() ([]Object, error) {
			objs, err := load()

			return slices.DeleteFunc(objs, func(o Object) bool { return !strings.EqualFold(o.Kind, want) }), err
		})
	}

	return h.writeErr(c, segmentErr(strings.Join(tail, "/")))
}

// refCollection serves POST and GET on <relationship>/$ref.
func (h *Handler) refCollection(c *echo.Context, id string, ops refOps) error {
	r := c.Request()

	switch r.Method {
	case http.MethodPost:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		target, ok := odataRefTarget(body)
		if !ok {
			return h.writeErr(c, badRequest("A value is required for property '@odata.id'."))
		}

		if err = ops.add(id, target); err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case http.MethodGet:
		objs, err := ops.load(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		refs := make([]map[string]any, 0, len(objs))

		for _, o := range objs {
			oid, _ := toMap(o.Entity)["id"].(string)
			refs = append(refs, map[string]any{"@odata.id": odataBase(r) + "/directoryObjects/" + oid})
		}

		return h.json(c, http.StatusOK, map[string]any{propValue: refs})
	}

	return h.methodNotAllowed(c)
}

func (h *Handler) assignments(c *echo.Context, id string, asResource bool, tail []string) error {
	r, b := c.Request(), h.Backend

	switch {
	case len(tail) == 0 && r.Method == http.MethodGet:
		list, err := b.ListAppRoleAssignments(id, asResource)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.list(c, "appRoleAssignments", "AppRoleAssignment", fieldsOf[AppRoleAssignment](), mapAll(list))
	case len(tail) == 0 && r.Method == http.MethodPost:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		a, err := b.CreateAppRoleAssignment(id, asResource, body)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusCreated, "appRoleAssignments", toMap(a))
	case len(tail) == 1 && r.Method == http.MethodDelete:
		if err := b.DeleteAppRoleAssignment(id, tail[0], asResource); err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	}

	return h.writeErr(c, segmentErr(strings.Join(tail, "/")))
}

func (h *Handler) clientGrants(c *echo.Context, id string) error {
	if c.Request().Method != http.MethodGet {
		return h.methodNotAllowed(c)
	}

	var items []map[string]any

	for _, g := range h.Backend.ListGrants() {
		if strings.EqualFold(g.ClientID, id) {
			items = append(items, toMap(g))
		}
	}

	return h.list(c, "oauth2PermissionGrants", "OAuth2PermissionGrant", fieldsOf[OAuth2PermissionGrant](), items)
}
