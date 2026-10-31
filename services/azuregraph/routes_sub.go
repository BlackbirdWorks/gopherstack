package azuregraph

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

func supports(kind string, allowed ...string) bool { return slices.Contains(allowed, kind) }

func (h *Handler) handleSub(c *echo.Context, coll *collection, ref Ref, rest []string) error {
	m, err := coll.get(h.Backend, ref)
	if err != nil {
		return h.writeErr(c, err)
	}

	id, _ := m["id"].(string)
	seg, tail := strings.ToLower(rest[0]), rest[1:]
	k := coll.kind
	appOrSP := supports(k, kindApplication, kindServicePrincipal)

	switch {
	case seg == "addpassword" && appOrSP && len(tail) == 0:
		return h.addPassword(c, id)
	case seg == "removepassword" && appOrSP && len(tail) == 0:
		return h.removeCred(c, id, h.Backend.RemovePassword)
	case seg == "addkey" && appOrSP && len(tail) == 0:
		return h.addKey(c, id)
	case seg == "removekey" && appOrSP && len(tail) == 0:
		return h.removeCred(c, id, h.Backend.RemoveKey)
	case seg == "owners" && supports(k, kindApplication, kindServicePrincipal, kindGroup):
		return h.refs(c, id, tail, h.Backend.AddOwner, h.Backend.RemoveOwner,
			func() ([]Object, error) { return h.Backend.Owners(id) })
	case seg == "members" && k == kindGroup:
		return h.refs(c, id, tail, h.Backend.AddMember, h.Backend.RemoveMember,
			func() ([]Object, error) { return h.Backend.Members(id) })
	case seg == "transitivemembers" && k == kindGroup && len(tail) == 0:
		return h.relation(c, func() ([]Object, error) { return h.Backend.TransitiveMembers(id) })
	case seg == "memberof" && supports(k, kindUser, kindGroup, kindServicePrincipal) && len(tail) == 0:
		return h.relation(c, func() ([]Object, error) { return h.Backend.MemberOf(id, false) })
	case seg == "transitivememberof" && supports(k, kindUser, kindGroup, kindServicePrincipal) && len(tail) == 0:
		return h.relation(c, func() ([]Object, error) { return h.Backend.MemberOf(id, true) })
	case seg == "approleassignments" && supports(k, kindServicePrincipal, kindUser, kindGroup):
		return h.assignments(c, id, false, tail)
	case seg == "approleassignedto" && k == kindServicePrincipal:
		return h.assignments(c, id, true, tail)
	case seg == "oauth2permissiongrants" && k == kindServicePrincipal && len(tail) == 0:
		return h.clientGrants(c, id)
	}

	return h.writeErr(c, segmentErr(rest[0]))
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
	if raw := body["passwordCredential"]; len(raw) > 0 && string(raw) != "null" {
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

// refs serves owners and members: GET list, POST /$ref add, DELETE /{id}/$ref remove, and type-cast lists.
func (h *Handler) refs(
	c *echo.Context, id string, tail []string,
	add, remove func(id, ref string) error, load func() ([]Object, error),
) error {
	r := c.Request()

	switch {
	case len(tail) == 0 && r.Method == http.MethodGet:
		return h.relation(c, load)
	case len(tail) == 1 && tail[0] == "$ref" && r.Method == http.MethodPost:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		target, ok := odataRefTarget(body)
		if !ok {
			return h.writeErr(c, badRequest("A value is required for property '@odata.id'."))
		}

		if err = add(id, target); err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case len(tail) == 1 && tail[0] == "$ref" && r.Method == http.MethodGet:
		objs, err := load()
		if err != nil {
			return h.writeErr(c, err)
		}

		refs := make([]map[string]any, 0, len(objs))
		for _, o := range objs {
			oid, _ := toMap(o.Entity)["id"].(string)
			refs = append(refs, map[string]any{"@odata.id": odataBase(r) + "/directoryObjects/" + oid})
		}

		return h.json(c, http.StatusOK, map[string]any{"value": refs})
	case len(tail) == 2 && tail[1] == "$ref" && r.Method == http.MethodDelete:
		if err := remove(id, tail[0]); err != nil {
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
