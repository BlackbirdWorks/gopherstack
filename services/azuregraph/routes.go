package azuregraph

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"
)

const maxGetByIDs = 1000

func segmentErr(seg string) error {
	return newErr(http.StatusBadRequest, codeBadSegment, "Resource not found for the segment '%s'.", seg)
}

func odataBase(r *http.Request) string {
	version := "v1.0"
	if strings.HasPrefix(r.URL.Path, "/beta") {
		version = "beta"
	}

	return "https://" + r.Host + "/" + version
}

func readJSON(c *echo.Context) (map[string]json.RawMessage, error) {
	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes))
	out := map[string]json.RawMessage{}

	if err != nil || json.Unmarshal(data, &out) != nil || out == nil {
		return nil, badRequest(
			"Unable to read JSON request payload. Please ensure Content-Type header is set and payload is of valid JSON format.")
	}

	return out, nil
}

func (h *Handler) json(c *echo.Context, status int, v any) error {
	c.Response().Header().Set("Content-Type", graphContentType)

	return c.JSON(status, v)
}

func (h *Handler) entity(c *echo.Context, status int, context string, m map[string]any) error {
	out := make(map[string]any, len(m)+1)
	out["@odata.context"] = odataBase(c.Request()) + "/$metadata#" + context + "/$entity"

	for k, v := range m {
		out[k] = v
	}

	return h.json(c, status, out)
}

// list renders a collection response through the OData query pipeline.
func (h *Handler) list(c *echo.Context, context, resource string, fields map[string]bool, items []map[string]any) error {
	r := c.Request()

	q, err := parseListQuery(r, resource, fields)
	if err != nil {
		return h.writeErr(c, err)
	}

	res := q.run(items)
	out := map[string]any{"@odata.context": odataBase(r) + "/$metadata#" + context, "value": res.items}

	if q.count {
		out["@odata.count"] = res.total
	}

	if res.next != "" {
		out["@odata.nextLink"] = nextLink(r, res.next)
	}

	return h.json(c, http.StatusOK, out)
}

func (h *Handler) dispatch(c *echo.Context, segs []string, claims jwt.MapClaims) error {
	name, key := splitKey(segs[0])

	switch name {
	case "me":
		return h.handleMe(c, segs, claims)
	case "organization":
		return h.handleOrganization(c, segs)
	case "domains":
		return h.handleDomains(c, segs)
	case "directoryObjects":
		return h.handleDirectoryObjects(c, segs)
	case "oauth2PermissionGrants":
		return h.handleGrants(c, segs)
	}

	coll, ok := h.colls[name]
	if !ok {
		return h.writeErr(c, segmentErr(segs[0]))
	}

	return h.handleCollection(c, coll, key, segs, claims)
}

func (h *Handler) handleCollection(c *echo.Context, coll *collection, key Ref, segs []string, claims jwt.MapClaims) error {
	r := c.Request()
	b := h.Backend

	if len(segs) == 1 && key.Field == "" {
		switch r.Method {
		case http.MethodGet:
			return h.list(c, coll.name, coll.resource, coll.fields, coll.list(b))
		case http.MethodPost:
			return h.createObject(c, coll, claims)
		}

		return h.methodNotAllowed(c)
	}

	if key.Field == "" && len(segs) == 2 && segs[1] == "$count" {
		return h.count(c, coll)
	}

	ref, rest := key, segs[1:]
	if key.Field == "" {
		ref, rest = Ref{ID: segs[1]}, segs[2:]
	}

	if len(rest) > 0 {
		return h.handleSub(c, coll, ref, rest)
	}

	switch r.Method {
	case http.MethodGet:
		m, err := coll.get(b, ref)
		if err != nil {
			return h.writeErr(c, err)
		}

		q, qerr := parseListQuery(r, coll.resource, coll.fields)
		if qerr != nil {
			return h.writeErr(c, qerr)
		}

		return h.entity(c, http.StatusOK, coll.name, q.project(m))
	case http.MethodPatch:
		body, err := readJSON(c)
		if err == nil {
			err = coll.patch(b, ref, body)
		}

		if err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case http.MethodDelete:
		if err := coll.del(b, ref); err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	}

	return h.methodNotAllowed(c)
}

func (h *Handler) methodNotAllowed(c *echo.Context) error {
	return h.writeErr(c, newErr(http.StatusMethodNotAllowed, codeBadRequest,
		"The HTTP verb '%s' is not supported for this resource.", c.Request().Method))
}

func (h *Handler) createObject(c *echo.Context, coll *collection, claims jwt.MapClaims) error {
	body, err := readJSON(c)
	if err != nil {
		return h.writeErr(c, err)
	}

	m, err := coll.create(h.Backend, body)
	if err != nil {
		return h.writeErr(c, err)
	}

	if _, bound := body["owners@odata.bind"]; bound {
		return h.entity(c, http.StatusCreated, coll.name, m)
	}

	if oid, _ := claims["oid"].(string); coll.kind != kindUser && oid != "" {
		if id, _ := m["id"].(string); id != "" {
			_ = h.Backend.AddOwner(id, oid)
		}
	}

	return h.entity(c, http.StatusCreated, coll.name, m)
}

func (h *Handler) count(c *echo.Context, coll *collection) error {
	if !eventual(c.Request()) {
		return h.writeErr(c, badRequest("$count is not currently supported."))
	}

	c.Response().Header().Set("Content-Type", "text/plain")

	return c.String(http.StatusOK, strconv.Itoa(len(coll.list(h.Backend))))
}

func (h *Handler) handleMe(c *echo.Context, segs []string, claims jwt.MapClaims) error {
	oid, _ := claims["oid"].(string)

	kind, ok := h.Backend.ObjectKind(oid)

	switch {
	case ok && kind == kindUser:
		return h.handleCollection(c, h.colls["users"], Ref{}, append([]string{"users", oid}, segs[1:]...), claims)
	case ok:
		return h.writeErr(c, newErr(http.StatusBadRequest, codeBadSegment,
			"/me request is only valid with delegated authentication flow."))
	}

	return h.writeErr(c, notFound(oid))
}

func (h *Handler) handleOrganization(c *echo.Context, segs []string) error {
	if c.Request().Method != http.MethodGet {
		return h.methodNotAllowed(c)
	}

	domains := make([]map[string]any, 0, len(h.Backend.domains))
	for _, d := range h.Backend.domains {
		domains = append(domains, map[string]any{
			"capabilities": "Email, OfficeCommunicationsOnline", "isDefault": d.IsDefault, "isInitial": d.IsInitial,
			"name": d.Name, "type": "Managed",
		})
	}

	org := map[string]any{
		"id": h.Backend.TenantID(), "displayName": "Gopherstack", "tenantType": "AAD", "verifiedDomains": domains,
		"assignedPlans": []any{}, "businessPhones": []any{}, "provisionedPlans": []any{},
		"technicalNotificationMails": []any{}, "onPremisesSyncEnabled": nil, "countryLetterCode": "US",
	}

	if len(segs) == 2 {
		if !strings.EqualFold(segs[1], h.Backend.TenantID()) {
			return h.writeErr(c, notFound(segs[1]))
		}

		return h.entity(c, http.StatusOK, "organization", org)
	}

	return h.list(c, "organization", "Organization", nil, []map[string]any{org})
}

func (h *Handler) handleDomains(c *echo.Context, segs []string) error {
	if c.Request().Method != http.MethodGet {
		return h.methodNotAllowed(c)
	}

	items := make([]map[string]any, 0, len(h.Backend.domains))
	for _, d := range h.Backend.domains {
		items = append(items, map[string]any{
			"id": d.Name, "authenticationType": "Managed", "isAdminManaged": true, "isDefault": d.IsDefault,
			"isInitial": d.IsInitial, "isRoot": true, "isVerified": true, "supportedServices": []any{},
		})
	}

	if len(segs) == 2 {
		for i, d := range h.Backend.domains {
			if strings.EqualFold(d.Name, segs[1]) {
				return h.entity(c, http.StatusOK, "domains", items[i])
			}
		}

		return h.writeErr(c, notFound(segs[1]))
	}

	return h.list(c, "domains", "Domain", nil, items)
}

func (h *Handler) handleDirectoryObjects(c *echo.Context, segs []string) error {
	r := c.Request()

	if len(segs) == 2 && r.Method == http.MethodPost && strings.HasSuffix(segs[1], "getByIds") {
		var in struct {
			IDs   []string `json:"ids"`
			Types []string `json:"types"`
		}

		data, _ := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxBodyBytes))
		if err := json.Unmarshal(data, &in); err != nil || len(in.IDs) == 0 || len(in.IDs) > maxGetByIDs {
			return h.writeErr(c, badRequest("The 'ids' collection must contain between 1 and %d object identifiers.", maxGetByIDs))
		}

		objs, err := h.Backend.ByIDs(in.IDs, in.Types)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.json(c, http.StatusOK, map[string]any{
			"@odata.context": odataBase(r) + "/$metadata#Collection(microsoft.graph.directoryObject)",
			"value":          typedObjects(objs),
		})
	}

	if len(segs) != 2 {
		return h.writeErr(c, segmentErr(segs[len(segs)-1]))
	}

	switch r.Method {
	case http.MethodGet:
		o, err := h.Backend.GetObject(segs[1])
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusOK, "directoryObjects", typed(o.Kind, o.Entity))
	case http.MethodDelete:
		kind, ok := h.Backend.ObjectKind(segs[1])
		if !ok {
			return h.writeErr(c, notFound(segs[1]))
		}

		for _, coll := range h.colls {
			if coll.kind == kind {
				if err := coll.del(h.Backend, Ref{ID: segs[1]}); err != nil {
					return h.writeErr(c, err)
				}
			}
		}

		return c.NoContent(http.StatusNoContent)
	}

	return h.methodNotAllowed(c)
}

func typedObjects(objs []Object) []map[string]any {
	out := make([]map[string]any, 0, len(objs))
	for _, o := range objs {
		out = append(out, typed(o.Kind, o.Entity))
	}

	return out
}

func (h *Handler) handleGrants(c *echo.Context, segs []string) error {
	r, b := c.Request(), h.Backend
	fields := fieldsOf[OAuth2PermissionGrant]()

	if len(segs) == 1 {
		switch r.Method {
		case http.MethodGet:
			return h.list(c, "oauth2PermissionGrants", "OAuth2PermissionGrant", fields, mapAll(b.ListGrants()))
		case http.MethodPost:
			body, err := readJSON(c)
			if err != nil {
				return h.writeErr(c, err)
			}

			g, err := b.CreateGrant(body)
			if err != nil {
				return h.writeErr(c, err)
			}

			return h.entity(c, http.StatusCreated, "oauth2PermissionGrants", toMap(g))
		}

		return h.methodNotAllowed(c)
	}

	if len(segs) > 2 {
		return h.writeErr(c, segmentErr(segs[2]))
	}

	switch r.Method {
	case http.MethodGet:
		for _, g := range b.ListGrants() {
			if strings.EqualFold(g.ID, segs[1]) {
				return h.entity(c, http.StatusOK, "oauth2PermissionGrants", toMap(g))
			}
		}

		return h.writeErr(c, notFound(segs[1]))
	case http.MethodPatch:
		body, err := readJSON(c)
		if err == nil {
			err = b.UpdateGrant(segs[1], body)
		}

		if err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case http.MethodDelete:
		if err := b.DeleteGrant(segs[1]); err != nil {
			return h.writeErr(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	}

	return h.methodNotAllowed(c)
}
