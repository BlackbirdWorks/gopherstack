package gcpstorage

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type aclBody struct {
	Entity string `json:"entity"`
	Role   string `json:"role"`
}

func aclOpNames() []string {
	var names []string

	for _, fam := range []string{"BucketAcl", "DefaultObjectAcl", "ObjectAcl"} {
		for _, verb := range []string{"List", "Insert", "Get", "Update", "Patch", "Delete"} {
			names = append(names, verb+fam)
		}
	}

	return names
}

func (h *Handler) registerACL() {
	for _, name := range aclOpNames() {
		h.ops[name] = h.aclOp
	}
}

func aclTargetOf(c *echo.Context, rt route) (aclTarget, error) {
	t := aclTarget{bucket: rt.bucket, object: rt.object, kind: rt.acl}

	if rt.acl == aclObject {
		gen, apiErr := parseGeneration(c.QueryParams(), "generation")
		if apiErr != nil {
			return t, apiErr
		}

		t.gen = gen
	}

	return t, nil
}

func (h *Handler) aclOp(c *echo.Context, rt route) error {
	t, err := aclTargetOf(c, rt)
	if err != nil {
		return err
	}

	base := baseURL(c.Request())

	switch c.Request().Method {
	case http.MethodGet:
		return h.aclRead(c, rt, t, base)
	case http.MethodDelete:
		if delErr := h.Backend.DeleteACL(t, rt.entity); delErr != nil {
			return delErr
		}

		return c.NoContent(http.StatusNoContent)
	default:
		return h.aclWrite(c, rt, t, base)
	}
}

func (h *Handler) aclRead(c *echo.Context, rt route, t aclTarget, base string) error {
	if rt.entity == "" {
		list, gen, err := h.Backend.ListACL(t)
		if err != nil {
			return err
		}

		t.gen = gen

		return c.JSON(http.StatusOK, map[string]any{keyKind: t.listKind(), "items": renderACLList(base, t, list)})
	}

	e, gen, err := h.Backend.GetACL(t, rt.entity)
	if err != nil {
		return err
	}

	t.gen = gen

	return c.JSON(http.StatusOK, renderACL(base, t, e))
}

func (h *Handler) aclWrite(c *echo.Context, rt route, t aclTarget, base string) error {
	body, err := readBody(c)
	if err != nil {
		return err
	}

	var in aclBody
	if jsonErr := json.Unmarshal(body, &in); jsonErr != nil {
		return errInvalid("Invalid JSON payload received.")
	}

	if rt.entity != "" {
		in.Entity = rt.entity
	}

	if in.Entity == "" || in.Role == "" {
		return errInvalid("Required: entity and role.")
	}

	e, gen, err := h.Backend.SetACL(t, ACLEntry{Entity: in.Entity, Role: in.Role})
	if err != nil {
		return err
	}

	t.gen = gen

	return c.JSON(http.StatusOK, renderACL(base, t, e))
}
