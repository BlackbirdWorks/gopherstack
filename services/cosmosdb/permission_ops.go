package cosmosdb

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

// permissionWire is the request body of a permission create or replace.
type permissionWire struct {
	ID           string          `json:"id"`
	Mode         string          `json:"permissionMode"`
	Resource     string          `json:"resource"`
	PartitionKey json.RawMessage `json:"resourcePartitionKey"`
}

func (h *Handler) handlePermissions(c *echo.Context, dbID, userID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		if isQueryRequest(c.Request()) {
			return h.queryPermissions(c, dbID, userID)
		}

		return h.createPermission(c, dbID, userID)
	case http.MethodGet:
		return h.listPermissions(c, dbID, userID)
	default:
		return h.methodNotAllowed(c)
	}
}

func (h *Handler) handlePermissionItem(c *echo.Context, dbID, userID, permID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getPermission(c, dbID, userID, permID)
	case http.MethodPut:
		return h.replacePermission(c, dbID, userID, permID)
	case http.MethodDelete:
		return h.deletePermission(c, dbID, userID, permID)
	default:
		return h.methodNotAllowed(c)
	}
}

func (h *Handler) readPermissionSpec(c *echo.Context) (PermissionSpec, bool) {
	var req permissionWire
	if !h.readJSONBody(c, &req) {
		return PermissionSpec{}, false
	}

	if req.ID == "" {
		_ = h.writeError(c, http.StatusBadRequest, "BadRequest", "id must not be empty.")

		return PermissionSpec{}, false
	}

	return PermissionSpec(req), true
}

func (h *Handler) createPermission(c *echo.Context, dbID, userID string) error {
	ttl, err := h.tokenExpiry(c.Request())
	if err != nil {
		return h.writeUserError(c, err)
	}

	spec, ok := h.readPermissionSpec(c)
	if !ok {
		return nil
	}

	info, err := h.Backend.CreatePermission(dbID, userID, spec)
	if err != nil {
		return h.writeUserError(c, err)
	}

	return h.writePermission(c, http.StatusCreated, info, ttl)
}

func (h *Handler) getPermission(c *echo.Context, dbID, userID, permID string) error {
	ttl, err := h.tokenExpiry(c.Request())
	if err != nil {
		return h.writeUserError(c, err)
	}

	info, err := h.Backend.GetPermission(dbID, userID, permID)
	if err != nil {
		return h.writeUserError(c, err)
	}

	return h.writePermission(c, http.StatusOK, info, ttl)
}

func (h *Handler) replacePermission(c *echo.Context, dbID, userID, permID string) error {
	ttl, err := h.tokenExpiry(c.Request())
	if err != nil {
		return h.writeUserError(c, err)
	}

	spec, ok := h.readPermissionSpec(c)
	if !ok {
		return nil
	}

	info, err := h.Backend.ReplacePermission(dbID, userID, permID, spec)
	if err != nil {
		return h.writeUserError(c, err)
	}

	return h.writePermission(c, http.StatusOK, info, ttl)
}

func (h *Handler) deletePermission(c *echo.Context, dbID, userID, permID string) error {
	if err := h.Backend.DeletePermission(dbID, userID, permID); err != nil {
		return h.writeUserError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) writePermission(c *echo.Context, status int, info PermissionInfo, ttl time.Duration) error {
	body, err := h.permissionBody(info, ttl)
	if err != nil {
		return h.writeUserError(c, err)
	}

	c.Response().Header().Set("ETag", resourceETag(info.Modified, info.RID))

	return h.writeJSON(c, status, body)
}

func (h *Handler) permissionBodies(c *echo.Context, dbID, userID string) ([]map[string]any, error) {
	ttl, err := h.tokenExpiry(c.Request())
	if err != nil {
		return nil, err
	}

	infos, err := h.Backend.ListPermissions(dbID, userID)
	if err != nil {
		return nil, err
	}

	bodies := make([]map[string]any, 0, len(infos))

	for _, info := range infos {
		body, bodyErr := h.permissionBody(info, ttl)
		if bodyErr != nil {
			return nil, bodyErr
		}

		bodies = append(bodies, body)
	}

	return bodies, nil
}

func (h *Handler) queryPermissions(c *echo.Context, dbID, userID string) error {
	bodies, err := h.permissionBodies(c, dbID, userID)
	if err != nil {
		return h.writeUserError(c, err)
	}

	return h.queryResources(c, permissionListKey, bodies)
}

func (h *Handler) listPermissions(c *echo.Context, dbID, userID string) error {
	bodies, err := h.permissionBodies(c, dbID, userID)
	if err != nil {
		return h.writeUserError(c, err)
	}

	return h.writeResourceList(c, permissionListKey, bodies)
}

func (h *Handler) permissionBody(info PermissionInfo, ttl time.Duration) (map[string]any, error) {
	token, err := h.permissionToken(info, ttl)
	if err != nil {
		return nil, err
	}

	userRID := fakeRID("dbs/" + info.DBID + "/users/" + info.UserID)

	body := map[string]any{
		"id":             info.ID,
		"permissionMode": info.Mode,
		"resource":       info.Resource,
		sysPropRID:       info.RID,
		sysPropSelf:      "dbs/" + fakeRID("dbs/"+info.DBID) + "/users/" + userRID + "/permissions/" + info.RID + "/",
		sysPropETag:      resourceETag(info.Modified, info.RID),
		sysPropTS:        resourceTS(info.Modified),
		"_token":         token,
	}

	if len(info.PartitionKey) > 0 {
		body["resourcePartitionKey"] = info.PartitionKey
	}

	return body, nil
}
