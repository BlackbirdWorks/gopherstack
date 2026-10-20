package cosmosdb

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opCreateUser        = "CreateUser"
	opListUsers         = "ListUsers"
	opQueryUsers        = "QueryUsers"
	opGetUser           = "GetUser"
	opReplaceUser       = "ReplaceUser"
	opDeleteUser        = "DeleteUser"
	opCreatePermission  = "CreatePermission"
	opListPermissions   = "ListPermissions"
	opQueryPermissions  = "QueryPermissions"
	opGetPermission     = "GetPermission"
	opReplacePermission = "ReplacePermission"
	opDeletePermission  = "DeletePermission"
	msgEntityNotFound   = "Entity with the specified id does not exist in the system."
	userListKey         = "Users"
	permissionListKey   = "Permissions"
)

func userSupportedOperations() []string {
	return []string{
		opCreateUser, opListUsers, opQueryUsers, opGetUser, opReplaceUser, opDeleteUser,
		opCreatePermission, opListPermissions, opQueryPermissions, opGetPermission, opReplacePermission,
		opDeletePermission,
	}
}

func userOperationFor(kind resourceKind, r *http.Request) (string, bool) {
	switch kind {
	case resourceUsers:
		return collectionOperationFor(r, opCreateUser, opListUsers, opQueryUsers), true
	case resourceUserItem:
		return itemOperationFor(r.Method, opGetUser, opReplaceUser, opDeleteUser), true
	case resourcePermissions:
		return collectionOperationFor(r, opCreatePermission, opListPermissions, opQueryPermissions), true
	case resourcePermissionItem:
		return itemOperationFor(r.Method, opGetPermission, opReplacePermission, opDeletePermission), true
	default:
		return "", false
	}
}

// dispatchUsers routes the user and permission kinds; handled is false for any other kind.
func (h *Handler) dispatchUsers(c *echo.Context, kind resourceKind, dbID, a, b string) (bool, error) {
	switch kind {
	case resourceUsers:
		return true, h.handleUsers(c, dbID)
	case resourceUserItem:
		return true, h.handleUserItem(c, dbID, a)
	case resourcePermissions:
		return true, h.handlePermissions(c, dbID, a)
	case resourcePermissionItem:
		return true, h.handlePermissionItem(c, dbID, a, b)
	default:
		return false, nil
	}
}

func (h *Handler) handleUsers(c *echo.Context, dbID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		if isQueryRequest(c.Request()) {
			return h.queryUsers(c, dbID)
		}

		return h.createUser(c, dbID)
	case http.MethodGet:
		return h.listUsers(c, dbID)
	default:
		return h.methodNotAllowed(c)
	}
}

func (h *Handler) handleUserItem(c *echo.Context, dbID, userID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getUser(c, dbID, userID)
	case http.MethodPut:
		return h.replaceUser(c, dbID, userID)
	case http.MethodDelete:
		return h.deleteUser(c, dbID, userID)
	default:
		return h.methodNotAllowed(c)
	}
}

func (h *Handler) writeUserError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(err, ErrUserNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFound", msgEntityNotFound)
	case errors.Is(err, ErrPermissionNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFound", msgEntityNotFound)
	case errors.Is(err, ErrUserAlreadyExists), errors.Is(err, ErrPermissionAlreadyExists):
		return h.writeError(c, http.StatusConflict, "Conflict",
			"Entity with the specified id already exists in the system.")
	case errors.Is(err, ErrInvalidPermission), errors.Is(err, ErrInvalidTokenExpiry):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// readJSONBody decodes the request body into dst, writing the error response itself on failure.
func (h *Handler) readJSONBody(c *echo.Context, dst any) bool {
	raw, err := httputils.ReadBody(c.Request())
	if err != nil {
		_ = h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")

		return false
	}

	if json.Unmarshal(raw, dst) != nil {
		_ = h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")

		return false
	}

	return true
}

type idBody struct {
	ID string `json:"id"`
}

func (h *Handler) readUserID(c *echo.Context) (string, bool) {
	var req idBody
	if !h.readJSONBody(c, &req) {
		return "", false
	}

	if req.ID == "" {
		_ = h.writeError(c, http.StatusBadRequest, "BadRequest", "id must not be empty.")

		return "", false
	}

	return req.ID, true
}

func (h *Handler) createUser(c *echo.Context, dbID string) error {
	id, ok := h.readUserID(c)
	if !ok {
		return nil
	}

	info, err := h.Backend.CreateUser(dbID, id)
	if err != nil {
		return h.writeUserError(c, err)
	}

	c.Response().Header().Set("ETag", resourceETag(info.Modified, info.RID))

	return h.writeJSON(c, http.StatusCreated, userBody(dbID, info))
}

func (h *Handler) getUser(c *echo.Context, dbID, userID string) error {
	info, err := h.Backend.GetUser(dbID, userID)
	if err != nil {
		return h.writeUserError(c, err)
	}

	c.Response().Header().Set("ETag", resourceETag(info.Modified, info.RID))

	return h.writeJSON(c, http.StatusOK, userBody(dbID, info))
}

func (h *Handler) replaceUser(c *echo.Context, dbID, userID string) error {
	id, ok := h.readUserID(c)
	if !ok {
		return nil
	}

	info, err := h.Backend.ReplaceUser(dbID, userID, id)
	if err != nil {
		return h.writeUserError(c, err)
	}

	c.Response().Header().Set("ETag", resourceETag(info.Modified, info.RID))

	return h.writeJSON(c, http.StatusOK, userBody(dbID, info))
}

func (h *Handler) deleteUser(c *echo.Context, dbID, userID string) error {
	if err := h.Backend.DeleteUser(dbID, userID); err != nil {
		return h.writeUserError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) userBodies(dbID string) ([]map[string]any, error) {
	infos, err := h.Backend.ListUsers(dbID)
	if err != nil {
		return nil, err
	}

	bodies := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		bodies = append(bodies, userBody(dbID, info))
	}

	return bodies, nil
}

func (h *Handler) queryUsers(c *echo.Context, dbID string) error {
	bodies, err := h.userBodies(dbID)
	if err != nil {
		return h.writeUserError(c, err)
	}

	return h.queryResources(c, userListKey, bodies)
}

func (h *Handler) listUsers(c *echo.Context, dbID string) error {
	bodies, err := h.userBodies(dbID)
	if err != nil {
		return h.writeUserError(c, err)
	}

	return h.writeResourceList(c, userListKey, bodies)
}

// writeResourceList writes one page of a resource feed in the standard {_rid, <Key>, _count} envelope.
func (h *Handler) writeResourceList(c *echo.Context, listKey string, bodies []map[string]any) error {
	token, limit, reqErr := pageRequest(c.Request())
	if reqErr != nil {
		return h.writeContinuationError(c)
	}

	pg := paginate(bodies, token, limit)

	setPageHeaders(c, len(pg.Data), pg.Next)
	setCharge(c, ruMetadata)

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: "", listKey: pg.Data, sysPropCount: len(pg.Data),
	})
}

func userBody(dbID string, info UserInfo) map[string]any {
	return map[string]any{
		"id":           info.ID,
		sysPropRID:     info.RID,
		sysPropSelf:    "dbs/" + fakeRID("dbs/"+dbID) + "/users/" + info.RID + "/",
		sysPropETag:    resourceETag(info.Modified, info.RID),
		sysPropTS:      resourceTS(info.Modified),
		"_permissions": permissionsSegment + "/",
	}
}
