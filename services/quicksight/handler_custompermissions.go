package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/body keys used only by custom-permissions/role/user-permission
// operations.
const (
	keyCustomPermissionsName = "CustomPermissionsName"
	keyCustomPermissions     = "CustomPermissions"
	keyCustomPermissionsList = "CustomPermissionsList"
	keyCapabilities          = "Capabilities"
	keyMembersList           = "MembersList"
)

func isCustomPermOp(op string) bool {
	switch op {
	case opCreateCustomPermissions, opDescribeCustomPermissions, opUpdateCustomPermissions,
		opDeleteCustomPermissions, opListCustomPermissions,
		opUpdateRoleCustomPermission, opGetRoleCustomPermission, opDeleteRoleCustomPermission,
		opCreateRoleMembership, opDeleteRoleMembership, opListRoleMemberships,
		opUpdateUserCustomPermission, opDeleteUserCustomPermission:
		return true
	}

	return false
}

func (h *Handler) dispatchCustomPerm(c *echo.Context, op string) error {
	switch op {
	case opCreateCustomPermissions:
		return h.handleCreateCustomPermissions(c)
	case opDescribeCustomPermissions:
		return h.handleDescribeCustomPermissions(c)
	case opUpdateCustomPermissions:
		return h.handleUpdateCustomPermissions(c)
	case opDeleteCustomPermissions:
		return h.handleDeleteCustomPermissions(c)
	case opListCustomPermissions:
		return h.handleListCustomPermissions(c)
	}

	return h.dispatchRoleAndUserCustomPerm(c, op)
}

func (h *Handler) dispatchRoleAndUserCustomPerm(c *echo.Context, op string) error {
	switch op {
	case opUpdateRoleCustomPermission:
		return h.handleUpdateRoleCustomPermission(c)
	case opGetRoleCustomPermission:
		return h.handleDescribeRoleCustomPermission(c)
	case opDeleteRoleCustomPermission:
		return h.handleDeleteRoleCustomPermission(c)
	case opCreateRoleMembership:
		return h.handleCreateRoleMembership(c)
	case opDeleteRoleMembership:
		return h.handleDeleteRoleMembership(c)
	case opListRoleMemberships:
		return h.handleListRoleMemberships(c)
	case opUpdateUserCustomPermission:
		return h.handleUpdateUserCustomPermission(c)
	case opDeleteUserCustomPermission:
		return h.handleDeleteUserCustomPermission(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

// ---- Custom permissions ----

func customPermissionsToMap(cp *CustomPermissions) map[string]any {
	return map[string]any{
		keyCustomPermissionsName: cp.Name,
		keyArn:                   cp.Arn,
		keyCapabilities:          cp.Capabilities,
	}
}

func (h *Handler) handleCreateCustomPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	name := strField(body, keyCustomPermissionsName)

	cp, err := h.Backend.CreateCustomPermissions(
		accountID, name, mapField(body, keyCapabilities), tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrCustomPermissionsAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       cp.Arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeCustomPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	name := seg(segs, segResID)

	cp, err := h.Backend.DescribeCustomPermissions(accountID, name)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyCustomPermissions: customPermissionsToMap(cp),
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

func (h *Handler) handleUpdateCustomPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	name := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	cp, err := h.Backend.UpdateCustomPermissions(accountID, name, mapField(body, keyCapabilities))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       cp.Arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteCustomPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	name := seg(segs, segResID)

	cp, err := h.Backend.DeleteCustomPermissions(accountID, name)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       cp.Arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListCustomPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	items, next, err := h.Backend.ListCustomPermissions(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	list := make([]map[string]any, 0, len(items))
	for _, cp := range items {
		list = append(list, customPermissionsToMap(cp))
	}

	resp := map[string]any{
		keyCustomPermissionsList: list,
		keyRequestID:             reqIDPlaceholder,
		keyStatus:                http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- Role custom permission ----

func (h *Handler) handleUpdateRoleCustomPermission(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	role := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if err = h.Backend.UpdateRoleCustomPermission(
		accountID, namespace, role, strField(body, keyCustomPermissionsName),
	); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeRoleCustomPermission(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	role := seg(segs, segSubResID)

	name, err := h.Backend.DescribeRoleCustomPermission(accountID, namespace, role)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyCustomPermissionsName: name,
		keyRequestID:             reqIDPlaceholder,
		keyStatus:                http.StatusOK,
	})
}

func (h *Handler) handleDeleteRoleCustomPermission(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	role := seg(segs, segSubResID)

	if err := h.Backend.DeleteRoleCustomPermission(accountID, namespace, role); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Role memberships ----

func (h *Handler) handleCreateRoleMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	role := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	if err := h.Backend.CreateRoleMembership(accountID, namespace, role, memberName); err != nil {
		if errors.Is(err, ErrRoleMembershipAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteRoleMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	role := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	if err := h.Backend.DeleteRoleMembership(accountID, namespace, role, memberName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListRoleMemberships(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	role := seg(segs, segSubResID)

	members, next, err := h.Backend.ListRoleMemberships(
		accountID, namespace, role, maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		keyMembersList: members,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- User custom permission ----

func (h *Handler) handleUpdateUserCustomPermission(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	userName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if err = h.Backend.UpdateUserCustomPermission(
		accountID, namespace, userName, strField(body, keyCustomPermissionsName),
	); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteUserCustomPermission(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	userName := seg(segs, segSubResID)

	if err := h.Backend.DeleteUserCustomPermission(accountID, namespace, userName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// classifyCustomPermissionsPaths routes /accounts/{id}/custom-permissions/... paths.
func classifyCustomPermissionsPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateCustomPermissions, accountID
		case http.MethodGet:
			return opListCustomPermissions, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeCustomPermissions, id
		case http.MethodPut:
			return opUpdateCustomPermissions, id
		case http.MethodDelete:
			return opDeleteCustomPermissions, id
		}
	}

	return opUnknown, ""
}
