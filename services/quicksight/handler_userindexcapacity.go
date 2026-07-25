package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON request/response keys used only by ListUsersIndexCapacity. Like the
// Space family, this op's wire shape is fully camelCase (confirmed against
// aws-sdk-go-v2/service/quicksight's (de)serializers.go for
// ListUsersIndexCapacity{Input,Output}/UserIndexCapacity) rather than this
// backend's usual PascalCase convention.
const (
	keyNamespaceCamel  = "namespace"
	keyMaxResultsCamel = "maxResults"
	keyNextTokenCamel  = "nextToken"
	keyUsersCamel      = "users"
	keyEmailCamel      = "email"
	keyKBCountCamel    = "kbCount"
	keyRoleCamel       = "role"
	keySpaceCountCamel = "spaceCount"
	keyUserArnCamel    = "userArn"
	keyUserNameCamel   = "userName"

	keyTotalCapacityBytesCamel      = "totalCapacityBytes"
	keyTotalKBCapacityBytesCamel    = "totalKBCapacityBytes"
	keyTotalSpaceCapacityBytesCamel = "totalSpaceCapacityBytes"
)

func userIndexCapacityToMap(u UserIndexCapacity) map[string]any {
	m := map[string]any{
		keyUserArnCamel:                 u.UserArn,
		keyUserNameCamel:                u.UserName,
		keyKBCountCamel:                 u.KBCount,
		keySpaceCountCamel:              u.SpaceCount,
		keyTotalCapacityBytesCamel:      u.TotalCapacityBytes,
		keyTotalKBCapacityBytesCamel:    u.TotalKBCapacityBytes,
		keyTotalSpaceCapacityBytesCamel: u.TotalSpaceCapacityBytes,
	}
	if u.Email != "" {
		m[keyEmailCamel] = u.Email
	}
	if u.Role != "" {
		m[keyRoleCamel] = u.Role
	}

	return m
}

func (h *Handler) handleListUsersIndexCapacity(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	users, next, err := h.Backend.ListUsersIndexCapacity(
		accountID,
		strField(body, keyNamespaceCamel),
		intField(body, keyMaxResultsCamel),
		strField(body, keyNextTokenCamel),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(users))
	for _, u := range users {
		items = append(items, userIndexCapacityToMap(u))
	}

	resp := map[string]any{
		keyUsersCamel:     items,
		keyRequestIDLower: reqIDPlaceholder,
	}
	if next != "" {
		resp[keyNextTokenCamel] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
