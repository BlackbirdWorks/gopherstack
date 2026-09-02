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

	keyFiltersCamel         = "filters"
	keySortByCamel          = "sortBy"
	keySortOrderCamel       = "sortOrder"
	keyMinBytesCamel        = "minBytes"
	keyMaxBytesCamel        = "maxBytes"
	keyPrefixCamel          = "prefix"
	keyUserNameOrEmailCamel = "userNameOrEmail"

	sortOrderAsc = "ASC"
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

// userIndexCapacityQueryFromBody parses ListUsersIndexCapacityInput's
// "filters"/"sortBy"/"sortOrder" body fields (quicksight@v1.123.1
// serializers.go's awsRestjson1_serializeOpDocumentListUsersIndexCapacityInput
// and awsRestjson1_serializeDocumentUserIndexCapacityFilter): each filters
// entry is either {"totalCapacityBytes":{"minBytes":N,"maxBytes":N}} or
// {"userNameOrEmail":{"prefix":"..."}}. SortOrder "Defaults to DESC if not
// specified" per api_op_ListUsersIndexCapacity.go; since
// UserIndexCapacitySortBy has exactly one legal member
// (TOTAL_CAPACITY_BYTES), sending either field is enough to mean "sort by
// capacity".
func userIndexCapacityQueryFromBody(body map[string]any) UserIndexCapacityQuery {
	var q UserIndexCapacityQuery

	filters, _ := body[keyFiltersCamel].([]any)
	for _, raw := range filters {
		entry, isMap := raw.(map[string]any)
		if !isMap {
			continue
		}
		applyCapacityBytesFilter(&q, entry)
		applyUserNameOrEmailFilter(&q, entry)
	}

	sortBy := strField(body, keySortByCamel)
	sortOrder := strField(body, keySortOrderCamel)
	q.SortByCapacity = sortBy != "" || sortOrder != ""
	q.SortDescending = sortOrder != sortOrderAsc

	return q
}

// applyCapacityBytesFilter reads entry's "totalCapacityBytes" union member
// (CapacityBytesRangeFilter's minBytes/maxBytes) into q, if present.
func applyCapacityBytesFilter(q *UserIndexCapacityQuery, entry map[string]any) {
	bytesFilter, isMap := entry[keyTotalCapacityBytesCamel].(map[string]any)
	if !isMap {
		return
	}
	if v, isNum := bytesFilter[keyMinBytesCamel].(float64); isNum {
		n := int64(v)
		q.MinCapacityBytes = &n
	}
	if v, isNum := bytesFilter[keyMaxBytesCamel].(float64); isNum {
		n := int64(v)
		q.MaxCapacityBytes = &n
	}
}

// applyUserNameOrEmailFilter reads entry's "userNameOrEmail" union member
// (UserNameOrEmailFilter's prefix) into q, if present.
func applyUserNameOrEmailFilter(q *UserIndexCapacityQuery, entry map[string]any) {
	prefixFilter, isMap := entry[keyUserNameOrEmailCamel].(map[string]any)
	if !isMap {
		return
	}
	if p, isStr := prefixFilter[keyPrefixCamel].(string); isStr {
		q.Prefix = &p
	}
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
		userIndexCapacityQueryFromBody(body),
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
