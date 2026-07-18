package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func isGroupOp(op string) bool {
	switch op {
	case opCreateGroup, opDescribeGroup, opUpdateGroup, opDeleteGroup, opListGroups, opSearchGroups,
		opCreateGroupMembership, opDescribeGroupMembership, opDeleteGroupMembership, opListGroupMemberships:
		return true
	}

	return false
}

func (h *Handler) dispatchGroup(c *echo.Context, op string) error {
	switch op {
	case opCreateGroup:
		return h.handleCreateGroup(c)
	case opDescribeGroup:
		return h.handleDescribeGroup(c)
	case opUpdateGroup:
		return h.handleUpdateGroup(c)
	case opDeleteGroup:
		return h.handleDeleteGroup(c)
	case opListGroups:
		return h.handleListGroups(c)
	case opSearchGroups:
		return h.handleSearchGroups(c)
	case opCreateGroupMembership:
		return h.handleCreateGroupMembership(c)
	case opDescribeGroupMembership:
		return h.handleDescribeGroupMembership(c)
	case opDeleteGroupMembership:
		return h.handleDeleteGroupMembership(c)
	case opListGroupMemberships:
		return h.handleListGroupMemberships(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// ---- Group handlers ----

func (h *Handler) handleCreateGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	groupName := strField(body, "GroupName")
	description := strField(body, "Description")

	g, err := h.Backend.CreateGroup(accountID, namespace, groupName, description)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyGroup:     groupToMap(g),
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	g, err := h.Backend.DescribeGroup(accountID, namespace, groupName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyGroup:     groupToMap(g),
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	description := strField(body, "Description")

	g, err := h.Backend.UpdateGroup(accountID, namespace, groupName, description)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyGroup:     groupToMap(g),
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteGroup(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	if err := h.Backend.DeleteGroup(accountID, namespace, groupName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListGroups(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	groups, next, err := h.Backend.ListGroups(accountID, namespace, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		items = append(items, groupToMap(g))
	}

	resp := map[string]any{
		keyGroupList: items,
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchGroups(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, _ := readBody(c)
	query := strField(body, "Query")
	maxResults := int32(0)
	if body != nil {
		maxResults = intField(body, "MaxResults")
	}
	nextToken := ""
	if body != nil {
		nextToken = strField(body, "NextToken")
	}

	groups, next, err := h.Backend.SearchGroups(accountID, namespace, query, maxResults, nextToken)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		items = append(items, groupToMap(g))
	}

	resp := map[string]any{
		keyGroupList: items,
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func groupToMap(g *Group) map[string]any {
	return map[string]any{
		keyArn:        g.Arn,
		"Description": g.Description,
		"GroupName":   g.GroupName,
		keyNamespace:  g.Namespace,
		"PrincipalId": g.PrincipalID,
	}
}

// ---- Group Membership handlers ----

func (h *Handler) handleCreateGroupMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	m, err := h.Backend.CreateGroupMembership(accountID, namespace, groupName, memberName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"GroupMember": map[string]any{
			keyArn:        m.Arn,
			keyMemberName: m.MemberName,
		},
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeGroupMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	m, err := h.Backend.DescribeGroupMembership(accountID, namespace, groupName, memberName)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"GroupMember": map[string]any{
			keyArn:        m.Arn,
			keyMemberName: m.MemberName,
		},
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteGroupMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)
	memberName := seg(segs, segSubSubResID)

	if err := h.Backend.DeleteGroupMembership(accountID, namespace, groupName, memberName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListGroupMemberships(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	groupName := seg(segs, segSubResID)

	members, next, err := h.Backend.ListGroupMemberships(
		accountID,
		namespace,
		groupName,
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(members))
	for _, m := range members {
		items = append(items, map[string]any{
			keyArn:        m.Arn,
			keyMemberName: m.MemberName,
		})
	}

	resp := map[string]any{
		"GroupMemberList": items,
		keyRequestID:      newReqID(),
		keyStatus:         http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
