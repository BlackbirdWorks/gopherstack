package securityhub

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyMembersPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == pathMembers:
		return opCreateMembers, ""
	case method == http.MethodPost && path == "/members/delete":
		return opDeleteMembers, ""
	case method == http.MethodPost && path == "/members/get":
		return opGetMembers, ""
	case method == http.MethodPost && path == "/members/invite":
		return opInviteMembers, ""
	case method == http.MethodGet && path == pathMembers:
		return opListMembers, ""
	case method == http.MethodPost && path == "/members/disassociate":
		return opDisassociateMembers, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleCreateMembers(c *echo.Context, body map[string]any) error {
	var accounts []map[string]any

	if raw, ok := body["AccountDetails"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok { //nolint:govet // existing issue.
				accounts = append(accounts, m)
			}
		}
	}

	created, unprocessed := h.Backend.CreateMembers(accounts)

	resp := map[string]any{}

	if len(unprocessed) > 0 {
		resp["UnprocessedAccounts"] = unprocessed
	} else {
		resp["UnprocessedAccounts"] = []any{}
	}

	_ = created

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteMembers(c *echo.Context, body map[string]any) error {
	var accountIDs []string

	if raw, ok := body["AccountIds"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				accountIDs = append(accountIDs, s)
			}
		}
	}

	_, unprocessed := h.Backend.DeleteMembers(accountIDs)

	resp := map[string]any{}

	if len(unprocessed) > 0 {
		resp["UnprocessedAccounts"] = unprocessed
	} else {
		resp["UnprocessedAccounts"] = []any{}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetMembers(c *echo.Context, body map[string]any) error {
	var accountIDs []string

	if raw, ok := body["AccountIds"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				accountIDs = append(accountIDs, s)
			}
		}
	}

	members, unprocessed := h.Backend.GetMembers(accountIDs)

	var membersOut []map[string]any //nolint:prealloc // existing issue.

	for _, m := range members {
		membersOut = append(membersOut, map[string]any{
			keyAccountID:      m.AccountId,
			"AdministratorId": m.AdministratorId,
			"MasterId":        m.MasterId,
			"Email":           m.Email,
			keyMemberStatus:   m.MemberStatus,
			keyInvitedAt:      m.InvitedAt,
			"UpdatedAt":       m.UpdatedAt, //nolint:goconst // existing issue.
		})
	}

	if membersOut == nil {
		membersOut = []map[string]any{}
	}

	resp := map[string]any{
		"Members": membersOut,
	}

	if len(unprocessed) > 0 {
		resp["UnprocessedAccounts"] = unprocessed
	} else {
		resp["UnprocessedAccounts"] = []any{}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleInviteMembers(c *echo.Context, body map[string]any) error {
	var accountIDs []string

	if raw, ok := body["AccountIds"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				accountIDs = append(accountIDs, s)
			}
		}
	}

	unprocessed := h.Backend.InviteMembers(accountIDs)

	resp := map[string]any{}

	if len(unprocessed) > 0 {
		resp["UnprocessedAccounts"] = unprocessed
	} else {
		resp["UnprocessedAccounts"] = []any{}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListMembers(c *echo.Context) error {
	onlyAssociated := false

	if v := c.QueryParam("OnlyAssociated"); v != "" {
		onlyAssociated = strings.EqualFold(v, "true")
	}

	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	members, next := h.Backend.ListMembers(onlyAssociated, nextToken, maxResults)

	var membersOut []map[string]any //nolint:prealloc // existing issue.

	for _, m := range members {
		membersOut = append(membersOut, map[string]any{
			keyAccountID:      m.AccountId,
			"AdministratorId": m.AdministratorId,
			"MasterId":        m.MasterId,
			"Email":           m.Email,
			keyMemberStatus:   m.MemberStatus,
			keyInvitedAt:      m.InvitedAt,
			"UpdatedAt":       m.UpdatedAt,
		})
	}

	if membersOut == nil {
		membersOut = []map[string]any{}
	}

	resp := map[string]any{"Members": membersOut}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDisassociateMembers(c *echo.Context, body map[string]any) error {
	var accountIDs []string

	if raw, ok := body["AccountIds"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				accountIDs = append(accountIDs, s)
			}
		}
	}

	if err := h.Backend.DisassociateMembers(accountIDs); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// membersOpHandlers returns the Members operation dispatch table for
// handleREST.
func (h *Handler) membersOpHandlers(c *echo.Context, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opCreateMembers:       func() error { return h.handleCreateMembers(c, body) },
		opDeleteMembers:       func() error { return h.handleDeleteMembers(c, body) },
		opGetMembers:          func() error { return h.handleGetMembers(c, body) },
		opInviteMembers:       func() error { return h.handleInviteMembers(c, body) },
		opListMembers:         func() error { return h.handleListMembers(c) },
		opDisassociateMembers: func() error { return h.handleDisassociateMembers(c, body) },
	}
}
