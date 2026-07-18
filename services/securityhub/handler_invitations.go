package securityhub

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func classifyInvitationsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/invitations/decline":
		return opDeclineInvitations, ""
	case method == http.MethodPost && path == "/invitations/delete":
		return opDeleteInvitations, ""
	case method == http.MethodGet && path == "/invitations/count":
		return opGetInvitationsCount, ""
	case method == http.MethodGet && path == "/invitations":
		return opListInvitations, ""
	}

	return opUnknown, ""
}

func classifyAdministratorPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == pathAdministrator:
		return opAcceptAdministratorInvitation, ""
	case method == http.MethodGet && path == pathAdministrator:
		return opGetAdministratorAccount, ""
	case method == http.MethodPost && path == "/administrator/disassociate":
		return opDisassociateFromAdministratorAccount, ""
	}

	return opUnknown, ""
}

func classifyMasterPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == pathMaster:
		return opAcceptInvitation, ""
	case method == http.MethodGet && path == pathMaster:
		return opGetMasterAccount, ""
	case method == http.MethodPost && path == "/master/disassociate":
		return opDisassociateFromMasterAccount, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleAcceptAdministratorInvitation(c *echo.Context, body map[string]any) error {
	administratorID, _ := body["AdministratorId"].(string)
	invitationID, _ := body["InvitationId"].(string)

	if err := h.Backend.AcceptAdministratorInvitation(administratorID, invitationID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleAcceptInvitation(c *echo.Context, body map[string]any) error {
	masterID, _ := body["MasterId"].(string)
	invitationID, _ := body["InvitationId"].(string)

	if err := h.Backend.AcceptInvitation(masterID, invitationID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeclineInvitations(c *echo.Context, body map[string]any) error {
	var accountIDs []string

	if raw, ok := body["AccountIds"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				accountIDs = append(accountIDs, s)
			}
		}
	}

	declined, unprocessed := h.Backend.DeclineInvitations(accountIDs)

	resp := map[string]any{
		"ProcessedAccounts": declined,
	}

	if len(unprocessed) > 0 {
		resp["UnprocessedAccounts"] = unprocessed
	} else {
		resp["UnprocessedAccounts"] = []any{}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteInvitations(c *echo.Context, body map[string]any) error {
	var accountIDs []string

	if raw, ok := body["AccountIds"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				accountIDs = append(accountIDs, s)
			}
		}
	}

	deleted, unprocessed := h.Backend.DeleteInvitations(accountIDs)

	resp := map[string]any{
		"ProcessedAccounts": deleted,
	}

	if len(unprocessed) > 0 {
		resp["UnprocessedAccounts"] = unprocessed
	} else {
		resp["UnprocessedAccounts"] = []any{}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetInvitationsCount(c *echo.Context) error {
	count := h.Backend.GetInvitationsCount()

	return c.JSON(http.StatusOK, map[string]any{
		"InvitationsCount": count,
	})
}

func (h *Handler) handleListInvitations(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	invitations, next := h.Backend.ListInvitations(nextToken, maxResults)

	var invOut []map[string]any //nolint:prealloc // existing issue.

	for _, inv := range invitations {
		invOut = append(invOut, map[string]any{
			keyAccountID:    inv.AccountId,
			"InvitationId":  inv.InvitationId, //nolint:goconst // existing issue.
			keyInvitedAt:    inv.InvitedAt,
			keyMemberStatus: inv.MemberStatus,
		})
	}

	if invOut == nil {
		invOut = []map[string]any{}
	}

	resp := map[string]any{"Invitations": invOut}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetAdministratorAccount(c *echo.Context) error {
	admin, err := h.Backend.GetAdministratorAccount()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	if admin == nil {
		return c.JSON(http.StatusOK, map[string]any{
			"Administrator": nil,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Administrator": map[string]any{
			keyAccountID:         admin.AccountId,
			"InvitationId":       admin.InvitationId,
			keyInvitedAt:         admin.InvitedAt,
			"RelationshipStatus": admin.RelationshipStatus,
		},
	})
}

func (h *Handler) handleGetMasterAccount(c *echo.Context) error {
	master, err := h.Backend.GetMasterAccount()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	if master == nil {
		return c.JSON(http.StatusOK, map[string]any{
			"Master": nil,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Master": map[string]any{
			keyAccountID:         master.AccountId,
			"InvitationId":       master.InvitationId,
			keyInvitedAt:         master.InvitedAt,
			"RelationshipStatus": master.RelationshipStatus,
		},
	})
}

func (h *Handler) handleDisassociateFromAdministratorAccount(c *echo.Context) error {
	if err := h.Backend.DisassociateFromAdministratorAccount(); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisassociateFromMasterAccount(c *echo.Context) error {
	if err := h.Backend.DisassociateFromMasterAccount(); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// invitationsOpHandlers returns the Invitations/Administrator/Master
// operation dispatch table for handleREST.
func (h *Handler) invitationsOpHandlers(c *echo.Context, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opAcceptAdministratorInvitation:        func() error { return h.handleAcceptAdministratorInvitation(c, body) },
		opAcceptInvitation:                     func() error { return h.handleAcceptInvitation(c, body) },
		opDeclineInvitations:                   func() error { return h.handleDeclineInvitations(c, body) },
		opDeleteInvitations:                    func() error { return h.handleDeleteInvitations(c, body) },
		opGetInvitationsCount:                  func() error { return h.handleGetInvitationsCount(c) },
		opListInvitations:                      func() error { return h.handleListInvitations(c) },
		opGetAdministratorAccount:              func() error { return h.handleGetAdministratorAccount(c) },
		opGetMasterAccount:                     func() error { return h.handleGetMasterAccount(c) },
		opDisassociateFromAdministratorAccount: func() error { return h.handleDisassociateFromAdministratorAccount(c) },
		opDisassociateFromMasterAccount:        func() error { return h.handleDisassociateFromMasterAccount(c) },
	}
}
