package detective

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn string `json:"GraphArn"`
		Message  string `json:"Message"`
		Accounts []struct {
			AccountID    string `json:"AccountId"`
			EmailAddress string `json:"EmailAddress"`
		} `json:"Accounts"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	accounts := make([]Account, 0, len(req.Accounts))
	for _, a := range req.Accounts {
		accounts = append(accounts, Account{
			AccountID:    a.AccountID,
			EmailAddress: a.EmailAddress,
		})
	}

	members, unprocessed, createErr := h.Backend.CreateMembers(req.GraphArn, accounts, req.Message)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Members":              memberDetailsToJSON(members),
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleDeleteMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string   `json:"GraphArn"`
		AccountIDs []string `json:"AccountIds"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	deleted, unprocessed, delErr := h.Backend.DeleteMembers(req.GraphArn, req.AccountIDs)
	if delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"AccountIds":           deleted,
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleGetMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string   `json:"GraphArn"`
		AccountIDs []string `json:"AccountIds"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	members, unprocessed, getErr := h.Backend.GetMembers(req.GraphArn, req.AccountIDs)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MemberDetails":        memberDetailsToJSON(members),
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleListMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		NextToken  string `json:"NextToken"`
		GraphArn   string `json:"GraphArn"`
		MaxResults int32  `json:"MaxResults"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	members, nextToken, listErr := h.Backend.ListMembers(req.GraphArn, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	resp := map[string]any{
		"MemberDetails": memberDetailsToJSON(members),
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleAcceptInvitation(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn string `json:"GraphArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if acceptErr := h.Backend.AcceptInvitation(req.GraphArn); acceptErr != nil {
		return h.mapError(c, acceptErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRejectInvitation(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn string `json:"GraphArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if rejectErr := h.Backend.RejectInvitation(req.GraphArn); rejectErr != nil {
		return h.mapError(c, rejectErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInvitations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		NextToken  string `json:"NextToken"`
		MaxResults int32  `json:"MaxResults"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	invitations, nextToken, listErr := h.Backend.ListInvitations(req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	resp := map[string]any{
		"Invitations": memberDetailsToJSON(invitations),
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDisassociateMembership(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn string `json:"GraphArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if disErr := h.Backend.DisassociateMembership(req.GraphArn); disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func memberDetailsToJSON(members []*MemberDetail) []map[string]any {
	result := make([]map[string]any, 0, len(members))
	for _, m := range members {
		result = append(result, map[string]any{
			keyAccountID:      m.AccountID,
			"AdministratorId": m.AdministratorID,
			"EmailAddress":    m.EmailAddress,
			keyGraphArn:       m.GraphARN,
			"InvitedTime":     m.InvitedTime.Format("2006-01-02T15:04:05.000Z"),
			"MasterId":        m.AdministratorID,
			keyStatusField:    m.Status,
			"UpdatedTime":     m.UpdatedTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return result
}

func unprocessedToJSON(accounts []UnprocessedAccount) []map[string]any {
	result := make([]map[string]any, 0, len(accounts))
	for _, a := range accounts {
		result = append(result, map[string]any{
			keyAccountID: a.AccountID,
			"Reason":     a.Reason,
		})
	}

	return result
}
