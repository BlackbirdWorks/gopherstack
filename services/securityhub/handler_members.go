package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// --- Classify functions ---

func classifyMembersPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/members":
		return opCreateMembers, ""
	case method == http.MethodPost && path == "/members/delete":
		return opDeleteMembers, ""
	case method == http.MethodPost && path == "/members/get":
		return opGetMembers, ""
	case method == http.MethodPost && path == "/members/invite":
		return opInviteMembers, ""
	case method == http.MethodGet && path == "/members":
		return opListMembers, ""
	case method == http.MethodPost && path == "/members/disassociate":
		return opDisassociateMembers, ""
	}

	return opUnknown, ""
}

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
	case method == http.MethodPost && path == "/administrator":
		return opAcceptAdministratorInvitation, ""
	case method == http.MethodGet && path == "/administrator":
		return opGetAdministratorAccount, ""
	case method == http.MethodPost && path == "/administrator/disassociate":
		return opDisassociateFromAdministratorAccount, ""
	}

	return opUnknown, ""
}

func classifyMasterPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/master":
		return opAcceptInvitation, ""
	case method == http.MethodGet && path == "/master":
		return opGetMasterAccount, ""
	case method == http.MethodPost && path == "/master/disassociate":
		return opDisassociateFromMasterAccount, ""
	}

	return opUnknown, ""
}

func classifyOrganizationPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/organization/configuration":
		return opDescribeOrganizationConfiguration, ""
	case method == http.MethodPost && path == "/organization/configuration":
		return opUpdateOrganizationConfiguration, ""
	case method == http.MethodPost && path == "/organization/admin/enable":
		return opEnableOrganizationAdminAccount, ""
	case method == http.MethodPost && path == "/organization/admin/disable":
		return opDisableOrganizationAdminAccount, ""
	case method == http.MethodGet && path == "/organization/admin":
		return opListOrganizationAdminAccounts, ""
	}

	return opUnknown, ""
}

// --- Handler functions ---

func (h *Handler) handleCreateMembers(c *echo.Context, body map[string]any) error {
	var accounts []map[string]any

	if raw, ok := body["AccountDetails"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
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
			if s, ok := v.(string); ok {
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
			if s, ok := v.(string); ok {
				accountIDs = append(accountIDs, s)
			}
		}
	}

	members, unprocessed := h.Backend.GetMembers(accountIDs)

	var membersOut []map[string]any

	for _, m := range members {
		membersOut = append(membersOut, map[string]any{
			"AccountId":       m.AccountId,
			"AdministratorId": m.AdministratorId,
			"MasterId":        m.MasterId,
			"Email":           m.Email,
			"MemberStatus":    m.MemberStatus,
			"InvitedAt":       m.InvitedAt,
			"UpdatedAt":       m.UpdatedAt,
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
			if s, ok := v.(string); ok {
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

	var membersOut []map[string]any

	for _, m := range members {
		membersOut = append(membersOut, map[string]any{
			"AccountId":       m.AccountId,
			"AdministratorId": m.AdministratorId,
			"MasterId":        m.MasterId,
			"Email":           m.Email,
			"MemberStatus":    m.MemberStatus,
			"InvitedAt":       m.InvitedAt,
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
			if s, ok := v.(string); ok {
				accountIDs = append(accountIDs, s)
			}
		}
	}

	if err := h.Backend.DisassociateMembers(accountIDs); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
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
			if s, ok := v.(string); ok {
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
			if s, ok := v.(string); ok {
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

	var invOut []map[string]any

	for _, inv := range invitations {
		invOut = append(invOut, map[string]any{
			"AccountId":    inv.AccountId,
			"InvitationId": inv.InvitationId,
			"InvitedAt":    inv.InvitedAt,
			"MemberStatus": inv.MemberStatus,
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
			"AccountId":          admin.AccountId,
			"InvitationId":       admin.InvitationId,
			"InvitedAt":          admin.InvitedAt,
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
			"AccountId":          master.AccountId,
			"InvitationId":       master.InvitationId,
			"InvitedAt":          master.InvitedAt,
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

func (h *Handler) handleDescribeOrganizationConfiguration(c *echo.Context) error {
	cfg := h.Backend.DescribeOrganizationConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		"AutoEnable":                cfg.AutoEnable,
		"MemberAccountLimitReached": cfg.MemberAccountLimitReached,
		"AutoEnableStandards":       cfg.AutoEnableStandards,
		"OrganizationConfiguration": map[string]any{
			"ConfigurationType": cfg.OrganizationConfigurationType,
		},
	})
}

func (h *Handler) handleUpdateOrganizationConfiguration(c *echo.Context, body map[string]any) error {
	autoEnable, _ := body["AutoEnable"].(bool)
	autoEnableStandards, _ := body["AutoEnableStandards"].(string)

	orgConfigType := ""

	if oc, ok := body["OrganizationConfiguration"].(map[string]any); ok {
		orgConfigType, _ = oc["ConfigurationType"].(string)
	}

	if err := h.Backend.UpdateOrganizationConfiguration(autoEnable, autoEnableStandards, orgConfigType); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleEnableOrganizationAdminAccount(c *echo.Context, body map[string]any) error {
	accountID, _ := body["AdminAccountId"].(string)

	if accountID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: "AdminAccountId is required",
		})
	}

	if err := h.Backend.EnableOrganizationAdminAccount(accountID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableOrganizationAdminAccount(c *echo.Context, body map[string]any) error {
	accountID, _ := body["AdminAccountId"].(string)

	if accountID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: "AdminAccountId is required",
		})
	}

	if err := h.Backend.DisableOrganizationAdminAccount(accountID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListOrganizationAdminAccounts(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	accounts, next := h.Backend.ListOrganizationAdminAccounts(nextToken, maxResults)

	var out []map[string]any

	for _, a := range accounts {
		out = append(out, map[string]any{
			"AccountId": a.AccountId,
			"Status":    a.Status,
		})
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"AdminAccounts": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Finding Aggregator handlers ---

func classifyFindingAggregatorPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/findingAggregator/create":
		return opCreateFindingAggregator, ""
	case method == http.MethodGet && strings.HasPrefix(path, "/findingAggregator/get/"):
		return opGetFindingAggregator, strings.TrimPrefix(path, "/findingAggregator/get/")
	case method == http.MethodGet && path == "/findingAggregator/list":
		return opListFindingAggregators, ""
	case method == http.MethodPatch && path == "/findingAggregator/update":
		return opUpdateFindingAggregator, ""
	case method == http.MethodDelete && strings.HasPrefix(path, "/findingAggregator/delete/"):
		return opDeleteFindingAggregator, strings.TrimPrefix(path, "/findingAggregator/delete/")
	}

	return opUnknown, ""
}

func (h *Handler) handleCreateFindingAggregator(c *echo.Context, body map[string]any) error {
	regionLinkingMode, _ := body["RegionLinkingMode"].(string)

	var regions []string

	if raw, ok := body["Regions"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				regions = append(regions, s)
			}
		}
	}

	agg, err := h.Backend.CreateFindingAggregator(regionLinkingMode, regions)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingAggregatorArn":     agg.FindingAggregatorArn,
		"FindingAggregationRegion": agg.FindingAggregationRegion,
		"RegionLinkingMode":        agg.RegionLinkingMode,
		"Regions":                  agg.Regions,
	})
}

func (h *Handler) handleGetFindingAggregator(c *echo.Context, arn string) error {
	agg, err := h.Backend.GetFindingAggregator(arn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Finding aggregator not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingAggregatorArn":     agg.FindingAggregatorArn,
		"FindingAggregationRegion": agg.FindingAggregationRegion,
		"RegionLinkingMode":        agg.RegionLinkingMode,
		"Regions":                  agg.Regions,
	})
}

func (h *Handler) handleListFindingAggregators(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	aggs, next := h.Backend.ListFindingAggregators(nextToken, maxResults)

	var out []map[string]any

	for _, agg := range aggs {
		out = append(out, map[string]any{
			"FindingAggregatorArn": agg.FindingAggregatorArn,
		})
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"FindingAggregators": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateFindingAggregator(c *echo.Context, body map[string]any) error {
	arn, _ := body["FindingAggregatorArn"].(string)
	regionLinkingMode, _ := body["RegionLinkingMode"].(string)

	var regions []string

	if raw, ok := body["Regions"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				regions = append(regions, s)
			}
		}
	}

	agg, err := h.Backend.UpdateFindingAggregator(arn, regionLinkingMode, regions)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Finding aggregator not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingAggregatorArn":     agg.FindingAggregatorArn,
		"FindingAggregationRegion": agg.FindingAggregationRegion,
		"RegionLinkingMode":        agg.RegionLinkingMode,
		"Regions":                  agg.Regions,
	})
}

func (h *Handler) handleDeleteFindingAggregator(c *echo.Context, arn string) error {
	if err := h.Backend.DeleteFindingAggregator(arn); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Finding aggregator not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
