package guardduty

import (
	"encoding/json"
	"net/http"
	"strings"
)

// --- new root path parsers ---

func parseAdminPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case 1: // /admin
		if method == http.MethodGet {
			return opListOrganizationAdminAccounts, ""
		}
	case 2: //nolint:mnd // existing issue.
		switch parts[1] {
		case "enable":
			if method == http.MethodPost {
				return opEnableOrganizationAdminAccount, ""
			}
		case "disable":
			if method == http.MethodPost {
				return opDisableOrganizationAdminAccount, ""
			}
		}
	}

	return opUnknown, ""
}

func parseInvitationPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case 1: // /invitation
		if method == http.MethodGet {
			return opListInvitations, ""
		}
	case 2: //nolint:mnd // existing issue.
		switch parts[1] {
		case "count":
			if method == http.MethodGet {
				return opGetInvitationsCount, ""
			}
		case "decline":
			if method == http.MethodPost {
				return opDeclineInvitations, ""
			}
		case "delete":
			if method == http.MethodPost {
				return opDeleteInvitations, ""
			}
		}
	}

	return opUnknown, ""
}

func parseMalwareProtectionPlanPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case 1: // /malware-protection-plan
		switch method {
		case http.MethodPost:
			return opCreateMalwareProtectionPlan, ""
		case http.MethodGet:
			return opListMalwareProtectionPlans, ""
		}
	case 2: //nolint:mnd // existing issue.
		planID := parts[1]
		switch method {
		case http.MethodGet:
			return opGetMalwareProtectionPlan, planID
		case http.MethodPost:
			return opUpdateMalwareProtectionPlan, planID
		case http.MethodDelete:
			return opDeleteMalwareProtectionPlan, planID
		}
	}

	return opUnknown, ""
}

func parseMalwareScanPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case 1: // /malware-scan
		if method == http.MethodPost {
			return opListMalwareScans, ""
		}
	case 2: //nolint:mnd // existing issue.
		switch parts[1] {
		case "start":
			if method == http.MethodPost {
				return opStartMalwareScan, ""
			}
		default:
			if method == http.MethodGet {
				return opGetMalwareScan, parts[1]
			}
		}
	}

	return opUnknown, ""
}

// --- member dispatch ---

func (h *Handler) dispatchMemberOps(op, path string, body []byte) (any, int, bool, error) {
	detectorID := extractID(path, pathDetector)

	switch op {
	case opCreateMembers:
		result, code, err := h.handleCreateMembers(detectorID, body)

		return result, code, true, err

	case opDeleteMembers:
		result, code, err := h.handleDeleteMembers(detectorID, body)

		return result, code, true, err

	case opGetMembers:
		result, code, err := h.handleGetMembers(detectorID, body)

		return result, code, true, err

	case opInviteMembers:
		result, code, err := h.handleInviteMembers(detectorID, body)

		return result, code, true, err

	case opListMembers:
		result, code, err := h.handleListMembers(detectorID)

		return result, code, true, err

	case opStartMonitoringMembers:
		result, code, err := h.handleStartMonitoringMembers(detectorID, body)

		return result, code, true, err

	case opStopMonitoringMembers:
		result, code, err := h.handleStopMonitoringMembers(detectorID, body)

		return result, code, true, err

	case opDisassociateMembers:
		result, code, err := h.handleDisassociateMembers(detectorID, body)

		return result, code, true, err

	case opGetMemberDetectors:
		result, code, err := h.handleGetMemberDetectors(detectorID, body)

		return result, code, true, err

	case opUpdateMemberDetectors:
		result, code, err := h.handleUpdateMemberDetectors(detectorID, body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

// --- invitation dispatch ---

func (h *Handler) dispatchInvitationOps(op, path string, body []byte) (any, int, bool, error) {
	detectorID := extractID(path, pathDetector)

	switch op {
	case opAcceptAdministratorInvitation:
		code, err := h.handleAcceptAdministratorInvitation(detectorID, body)

		return nil, code, true, err

	case opAcceptInvitation:
		code, err := h.handleAcceptInvitation(detectorID, body)

		return nil, code, true, err

	case opGetAdministratorAccount:
		result, code, err := h.handleGetAdministratorAccount(detectorID)

		return result, code, true, err

	case opGetMasterAccount:
		result, code, err := h.handleGetMasterAccount(detectorID)

		return result, code, true, err

	case opDisassociateFromAdministratorAccount:
		code, err := h.handleDisassociateFromAdministratorAccount(detectorID)

		return nil, code, true, err

	case opDisassociateFromMasterAccount:
		code, err := h.handleDisassociateFromMasterAccount(detectorID)

		return nil, code, true, err

	case opDeclineInvitations:
		result, code, err := h.handleDeclineInvitations(body)

		return result, code, true, err

	case opDeleteInvitations:
		result, code, err := h.handleDeleteInvitations(body)

		return result, code, true, err

	case opGetInvitationsCount:
		result, code := h.handleGetInvitationsCount()

		return result, code, true, nil

	case opListInvitations:
		result, code := h.handleListInvitations()

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

// --- org dispatch ---

func (h *Handler) dispatchOrgOps(op, path string, body []byte) (any, int, bool, error) {
	detectorID := extractID(path, pathDetector)

	switch op {
	case opEnableOrganizationAdminAccount:
		code, err := h.handleEnableOrganizationAdminAccount(body)

		return nil, code, true, err

	case opDisableOrganizationAdminAccount:
		code, err := h.handleDisableOrganizationAdminAccount(body)

		return nil, code, true, err

	case opListOrganizationAdminAccounts:
		result, code := h.handleListOrganizationAdminAccounts()

		return result, code, true, nil

	case opDescribeOrganizationConfiguration:
		result, code, err := h.handleDescribeOrganizationConfiguration(detectorID)

		return result, code, true, err

	case opUpdateOrganizationConfiguration:
		code, err := h.handleUpdateOrganizationConfiguration(detectorID, body)

		return nil, code, true, err

	case opGetOrganizationStatistics:
		result, code := h.handleGetOrganizationStatistics()

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

// --- publishing destination dispatch ---

func (h *Handler) dispatchPublishingDestOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreatePublishingDestination:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreatePublishingDestination(detectorID, body)

		return result, code, true, err

	case opDeletePublishingDestination:
		detectorID, destID := extractDetectorAndSubID(path)
		code, err := h.handleDeletePublishingDestination(detectorID, destID)

		return nil, code, true, err

	case opDescribePublishingDestination:
		detectorID, destID := extractDetectorAndSubID(path)
		result, code, err := h.handleDescribePublishingDestination(detectorID, destID)

		return result, code, true, err

	case opListPublishingDestinations:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListPublishingDestinations(detectorID)

		return result, code, true, err

	case opUpdatePublishingDestination:
		detectorID, destID := extractDetectorAndSubID(path)
		code, err := h.handleUpdatePublishingDestination(detectorID, destID, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

// --- malware dispatch ---

func (h *Handler) dispatchMalwareOps( //nolint:cyclop,funlen // existing issue.
	op, path string,
	body []byte,
) (any, int, bool, error) {
	detectorID := extractID(path, pathDetector)

	switch op {
	case opDescribeMalwareScans:
		result, code, err := h.handleDescribeMalwareScans(detectorID)

		return result, code, true, err

	case opListMalwareScans:
		result, code := h.handleListMalwareScans()

		return result, code, true, nil

	case opStartMalwareScan:
		result, code, err := h.handleStartMalwareScan(body)

		return result, code, true, err

	case opGetMalwareScan:
		scanID := extractGlobalResourceID(path, pathMalwareScan)
		result, code, err := h.handleGetMalwareScan(scanID)

		return result, code, true, err

	case opGetMalwareScanSettings:
		result, code, err := h.handleGetMalwareScanSettings(detectorID)

		return result, code, true, err

	case opUpdateMalwareScanSettings:
		code, err := h.handleUpdateMalwareScanSettings(detectorID, body)

		return nil, code, true, err

	case opGetUsageStatistics:
		result, code, err := h.handleGetUsageStatistics(detectorID)

		return result, code, true, err

	case opGetRemainingFreeTrialDays:
		result, code, err := h.handleGetRemainingFreeTrialDays(detectorID, body)

		return result, code, true, err

	case opGetCoverageStatistics:
		result, code, err := h.handleGetCoverageStatistics(detectorID)

		return result, code, true, err

	case opListCoverage:
		result, code, err := h.handleListCoverage(detectorID)

		return result, code, true, err

	case opCreateMalwareProtectionPlan:
		result, code, err := h.handleCreateMalwareProtectionPlan(body)

		return result, code, true, err

	case opDeleteMalwareProtectionPlan:
		planID := extractGlobalResourceID(path, pathMalwareProtectionPlan)
		code, err := h.handleDeleteMalwareProtectionPlan(planID)

		return nil, code, true, err

	case opGetMalwareProtectionPlan:
		planID := extractGlobalResourceID(path, pathMalwareProtectionPlan)
		result, code, err := h.handleGetMalwareProtectionPlan(planID)

		return result, code, true, err

	case opListMalwareProtectionPlans:
		result, code := h.handleListMalwareProtectionPlans()

		return result, code, true, nil

	case opUpdateMalwareProtectionPlan:
		planID := extractGlobalResourceID(path, pathMalwareProtectionPlan)
		code, err := h.handleUpdateMalwareProtectionPlan(planID, body)

		return nil, code, true, err

	case opSendObjectMalwareScan:
		result, code, err := h.handleSendObjectMalwareScan(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

// --- entity set dispatch ---

func (h *Handler) dispatchEntitySetOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateThreatEntitySet:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateThreatEntitySet(detectorID, body)

		return result, code, true, err

	case opGetThreatEntitySet:
		detectorID, setID := extractDetectorAndSubID(path)
		result, code, err := h.handleGetThreatEntitySet(detectorID, setID)

		return result, code, true, err

	case opListThreatEntitySets:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListThreatEntitySets(detectorID)

		return result, code, true, err

	case opUpdateThreatEntitySet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleUpdateThreatEntitySet(detectorID, setID, body)

		return nil, code, true, err

	case opDeleteThreatEntitySet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleDeleteThreatEntitySet(detectorID, setID)

		return nil, code, true, err

	case opCreateTrustedEntitySet:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateTrustedEntitySet(detectorID, body)

		return result, code, true, err

	case opGetTrustedEntitySet:
		detectorID, setID := extractDetectorAndSubID(path)
		result, code, err := h.handleGetTrustedEntitySet(detectorID, setID)

		return result, code, true, err

	case opListTrustedEntitySets:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListTrustedEntitySets(detectorID)

		return result, code, true, err

	case opUpdateTrustedEntitySet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleUpdateTrustedEntitySet(detectorID, setID, body)

		return nil, code, true, err

	case opDeleteTrustedEntitySet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleDeleteTrustedEntitySet(detectorID, setID)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

// --- member handlers ---

func (h *Handler) handleCreateMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountDetails []map[string]any `json:"accountDetails"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	_, unprocessed := h.Backend.CreateMembers(detectorID, req.AccountDetails)

	return map[string]any{
		"unprocessedAccounts": orEmpty(unprocessed), //nolint:goconst // existing issue.
	}, http.StatusOK, nil
}

func (h *Handler) handleDeleteMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.DeleteMembers(detectorID, req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleGetMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	members, unprocessed := h.Backend.GetMembers(detectorID, req.AccountIDs)

	membersOut := make([]map[string]any, 0, len(members))
	for _, m := range members {
		membersOut = append(membersOut, memberToMap(m))
	}

	return map[string]any{
		"members":             membersOut,
		"unprocessedAccounts": orEmpty(unprocessed),
	}, http.StatusOK, nil
}

func (h *Handler) handleInviteMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.InviteMembers(detectorID, req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleListMembers(detectorID string) (any, int, error) {
	members, err := h.Backend.ListMembers(detectorID, false)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	out := make([]map[string]any, 0, len(members))
	for _, m := range members {
		out = append(out, memberToMap(m))
	}

	return map[string]any{"members": out}, http.StatusOK, nil
}

func (h *Handler) handleStartMonitoringMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.StartMonitoringMembers(detectorID, req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleStopMonitoringMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.StopMonitoringMembers(detectorID, req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.DisassociateMembers(detectorID, req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleGetMemberDetectors(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	details, unprocessed := h.Backend.GetMemberDetectors(detectorID, req.AccountIDs)

	return map[string]any{
		"memberDataSources":   orEmptyAny(details),
		"unprocessedAccounts": orEmpty(unprocessed),
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateMemberDetectors(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.UpdateMemberDetectors(detectorID, req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

// --- invitation handlers ---

func (h *Handler) handleAcceptAdministratorInvitation(detectorID string, body []byte) (int, error) {
	var req struct {
		AdministratorID string `json:"administratorId"`
		InvitationID    string `json:"invitationId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.AcceptAdministratorInvitation(detectorID, req.AdministratorID, req.InvitationID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleAcceptInvitation(detectorID string, body []byte) (int, error) {
	var req struct {
		MasterID     string `json:"masterId"`
		InvitationID string `json:"invitationId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.AcceptInvitation(detectorID, req.MasterID, req.InvitationID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetAdministratorAccount(detectorID string) (any, int, error) {
	a, err := h.Backend.GetAdministratorAccount(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"administrator": adminAccountToMap(a)}, http.StatusOK, nil
}

func (h *Handler) handleGetMasterAccount(detectorID string) (any, int, error) {
	a, err := h.Backend.GetMasterAccount(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"master": adminAccountToMap(a)}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateFromAdministratorAccount(detectorID string) (int, error) {
	if err := h.Backend.DisassociateFromAdministratorAccount(detectorID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDisassociateFromMasterAccount(detectorID string) (int, error) {
	if err := h.Backend.DisassociateFromMasterAccount(detectorID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeclineInvitations(body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.DeclineInvitations(req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleDeleteInvitations(body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed := h.Backend.DeleteInvitations(req.AccountIDs)

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleGetInvitationsCount() (any, int) {
	count := h.Backend.GetInvitationsCount()

	return map[string]any{"invitationsCount": count}, http.StatusOK
}

func (h *Handler) handleListInvitations() (any, int) {
	invitations := h.Backend.ListInvitations()

	out := make([]map[string]any, 0, len(invitations))
	for _, inv := range invitations {
		out = append(out, map[string]any{
			"accountId":          inv.AccountID, //nolint:goconst // existing issue.
			"invitationId":       inv.InvitationID,
			"invitedAt":          inv.InvitedAt,          //nolint:goconst // existing issue.
			"relationshipStatus": inv.RelationshipStatus, //nolint:goconst // existing issue.
		})
	}

	return map[string]any{"invitations": out}, http.StatusOK
}

// --- org handlers ---

func (h *Handler) handleEnableOrganizationAdminAccount(body []byte) (int, error) {
	var req struct {
		AdminAccountID string `json:"adminAccountId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.EnableOrganizationAdminAccount(req.AdminAccountID); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDisableOrganizationAdminAccount(body []byte) (int, error) {
	var req struct {
		AdminAccountID string `json:"adminAccountId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.DisableOrganizationAdminAccount(req.AdminAccountID); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListOrganizationAdminAccounts() (any, int) {
	accounts := h.Backend.ListOrganizationAdminAccounts()

	out := make([]map[string]any, 0, len(accounts))
	for _, a := range accounts {
		out = append(out, map[string]any{
			"adminAccountId": a.AdminAccountID,
			"adminStatus":    a.AdminStatus,
		})
	}

	return map[string]any{"adminAccounts": out}, http.StatusOK
}

func (h *Handler) handleDescribeOrganizationConfiguration(detectorID string) (any, int, error) {
	cfg, err := h.Backend.DescribeOrganizationConfiguration(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		"autoEnable":                cfg.AutoEnable,
		"memberAccountLimitReached": cfg.MemberAccountLimitReached,
		"dataSources":               cfg.DataSources,
		"features":                  cfg.Features, //nolint:goconst // existing issue.
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateOrganizationConfiguration(detectorID string, body []byte) (int, error) {
	var req struct {
		Features   []OrgFeature `json:"features"`
		AutoEnable bool         `json:"autoEnable"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateOrganizationConfiguration(detectorID, req.AutoEnable, req.Features); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetOrganizationStatistics() (any, int) {
	stats := h.Backend.GetOrganizationStatistics()

	return stats, http.StatusOK
}

// --- publishing destination handlers ---

func (h *Handler) handleCreatePublishingDestination(detectorID string, body []byte) (any, int, error) {
	var req struct {
		DestinationProperties DestinationProperties `json:"destinationProperties"`
		DestinationType       string                `json:"destinationType"`
		ClientToken           string                `json:"clientToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	dest, err := h.Backend.CreatePublishingDestination(detectorID, req.DestinationType, req.DestinationProperties)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"destinationId": dest.DestinationID}, http.StatusOK, nil //nolint:goconst // existing issue.
}

func (h *Handler) handleDeletePublishingDestination(detectorID, destID string) (int, error) {
	if err := h.Backend.DeletePublishingDestination(detectorID, destID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDescribePublishingDestination(detectorID, destID string) (any, int, error) {
	dest, err := h.Backend.DescribePublishingDestination(detectorID, destID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		"destinationId":              dest.DestinationID,
		"destinationType":            dest.DestinationType,
		"status":                     dest.Status, //nolint:goconst // existing issue.
		"publishingFailureStartedAt": dest.PublishingFailureStartedAt,
		"destinationProperties":      dest.DestinationProperties,
	}, http.StatusOK, nil
}

func (h *Handler) handleListPublishingDestinations(detectorID string) (any, int, error) {
	dests, err := h.Backend.ListPublishingDestinations(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	out := make([]map[string]any, 0, len(dests))
	for _, d := range dests {
		out = append(out, map[string]any{
			"destinationId":   d.DestinationID,
			"destinationType": d.DestinationType,
			"status":          d.Status,
		})
	}

	return map[string]any{"destinations": out}, http.StatusOK, nil
}

func (h *Handler) handleUpdatePublishingDestination(detectorID, destID string, body []byte) (int, error) {
	var req struct {
		DestinationProperties DestinationProperties `json:"destinationProperties"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdatePublishingDestination(detectorID, destID, req.DestinationProperties); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

// --- malware scan handlers ---

func (h *Handler) handleDescribeMalwareScans(detectorID string) (any, int, error) {
	scans, err := h.Backend.DescribeMalwareScans(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"scans": orEmptyAny(scanSliceToAny(scans))}, http.StatusOK, nil
}

func (h *Handler) handleListMalwareScans() (any, int) {
	scans := h.Backend.ListMalwareScans()

	return map[string]any{"scans": orEmptyAny(scanSliceToAny(scans))}, http.StatusOK
}

func (h *Handler) handleStartMalwareScan(body []byte) (any, int, error) {
	var req struct {
		ResourceArn string `json:"resourceArn"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	scanID, err := h.Backend.StartMalwareScan(req.ResourceArn)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"scanId": scanID}, http.StatusOK, nil //nolint:goconst // existing issue.
}

func (h *Handler) handleGetMalwareScan(scanID string) (any, int, error) {
	scan, err := h.Backend.GetMalwareScan(scanID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		"scanId":          scan.ScanID,
		"detectorId":      scan.DetectorID, //nolint:goconst // existing issue.
		"accountId":       scan.AccountID,
		"scanStatus":      scan.ScanStatus,
		"scanType":        scan.ScanType,
		"scanStartTime":   scan.ScanStartTime,
		"scanEndTime":     scan.ScanEndTime,
		"resourceDetails": scan.ResourceDetails,
		"findings":        scan.Findings,
	}, http.StatusOK, nil
}

func (h *Handler) handleGetMalwareScanSettings(detectorID string) (any, int, error) {
	settings, err := h.Backend.GetMalwareScanSettings(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		"ebsSnapshotPreservation": settings.EbsSnapshotPreservation,
		"scanResourceCriteria":    settings.ScanResourceCriteria,
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateMalwareScanSettings(detectorID string, body []byte) (int, error) {
	var req struct {
		ScanResourceCriteria    map[string]any `json:"scanResourceCriteria"`
		EbsSnapshotPreservation string         `json:"ebsSnapshotPreservation"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	settings := &MalwareScanSettings{
		EbsSnapshotPreservation: req.EbsSnapshotPreservation,
		ScanResourceCriteria:    req.ScanResourceCriteria,
	}

	if err := h.Backend.UpdateMalwareScanSettings(detectorID, settings); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetUsageStatistics(detectorID string) (any, int, error) {
	stats, err := h.Backend.GetUsageStatistics(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleGetRemainingFreeTrialDays(
	detectorID string,
	body []byte, //nolint:revive,unparam // existing issue.
) (any, int, error) {
	result, err := h.Backend.GetRemainingFreeTrialDays(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return result, http.StatusOK, nil
}

func (h *Handler) handleGetCoverageStatistics(detectorID string) (any, int, error) {
	stats, err := h.Backend.GetCoverageStatistics(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleListCoverage(detectorID string) (any, int, error) {
	resources, err := h.Backend.ListCoverage(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"resources": orEmptyAny(coverageToAny(resources))}, http.StatusOK, nil
}

// --- malware protection plan handlers ---

func (h *Handler) handleCreateMalwareProtectionPlan(body []byte) (any, int, error) {
	var req struct {
		Actions           map[string]any    `json:"actions"`
		ProtectedResource map[string]any    `json:"protectedResource"`
		Tags              map[string]string `json:"tags"`
		Role              string            `json:"role"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	plan, err := h.Backend.CreateMalwareProtectionPlan(req.Role, req.ProtectedResource, req.Actions, req.Tags)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{
		"malwareProtectionPlanId": plan.MalwareProtectionPlanID, //nolint:goconst // existing issue.
		"arn":                     plan.Arn,                     //nolint:goconst // existing issue.
	}, http.StatusOK, nil
}

func (h *Handler) handleDeleteMalwareProtectionPlan(planID string) (int, error) {
	if err := h.Backend.DeleteMalwareProtectionPlan(planID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetMalwareProtectionPlan(planID string) (any, int, error) {
	plan, err := h.Backend.GetMalwareProtectionPlan(planID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		"malwareProtectionPlanId": plan.MalwareProtectionPlanID,
		"arn":                     plan.Arn,
		"role":                    plan.Role,
		"status":                  plan.Status,
		"createdAt":               plan.CreatedAt,
		"statusReasons":           plan.StatusReasons,
		"protectedResource":       plan.ProtectedResource,
		"actions":                 plan.Actions,
		"tags":                    tagsOrEmpty(plan.Tags),
	}, http.StatusOK, nil
}

func (h *Handler) handleListMalwareProtectionPlans() (any, int) {
	plans := h.Backend.ListMalwareProtectionPlans()

	out := make([]map[string]any, 0, len(plans))
	for _, p := range plans {
		out = append(out, map[string]any{
			"malwareProtectionPlanId": p.MalwareProtectionPlanID,
			"arn":                     p.Arn,
		})
	}

	return map[string]any{"malwareProtectionPlans": out}, http.StatusOK
}

func (h *Handler) handleUpdateMalwareProtectionPlan(planID string, body []byte) (int, error) {
	var req struct {
		Actions           map[string]any `json:"actions"`
		ProtectedResource map[string]any `json:"protectedResource"`
		Role              string         `json:"role"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateMalwareProtectionPlan(planID, req.Role, req.ProtectedResource, req.Actions); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleSendObjectMalwareScan(body []byte) (any, int, error) {
	var req struct {
		S3ObjectDetails map[string]any `json:"s3ObjectDetails"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	scanID, err := h.Backend.SendObjectMalwareScan(req.S3ObjectDetails)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"scanId": scanID}, http.StatusOK, nil
}

// --- threat entity set handlers ---

func (h *Handler) handleCreateThreatEntitySet( //nolint:dupl // existing issue.
	detectorID string,
	body []byte,
) (any, int, error) {
	var req struct {
		Tags     map[string]string `json:"tags"`
		Activate *bool             `json:"activate"`
		Name     string            `json:"name"`
		Format   string            `json:"format"`
		Location string            `json:"location"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Format == "" || req.Location == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	activate := false
	if req.Activate != nil {
		activate = *req.Activate
	}

	s, err := h.Backend.CreateThreatEntitySet(detectorID, req.Name, req.Format, req.Location, activate, req.Tags)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"threatEntitySetId": s.ThreatEntitySetID}, http.StatusOK, nil
}

func (h *Handler) handleGetThreatEntitySet(detectorID, setID string) (any, int, error) {
	s, err := h.Backend.GetThreatEntitySet(detectorID, setID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyName:    s.Name,
		"format":   s.Format,   //nolint:goconst // existing issue.
		"location": s.Location, //nolint:goconst // existing issue.
		keyStatus:  s.Status,
		keyTags:    tagsOrEmpty(s.Tags),
	}, http.StatusOK, nil
}

func (h *Handler) handleListThreatEntitySets(detectorID string) (any, int, error) {
	ids, err := h.Backend.ListThreatEntitySets(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"threatEntitySetIds": ids}, http.StatusOK, nil
}
func (h *Handler) handleUpdateThreatEntitySet(detectorID, setID string, body []byte) (int, error) {
	var req struct {
		Activate *bool  `json:"activate"`
		Name     string `json:"name"`
		Location string `json:"location"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateThreatEntitySet(detectorID, setID, req.Name, req.Location, req.Activate); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteThreatEntitySet(detectorID, setID string) (int, error) {
	if err := h.Backend.DeleteThreatEntitySet(detectorID, setID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

// --- trusted entity set handlers ---

func (h *Handler) handleCreateTrustedEntitySet( //nolint:dupl // existing issue.
	detectorID string,
	body []byte,
) (any, int, error) {
	var req struct {
		Tags     map[string]string `json:"tags"`
		Activate *bool             `json:"activate"`
		Name     string            `json:"name"`
		Format   string            `json:"format"`
		Location string            `json:"location"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Format == "" || req.Location == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	activate := false
	if req.Activate != nil {
		activate = *req.Activate
	}

	s, err := h.Backend.CreateTrustedEntitySet(detectorID, req.Name, req.Format, req.Location, activate, req.Tags)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"trustedEntitySetId": s.TrustedEntitySetID}, http.StatusOK, nil
}

func (h *Handler) handleGetTrustedEntitySet(detectorID, setID string) (any, int, error) {
	s, err := h.Backend.GetTrustedEntitySet(detectorID, setID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyName:    s.Name,
		"format":   s.Format,
		"location": s.Location,
		keyStatus:  s.Status,
		keyTags:    tagsOrEmpty(s.Tags),
	}, http.StatusOK, nil
}

func (h *Handler) handleListTrustedEntitySets(detectorID string) (any, int, error) {
	ids, err := h.Backend.ListTrustedEntitySets(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"trustedEntitySetIds": ids}, http.StatusOK, nil
}
func (h *Handler) handleUpdateTrustedEntitySet(detectorID, setID string, body []byte) (int, error) {
	var req struct {
		Activate *bool  `json:"activate"`
		Name     string `json:"name"`
		Location string `json:"location"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateTrustedEntitySet(detectorID, setID, req.Name, req.Location, req.Activate); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteTrustedEntitySet(detectorID, setID string) (int, error) {
	if err := h.Backend.DeleteTrustedEntitySet(detectorID, setID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

// --- helpers ---

func memberToMap(m *Member) map[string]any {
	return map[string]any{
		"accountId":          m.AccountID,
		"administratorId":    m.AdministratorID,
		"masterId":           m.MasterID,
		"detectorId":         m.DetectorID,
		"email":              m.Email,
		"relationshipStatus": m.RelationshipStatus,
		"invitedAt":          m.InvitedAt,
		"updatedAt":          m.UpdatedAt,
	}
}

func adminAccountToMap(a *AdminAccount) map[string]any {
	if a == nil {
		return map[string]any{}
	}

	return map[string]any{
		"accountId":          a.AccountID,
		"invitationId":       a.InvitationID,
		"invitedAt":          a.InvitedAt,
		"relationshipStatus": a.RelationshipStatus,
	}
}

func orEmpty(s []map[string]any) []map[string]any {
	if s == nil {
		return []map[string]any{}
	}

	return s
}

func orEmptyAny[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func scanSliceToAny(scans []*MalwareScan) []map[string]any {
	out := make([]map[string]any, 0, len(scans))
	for _, s := range scans {
		out = append(out, map[string]any{
			"scanId":     s.ScanID,
			"scanStatus": s.ScanStatus,
			"scanType":   s.ScanType,
			"accountId":  s.AccountID,
		})
	}

	return out
}

func coverageToAny(resources []map[string]any) []map[string]any {
	if resources == nil {
		return []map[string]any{}
	}

	return resources
}

// extractGlobalResourceID extracts the ID from /prefix/{id} global paths.
func extractGlobalResourceID(path, prefix string) string {
	stripped := strings.TrimPrefix(path, "/"+prefix+"/")
	before, _, found := strings.Cut(stripped, "/")

	if !found {
		return stripped
	}

	return before
}
