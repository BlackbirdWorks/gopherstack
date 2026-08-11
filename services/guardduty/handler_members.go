package guardduty

import (
	"encoding/json"
	"net/http"
	"net/url"
)

func (h *Handler) dispatchMemberOps(op, path, query string, body []byte) (any, int, bool, error) {
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
		result, code, err := h.handleListMembers(detectorID, query)

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

	unprocessed, err := h.Backend.DeleteMembers(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleGetMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	members, unprocessed, err := h.Backend.GetMembers(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

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

	unprocessed, err := h.Backend.InviteMembers(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleListMembers(detectorID, query string) (any, int, error) {
	members, err := h.Backend.ListMembers(detectorID, onlyAssociatedFromQuery(query))
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

	unprocessed, err := h.Backend.StartMonitoringMembers(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleStopMonitoringMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed, err := h.Backend.StopMonitoringMembers(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateMembers(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed, err := h.Backend.DisassociateMembers(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

func (h *Handler) handleGetMemberDetectors(detectorID string, body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	details, unprocessed, err := h.Backend.GetMemberDetectors(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

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

	unprocessed, err := h.Backend.UpdateMemberDetectors(detectorID, req.AccountIDs)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"unprocessedAccounts": orEmpty(unprocessed)}, http.StatusOK, nil
}

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

// onlyAssociatedFromQuery parses ListMembersInput's onlyAssociated query
// parameter (real wire key, see aws-sdk-go-v2/service/guardduty
// serializers.go: encoder.SetQuery("onlyAssociated")). Any value other than
// "true" behaves like the parameter being absent (returns every member).
func onlyAssociatedFromQuery(query string) bool {
	values, err := url.ParseQuery(query)
	if err != nil {
		return false
	}

	return values.Get("onlyAssociated") == "true"
}

func memberToMap(m *Member) map[string]any {
	return map[string]any{
		"accountId":          m.AccountID,
		"administratorId":    m.AdministratorID,
		"masterId":           m.MasterID,
		"detectorId":         m.DetectorID, //nolint:goconst // existing issue.
		"email":              m.Email,
		"relationshipStatus": m.RelationshipStatus,
		"invitedAt":          m.InvitedAt,
		// MemberOutput.UpdatedAt is a plain ISO8601 string on the wire (like
		// GetDetectorOutput's, unlike ThreatEntitySet's epoch numbers) -- see
		// aws-sdk-go-v2/service/guardduty deserializers.go's
		// awsRestjson1_deserializeDocumentMember.
		keyUpdatedAt: m.UpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

func orEmpty(s []map[string]any) []map[string]any {
	if s == nil {
		return []map[string]any{}
	}

	return s
}
