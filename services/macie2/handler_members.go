package macie2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseMembersPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /members
		switch method {
		case http.MethodPost:
			return opCreateMember, ""
		case http.MethodGet:
			return opListMembers, ""
		}
	case depthResource: // /members/{id|disassociate}
		switch parts[1] {
		case segDisassociate:
			// /members/disassociate — no, that's in a 3-part path below
		default:
			switch method {
			case http.MethodGet:
				return opGetMember, parts[1]
			case http.MethodDelete:
				return opDeleteMember, parts[1]
			}
		}
	case 3: //nolint:mnd // existing issue.
		if parts[1] == segDisassociate && method == http.MethodPost {
			return opDisassociateMember, parts[2]
		}
	}

	return opUnknown, ""
}

func parseInvitationsPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /invitations
		switch method {
		case http.MethodPost:
			return opCreateInvitations, ""
		case http.MethodGet:
			return opListInvitations, ""
		}
	case depthResource: // /invitations/{action}
		switch parts[1] {
		case "accept":
			if method == http.MethodPost {
				return opAcceptInvitation, ""
			}
		case "decline":
			if method == http.MethodPost {
				return opDeclineInvitations, ""
			}
		case "delete":
			if method == http.MethodPost {
				return opDeleteInvitations, ""
			}
		case "count":
			if method == http.MethodGet {
				return opGetInvitationsCount, ""
			}
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchMemberOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateMember:
		code, err := h.handleCreateMember(body)

		return nil, code, true, err

	case opGetMember:
		accountID := extractID(path, pathMembers)
		result, code, err := h.handleGetMember(accountID)

		return result, code, true, err

	case opDeleteMember:
		accountID := extractID(path, pathMembers)
		code, err := h.handleDeleteMember(accountID)

		return nil, code, true, err

	case opListMembers:
		result, code, err := h.handleListMembers()

		return result, code, true, err

	case opDisassociateMember:
		accountID := extractDisassociateMemberID(path)
		code, err := h.handleDisassociateMember(accountID)

		return nil, code, true, err

	case opUpdateMemberSession:
		accountID := extractMacieMemberID(path)
		code, err := h.handleUpdateMemberSession(accountID, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchInvitationOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateInvitations:
		result, code, err := h.handleCreateInvitations(body)

		return result, code, true, err

	case opAcceptInvitation:
		code, err := h.handleAcceptInvitation(body)

		return nil, code, true, err

	case opDeclineInvitations:
		result, code, err := h.handleDeclineInvitations(body)

		return result, code, true, err

	case opDeleteInvitations:
		result, code, err := h.handleDeleteInvitations(body)

		return result, code, true, err

	case opGetInvitationsCount:
		result, code, err := h.handleGetInvitationsCount()

		return result, code, true, err

	case opListInvitations:
		result, code, err := h.handleListInvitations()

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateMember(body []byte) (int, error) {
	var req struct {
		Account map[string]string `json:"account"`
		Tags    map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	accountID := req.Account["accountId"]
	email := req.Account["email"]

	if accountID == "" {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.CreateMember(accountID, email, req.Tags); err != nil {
		if errors.Is(err, awserr.ErrConflict) {
			return http.StatusConflict, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetMember(accountID string) (any, int, error) {
	m, err := h.Backend.GetMember(accountID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return m, http.StatusOK, nil
}

func (h *Handler) handleDeleteMember(accountID string) (int, error) {
	if err := h.Backend.DeleteMember(accountID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListMembers() (any, int, error) {
	members, err := h.Backend.ListMembers(false)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"members": members}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateMember(accountID string) (int, error) {
	if err := h.Backend.DisassociateMember(accountID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateMemberSession(accountID string, body []byte) (int, error) {
	var req struct {
		Status string `json:"status"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.UpdateMemberSession(accountID, req.Status); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleCreateInvitations(body []byte) (any, int, error) {
	var req struct {
		Message                  string   `json:"message"`
		AccountIDs               []string `json:"accountIds"`
		DisableEmailNotification bool     `json:"disableEmailNotification"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed, err := h.Backend.CreateInvitations(req.AccountIDs, req.Message, req.DisableEmailNotification)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"unprocessedAccounts": unprocessed}, http.StatusOK, nil //nolint:goconst // existing issue.
}

func (h *Handler) handleAcceptInvitation(body []byte) (int, error) {
	var req struct {
		AdministratorAccountID string `json:"administratorAccountId"`
		InvitationID           string `json:"invitationId"`
		MasterId               string `json:"masterId"` //nolint:revive,staticcheck // existing issue.
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	adminID := req.AdministratorAccountID
	if adminID == "" {
		adminID = req.MasterId
	}

	if err := h.Backend.AcceptInvitation(adminID, req.InvitationID); err != nil {
		return http.StatusInternalServerError, err
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

	unprocessed, err := h.Backend.DeclineInvitations(req.AccountIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"unprocessedAccounts": unprocessed}, http.StatusOK, nil
}

func (h *Handler) handleDeleteInvitations(body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed, err := h.Backend.DeleteInvitations(req.AccountIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"unprocessedAccounts": unprocessed}, http.StatusOK, nil
}

func (h *Handler) handleGetInvitationsCount() (any, int, error) {
	count, err := h.Backend.GetInvitationsCount()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]int64{"invitationsCount": count}, http.StatusOK, nil
}

func (h *Handler) handleListInvitations() (any, int, error) {
	invitations, err := h.Backend.ListInvitations()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"invitations": invitations}, http.StatusOK, nil
}

func extractDisassociateMemberID(path string) string {
	// /members/disassociate/{id}
	trimmed := strings.TrimPrefix(path, "/"+pathMembers+"/disassociate/")

	return trimmed
}

func extractMacieMemberID(path string) string {
	// /macie/members/{id}
	trimmed := strings.TrimPrefix(path, "/"+pathMacie+"/"+pathMembers+"/")

	return trimmed
}
