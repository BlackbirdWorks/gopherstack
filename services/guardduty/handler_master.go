package guardduty

import "net/http"

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

func adminAccountToMap(a *AdminAccount) map[string]any {
	if a == nil {
		return map[string]any{}
	}

	return map[string]any{
		"accountId":          a.AccountID, //nolint:goconst // existing issue.
		"invitationId":       a.InvitationID,
		"invitedAt":          a.InvitedAt,          //nolint:goconst // existing issue.
		"relationshipStatus": a.RelationshipStatus, //nolint:goconst // existing issue.
	}
}
