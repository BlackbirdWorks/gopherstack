package macie2

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseMaciePath(method string, parts []string) (string, string) {
	// /macie/members/{id} → UpdateMemberSession
	if len(parts) == 3 && parts[1] == pathMembers && method == http.MethodPatch {
		return opUpdateMemberSession, parts[2]
	}

	switch method {
	case http.MethodGet:
		return opGetMacieSession, ""
	case http.MethodPost:
		return opEnableMacie, ""
	case http.MethodDelete:
		return opDisableMacie, ""
	case http.MethodPatch:
		return opUpdateMacieSession, ""
	}

	return opUnknown, ""
}

func (h *Handler) dispatchSessionOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetMacieSession:
		result, code := h.handleGetMacieSession()

		return result, code, true, nil

	case opEnableMacie:
		code, err := h.handleEnableMacie(body)

		return nil, code, true, err

	case opDisableMacie:
		code, err := h.handleDisableMacie()

		return nil, code, true, err

	case opUpdateMacieSession:
		result, code, err := h.handleUpdateMacieSession(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetMacieSession() (any, int) {
	session := h.Backend.GetSession()
	if session == nil {
		return errBody(errMacieNotEnabled, "Macie is not enabled for this account"), http.StatusForbidden
	}

	return map[string]any{
		"createdAt":                  session.CreatedAt.UTC().Format(time.RFC3339),
		"findingPublishingFrequency": session.FindingPublishingFrequency,
		"serviceRole":                session.ServiceRole,
		"status":                     session.Status,
		"updatedAt":                  session.UpdatedAt.UTC().Format(time.RFC3339),
	}, http.StatusOK
}

func (h *Handler) handleEnableMacie(body []byte) (int, error) {
	var req struct {
		ClientToken                string `json:"clientToken"`
		FindingPublishingFrequency string `json:"findingPublishingFrequency"`
		Status                     string `json:"status"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if req.FindingPublishingFrequency != "" && !validFrequency(req.FindingPublishingFrequency) {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.EnableMacie(req.ClientToken, req.FindingPublishingFrequency, req.Status); err != nil {
		if errors.Is(err, awserr.ErrConflict) {
			return http.StatusConflict, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDisableMacie() (int, error) {
	if err := h.Backend.DisableMacie(); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateMacieSession(body []byte) (any, int, error) {
	if h.Backend.GetSession() == nil {
		return errBody(errMacieNotEnabled, "Macie is not currently enabled for this account"), http.StatusForbidden, nil
	}

	var req struct {
		FindingPublishingFrequency string `json:"findingPublishingFrequency"`
		Status                     string `json:"status"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	if req.FindingPublishingFrequency != "" && !validFrequency(req.FindingPublishingFrequency) {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateMacieSession(req.FindingPublishingFrequency, req.Status); err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return nil, http.StatusOK, nil
}

func validFrequency(f string) bool {
	return f == "FIFTEEN_MINUTES" || f == "ONE_HOUR" || f == "SIX_HOURS"
}
