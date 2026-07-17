package macie2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseRevealCfgPath(method string, _ []string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetRevealConfiguration, ""
	case http.MethodPut:
		return opUpdateRevealConfiguration, ""
	}

	return opUnknown, ""
}

func (h *Handler) dispatchRevealOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetRevealConfiguration:
		result, code, err := h.handleGetRevealConfiguration()

		return result, code, true, err

	case opUpdateRevealConfiguration:
		code, err := h.handleUpdateRevealConfiguration(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetRevealConfiguration() (any, int, error) {
	cfg, err := h.Backend.GetRevealConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{keyConfiguration: cfg}, http.StatusOK, nil
}

func (h *Handler) handleUpdateRevealConfiguration(body []byte) (int, error) {
	var req struct {
		Configuration *RevealConfiguration `json:"configuration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if req.Configuration == nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateRevealConfiguration(req.Configuration.KmsKeyID, req.Configuration.Status); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) dispatchFindingRevealOps(op, path string) (any, int, bool, error) {
	switch op {
	case opGetSensitiveDataOccurrences:
		findingID := extractFindingRevealID(path)
		result, code, err := h.handleGetSensitiveDataOccurrences(findingID)

		return result, code, true, err

	case opGetSensitiveDataOccurrencesAvailability:
		findingID := extractFindingRevealID(path)
		result, code, err := h.handleGetSensitiveDataOccurrencesAvailability(findingID)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func extractFindingRevealID(path string) string {
	// /findings/{findingId}/reveal[/availability]
	trimmed := strings.TrimPrefix(path, "/"+pathFindings+"/")
	parts := strings.SplitN(trimmed, "/", splitTwo)

	return parts[0]
}

func (h *Handler) handleGetSensitiveDataOccurrences(findingID string) (any, int, error) {
	result, err := h.Backend.GetSensitiveDataOccurrences(findingID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return result, http.StatusOK, nil
}

func (h *Handler) handleGetSensitiveDataOccurrencesAvailability(findingID string) (any, int, error) {
	status, reasons, err := h.Backend.GetSensitiveDataOccurrencesAvailability(findingID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"code": status}
	if len(reasons) > 0 {
		resp["reasons"] = reasons
	}

	return resp, http.StatusOK, nil
}
