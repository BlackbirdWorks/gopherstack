package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) dispatchIPSetOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateIPSet:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateIPSet(detectorID, body)

		return result, code, true, err

	case opGetIPSet:
		detectorID, ipSetID := extractDetectorAndSubID(path)
		result, code, err := h.handleGetIPSet(detectorID, ipSetID)

		return result, code, true, err

	case opUpdateIPSet:
		detectorID, ipSetID := extractDetectorAndSubID(path)
		code, err := h.handleUpdateIPSet(detectorID, ipSetID, body)

		return nil, code, true, err

	case opDeleteIPSet:
		detectorID, ipSetID := extractDetectorAndSubID(path)
		code, err := h.handleDeleteIPSet(detectorID, ipSetID)

		return nil, code, true, err

	case opListIPSets:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListIPSets(detectorID)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

//nolint:dupl // IPSet and ThreatIntelSet have identical handler patterns
func (h *Handler) handleCreateIPSet(detectorID string, body []byte) (any, int, error) {
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

	s, err := h.Backend.CreateIPSet(detectorID, req.Name, req.Format, req.Location, activate, req.Tags)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"ipSetId": s.IPSetID}, http.StatusOK, nil
}

func (h *Handler) handleGetIPSet(detectorID, ipSetID string) (any, int, error) {
	s, err := h.Backend.GetIPSet(detectorID, ipSetID)
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

func (h *Handler) handleUpdateIPSet(detectorID, ipSetID string, body []byte) (int, error) {
	var req struct {
		Activate *bool  `json:"activate"`
		Name     string `json:"name"`
		Location string `json:"location"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateIPSet(detectorID, ipSetID, req.Name, req.Location, req.Activate); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteIPSet(detectorID, ipSetID string) (int, error) {
	if err := h.Backend.DeleteIPSet(detectorID, ipSetID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListIPSets(detectorID string) (any, int, error) {
	ids, err := h.Backend.ListIPSets(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"ipSetIds": ids}, http.StatusOK, nil
}

func (h *Handler) dispatchThreatIntelSetOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateThreatIntelSet:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateThreatIntelSet(detectorID, body)

		return result, code, true, err

	case opGetThreatIntelSet:
		detectorID, setID := extractDetectorAndSubID(path)
		result, code, err := h.handleGetThreatIntelSet(detectorID, setID)

		return result, code, true, err

	case opUpdateThreatIntelSet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleUpdateThreatIntelSet(detectorID, setID, body)

		return nil, code, true, err

	case opDeleteThreatIntelSet:
		detectorID, setID := extractDetectorAndSubID(path)
		code, err := h.handleDeleteThreatIntelSet(detectorID, setID)

		return nil, code, true, err

	case opListThreatIntelSets:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListThreatIntelSets(detectorID)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

//nolint:dupl // IPSet and ThreatIntelSet have identical handler patterns
func (h *Handler) handleCreateThreatIntelSet(detectorID string, body []byte) (any, int, error) {
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

	s, err := h.Backend.CreateThreatIntelSet(detectorID, req.Name, req.Format, req.Location, activate, req.Tags)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"threatIntelSetId": s.ThreatIntelSetID}, http.StatusOK, nil
}

func (h *Handler) handleGetThreatIntelSet(detectorID, setID string) (any, int, error) {
	s, err := h.Backend.GetThreatIntelSet(detectorID, setID)
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

func (h *Handler) handleUpdateThreatIntelSet(detectorID, setID string, body []byte) (int, error) {
	var req struct {
		Activate *bool  `json:"activate"`
		Name     string `json:"name"`
		Location string `json:"location"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateThreatIntelSet(detectorID, setID, req.Name, req.Location, req.Activate); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteThreatIntelSet(detectorID, setID string) (int, error) {
	if err := h.Backend.DeleteThreatIntelSet(detectorID, setID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListThreatIntelSets(detectorID string) (any, int, error) {
	ids, err := h.Backend.ListThreatIntelSets(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{"threatIntelSetIds": ids}, http.StatusOK, nil
}
