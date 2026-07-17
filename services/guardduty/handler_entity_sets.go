package guardduty

import (
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

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
		// GetThreatEntitySetOutput.CreatedAt/UpdatedAt are epoch-seconds
		// numbers on the wire (unlike GetDetectorOutput's ISO8601 strings) --
		// see aws-sdk-go-v2/service/guardduty deserializers.go's
		// awsRestjson1_deserializeOpDocumentGetThreatEntitySetOutput, which
		// parses them via smithytime.ParseEpochSeconds.
		keyCreatedAt: awstime.Epoch(s.CreatedAt),
		keyUpdatedAt: awstime.Epoch(s.UpdatedAt),
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
		// See handleGetThreatEntitySet: GetTrustedEntitySetOutput.CreatedAt/
		// UpdatedAt are epoch-seconds numbers on the wire.
		keyCreatedAt: awstime.Epoch(s.CreatedAt),
		keyUpdatedAt: awstime.Epoch(s.UpdatedAt),
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
