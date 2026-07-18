package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) dispatchDetectorOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateDetector:
		result, code, err := h.handleCreateDetector(body)

		return result, code, true, err

	case opGetDetector:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleGetDetector(detectorID)

		return result, code, true, err

	case opUpdateDetector:
		detectorID := extractID(path, pathDetector)
		code, err := h.handleUpdateDetector(detectorID, body)

		return nil, code, true, err

	case opDeleteDetector:
		detectorID := extractID(path, pathDetector)
		code, err := h.handleDeleteDetector(detectorID)

		return nil, code, true, err

	case opListDetectors:
		result, code := h.handleListDetectors()

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateDetector(body []byte) (any, int, error) {
	var req struct {
		Enable                     *bool             `json:"enable"`
		ClientToken                string            `json:"clientToken"`
		FindingPublishingFrequency string            `json:"findingPublishingFrequency"`
		Tags                       map[string]string `json:"tags"`
		Features                   []DetectorFeature `json:"features"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Enable == nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	d, err := h.Backend.CreateDetector(*req.Enable, req.FindingPublishingFrequency, req.Tags, req.Features)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"detectorId": d.DetectorID}, http.StatusOK, nil //nolint:goconst // existing issue.
}

func (h *Handler) handleGetDetector(detectorID string) (any, int, error) {
	d, err := h.Backend.GetDetector(detectorID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyStatus:                    d.Status,
		"serviceRole":                d.ServiceRole,
		"findingPublishingFrequency": d.FindingPublishingFrequency,
		keyCreatedAt:                 d.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		keyUpdatedAt:                 d.UpdatedAt.Format("2006-01-02T15:04:05.000Z"),
		keyTags:                      tagsOrEmpty(d.Tags),
		"features":                   d.Features, //nolint:goconst // existing issue.
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateDetector(detectorID string, body []byte) (int, error) {
	var req struct {
		Enable                     *bool             `json:"enable"`
		FindingPublishingFrequency string            `json:"findingPublishingFrequency"`
		Features                   []DetectorFeature `json:"features"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateDetector(
		detectorID, req.Enable, req.FindingPublishingFrequency, req.Features,
	); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeleteDetector(detectorID string) (int, error) {
	if err := h.Backend.DeleteDetector(detectorID); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListDetectors() (any, int) {
	ids := h.Backend.ListDetectors()

	return map[string]any{"detectorIds": ids}, http.StatusOK
}
