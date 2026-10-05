package guardduty

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) dispatchDetectorOps(op, path, query string, body []byte) (any, int, bool, error) {
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
		result, code, err := h.handleListDetectors(query)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateDetector(body []byte) (any, int, error) {
	var req struct {
		Enable                     *bool             `json:"enable"`
		Tags                       map[string]string `json:"tags"`
		DataSources                *dataSources      `json:"dataSources"`
		ClientToken                string            `json:"clientToken"`
		FindingPublishingFrequency string            `json:"findingPublishingFrequency"`
		Features                   []DetectorFeature `json:"features"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Enable == nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	token := req.ClientToken
	req.ClientToken = ""
	features := mergeFeatures(req.DataSources.features(), req.Features)

	id, err := h.createOnce(
		opCreateDetector, "", token, req,
		func(id string) error { return only(h.Backend.GetDetector(id)) },
		func() (string, error) {
			d, createErr := h.Backend.CreateDetector(*req.Enable, req.FindingPublishingFrequency, req.Tags, features)
			if createErr != nil {
				return "", createErr
			}

			return d.DetectorID, nil
		},
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{"detectorId": id}, http.StatusOK, nil
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
		keyCreatedAt:                 d.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyUpdatedAt:                 d.UpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyTags:                      tagsOrEmpty(d.Tags),
		keyFeatures:                  d.Features,
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateDetector(detectorID string, body []byte) (int, error) {
	var req struct {
		Enable                     *bool             `json:"enable"`
		DataSources                *dataSources      `json:"dataSources"`
		FindingPublishingFrequency string            `json:"findingPublishingFrequency"`
		Features                   []DetectorFeature `json:"features"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateDetector(
		detectorID, req.Enable, req.FindingPublishingFrequency, mergeFeatures(req.DataSources.features(), req.Features),
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

func (h *Handler) handleListDetectors(query string) (any, int, error) {
	maxResults, nextToken := paginationParamsFromQuery(query)

	ids, next, err := h.Backend.ListDetectors(maxResults, nextToken)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	resp := map[string]any{"detectorIds": ids}
	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}
