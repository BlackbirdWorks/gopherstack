package guardduty

import (
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func (h *Handler) dispatchFilterOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateFilter:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleCreateFilter(detectorID, body)

		return result, code, true, err

	case opGetFilter:
		detectorID, filterName := extractDetectorAndSubID(path)
		result, code, err := h.handleGetFilter(detectorID, filterName)

		return result, code, true, err

	case opUpdateFilter:
		detectorID, filterName := extractDetectorAndSubID(path)
		result, code, err := h.handleUpdateFilter(detectorID, filterName, body)

		return result, code, true, err

	case opDeleteFilter:
		detectorID, filterName := extractDetectorAndSubID(path)
		code, err := h.handleDeleteFilter(detectorID, filterName)

		return nil, code, true, err

	case opListFilters:
		detectorID := extractID(path, pathDetector)
		result, code, err := h.handleListFilters(detectorID, query)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateFilter(detectorID string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any    `json:"findingCriteria"`
		Tags            map[string]string `json:"tags"`
		Name            string            `json:"name"`
		Description     string            `json:"description"`
		Action          string            `json:"action"`
		Rank            int32             `json:"rank"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Action == "" {
		req.Action = "NOOP"
	}

	f, err := h.Backend.CreateFilter(
		detectorID,
		req.Name,
		req.Description,
		req.Action,
		req.Rank,
		req.FindingCriteria,
		req.Tags,
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return map[string]any{keyName: f.Name}, http.StatusOK, nil
}

func (h *Handler) handleGetFilter(detectorID, filterName string) (any, int, error) {
	f, err := h.Backend.GetFilter(detectorID, filterName)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{
		keyName:           f.Name,
		"description":     f.Description,
		"action":          f.Action,
		"rank":            f.Rank,
		"findingCriteria": f.FindingCriteria,
		keyTags:           tagsOrEmpty(f.Tags),
		// GetFilterOutput.CreatedAt/UpdatedAt are epoch-seconds numbers, and
		// Version increments by 1 on every update -- all three were tracked
		// by the backend already but never emitted here. See
		// aws-sdk-go-v2/service/guardduty deserializers.go's
		// awsRestjson1_deserializeOpDocumentGetFilterOutput.
		keyCreatedAt: awstime.Epoch(f.CreatedAt),
		keyUpdatedAt: awstime.Epoch(f.UpdatedAt),
		"version":    f.Version,
	}, http.StatusOK, nil
}

func (h *Handler) handleUpdateFilter(detectorID, filterName string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any `json:"findingCriteria"`
		Description     string         `json:"description"`
		Action          string         `json:"action"`
		Rank            int32          `json:"rank"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	f, err := h.Backend.UpdateFilter(detectorID, filterName, req.Description, req.Action, req.Rank, req.FindingCriteria)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{keyName: f.Name}, http.StatusOK, nil
}

func (h *Handler) handleDeleteFilter(detectorID, filterName string) (int, error) {
	if err := h.Backend.DeleteFilter(detectorID, filterName); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListFilters(detectorID, query string) (any, int, error) {
	maxResults, nextToken := paginationParamsFromQuery(query)

	names, next, err := h.Backend.ListFilters(detectorID, maxResults, nextToken)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	resp := map[string]any{"filterNames": names}
	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}
