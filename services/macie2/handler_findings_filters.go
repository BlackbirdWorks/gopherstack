package macie2

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseFindingsFilterPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /findingsfilters
		switch method {
		case http.MethodPost:
			return opCreateFindingsFilter, ""
		case http.MethodGet:
			return opListFindingsFilters, ""
		}
	case depthResource: // /findingsfilters/{id}
		id := parts[1]
		switch method {
		case http.MethodGet:
			return opGetFindingsFilter, id
		case http.MethodPatch:
			return opUpdateFindingsFilter, id
		case http.MethodDelete:
			return opDeleteFindingsFilter, id
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchFindingsFilterOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateFindingsFilter:
		result, code, err := h.handleCreateFindingsFilter(body)

		return result, code, true, err

	case opGetFindingsFilter:
		id := extractID(path, pathFindingsFilter)
		result, code, err := h.handleGetFindingsFilter(id)

		return result, code, true, err

	case opUpdateFindingsFilter:
		id := extractID(path, pathFindingsFilter)
		result, code, err := h.handleUpdateFindingsFilter(id, body)

		return result, code, true, err

	case opDeleteFindingsFilter:
		id := extractID(path, pathFindingsFilter)
		code, err := h.handleDeleteFindingsFilter(id)

		return nil, code, true, err

	case opListFindingsFilters:
		result, code := h.handleListFindingsFilters(query)

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateFindingsFilter(body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any    `json:"findingCriteria"`
		Position        *int32            `json:"position"`
		Tags            map[string]string `json:"tags"`
		Action          string            `json:"action"`
		ClientToken     string            `json:"clientToken"`
		Description     string            `json:"description"`
		Name            string            `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Action == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	ff, err := h.Backend.CreateFindingsFilter(
		req.Name, req.Description, req.Action,
		req.Position, req.FindingCriteria, req.Tags,
	)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: ff.Arn, "id": ff.ID}, http.StatusOK, nil
}

func (h *Handler) handleGetFindingsFilter(id string) (any, int, error) {
	ff, err := h.Backend.GetFindingsFilter(id)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return ff, http.StatusOK, nil
}

func (h *Handler) handleUpdateFindingsFilter(id string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any `json:"findingCriteria"`
		Position        *int32         `json:"position"`
		Action          string         `json:"action"`
		Description     string         `json:"description"`
		Name            string         `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	ff, err := h.Backend.UpdateFindingsFilter(
		id, req.Name, req.Description, req.Action,
		req.Position, req.FindingCriteria,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: ff.Arn, "id": ff.ID}, http.StatusOK, nil
}

func (h *Handler) handleDeleteFindingsFilter(id string) (int, error) {
	if err := h.Backend.DeleteFindingsFilter(id); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListFindingsFilters(query string) (any, int) {
	q, _ := url.ParseQuery(query)
	limit, _ := strconv.Atoi(q.Get("maxResults"))
	filters, nextToken, _ := h.Backend.ListFindingsFilters(limit, q.Get("nextToken"))

	resp := map[string]any{"findingsFilterListItems": filters}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK
}

// Finding handlers
