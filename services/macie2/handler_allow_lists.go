package macie2

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseAllowListPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /allow-lists
		switch method {
		case http.MethodPost:
			return opCreateAllowList, ""
		case http.MethodGet:
			return opListAllowLists, ""
		}
	case depthResource: // /allow-lists/{id}
		id := parts[1]
		switch method {
		case http.MethodGet:
			return opGetAllowList, id
		case http.MethodPut:
			return opUpdateAllowList, id
		case http.MethodDelete:
			return opDeleteAllowList, id
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchAllowListOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateAllowList:
		result, code, err := h.handleCreateAllowList(body)

		return result, code, true, err

	case opGetAllowList:
		id := extractID(path, pathAllowLists)
		result, code, err := h.handleGetAllowList(id)

		return result, code, true, err

	case opUpdateAllowList:
		id := extractID(path, pathAllowLists)
		result, code, err := h.handleUpdateAllowList(id, body)

		return result, code, true, err

	case opDeleteAllowList:
		id := extractID(path, pathAllowLists)
		code, err := h.handleDeleteAllowList(id)

		return nil, code, true, err

	case opListAllowLists:
		result, code := h.handleListAllowLists(query)

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateAllowList(body []byte) (any, int, error) {
	var req struct {
		Criteria    *AllowListCriteria `json:"criteria"`
		Tags        map[string]string  `json:"tags"`
		ClientToken string             `json:"clientToken"`
		Description string             `json:"description"`
		Name        string             `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	criteria := AllowListCriteria{}
	if req.Criteria != nil {
		criteria = *req.Criteria
	}

	al, err := h.Backend.CreateAllowList(req.Name, req.Description, criteria, req.Tags)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: al.Arn, "id": al.ID}, http.StatusOK, nil
}

func (h *Handler) handleGetAllowList(id string) (any, int, error) {
	al, err := h.Backend.GetAllowList(id)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return al, http.StatusOK, nil
}

func (h *Handler) handleUpdateAllowList(id string, body []byte) (any, int, error) {
	var req struct {
		Criteria    *AllowListCriteria `json:"criteria"`
		Description string             `json:"description"`
		Name        string             `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	criteria := AllowListCriteria{}
	if req.Criteria != nil {
		criteria = *req.Criteria
	}

	al, err := h.Backend.UpdateAllowList(id, req.Name, req.Description, criteria)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: al.Arn, "id": al.ID}, http.StatusOK, nil
}

func (h *Handler) handleDeleteAllowList(id string) (int, error) {
	if err := h.Backend.DeleteAllowList(id); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListAllowLists(query string) (any, int) {
	q, _ := url.ParseQuery(query)
	limit, _ := strconv.Atoi(q.Get("maxResults"))
	lists, nextToken, _ := h.Backend.ListAllowLists(limit, q.Get("nextToken"))

	resp := map[string]any{"allowLists": lists}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK
}

// Custom data identifier handlers
