package macie2

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseCustomDataIDPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /custom-data-identifiers
		if method == http.MethodPost {
			return opCreateCustomDataID, ""
		}
	case depthResource: // /custom-data-identifiers/{id|list|test}
		sub := parts[1]
		switch sub {
		case "list": //nolint:goconst // existing issue.
			if method == http.MethodPost {
				return opListCustomDataIDs, ""
			}
		case "test":
			if method == http.MethodPost {
				return opTestCustomDataID, ""
			}
		case "get":
			if method == http.MethodPost {
				return opBatchGetCustomDataIdentifiers, ""
			}
		default:
			switch method {
			case http.MethodGet:
				return opGetCustomDataID, sub
			case http.MethodDelete:
				return opDeleteCustomDataID, sub
			}
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchCustomDataIDOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateCustomDataID:
		result, code, err := h.handleCreateCustomDataID(body)

		return result, code, true, err

	case opGetCustomDataID:
		id := extractID(path, pathCustomDataIDs)
		result, code, err := h.handleGetCustomDataID(id)

		return result, code, true, err

	case opDeleteCustomDataID:
		id := extractID(path, pathCustomDataIDs)
		code, err := h.handleDeleteCustomDataID(id)

		return nil, code, true, err

	case opListCustomDataIDs:
		result, code, err := h.handleListCustomDataIDs(body)

		return result, code, true, err

	case opTestCustomDataID:
		result, code, err := h.handleTestCustomDataID(body)

		return result, code, true, err

	case opBatchGetCustomDataIdentifiers:
		result, code, err := h.handleBatchGetCustomDataIdentifiers(body)

		return result, code, true, err

	case opListManagedDataIdentifiers:
		result, code, err := h.handleListManagedDataIdentifiers()

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleCreateCustomDataID(body []byte) (any, int, error) {
	var req struct {
		MaximumMatchDistance *int32            `json:"maximumMatchDistance"`
		Tags                 map[string]string `json:"tags"`
		ClientToken          string            `json:"clientToken"`
		Description          string            `json:"description"`
		Name                 string            `json:"name"`
		Regex                string            `json:"regex"`
		IgnoreWords          []string          `json:"ignoreWords"`
		Keywords             []string          `json:"keywords"`
		SeverityLevels       []SeverityLevel   `json:"severityLevels"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Regex == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	id, err := h.Backend.CreateCustomDataIdentifier(
		req.Name, req.Description, req.Regex,
		req.IgnoreWords, req.Keywords, req.SeverityLevels, req.MaximumMatchDistance,
		req.Tags,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return nil, http.StatusBadRequest, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{"customDataIdentifierId": id}, http.StatusOK, nil
}

func (h *Handler) handleGetCustomDataID(id string) (any, int, error) {
	cdi, err := h.Backend.GetCustomDataIdentifier(id)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return cdi, http.StatusOK, nil
}

func (h *Handler) handleDeleteCustomDataID(id string) (int, error) {
	if err := h.Backend.DeleteCustomDataIdentifier(id); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListCustomDataIDs(body []byte) (any, int, error) {
	var req struct {
		MaxResults *int32 `json:"maxResults"`
		NextToken  string `json:"nextToken"`
	}
	_ = json.Unmarshal(body, &req)

	limit := 0
	if req.MaxResults != nil {
		limit = int(*req.MaxResults)
	}

	items, nextToken, err := h.Backend.ListCustomDataIdentifiers(limit, req.NextToken)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{keyItems: items}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleTestCustomDataID(body []byte) (any, int, error) {
	var req struct {
		MaximumMatchDistance *int32   `json:"maximumMatchDistance"`
		Regex                string   `json:"regex"`
		SampleText           string   `json:"sampleText"`
		IgnoreWords          []string `json:"ignoreWords"`
		Keywords             []string `json:"keywords"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	count, err := h.Backend.TestCustomDataIdentifier(
		req.Regex, req.IgnoreWords, req.Keywords,
		req.MaximumMatchDistance, req.SampleText,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return nil, http.StatusBadRequest, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]int32{"matchCount": count}, http.StatusOK, nil
}

func parseManagedDataIDsPath(method string, parts []string) (string, string) {
	// /managed-data-identifiers/list
	if len(parts) == depthResource && parts[1] == "list" && method == http.MethodPost {
		return opListManagedDataIdentifiers, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleBatchGetCustomDataIdentifiers(body []byte) (any, int, error) {
	var req struct {
		IDs []string `json:"ids"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	items, err := h.Backend.BatchGetCustomDataIdentifiers(req.IDs)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	found := make(map[string]bool, len(items))
	for _, item := range items {
		found[item.ID] = true
	}

	notFound := make([]string, 0)

	for _, id := range req.IDs {
		if !found[id] {
			notFound = append(notFound, id)
		}
	}

	resp := map[string]any{"customDataIdentifiers": items}
	if len(notFound) > 0 {
		resp["notFoundIdentifierIds"] = notFound
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleListManagedDataIdentifiers() (any, int, error) {
	items, err := h.Backend.ListManagedDataIdentifiers()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{keyItems: items}, http.StatusOK, nil
}
