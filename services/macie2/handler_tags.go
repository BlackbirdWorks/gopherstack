package macie2

import (
	"encoding/json"
	"net/http"
	"strings"
)

func parseTagPath(method string, parts []string) (string, string) {
	if len(parts) < minTagPathParts {
		return opUnknown, ""
	}

	resourceARN := strings.Join(parts[1:], "/")
	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceARN
	case http.MethodPost:
		return opTagResource, resourceARN
	case http.MethodDelete:
		return opUntagResource, resourceARN
	}

	return opUnknown, ""
}

func (h *Handler) dispatchTagOps(op, path, query string, body []byte) (any, int, error) {
	switch op {
	case opListTagsForResource:
		resourceARN := extractTagARN(path)
		result, code, err := h.handleListTagsForResource(resourceARN)

		return result, code, err

	case opTagResource:
		resourceARN := extractTagARN(path)
		code, err := h.handleTagResource(resourceARN, body)

		return nil, code, err

	case opUntagResource:
		resourceARN := extractTagARN(path)
		code, err := h.handleUntagResource(resourceARN, query)

		return nil, code, err
	}

	return nil, http.StatusNotFound, nil
}

func (h *Handler) handleListTagsForResource(resourceARN string) (any, int, error) {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"tags": tags}, http.StatusOK, nil
}

func (h *Handler) handleTagResource(resourceARN string, body []byte) (int, error) {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUntagResource(resourceARN, query string) (int, error) {
	tagKeys := parseTagKeys(query)
	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func extractTagARN(path string) string {
	trimmed := strings.TrimPrefix(path, "/"+pathTags+"/")

	return trimmed
}

func parseTagKeys(query string) []string {
	var keys []string

	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, "tagKeys="); ok {
			keys = append(keys, v)
		}
	}

	return keys
}
