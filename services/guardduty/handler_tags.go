package guardduty

import (
	"encoding/json"
	"net/http"
	"strings"
)

func (h *Handler) dispatchTagOps(op, path, query string, body []byte) (any, int, error) {
	resourceARN := extractTagResourceARN(path)

	switch op {
	case opListTagsForResource:
		result, code, err := h.handleListTagsForResource(resourceARN)

		return result, code, err

	case opTagResource:
		code, err := h.handleTagResource(resourceARN, body)

		return nil, code, err

	case opUntagResource:
		code, err := h.handleUntagResource(resourceARN, query)

		return nil, code, err
	}

	return nil, http.StatusNotFound, errorf(errResourceNotFound)
}

func (h *Handler) handleListTagsForResource(resourceARN string) (any, int, error) {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	return map[string]any{keyTags: tags}, http.StatusOK, nil
}

func (h *Handler) handleTagResource(resourceARN string, body []byte) (int, error) {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUntagResource(resourceARN, query string) (int, error) {
	keys := parseTagKeys(query)

	if err := h.Backend.UntagResource(resourceARN, keys); err != nil {
		return http.StatusBadRequest, err
	}

	return http.StatusOK, nil
}

// extractTagResourceARN extracts the resource ARN from /tags/{arn}.
func extractTagResourceARN(path string) string {
	return strings.TrimPrefix(path, "/"+pathTags+"/")
}

// parseTagKeys extracts tagKeys from query string: tagKeys=k1&tagKeys=k2.
func parseTagKeys(query string) []string {
	var keys []string

	for part := range strings.SplitSeq(query, "&") {
		if val, ok := strings.CutPrefix(part, "tagKeys="); ok {
			keys = append(keys, val)
		}
	}

	return keys
}
