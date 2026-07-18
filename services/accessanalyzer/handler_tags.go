package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"strings"
)

const (
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
)

// dispatchTagOps routes tag operations.
func (h *Handler) dispatchTagOps(op, path, query string, body []byte) (any, int, error) {
	switch op {
	case opTagResource:
		return h.handleTagResource(path, body)
	case opUntagResource:
		return h.handleUntagResource(path, query)
	case opListTagsForResource:
		return h.handleListTagsForResource(path)
	}

	return nil, http.StatusNotFound, nil
}

// ---- operation handlers ----

func (h *Handler) handleTagResource(path string, body []byte) (any, int, error) {
	resourceARN := strings.TrimPrefix(path, "/"+pathTags+"/")

	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleUntagResource(path, query string) (any, int, error) {
	resourceARN := strings.TrimPrefix(path, "/"+pathTags+"/")

	var tagKeys []string

	for part := range strings.SplitSeq(query, "&") {
		if after, ok := strings.CutPrefix(part, "tagKeys="); ok {
			tagKeys = append(tagKeys, after)
		}
	}

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleListTagsForResource(path string) (any, int, error) {
	resourceARN := strings.TrimPrefix(path, "/"+pathTags+"/")

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, 0, err
	}

	if tags == nil {
		tags = make(map[string]string)
	}

	return map[string]any{keyTags: tags}, http.StatusOK, nil
}

// ---- URL path parsing ----

func parseTagsPath(method string, segments []string) (string, string) {
	if len(segments) < segmentDepthResource || segments[1] == "" {
		return opUnknown, ""
	}

	resourceARN := strings.Join(segments[1:], "/")

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
