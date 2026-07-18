package apigateway

import (
	"encoding/json"
	"net/http"
	"strings"
)

type getTagsInput struct {
	ResourceARN string `json:"resourceArn"`
}

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceARN string            `json:"resourceArn"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

// parseAPIGWTagsPath handles /tags/{resourceArn} paths.
func parseAPIGWTagsPath(method string, segs []string, n int) (string, map[string]string, bool) {
	if n < apiGWMinTagPathSegs {
		return apiGWUnknownOp, nil, false
	}
	// Re-join remaining segs to reconstruct the ARN (which may contain slashes).
	arn := strings.Join(segs[1:], "/")

	switch method {
	// GET /tags/{resourceArn} → GetTags
	case http.MethodGet:
		return opGetTags, map[string]string{keyResourceArn: arn}, true
	// PUT /tags/{resourceArn} → TagResource
	case http.MethodPut:
		return opTagResource, map[string]string{keyResourceArn: arn}, true
	// DELETE /tags/{resourceArn} → UntagResource
	case http.MethodDelete:
		return opUntagResource, map[string]string{keyResourceArn: arn}, true
	}

	return apiGWUnknownOp, nil, false
}

// tagActions returns the action map for resource tagging operations.
func (h *Handler) tagActions() map[string]actionFn {
	return map[string]actionFn{
		opGetTags: func(b []byte) (int, any, error) {
			var input getTagsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			t, err := h.Backend.GetResourceTags(input.ResourceARN)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{"tags": t}, nil
		},
		opTagResource: func(b []byte) (int, any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.TagResource(input.ResourceARN, input.Tags); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		opUntagResource: func(b []byte) (int, any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.UntagResource(input.ResourceARN, input.TagKeys); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
	}
}
