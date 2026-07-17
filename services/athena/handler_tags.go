package athena

import "encoding/json"

type tagResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []Tag  `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) tagOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"TagResource": func(b []byte) (any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.TagResource(input.ResourceARN, tagsFromSlice(input.Tags))
		},
		"UntagResource": func(b []byte) (any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UntagResource(input.ResourceARN, input.TagKeys)
		},
		"ListTagsForResource": func(b []byte) (any, error) {
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			tags, err := h.Backend.ListTagsForResource(input.ResourceARN)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Tags": tags}, nil
		},
	}
}
