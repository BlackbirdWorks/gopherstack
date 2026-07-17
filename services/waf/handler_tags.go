package waf

// --- Tags ---

func (h *Handler) opTagResource(body []byte) (any, error) {
	var in struct {
		ResourceARN string `json:"ResourceARN"`
		Tags        []Tag  `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(in.ResourceARN, tagsToMap(in.Tags)); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opUntagResource(body []byte) (any, error) {
	var in struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opListTagsForResource(body []byte) (any, error) {
	var in struct {
		ResourceARN string `json:"ResourceARN"`
		NextMarker  string `json:"NextMarker"`
		Limit       int    `json:"Limit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	page, next := paginate(tags, in.NextMarker, in.Limit, func(t Tag) string { return t.Key })
	tagInfo := map[string]any{
		"ResourceARN": in.ResourceARN,
		"TagList":     page,
	}
	if next != "" {
		tagInfo["NextMarker"] = next
	}

	return map[string]any{"TagInfoForResource": tagInfo}, nil
}
