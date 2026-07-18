package personalize

// --- Tags ---

func (h *Handler) tagResource(input map[string]any) (map[string]any, error) {
	resourceArn, _ := input["resourceArn"].(string)
	tags := extractTagsFromSlice(input, "tags")

	return map[string]any{}, h.Backend.TagResource(resourceArn, tags)
}

func (h *Handler) untagResource(input map[string]any) (map[string]any, error) {
	resourceArn, _ := input["resourceArn"].(string)
	keys := strSlice(input, "tagKeys")

	return map[string]any{}, h.Backend.UntagResource(resourceArn, keys)
}

func (h *Handler) listTagsForResource(input map[string]any) (map[string]any, error) {
	resourceArn, _ := input["resourceArn"].(string)

	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return nil, err
	}

	tagSlice := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagSlice = append(tagSlice, map[string]any{"tagKey": k, "tagValue": v})
	}

	return map[string]any{"tags": tagSlice}, nil
}
