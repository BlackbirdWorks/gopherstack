package comprehend

func (h *Handler) tagResource(input map[string]any) (map[string]any, error) {
	err := h.Backend.TagResource(stringValue(input, "ResourceArn", ""), inputTags(input))

	return map[string]any{}, err
}

func (h *Handler) untagResource(input map[string]any) (map[string]any, error) {
	keys := stringSliceValue(input, "TagKeys")
	err := h.Backend.UntagResource(stringValue(input, "ResourceArn", ""), keys)

	return map[string]any{}, err
}

func (h *Handler) listTags(input map[string]any) (map[string]any, error) {
	tags, err := h.Backend.ListTags(stringValue(input, "ResourceArn", ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{"ResourceArn": input["ResourceArn"], "Tags": tags}, nil
}

func inputTags(input map[string]any) []Tag {
	raw, _ := input["Tags"].([]any)
	tags := make([]Tag, 0, len(raw))
	for _, item := range raw {
		entry, _ := item.(map[string]any)
		tags = append(tags, Tag{Key: stringValue(entry, "Key", ""), Value: stringValue(entry, "Value", "")})
	}

	return tags
}
