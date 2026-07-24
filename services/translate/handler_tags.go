package translate

import "fmt"

// --- Tags ---
//
// TagResource, UntagResource, and ListTagsForResource all model
// InvalidParameterValueException but never InvalidRequestException
// (api-2.json), so a missing ResourceArn uses ErrInvalidParameter.

func (h *Handler) tagResource(input map[string]any) (map[string]any, error) {
	resourceARN, _ := input[keyResourceARN].(string)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	tags := extractTagsFromSlice(input, "Tags")

	if err := h.Backend.TagResource(resourceARN, tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) untagResource(input map[string]any) (map[string]any, error) {
	resourceARN, _ := input[keyResourceARN].(string)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	keys := strSliceField(input, "TagKeys")

	if err := h.Backend.UntagResource(resourceARN, keys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) listTagsForResource(input map[string]any) (map[string]any, error) {
	resourceARN, _ := input[keyResourceARN].(string)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}

	tagSlice := make([]map[string]any, 0, len(tags))

	for k, v := range tags {
		tagSlice = append(tagSlice, map[string]any{"Key": k, "Value": v})
	}

	return map[string]any{
		"Tags": tagSlice,
	}, nil
}
