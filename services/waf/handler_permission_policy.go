package waf

// --- PermissionPolicy ---

func (h *Handler) opPutPermissionPolicy(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
		Policy      string `json:"Policy"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.PutPermissionPolicy(in.ResourceArn, in.Policy); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opGetPermissionPolicy(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	policy, err := h.Backend.GetPermissionPolicy(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Policy": policy}, nil
}

func (h *Handler) opDeletePermissionPolicy(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeletePermissionPolicy(in.ResourceArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}
