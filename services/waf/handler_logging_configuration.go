package waf

// --- Logging ---

func (h *Handler) opPutLoggingConfiguration(body []byte) (any, error) {
	var in struct {
		LoggingConfiguration LoggingConfiguration `json:"LoggingConfiguration"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	cfg, err := h.Backend.PutLoggingConfiguration(in.LoggingConfiguration)
	if err != nil {
		return nil, err
	}

	return map[string]any{"LoggingConfiguration": cfg}, nil
}

func (h *Handler) opGetLoggingConfiguration(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	cfg, err := h.Backend.GetLoggingConfiguration(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"LoggingConfiguration": cfg}, nil
}

func (h *Handler) opDeleteLoggingConfiguration(body []byte) (any, error) {
	var in struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteLoggingConfiguration(in.ResourceArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opListLoggingConfigurations(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListLoggingConfigurations(), in.NextMarker, in.Limit,
		func(c LoggingConfiguration) string { return c.ResourceArn })
	result := map[string]any{"LoggingConfigurations": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}
