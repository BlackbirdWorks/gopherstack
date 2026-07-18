package waf

// --- IPSet ---

func (h *Handler) opCreateIPSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	ipSet, err := h.Backend.CreateIPSet(in.Name, in.ChangeToken, nil)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"IPSet":        ipSet,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetIPSet(body []byte) (any, error) {
	var in struct {
		IPSetId string `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	ipSet, err := h.Backend.GetIPSet(in.IPSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"IPSet": ipSet}, nil
}

func (h *Handler) opUpdateIPSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string        `json:"ChangeToken"`
		IPSetId     string        `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []IPSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateIPSet(in.IPSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteIPSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		IPSetId     string `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteIPSet(in.IPSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListIPSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListIPSets(), in.NextMarker, in.Limit,
		func(s IPSetSummary) string { return s.IPSetId })
	result := map[string]any{"IPSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}
