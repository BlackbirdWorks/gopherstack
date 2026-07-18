package waf

// --- Rule ---

func (h *Handler) opCreateRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
		MetricName  string `json:"MetricName"`
		Tags        []Tag  `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.CreateRule(in.Name, in.MetricName, in.ChangeToken, tagsToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyRule:        rule,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRule(body []byte) (any, error) {
	var in struct {
		RuleId string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.GetRule(in.RuleId)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRule: rule}, nil
}

func (h *Handler) opUpdateRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string       `json:"ChangeToken"`
		RuleId      string       `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []RuleUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRule(in.RuleId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		RuleId      string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRule(in.RuleId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRules(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListRules(), in.NextMarker, in.Limit,
		func(s RuleSummary) string { return s.RuleId })
	result := map[string]any{"Rules": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}
