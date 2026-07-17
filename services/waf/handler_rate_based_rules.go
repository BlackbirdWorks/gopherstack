package waf

// --- RateBasedRule ---

func (h *Handler) opCreateRateBasedRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
		MetricName  string `json:"MetricName"`
		RateKey     string `json:"RateKey"`
		Tags        []Tag  `json:"Tags"`
		RateLimit   int64  `json:"RateLimit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.CreateRateBasedRule(
		in.Name, in.MetricName, in.RateKey, in.RateLimit, in.ChangeToken, tagsToMap(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyRule:        rule,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRateBasedRule(body []byte) (any, error) {
	var in struct {
		RuleId string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rule, err := h.Backend.GetRateBasedRule(in.RuleId)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRule: rule}, nil
}

func (h *Handler) opUpdateRateBasedRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string       `json:"ChangeToken"`
		RuleId      string       `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []RuleUpdate `json:"Updates"`
		RateLimit   int64        `json:"RateLimit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRateBasedRule(in.RuleId, in.ChangeToken, in.RateLimit, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRateBasedRule(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		RuleId      string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRateBasedRule(in.RuleId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRateBasedRules(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListRateBasedRules(), in.NextMarker, in.Limit,
		func(s RateBasedRuleSummary) string { return s.RuleId })
	result := map[string]any{"Rules": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

func (h *Handler) opGetRateBasedRuleManagedKeys(body []byte) (any, error) {
	var in struct {
		RuleId     string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
		NextMarker string `json:"NextMarker"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	keys, err := h.Backend.GetRateBasedRuleManagedKeys(in.RuleId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ManagedKeys": keys}, nil
}
