package waf

// --- RuleGroup ---

func (h *Handler) opCreateRuleGroup(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
		MetricName  string `json:"MetricName"`
		Tags        []Tag  `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rg, err := h.Backend.CreateRuleGroup(in.Name, in.MetricName, in.ChangeToken, tagsToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"RuleGroup":    rg,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRuleGroup(body []byte) (any, error) {
	var in struct {
		RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rg, err := h.Backend.GetRuleGroup(in.RuleGroupId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RuleGroup": rg}, nil
}

func (h *Handler) opUpdateRuleGroup(body []byte) (any, error) {
	var in struct {
		ChangeToken string                `json:"ChangeToken"`
		RuleGroupId string                `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates     []ActivatedRuleUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRuleGroup(in.RuleGroupId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRuleGroup(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRuleGroup(in.RuleGroupId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRuleGroups(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListRuleGroups(), in.NextMarker, in.Limit,
		func(s RuleGroupSummary) string { return s.RuleGroupId })
	result := map[string]any{"RuleGroups": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

func (h *Handler) opListActivatedRulesInRuleGroup(body []byte) (any, error) {
	var in struct {
		RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
		NextMarker  string `json:"NextMarker"`
		Limit       int    `json:"Limit"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rules, err := h.Backend.ListActivatedRulesInRuleGroup(in.RuleGroupId)
	if err != nil {
		return nil, err
	}

	page, next := paginate(rules, in.NextMarker, in.Limit,
		func(r ActivatedRule) string { return r.RuleId })
	result := map[string]any{"ActivatedRules": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

func (h *Handler) opListSubscribedRuleGroups(_ []byte) (any, error) {
	return map[string]any{"RuleGroups": h.Backend.ListSubscribedRuleGroups()}, nil
}
