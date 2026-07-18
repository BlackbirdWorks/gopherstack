package waf

// --- WebACL ---

func (h *Handler) opCreateWebACL(body []byte) (any, error) {
	var in struct {
		ChangeToken   string    `json:"ChangeToken"`
		Name          string    `json:"Name"`
		MetricName    string    `json:"MetricName"`
		DefaultAction WafAction `json:"DefaultAction"`
		Tags          []Tag     `json:"Tags"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	acl, err := h.Backend.CreateWebACL(in.Name, in.MetricName, in.DefaultAction, in.ChangeToken, tagsToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"WebACL":       acl,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetWebACL(body []byte) (any, error) {
	var in struct {
		WebACLId string `json:"WebACLId"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	acl, err := h.Backend.GetWebACL(in.WebACLId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"WebACL": acl}, nil
}

func (h *Handler) opUpdateWebACL(body []byte) (any, error) {
	var in struct {
		ChangeToken   string         `json:"ChangeToken"`
		WebACLId      string         `json:"WebACLId"`
		DefaultAction *WafAction     `json:"DefaultAction"`
		Updates       []WebACLUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateWebACL(in.WebACLId, in.ChangeToken, in.DefaultAction, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteWebACL(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		WebACLId    string `json:"WebACLId"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteWebACL(in.WebACLId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListWebACLs(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListWebACLs(), in.NextMarker, in.Limit,
		func(s WebACLSummary) string { return s.WebACLId })
	result := map[string]any{"WebACLs": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

// --- Migration ---

func (h *Handler) opCreateWebACLMigrationStack(body []byte) (any, error) {
	var in struct {
		WebACLId              string `json:"WebACLId"`
		S3BucketName          string `json:"S3BucketName"`
		IgnoreUnsupportedType bool   `json:"IgnoreUnsupportedType"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	s3URL := "https://" + in.S3BucketName + ".s3.amazonaws.com/webacl/" + in.WebACLId + "/template.json"

	return map[string]any{"S3ObjectUrl": s3URL}, nil
}
