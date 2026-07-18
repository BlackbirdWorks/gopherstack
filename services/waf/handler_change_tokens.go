package waf

// --- change token ---

func (h *Handler) opGetChangeToken(_ []byte) (any, error) {
	return map[string]any{
		keyChangeToken: h.Backend.GetChangeToken(),
	}, nil
}

func (h *Handler) opGetChangeTokenStatus(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	return map[string]any{
		"ChangeTokenStatus": h.Backend.GetChangeTokenStatus(in.ChangeToken),
	}, nil
}
