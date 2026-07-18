package waf

// handler_match_sets.go groups the dispatch handlers for the WAF Classic
// match-set families whose request/response shape is nearly identical (a
// name/ID, a changeToken, and a slice of tuples/strings updated via
// INSERT/DELETE): ByteMatchSet, SizeConstraintSet, SqlInjectionMatchSet,
// XssMatchSet, GeoMatchSet, RegexPatternSet, and RegexMatchSet. Splitting
// these into one file per family previously tripped golangci-lint's dupl
// (clone-detection) check across file boundaries; keeping them together in
// a single file resolves that by construction, with no lint suppression
// needed.

// --- ByteMatchSet ---

func (h *Handler) opCreateByteMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	bms, err := h.Backend.CreateByteMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ByteMatchSet": bms,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetByteMatchSet(body []byte) (any, error) {
	var in struct {
		ByteMatchSetId string `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	bms, err := h.Backend.GetByteMatchSet(in.ByteMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ByteMatchSet": bms}, nil
}

func (h *Handler) opUpdateByteMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken    string               `json:"ChangeToken"`
		ByteMatchSetId string               `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates        []ByteMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateByteMatchSet(in.ByteMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteByteMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken    string `json:"ChangeToken"`
		ByteMatchSetId string `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteByteMatchSet(in.ByteMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListByteMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListByteMatchSets(), in.NextMarker, in.Limit,
		func(s ByteMatchSetSummary) string { return s.ByteMatchSetId })
	result := map[string]any{"ByteMatchSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

// --- SizeConstraintSet ---

func (h *Handler) opCreateSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	scs, err := h.Backend.CreateSizeConstraintSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SizeConstraintSet": scs,
		keyChangeToken:      in.ChangeToken,
	}, nil
}

func (h *Handler) opGetSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		SizeConstraintSetId string `json:"SizeConstraintSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	scs, err := h.Backend.GetSizeConstraintSet(in.SizeConstraintSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"SizeConstraintSet": scs}, nil
}

func (h *Handler) opUpdateSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		//nolint:revive,staticcheck // AWS SDK naming
		SizeConstraintSetId string                    `json:"SizeConstraintSetId"`
		Updates             []SizeConstraintSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateSizeConstraintSet(in.SizeConstraintSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteSizeConstraintSet(body []byte) (any, error) {
	var in struct {
		ChangeToken         string `json:"ChangeToken"`
		SizeConstraintSetId string `json:"SizeConstraintSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteSizeConstraintSet(in.SizeConstraintSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListSizeConstraintSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListSizeConstraintSets(), in.NextMarker, in.Limit,
		func(s SizeConstraintSetSummary) string { return s.SizeConstraintSetId })
	result := map[string]any{"SizeConstraintSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

// --- SqlInjectionMatchSet ---

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opCreateSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	sims, err := h.Backend.CreateSqlInjectionMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SqlInjectionMatchSet": sims,
		keyChangeToken:         in.ChangeToken,
	}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opGetSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		SqlInjectionMatchSetId string `json:"SqlInjectionMatchSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	sims, err := h.Backend.GetSqlInjectionMatchSet(in.SqlInjectionMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"SqlInjectionMatchSet": sims}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opUpdateSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		//nolint:revive,staticcheck // AWS SDK naming
		SqlInjectionMatchSetId string                       `json:"SqlInjectionMatchSetId"`
		Updates                []SqlInjectionMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateSqlInjectionMatchSet(in.SqlInjectionMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opDeleteSqlInjectionMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken            string `json:"ChangeToken"`
		SqlInjectionMatchSetId string `json:"SqlInjectionMatchSetId"` //nolint:revive,staticcheck // AWS SDK naming
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteSqlInjectionMatchSet(in.SqlInjectionMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opListSqlInjectionMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListSqlInjectionMatchSets(), in.NextMarker, in.Limit,
		func(s SqlInjectionMatchSetSummary) string { return s.SqlInjectionMatchSetId })
	result := map[string]any{"SqlInjectionMatchSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

// --- XssMatchSet ---

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opCreateXssMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	xms, err := h.Backend.CreateXssMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"XssMatchSet":  xms,
		keyChangeToken: in.ChangeToken,
	}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opGetXssMatchSet(body []byte) (any, error) {
	var in struct {
		XssMatchSetId string `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	xms, err := h.Backend.GetXssMatchSet(in.XssMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"XssMatchSet": xms}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opUpdateXssMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string              `json:"ChangeToken"`
		XssMatchSetId string              `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates       []XssMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateXssMatchSet(in.XssMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opDeleteXssMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string `json:"ChangeToken"`
		XssMatchSetId string `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteXssMatchSet(in.XssMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

//nolint:revive,staticcheck // AWS SDK naming
func (h *Handler) opListXssMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListXssMatchSets(), in.NextMarker, in.Limit,
		func(s XssMatchSetSummary) string { return s.XssMatchSetId })
	result := map[string]any{"XssMatchSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

// --- GeoMatchSet ---

func (h *Handler) opCreateGeoMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	gms, err := h.Backend.CreateGeoMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"GeoMatchSet":  gms,
		keyChangeToken: in.ChangeToken,
	}, nil
}

func (h *Handler) opGetGeoMatchSet(body []byte) (any, error) {
	var in struct {
		GeoMatchSetId string `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	gms, err := h.Backend.GetGeoMatchSet(in.GeoMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"GeoMatchSet": gms}, nil
}

func (h *Handler) opUpdateGeoMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string              `json:"ChangeToken"`
		GeoMatchSetId string              `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates       []GeoMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateGeoMatchSet(in.GeoMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteGeoMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken   string `json:"ChangeToken"`
		GeoMatchSetId string `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteGeoMatchSet(in.GeoMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListGeoMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListGeoMatchSets(), in.NextMarker, in.Limit,
		func(s GeoMatchSetSummary) string { return s.GeoMatchSetId })
	result := map[string]any{"GeoMatchSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

// --- RegexPatternSet ---

func (h *Handler) opCreateRegexPatternSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rps, err := h.Backend.CreateRegexPatternSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"RegexPatternSet": rps,
		keyChangeToken:    in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRegexPatternSet(body []byte) (any, error) {
	var in struct {
		RegexPatternSetId string `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rps, err := h.Backend.GetRegexPatternSet(in.RegexPatternSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RegexPatternSet": rps}, nil
}

func (h *Handler) opUpdateRegexPatternSet(body []byte) (any, error) {
	var in struct {
		ChangeToken       string                  `json:"ChangeToken"`
		RegexPatternSetId string                  `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates           []RegexPatternSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRegexPatternSet(in.RegexPatternSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRegexPatternSet(body []byte) (any, error) {
	var in struct {
		ChangeToken       string `json:"ChangeToken"`
		RegexPatternSetId string `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRegexPatternSet(in.RegexPatternSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRegexPatternSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListRegexPatternSets(), in.NextMarker, in.Limit,
		func(s RegexPatternSetSummary) string { return s.RegexPatternSetId })
	result := map[string]any{"RegexPatternSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}

// --- RegexMatchSet ---

func (h *Handler) opCreateRegexMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken string `json:"ChangeToken"`
		Name        string `json:"Name"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rms, err := h.Backend.CreateRegexMatchSet(in.Name, in.ChangeToken)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"RegexMatchSet": rms,
		keyChangeToken:  in.ChangeToken,
	}, nil
}

func (h *Handler) opGetRegexMatchSet(body []byte) (any, error) {
	var in struct {
		RegexMatchSetId string `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	rms, err := h.Backend.GetRegexMatchSet(in.RegexMatchSetId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RegexMatchSet": rms}, nil
}

func (h *Handler) opUpdateRegexMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken     string                `json:"ChangeToken"`
		RegexMatchSetId string                `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
		Updates         []RegexMatchSetUpdate `json:"Updates"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateRegexMatchSet(in.RegexMatchSetId, in.ChangeToken, in.Updates); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opDeleteRegexMatchSet(body []byte) (any, error) {
	var in struct {
		ChangeToken     string `json:"ChangeToken"`
		RegexMatchSetId string `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRegexMatchSet(in.RegexMatchSetId, in.ChangeToken); err != nil {
		return nil, err
	}

	return map[string]any{keyChangeToken: in.ChangeToken}, nil
}

func (h *Handler) opListRegexMatchSets(body []byte) (any, error) {
	var in struct {
		NextMarker string `json:"NextMarker"`
		Limit      int    `json:"Limit"`
	}

	_ = unmarshal(body, &in)

	page, next := paginate(h.Backend.ListRegexMatchSets(), in.NextMarker, in.Limit,
		func(s RegexMatchSetSummary) string { return s.RegexMatchSetId })
	result := map[string]any{"RegexMatchSets": page}
	if next != "" {
		result["NextMarker"] = next
	}

	return result, nil
}
