package waf

// --- GetSampledRequests ---

func (h *Handler) opGetSampledRequests(body []byte) (any, error) {
	// TimeWindow.StartTime/EndTime are unixTimestamp shapes: the awsjson1.1
	// protocol serializes them as JSON numbers of seconds since the epoch
	// (see aws-sdk-go-v2/service/waf's serializeDocumentTimeWindow), not
	// ISO8601 strings, so the request-side fields must accept a JSON number.
	var in struct {
		WebAclId   string `json:"WebAclId"` //nolint:revive,staticcheck // AWS SDK field name
		RuleId     string `json:"RuleId"`   //nolint:revive,staticcheck // AWS SDK field name
		TimeWindow struct {
			StartTime float64 `json:"StartTime"`
			EndTime   float64 `json:"EndTime"`
		} `json:"TimeWindow"`
		MaxItems int64 `json:"MaxItems"`
	}

	if err := unmarshal(body, &in); err != nil {
		return nil, err
	}

	samples := h.Backend.GetSampledRequests(in.WebAclId, in.RuleId, in.MaxItems)

	return map[string]any{
		"SampledRequests": samples,
		"PopulationSize":  int64(len(samples)),
		"TimeWindow": map[string]any{
			"StartTime": in.TimeWindow.StartTime,
			"EndTime":   in.TimeWindow.EndTime,
		},
	}, nil
}
