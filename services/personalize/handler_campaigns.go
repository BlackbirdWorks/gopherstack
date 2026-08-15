package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- Campaign ---

func (h *Handler) createCampaign(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	minProvisionedTPS := int32Field(input, "minProvisionedTPS")
	campaignConfig := decodeConfig[CampaignConfig](rawMap(input, "campaignConfig"))
	tags := extractTags(input)

	c, err := h.Backend.CreateCampaign(name, solutionVersionArn, minProvisionedTPS, campaignConfig, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyCampaignArn: c.CampaignArn}, nil
}

func (h *Handler) describeCampaign(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["campaignArn"].(string)

	c, err := h.Backend.DescribeCampaign(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"campaign": campaignToMap(c)}, nil
}

func (h *Handler) updateCampaign(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["campaignArn"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	minProvisionedTPS := int32Field(input, "minProvisionedTPS")
	campaignConfig := decodeConfig[CampaignConfig](rawMap(input, "campaignConfig"))

	c, err := h.Backend.UpdateCampaign(nameOrArn, solutionVersionArn, minProvisionedTPS, campaignConfig)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyCampaignArn: c.CampaignArn}, nil
}

func (h *Handler) deleteCampaign(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["campaignArn"].(string)

	return map[string]any{}, h.Backend.DeleteCampaign(nameOrArn)
}

func (h *Handler) listCampaigns(input map[string]any) (map[string]any, error) {
	solutionArn, _ := input["solutionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListCampaigns(solutionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, c := range list {
		summaries = append(summaries, campaignSummaryToMap(c))
	}

	result := map[string]any{"campaigns": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func campaignToMap(c *Campaign) map[string]any {
	m := map[string]any{
		keyCampaignArn:         c.CampaignArn,
		keyName:                c.Name,
		keySolutionVersionArn:  c.SolutionVersionArn,
		"minProvisionedTPS":    c.MinProvisionedTPS,
		keyStatus:              c.Status,
		keyCreationDateTime:    awstime.Epoch(c.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(c.LastUpdatedDateTime),
	}
	if c.CampaignConfig != nil {
		m["campaignConfig"] = c.CampaignConfig
	}
	// latestCampaignUpdate is only returned once the campaign has had at
	// least one UpdateCampaign call -- matches the real API's doc comment on
	// Campaign.LatestCampaignUpdate.
	if c.LatestCampaignUpdate != nil {
		m["latestCampaignUpdate"] = c.LatestCampaignUpdate
	}

	return m
}

// campaignSummaryToMap builds the types.CampaignSummary shape (types.go:481)
// -- no solutionVersionArn, minProvisionedTPS, campaignConfig, or
// latestCampaignUpdate. failureReason is a real CampaignSummary member, but
// the backend's Campaign model has no source for it (campaigns never fail
// asynchronously here), so it stays absent rather than being fabricated.
func campaignSummaryToMap(c *Campaign) map[string]any {
	return map[string]any{
		keyCampaignArn:         c.CampaignArn,
		keyName:                c.Name,
		keyStatus:              c.Status,
		keyCreationDateTime:    awstime.Epoch(c.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(c.LastUpdatedDateTime),
	}
}
