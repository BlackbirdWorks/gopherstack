package pinpoint

// GetCampaignActivities returns campaign activities.
func (b *InMemoryBackend) GetCampaignActivities(
	appID, campaignID string,
) (*campaignActivitiesResponse, error) {
	b.mu.RLock("GetCampaignActivities")
	defer b.mu.RUnlock()

	c, ok := b.campaigns.Get(campaignID)
	if !ok || c.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	actKey := appID + "/" + campaignID
	activities := b.campaignActivities[actKey]

	if activities == nil {
		activities = []campaignActivity{}
	}

	return &campaignActivitiesResponse{Item: activities}, nil
}
