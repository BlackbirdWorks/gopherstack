package pinpoint

// GetInAppMessages returns in-app messages for an endpoint.
func (b *InMemoryBackend) GetInAppMessages(appID, _ string) (*inAppMessagesResponse, error) {
	b.mu.RLock("GetInAppMessages")
	defer b.mu.RUnlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	// Collect in-app templates as message campaigns for this app.
	var campaigns []inAppMessageCampaign

	for _, t := range b.inAppTemplates.All() {
		campaigns = append(campaigns, inAppMessageCampaign{CampaignID: t.TemplateName})
	}

	if campaigns == nil {
		campaigns = []inAppMessageCampaign{}
	}

	return &inAppMessagesResponse{
		InAppMessageCampaigns: campaigns,
	}, nil
}
