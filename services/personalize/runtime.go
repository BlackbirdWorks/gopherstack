package personalize

import "fmt"

// --- Runtime validation ---

// ValidateCampaignOrRecommender returns nil if either campaignArn or recommenderArn resolves
// to an existing resource. Returns ErrNotFound if neither exists.
func (b *InMemoryBackend) ValidateCampaignOrRecommender(campaignArn, recommenderArn string) error {
	b.mu.RLock("ValidateCampaignOrRecommender")
	defer b.mu.RUnlock()

	if campaignArn != "" {
		if b.findCampaign(campaignArn) != nil {
			return nil
		}
	}
	if recommenderArn != "" {
		if b.findRecommender(recommenderArn) != nil {
			return nil
		}
	}

	ref := campaignArn
	if ref == "" {
		ref = recommenderArn
	}

	return fmt.Errorf("%w: campaign or recommender %q not found", ErrNotFound, ref)
}

// ValidateCampaign returns nil if campaignArn resolves to an existing campaign.
func (b *InMemoryBackend) ValidateCampaign(campaignArn string) error {
	b.mu.RLock("ValidateCampaign")
	defer b.mu.RUnlock()

	if b.findCampaign(campaignArn) != nil {
		return nil
	}

	return fmt.Errorf("%w: campaign %q not found", ErrNotFound, campaignArn)
}
