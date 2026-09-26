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

// ValidateActionRecommenderCampaign returns nil if campaignArn resolves to an
// existing campaign whose deployed solution version was trained with a
// PERSONALIZED_ACTIONS recipe (the recipe family GetActionRecommendations
// requires). Returns ErrNotFound if the campaign doesn't exist, or
// ErrValidation (InvalidInputException on the wire) if it exists but was
// trained with a different recipe -- getBuiltinRecipes has no
// PERSONALIZED_ACTIONS entry, so every real campaign in this backend hits
// the latter case today; this checks the actual recipe type dynamically
// rather than hardcoding "always rejects", so it stays correct if a
// PERSONALIZED_ACTIONS recipe is ever added to the catalog.
func (b *InMemoryBackend) ValidateActionRecommenderCampaign(campaignArn string) error {
	b.mu.RLock("ValidateActionRecommenderCampaign")
	defer b.mu.RUnlock()

	c := b.findCampaign(campaignArn)
	if c == nil {
		return fmt.Errorf("%w: campaign %q not found", ErrNotFound, campaignArn)
	}

	sv, ok := b.solutionVersions.Get(c.SolutionVersionArn)
	if !ok || recipeTypeForArn(sv.RecipeArn) != recipeTypePersonalizedActions {
		return fmt.Errorf(
			"%w: campaign %q was not trained with a %s recipe",
			ErrValidation,
			campaignArn,
			recipeTypePersonalizedActions,
		)
	}

	return nil
}
