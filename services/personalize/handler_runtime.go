package personalize

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// --- Personalize Runtime ---

const (
	defaultNumRecommendations = 25
	keyRecommendationID       = "recommendationId"
)

func (h *Handler) getRecommendations(input map[string]any) (map[string]any, error) {
	campaignArn, _ := input["campaignArn"].(string)
	recommenderArn, _ := input["recommenderArn"].(string)
	userID, _ := input["userId"].(string)
	numResults := intField(input, "numResults")
	if numResults <= 0 {
		numResults = defaultNumRecommendations
	}

	if err := h.Backend.ValidateCampaignOrRecommender(campaignArn, recommenderArn); err != nil {
		return nil, err
	}

	seed := campaignArn + recommenderArn + "|" + userID
	items := syntheticItemList(seed, numResults)

	return map[string]any{
		keyRecommendationID: uuid.NewString(),
		"itemList":          items,
	}, nil
}

// getActionRecommendations serves GetActionRecommendations. It genuinely
// validates the campaign (ErrNotFound for an unknown ARN) and its recipe
// type, but -- unlike getRecommendations/getPersonalizedRanking above --
// deliberately does NOT fabricate scored action items: this backend has no
// PERSONALIZED_ACTIONS recipe in its built-in catalog (recipes.go) and no
// actions-dataset item store to source real action IDs from, so every
// campaign fails ValidateActionRecommenderCampaign's recipe-type check with
// the real SDK-declared InvalidInputException, matching what real AWS
// returns for a campaign trained with any other recipe. See PARITY.md.
func (h *Handler) getActionRecommendations(input map[string]any) (map[string]any, error) {
	campaignArn, _ := input[keyCampaignArn].(string)

	if campaignArn == "" {
		return nil, fmt.Errorf("%w: campaignArn is required", ErrValidation)
	}

	if err := h.Backend.ValidateActionRecommenderCampaign(campaignArn); err != nil {
		return nil, err
	}

	return map[string]any{
		keyRecommendationID: uuid.NewString(),
		"actionList":        []map[string]any{},
	}, nil
}

func (h *Handler) getPersonalizedRanking(input map[string]any) (map[string]any, error) {
	campaignArn, _ := input["campaignArn"].(string)
	userID, _ := input["userId"].(string)

	if err := h.Backend.ValidateCampaign(campaignArn); err != nil {
		return nil, err
	}

	rawList, _ := input["inputList"].([]any)
	inputIDs := make([]string, 0, len(rawList))
	for _, v := range rawList {
		if s, ok := v.(string); ok {
			inputIDs = append(inputIDs, s)
		}
	}

	seed := campaignArn + "|" + userID
	ranked := make([]map[string]any, 0, len(inputIDs))
	for i, itemID := range inputIDs {
		score := deterministicScore(seed+itemID, len(inputIDs)-i)
		ranked = append(ranked, map[string]any{
			"itemId": itemID,
			"score":  score,
		})
	}

	return map[string]any{
		keyRecommendationID:   uuid.NewString(),
		"personalizedRanking": ranked,
	}, nil
}

func syntheticItemList(seed string, n int) []map[string]any {
	items := make([]map[string]any, n)
	for i := range items {
		itemID := fmt.Sprintf("item-%d", i+1)
		items[i] = map[string]any{
			"itemId": itemID,
			"score":  deterministicScore(seed+itemID, n-i),
		}
	}

	return items
}

func deterministicScore(seed string, rank int) float64 {
	const (
		scoreBuckets = 1000  // hash buckets mapped into a [0,1) base score
		rankDecay    = 100.0 // divisor controlling per-rank score decay
	)
	h := httputils.FNV32a(seed)
	base := float64(h%scoreBuckets) / float64(scoreBuckets)
	decay := float64(rank) / rankDecay
	score := base - decay
	if score < 0 {
		score = 0
	}

	return score
}
