package personalize

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Recommender ---

func (h *Handler) createRecommender(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	recipeArn, _ := input["recipeArn"].(string)
	recommenderConfig, _ := input["recommenderConfig"].(map[string]any)
	minRPS := int32Field(recommenderConfig, "minRecommendationRequestsPerSecond")
	tags := extractTags(input)

	r, err := h.Backend.CreateRecommender(name, datasetGroupArn, recipeArn, minRPS, recommenderConfig, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

func (h *Handler) describeRecommender(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["recommenderArn"].(string)

	r, err := h.Backend.DescribeRecommender(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"recommender": recommenderToMap(r)}, nil
}

func (h *Handler) updateRecommender(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["recommenderArn"].(string)
	recommenderConfig, _ := input["recommenderConfig"].(map[string]any)
	minRPS := int32Field(recommenderConfig, "minRecommendationRequestsPerSecond")

	r, err := h.Backend.UpdateRecommender(nameOrArn, minRPS, recommenderConfig)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

func (h *Handler) deleteRecommender(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["recommenderArn"].(string)

	return map[string]any{}, h.Backend.DeleteRecommender(nameOrArn)
}

func (h *Handler) listRecommenders(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListRecommenders(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, r := range list {
		summaries = append(summaries, recommenderToMap(r))
	}

	result := map[string]any{"recommenders": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func (h *Handler) startRecommender(input map[string]any) (map[string]any, error) {
	recommenderArn, _ := input["recommenderArn"].(string)

	r, err := h.Backend.StartRecommender(recommenderArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

func (h *Handler) stopRecommender(input map[string]any) (map[string]any, error) {
	recommenderArn, _ := input["recommenderArn"].(string)

	r, err := h.Backend.StopRecommender(recommenderArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyRecommenderArn: r.RecommenderArn}, nil
}

func recommenderToMap(r *Recommender) map[string]any {
	// recommenderConfig always includes minRecommendationRequestsPerSecond
	// (the one field this backend also tracks as its own struct field, for
	// callers that only ever set that one sub-field) merged with whatever
	// other sub-fields the caller originally supplied.
	cfg := map[string]any{
		"minRecommendationRequestsPerSecond": r.MinRecommendationRequestsPerSecond,
	}
	maps.Copy(cfg, r.RecommenderConfig)

	m := map[string]any{
		keyRecommenderArn:      r.RecommenderArn,
		keyName:                r.Name,
		keyDatasetGroupArn:     r.DatasetGroupArn,
		keyRecipeArn:           r.RecipeArn,
		keyStatus:              r.Status,
		"recommenderConfig":    cfg,
		keyCreationDateTime:    awstime.Epoch(r.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(r.LastUpdatedDateTime),
	}
	if r.LatestRecommenderUpdate != nil {
		m["latestRecommenderUpdate"] = r.LatestRecommenderUpdate
	}

	return m
}
