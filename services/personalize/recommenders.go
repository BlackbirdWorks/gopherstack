package personalize

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Recommender ---

// CreateRecommender creates a new recommender.
func (b *InMemoryBackend) CreateRecommender(
	name, datasetGroupArn, recipeArn string,
	minRPS int32,
	recommenderConfig *RecommenderConfig,
	tags map[string]string,
) (*Recommender, error) {
	b.mu.Lock("CreateRecommender")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.recommenders.Has(name) {
		return nil, fmt.Errorf("%w: recommender %q already exists", ErrAlreadyExists, name)
	}
	if b.findDatasetGroup(datasetGroupArn) == nil {
		return nil, fmt.Errorf("%w: dataset group %q not found", ErrNotFound, datasetGroupArn)
	}
	if !recipeExists(recipeArn) {
		return nil, fmt.Errorf("%w: recipe %q not found", ErrNotFound, recipeArn)
	}

	recommenderConfig = withMinRPS(recommenderConfig, minRPS)
	now := time.Now().UTC()
	r := &Recommender{
		RecommenderArn:                     b.personalizeARN("recommender", name),
		Name:                               name,
		DatasetGroupArn:                    datasetGroupArn,
		RecipeArn:                          recipeArn,
		Status:                             statusActive,
		MinRecommendationRequestsPerSecond: minRPS,
		RecommenderConfig:                  recommenderConfig,
		CreationDateTime:                   now,
		LastUpdatedDateTime:                now,
	}
	b.recommenders.Put(r)
	if len(tags) > 0 {
		b.tags[r.RecommenderArn] = copyStringMap(tags)
	}

	return r, nil
}

// DescribeRecommender returns a recommender by name or ARN.
func (b *InMemoryBackend) DescribeRecommender(nameOrArn string) (*Recommender, error) {
	b.mu.RLock("DescribeRecommender")
	defer b.mu.RUnlock()

	if r := b.findRecommender(nameOrArn); r != nil {
		return r, nil
	}

	return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, nameOrArn)
}

// UpdateRecommender updates a recommender's configuration. recommenderConfig
// is required on the real UpdateRecommenderInput; every successful call
// records a types.RecommenderUpdateSummary-shaped snapshot on
// LatestRecommenderUpdate, matching real AWS which only populates that field
// once the recommender has had at least one update.
func (b *InMemoryBackend) UpdateRecommender(
	nameOrArn string,
	minRPS int32,
	recommenderConfig *RecommenderConfig,
) (*Recommender, error) {
	b.mu.Lock("UpdateRecommender")
	defer b.mu.Unlock()

	// recommenderConfig is a required member on the real UpdateRecommenderInput
	// (unlike CreateRecommender, where it is optional).
	if recommenderConfig == nil {
		return nil, fmt.Errorf("%w: recommenderConfig is required", ErrValidation)
	}

	r := b.findRecommender(nameOrArn)
	if r == nil {
		return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, nameOrArn)
	}
	if minRPS > 0 {
		r.MinRecommendationRequestsPerSecond = minRPS
	}
	r.RecommenderConfig = withMinRPS(recommenderConfig, minRPS)
	r.LastUpdatedDateTime = time.Now().UTC()
	r.LatestRecommenderUpdate = map[string]any{
		keyCreationDateTime:    awstime.Epoch(r.LastUpdatedDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(r.LastUpdatedDateTime),
		"recommenderConfig":    r.RecommenderConfig,
		keyStatus:              r.Status,
	}

	return r, nil
}

// DeleteRecommender removes a recommender.
func (b *InMemoryBackend) DeleteRecommender(nameOrArn string) error {
	b.mu.Lock("DeleteRecommender")
	defer b.mu.Unlock()

	r := b.findRecommender(nameOrArn)
	if r == nil {
		return fmt.Errorf("%w: recommender %q not found", ErrNotFound, nameOrArn)
	}
	b.recommenders.Delete(r.Name)
	delete(b.tags, r.RecommenderArn)

	return nil
}

// ListRecommenders returns recommenders, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListRecommenders(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*Recommender, string) {
	b.mu.RLock("ListRecommenders")
	defer b.mu.RUnlock()

	all := b.recommenders.Snapshot()
	filtered := make([]*Recommender, 0, len(all))
	for _, r := range all {
		if datasetGroupArn == "" || r.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, r)
		}
	}

	return paginateItems(filtered, recommenderKeyFn, maxResults, nextToken)
}

// StartRecommender transitions a recommender to ACTIVE.
func (b *InMemoryBackend) StartRecommender(recommenderArn string) (*Recommender, error) {
	b.mu.Lock("StartRecommender")
	defer b.mu.Unlock()

	r := b.findRecommender(recommenderArn)
	if r == nil {
		return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, recommenderArn)
	}
	r.Status = statusActive
	r.LastUpdatedDateTime = time.Now().UTC()

	return r, nil
}

// StopRecommender transitions a recommender to INACTIVE.
func (b *InMemoryBackend) StopRecommender(recommenderArn string) (*Recommender, error) {
	b.mu.Lock("StopRecommender")
	defer b.mu.Unlock()

	r := b.findRecommender(recommenderArn)
	if r == nil {
		return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, recommenderArn)
	}
	r.Status = "INACTIVE"
	r.LastUpdatedDateTime = time.Now().UTC()

	return r, nil
}

func (b *InMemoryBackend) findRecommender(nameOrArn string) *Recommender {
	if r, ok := b.recommenders.Get(nameOrArn); ok {
		return r
	}
	for _, r := range b.recommenders.All() {
		if r.RecommenderArn == nameOrArn {
			return r
		}
	}

	return nil
}

// withMinRPS returns cfg with MinRecommendationRequestsPerSecond set to
// minRPS, allocating cfg if nil. Keeps the typed sub-object authoritative
// instead of the separate Recommender.MinRecommendationRequestsPerSecond
// bookkeeping field drifting out of sync with it.
func withMinRPS(cfg *RecommenderConfig, minRPS int32) *RecommenderConfig {
	if minRPS <= 0 {
		return cfg
	}
	if cfg == nil {
		cfg = &RecommenderConfig{}
	}
	cfg.MinRecommendationRequestsPerSecond = &minRPS

	return cfg
}
