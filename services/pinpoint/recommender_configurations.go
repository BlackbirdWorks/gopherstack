package pinpoint

import (
	"sort"

	"github.com/google/uuid"
)

// CreateRecommenderConfiguration creates a new Pinpoint recommender configuration.
func (b *InMemoryBackend) CreateRecommenderConfiguration(
	req createRecommenderConfigRequest,
) (*RecommenderConfiguration, error) {
	b.mu.Lock("CreateRecommenderConfiguration")
	defer b.mu.Unlock()

	if !isValidRecommenderIDType(req.RecommendationProviderIDType) {
		return nil, ErrValidation
	}

	id := uuid.NewString()
	now := nowRFC3339()

	r := &RecommenderConfiguration{
		Attributes:                    nonNilAttrsCopy(req.Attributes),
		ID:                            id,
		Name:                          req.Name,
		Description:                   req.Description,
		RecommendationProviderIDType:  req.RecommendationProviderIDType,
		RecommendationProviderRoleARN: req.RecommendationProviderRoleArn,
		RecommendationProviderURI:     req.RecommendationProviderURI,
		RecommendationsPerMessage:     req.RecommendationsPerMessage,
		CreationDate:                  now,
		LastModifiedDate:              now,
	}

	b.recommenders.Put(r)

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}

// GetRecommenderConfiguration retrieves a recommender by ID.
func (b *InMemoryBackend) GetRecommenderConfiguration(
	recommenderID string,
) (*RecommenderConfiguration, error) {
	b.mu.RLock("GetRecommenderConfiguration")
	defer b.mu.RUnlock()

	r, ok := b.recommenders.Get(recommenderID)
	if !ok {
		return nil, ErrAppNotFound
	}

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}

// GetRecommenderConfigurations returns all recommender configurations.
func (b *InMemoryBackend) GetRecommenderConfigurations() ([]*RecommenderConfiguration, error) {
	b.mu.RLock("GetRecommenderConfigurations")
	defer b.mu.RUnlock()

	results := make([]*RecommenderConfiguration, 0, b.recommenders.Len())

	for _, r := range b.recommenders.All() {
		cp := *r
		cp.Attributes = nonNilAttrsCopy(r.Attributes)
		results = append(results, &cp)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	return results, nil
}

// isValidRecommenderIDType returns true if the given type is a valid RecommendationProviderIdType.
func isValidRecommenderIDType(t string) bool {
	return t == "" || t == "PINPOINT_ENDPOINT_ID" || t == "PINPOINT_USER_ID"
}

// applyRecommenderScalars applies scalar fields from req to r, returning whether any changed.
func applyRecommenderScalars(r *RecommenderConfiguration, req createRecommenderConfigRequest) bool {
	changed := false

	if req.Name != "" && req.Name != r.Name {
		r.Name = req.Name
		changed = true
	}

	if req.Description != "" && req.Description != r.Description {
		r.Description = req.Description
		changed = true
	}

	if req.RecommendationProviderIDType != "" &&
		req.RecommendationProviderIDType != r.RecommendationProviderIDType {
		r.RecommendationProviderIDType = req.RecommendationProviderIDType
		changed = true
	}

	if req.RecommendationProviderRoleArn != "" &&
		req.RecommendationProviderRoleArn != r.RecommendationProviderRoleARN {
		r.RecommendationProviderRoleARN = req.RecommendationProviderRoleArn
		changed = true
	}

	if req.RecommendationProviderURI != "" &&
		req.RecommendationProviderURI != r.RecommendationProviderURI {
		r.RecommendationProviderURI = req.RecommendationProviderURI
		changed = true
	}

	if req.RecommendationsPerMessage != 0 &&
		req.RecommendationsPerMessage != r.RecommendationsPerMessage {
		r.RecommendationsPerMessage = req.RecommendationsPerMessage
		changed = true
	}

	return changed
}

// UpdateRecommenderConfiguration updates an existing recommender.
func (b *InMemoryBackend) UpdateRecommenderConfiguration(
	recommenderID string,
	req createRecommenderConfigRequest,
) (*RecommenderConfiguration, error) {
	b.mu.Lock("UpdateRecommenderConfiguration")
	defer b.mu.Unlock()

	r, ok := b.recommenders.Get(recommenderID)
	if !ok {
		return nil, ErrAppNotFound
	}

	if !isValidRecommenderIDType(req.RecommendationProviderIDType) {
		return nil, ErrValidation
	}

	changed := applyRecommenderScalars(r, req)

	if len(req.Attributes) > 0 {
		newAttrs := nonNilAttrsCopy(req.Attributes)

		for k, v := range newAttrs {
			if r.Attributes[k] != v {
				changed = true

				break
			}
		}

		r.Attributes = newAttrs
	}

	if changed {
		r.LastModifiedDate = nowRFC3339()
	}

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}

// DeleteRecommenderConfiguration deletes a recommender by ID.
func (b *InMemoryBackend) DeleteRecommenderConfiguration(
	recommenderID string,
) (*RecommenderConfiguration, error) {
	b.mu.Lock("DeleteRecommenderConfiguration")
	defer b.mu.Unlock()

	r, ok := b.recommenders.Get(recommenderID)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.recommenders.Delete(recommenderID)

	cp := *r
	cp.Attributes = nonNilAttrsCopy(r.Attributes)

	return &cp, nil
}
