package accessanalyzer

import (
	"sort"
	"time"
)

// StartResourceScan re-analyzes a known analyzed resource and records it as the
// analyzer's most recently analyzed resource; an unknown resource is not scanned.
func (b *InMemoryBackend) StartResourceScan(analyzerARN, resourceARN string) error {
	b.mu.Lock("StartResourceScan")
	defer b.mu.Unlock()

	for _, a := range b.analyzers.All() {
		if a.Arn != analyzerARN {
			continue
		}

		ar, ok := b.analyzedResources.Get(analyzedResourceKey(analyzerARN, resourceARN))
		if !ok {
			return nil
		}

		now := time.Now().UTC()
		ar.AnalyzedAt = now
		ar.UpdatedAt = now
		a.LastResourceAnalyzed = resourceARN
		a.LastResourceAnalyzedAt = &now

		return nil
	}

	return ErrAnalyzerNotFound
}

// GetAnalyzedResource returns an analyzed resource for an analyzer by resource ARN.
func (b *InMemoryBackend) GetAnalyzedResource(analyzerArn, resourceArn string) (*AnalyzedResource, error) {
	b.mu.RLock("GetAnalyzedResource")
	defer b.mu.RUnlock()

	ar, ok := b.analyzedResources.Get(analyzedResourceKey(analyzerArn, resourceArn))
	if !ok {
		return nil, ErrAnalyzedResourceNotFound
	}

	cp := *ar

	return &cp, nil
}

// ListAnalyzedResources returns analyzed resources for an analyzer, optionally filtered by type.
func (b *InMemoryBackend) ListAnalyzedResources(
	analyzerArn, resourceType string,
	maxResults int,
	nextToken string,
) ([]*AnalyzedResource, string, error) {
	b.mu.RLock("ListAnalyzedResources")
	defer b.mu.RUnlock()

	result := make([]*AnalyzedResource, 0)

	for _, ar := range b.analyzedResources.All() {
		if ar.AnalyzerArn != analyzerArn {
			continue
		}

		if resourceType != "" && ar.ResourceType != resourceType {
			continue
		}

		cp := *ar
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ResourceArn < result[j].ResourceArn
	})

	start := 0

	if nextToken != "" {
		for i, ar := range result {
			if ar.ResourceArn == nextToken {
				start = i

				break
			}
		}
	}

	result = result[start:]

	if maxResults > 0 && len(result) > maxResults {
		return result[:maxResults], result[maxResults].ResourceArn, nil
	}

	return result, "", nil
}

// AddAnalyzedResource adds a synthetic analyzed resource (for testing).
func (b *InMemoryBackend) AddAnalyzedResource(
	analyzerArn, resourceArn, resourceType string,
	isPublic bool,
) (*AnalyzedResource, error) {
	b.mu.Lock("AddAnalyzedResource")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	ar := &AnalyzedResource{
		ResourceArn:  resourceArn,
		ResourceType: resourceType,
		AnalyzerArn:  analyzerArn,
		IsPublic:     isPublic,
		CreatedAt:    now,
		AnalyzedAt:   now,
		UpdatedAt:    now,
	}

	b.analyzedResources.Put(ar)

	cp := *ar

	return &cp, nil
}
