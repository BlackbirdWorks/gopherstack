package personalize

import (
	"fmt"
	"maps"
)

// --- Tags ---

// TagResource adds tags to a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceArn)
	}
	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceArn], newTags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceArn)
	}
	for _, k := range keys {
		delete(b.tags[resourceArn], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceArn) {
		return nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceArn)
	}

	return copyStringMap(b.tags[resourceArn]), nil
}

func (b *InMemoryBackend) arnExists(resourceArn string) bool {
	return b.arnExistsInCoreEntities(resourceArn) ||
		b.arnExistsInTrackingEntities(resourceArn) ||
		b.arnExistsInJobs(resourceArn)
}

func (b *InMemoryBackend) arnExistsInCoreEntities(resourceArn string) bool {
	for _, dg := range b.datasetGroups.All() {
		if dg.DatasetGroupArn == resourceArn {
			return true
		}
	}
	for _, ds := range b.datasets.All() {
		if ds.DatasetArn == resourceArn {
			return true
		}
	}
	for _, s := range b.schemas.All() {
		if s.SchemaArn == resourceArn {
			return true
		}
	}
	for _, sol := range b.solutions.All() {
		if sol.SolutionArn == resourceArn {
			return true
		}
	}

	return b.solutionVersions.Has(resourceArn)
}

func (b *InMemoryBackend) arnExistsInTrackingEntities(resourceArn string) bool {
	for _, c := range b.campaigns.All() {
		if c.CampaignArn == resourceArn {
			return true
		}
	}
	for _, et := range b.eventTrackers.All() {
		if et.EventTrackerArn == resourceArn {
			return true
		}
	}
	for _, f := range b.filters.All() {
		if f.FilterArn == resourceArn {
			return true
		}
	}
	for _, r := range b.recommenders.All() {
		if r.RecommenderArn == resourceArn {
			return true
		}
	}
	for _, ma := range b.metricAttributions.All() {
		if ma.MetricAttributionArn == resourceArn {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) arnExistsInJobs(resourceArn string) bool {
	return b.datasetImportJobs.Has(resourceArn) ||
		b.datasetExportJobs.Has(resourceArn) ||
		b.batchInferenceJobs.Has(resourceArn) ||
		b.batchSegmentJobs.Has(resourceArn) ||
		b.dataDeletionJobs.Has(resourceArn)
}
