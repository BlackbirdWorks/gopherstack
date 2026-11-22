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

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every dataset group, dataset, solution, solution version, campaign, job, event
// tracker, filter, recommender and metric attribution plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for key, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := b.resourceARNsPart1Locked()
	out = append(out, b.resourceARNsPart2Locked()...)

	return out
}

func (b *InMemoryBackend) resourceARNsPart1Locked() []string {
	out := make(
		[]string,
		0,
		b.datasetGroups.Len()+b.datasets.Len()+b.solutions.Len()+b.solutionVersions.Len()+b.campaigns.Len()+
			b.datasetImportJobs.Len()+b.datasetExportJobs.Len()+b.batchInferenceJobs.Len(),
	)

	for _, v := range b.datasetGroups.All() {
		out = append(out, v.DatasetGroupArn)
	}

	for _, v := range b.datasets.All() {
		out = append(out, v.DatasetArn)
	}

	for _, v := range b.solutions.All() {
		out = append(out, v.SolutionArn)
	}

	for _, v := range b.solutionVersions.All() {
		out = append(out, v.SolutionVersionArn)
	}

	for _, v := range b.campaigns.All() {
		out = append(out, v.CampaignArn)
	}

	for _, v := range b.datasetImportJobs.All() {
		out = append(out, v.DatasetImportJobArn)
	}

	for _, v := range b.datasetExportJobs.All() {
		out = append(out, v.DatasetExportJobArn)
	}

	for _, v := range b.batchInferenceJobs.All() {
		out = append(out, v.BatchInferenceJobArn)
	}

	return out
}

func (b *InMemoryBackend) resourceARNsPart2Locked() []string {
	out := make(
		[]string,
		0,
		b.batchSegmentJobs.Len()+b.dataDeletionJobs.Len()+b.eventTrackers.Len()+b.filters.Len()+
			b.recommenders.Len()+b.metricAttributions.Len(),
	)

	for _, v := range b.batchSegmentJobs.All() {
		out = append(out, v.BatchSegmentJobArn)
	}

	for _, v := range b.dataDeletionJobs.All() {
		out = append(out, v.DataDeletionJobArn)
	}

	for _, v := range b.eventTrackers.All() {
		out = append(out, v.EventTrackerArn)
	}

	for _, v := range b.filters.All() {
		out = append(out, v.FilterArn)
	}

	for _, v := range b.recommenders.All() {
		out = append(out, v.RecommenderArn)
	}

	for _, v := range b.metricAttributions.All() {
		out = append(out, v.MetricAttributionArn)
	}

	return out
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
