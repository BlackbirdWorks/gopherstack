package ce

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ListCostAllocationTags returns cost allocation tags, optionally filtered.
func (b *InMemoryBackend) ListCostAllocationTags(
	status, tagType string,
	tagKeys []string,
) []*CostAllocationTag {
	b.mu.RLock("ListCostAllocationTags")
	defer b.mu.RUnlock()

	keySet := make(map[string]struct{}, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	var result []*CostAllocationTag

	for _, tag := range b.costAllocationTags.All() {
		if status != "" && tag.Status != status {
			continue
		}

		if tagType != "" && tag.Type != tagType {
			continue
		}

		if len(keySet) > 0 {
			if _, ok := keySet[tag.TagKey]; !ok {
				continue
			}
		}

		cp := *tag
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TagKey < result[j].TagKey
	})

	return result
}

// UpdateCostAllocationTagsStatus updates the Active/Inactive status of cost allocation tags.
// Returns a list of errors for tags that could not be updated.
func (b *InMemoryBackend) UpdateCostAllocationTagsStatus(
	updates []CostAllocationTagStatusEntry,
) []CostAllocationTagError {
	b.mu.Lock("UpdateCostAllocationTagsStatus")
	defer b.mu.Unlock()

	var errs []CostAllocationTagError

	for _, u := range updates {
		if u.Status != "Active" && u.Status != "Inactive" {
			errs = append(errs, CostAllocationTagError{
				TagKey:  u.TagKey,
				Code:    "InvalidParameterException",
				Message: fmt.Sprintf("Status must be Active or Inactive, got %q", u.Status),
			})

			continue
		}

		if tag, ok := b.costAllocationTags.Get(u.TagKey); ok {
			tag.Status = u.Status
			tag.LastUpdatedDate = time.Now().UTC().Format(time.RFC3339)
		} else {
			b.costAllocationTags.Put(&CostAllocationTag{
				TagKey:          u.TagKey,
				Status:          u.Status,
				Type:            "UserDefined",
				LastUpdatedDate: time.Now().UTC().Format(time.RFC3339),
			})
		}
	}

	if errs == nil {
		errs = []CostAllocationTagError{}
	}

	return errs
}

// CreateBackfillJob creates a new cost allocation tag backfill job.
func (b *InMemoryBackend) CreateBackfillJob(backfillFrom string) *BackfillJob {
	b.mu.Lock("CreateBackfillJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	job := &BackfillJob{
		BackfillID:     uuid.NewString(),
		BackfillFrom:   backfillFrom,
		RequestedAt:    now.Format(time.RFC3339),
		BackfillStatus: statusProcessing,
		LastUpdatedAt:  now.Format(time.RFC3339),
	}

	b.backfillJobs = append(b.backfillJobs, job)

	return job
}

// ListBackfillHistory returns backfill jobs sorted by RequestedAt descending.
// b.backfillJobs is an append-only slice (not a Table.All() map walk), so its
// insertion order is already stable across calls; the BackfillID tiebreak
// below only matters for RequestedAt's second-precision ties, making the
// full order deterministic for pagination cursoring.
func (b *InMemoryBackend) ListBackfillHistory() []*BackfillJob {
	b.mu.RLock("ListBackfillHistory")
	defer b.mu.RUnlock()

	result := make([]*BackfillJob, len(b.backfillJobs))
	for i, j := range b.backfillJobs {
		cp := *j
		result[i] = &cp
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].RequestedAt != result[j].RequestedAt {
			return result[i].RequestedAt > result[j].RequestedAt
		}

		return result[i].BackfillID < result[j].BackfillID
	})

	return result
}
