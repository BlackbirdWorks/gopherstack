package transcribe

import (
	"fmt"
	"maps"
)

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[resourceArn]; !ok {
		b.resourceTags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[resourceArn], tags)

	return nil
}

// recordResourceTagsLocked attaches tags supplied at resource-creation time (Start*Job
// and Create* Tags parameters) to the resource's ARN, so they are retrievable via
// ListTagsForResource exactly like tags added later via TagResource. Real AWS treats
// creation-time Tags as immediately-attached resource tags. Callers must hold b.mu.
func (b *InMemoryBackend) recordResourceTagsLocked(resourceArn string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	dst := make(map[string]string, len(tags))
	maps.Copy(dst, tags)
	b.resourceTags[resourceArn] = dst
}

// forgetResourceTagsLocked removes any tags recorded for resourceArn. Called on resource
// deletion so ListTagsForResource doesn't keep returning tags for a resource that no
// longer exists. Callers must hold b.mu.
func (b *InMemoryBackend) forgetResourceTagsLocked(resourceArn string) {
	delete(b.resourceTags, resourceArn)
}

// UntagResource removes specific tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.resourceTags[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.resourceTags[resourceArn]
	if !ok {
		return map[string]string{}, nil
	}

	// Return a copy so callers can't mutate the stored map.
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// liveTagsLocked returns a copy of the live tags for resourceArn from the
// shared resourceTags store, rather than a resource's own creation-time Tags
// snapshot -- Get ops that echo tags back must read this live map so tags
// applied via TagResource/UntagResource after creation are visible. Caller
// must hold at least a read lock.
func (b *InMemoryBackend) liveTagsLocked(resourceArn string) map[string]string {
	existing, ok := b.resourceTags[resourceArn]
	if !ok || len(existing) == 0 {
		return nil
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result
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

// InventoryResources lists every job, vocabulary, vocabulary filter, language model and call analytics
// category plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	seen := make(map[string]struct{}, len(b.resourceTags))

	for key, t := range b.resourceTags {
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
	out := make(
		[]string,
		0,
		b.jobs.Len()+b.medicalTranscriptionJobs.Len()+b.callAnalyticsJobs.Len()+b.medicalScribeJobs.Len()+
			b.vocabularies.Len()+b.medicalVocabularies.Len()+b.vocabularyFilters.Len()+b.languageModels.Len()+
			b.callAnalyticsCategories.Len(),
	)

	for _, j := range b.jobs.All() {
		out = append(out, b.resourceARN(resourceTypeTranscriptionJob, j.JobName))
	}

	for _, j := range b.medicalTranscriptionJobs.All() {
		out = append(out, b.resourceARN(resourceTypeMedicalTranscriptionJob, j.MedicalTranscriptionJobName))
	}

	for _, j := range b.callAnalyticsJobs.All() {
		out = append(out, b.resourceARN(resourceTypeCallAnalyticsJob, j.CallAnalyticsJobName))
	}

	for _, j := range b.medicalScribeJobs.All() {
		out = append(out, b.resourceARN(resourceTypeMedicalScribeJob, j.MedicalScribeJobName))
	}

	for _, v := range b.vocabularies.All() {
		out = append(out, b.resourceARN(resourceTypeVocabulary, v.VocabularyName))
	}

	for _, v := range b.medicalVocabularies.All() {
		out = append(out, b.resourceARN(resourceTypeMedicalVocabulary, v.VocabularyName))
	}

	for _, f := range b.vocabularyFilters.All() {
		out = append(out, b.resourceARN(resourceTypeVocabularyFilter, f.VocabularyFilterName))
	}

	for _, m := range b.languageModels.All() {
		out = append(out, b.resourceARN(resourceTypeLanguageModel, m.ModelName))
	}

	for _, c := range b.callAnalyticsCategories.All() {
		out = append(out, b.resourceARN(resourceTypeCallAnalyticsCategory, c.CategoryName))
	}

	return out
}
