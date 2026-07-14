package sagemaker

import (
	"context"
	"fmt"
	"maps"
)

// AddTags adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) AddTags(ctx context.Context, resourceARN string, tags map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name, ok := b.modelARNIndexStore(region)[resourceARN]; ok {
		m := tableGet(b.modelsStore(region), name)
		m.Tags = mergeTags(m.Tags, tags)

		return nil
	}

	if name, ok := b.endpointConfigARNIndexStore(region)[resourceARN]; ok {
		ec := tableGet(b.endpointConfigsStore(region), name)
		ec.Tags = mergeTags(ec.Tags, tags)

		return nil
	}

	if name, ok := b.actionARNIndexStore(region)[resourceARN]; ok {
		a := tableGet(b.actionsStore(region), name)
		a.Tags = mergeTags(a.Tags, tags)

		return nil
	}

	if name, ok := b.algorithmARNIndexStore(region)[resourceARN]; ok {
		al := tableGet(b.algorithmsStore(region), name)
		al.Tags = mergeTags(al.Tags, tags)

		return nil
	}

	if _, ok := b.modelPackageARNIndexStore(region)[resourceARN]; ok {
		mp := tableGet(b.modelPackagesStore(region), resourceARN)
		mp.Tags = mergeTags(mp.Tags, tags)

		return nil
	}

	if name, ok := b.endpointARNIndexStore(region)[resourceARN]; ok {
		ep := tableGet(b.endpointsStore(region), name)
		ep.Tags = mergeTags(ep.Tags, tags)

		return nil
	}

	if name, ok := b.trainingJobARNIndexStore(region)[resourceARN]; ok {
		tj := tableGet(b.trainingJobsStore(region), name)
		tj.Tags = mergeTags(tj.Tags, tags)

		return nil
	}

	if name, ok := b.notebookARNIndexStore(region)[resourceARN]; ok {
		nb := tableGet(b.notebooksStore(region), name)
		nb.Tags = mergeTags(nb.Tags, tags)

		return nil
	}

	if name, ok := b.hpTuningJobARNIndexStore(region)[resourceARN]; ok {
		j := tableGet(b.hpTuningJobsStore(region), name)
		j.Tags = mergeTags(j.Tags, tags)

		return nil
	}

	if name, ok := b.processingJobARNIndexStore(region)[resourceARN]; ok {
		if pj, found := b.processingJobsStore(region).Get(name); found {
			pj.Tags = mergeTags(pj.Tags, tags)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
}

// ListTags returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTags(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tagMap := b.findTagMapLocked(resourceARN, region)
	if tagMap == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
	}

	result := make(map[string]string, len(*tagMap))
	maps.Copy(result, *tagMap)

	return result, nil
}

// findTagMapLocked returns a pointer to the tags map for a resource identified by ARN.
// Must be called with b.mu held. Returns nil if the resource is not found.
func (b *InMemoryBackend) findTagMapLocked(resourceARN string, region string) *map[string]string {
	if name, ok := b.modelARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.modelsStoreRO(region), name).Tags
	}

	if name, ok := b.endpointConfigARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.endpointConfigsStoreRO(region), name).Tags
	}

	if name, ok := b.actionARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.actionsStoreRO(region), name).Tags
	}

	if name, ok := b.algorithmARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.algorithmsStoreRO(region), name).Tags
	}

	if _, ok := b.modelPackageARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.modelPackagesStoreRO(region), resourceARN).Tags
	}

	if name, ok := b.endpointARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.endpointsStoreRO(region), name).Tags
	}

	if name, ok := b.trainingJobARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.trainingJobsStoreRO(region), name).Tags
	}

	if name, ok := b.notebookARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.notebooksStoreRO(region), name).Tags
	}

	if name, ok := b.hpTuningJobARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.hpTuningJobsStoreRO(region), name).Tags
	}

	if tags := b.findTagMapIndexedExtraLocked(resourceARN, region); tags != nil {
		return tags
	}

	return b.findTagMapStatefulLocked(resourceARN, region)
}

// findTagMapIndexedExtraLocked handles the remaining ARN-indexed resource
// kinds (processing jobs, transform jobs, clusters). Separated from
// findTagMapLocked to keep it within cyclomatic-complexity limits.
func (b *InMemoryBackend) findTagMapIndexedExtraLocked(resourceARN string, region string) *map[string]string {
	if name, ok := b.processingJobARNIndexStoreRO(region)[resourceARN]; ok {
		if pj, found := b.processingJobsStoreRO(region).Get(name); found {
			return &pj.Tags
		}
	}

	if name, ok := b.transformJobARNIndexStoreRO(region)[resourceARN]; ok {
		if tj, found := b.transformJobsStoreRO(region).Get(name); found {
			return &tj.Tags
		}
	}

	if name, ok := b.clusterARNIndexStoreRO(region)[resourceARN]; ok {
		if c, found := b.clustersStoreRO(region).Get(name); found {
			return &c.Tags
		}
	}

	return nil
}

// findTagMapStatefulLocked handles tag lookups for stateful resources (domains,
// featureGroups, pipelines, experiments, trials, trialComponents). Separated
// to keep findTagMapLocked within cognitive-complexity limits.
func (b *InMemoryBackend) findTagMapStatefulLocked(resourceARN string, region string) *map[string]string {
	for _, d := range b.domainsStoreRO(region).All() {
		if d.DomainArn == resourceARN {
			return &d.Tags
		}
	}
	for _, fg := range b.featureGroupsStoreRO(region).All() {
		if fg.FeatureGroupArn == resourceARN {
			return &fg.Tags
		}
	}
	for _, p := range b.pipelinesStoreRO(region).All() {
		if p.PipelineArn == resourceARN {
			return &p.Tags
		}
	}
	for _, e := range b.experimentsStoreRO(region).All() {
		if e.ExperimentArn == resourceARN {
			return &e.Tags
		}
	}
	for _, t := range b.trialsStoreRO(region).All() {
		if t.TrialArn == resourceARN {
			return &t.Tags
		}
	}
	for _, tc := range b.trialComponentsStoreRO(region).All() {
		if tc.TrialComponentArn == resourceARN {
			return &tc.Tags
		}
	}

	return nil
}

// DeleteTags removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) DeleteTags(ctx context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tags := b.findTagMapLocked(resourceARN, region)
	if tags == nil {
		return fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
	}

	for _, k := range tagKeys {
		delete(*tags, k)
	}

	return nil
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}
