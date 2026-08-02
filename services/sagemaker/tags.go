package sagemaker

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingSageMaker).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// appendTaggedResources walks every region's store.Table for one SageMaker resource
// kind, appending a TaggedEntry for every value that carries at least one tag. field
// extracts that kind's own ARN field and Tags field (every kind AddTags/findTagMapLocked
// above reaches stores both directly on the value already -- see e.g. Model.ModelARN/
// Model.Tags -- so no ARN-index lookup is needed here, unlike the by-ARN lookup those
// functions perform).
func appendTaggedResources[V any](
	out []TaggedEntry,
	byRegion map[string]*store.Table[V],
	field func(*V) (resourceARN string, resourceTags map[string]string),
) []TaggedEntry {
	for _, table := range byRegion {
		for _, v := range table.All() {
			resourceARN, tagMap := field(v)
			if len(tagMap) == 0 {
				continue
			}

			cp := make(map[string]string, len(tagMap))
			maps.Copy(cp, tagMap)
			out = append(out, TaggedEntry{ARN: resourceARN, Tags: cp})
		}
	}

	return out
}

// TaggedResources returns every SageMaker resource ARN, across every region and every
// resource kind AddTags/DeleteTags/ListTags support (models, endpoint configs,
// endpoints, training jobs, notebook instances, hyperparameter tuning jobs, actions,
// algorithms, clusters, model packages, processing jobs, transform jobs, domains,
// feature groups, pipelines, experiments, trials, and trial components -- see
// findTagMapLocked/findTagMapIndexedExtraLocked/findTagMapStatefulLocked above for the
// authoritative list this mirrors), that currently has at least one tag. Resource kinds
// not reachable through AddTags/DeleteTags/ListTags (e.g. artifacts, contexts,
// projects, monitoring schedules) are intentionally excluded: they are not taggable
// through this backend's own tagging API either.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	out = appendTaggedResources(out, b.models, func(v *Model) (string, map[string]string) {
		return v.ModelARN, v.Tags
	})
	out = appendTaggedResources(out, b.endpointConfigs, func(v *EndpointConfig) (string, map[string]string) {
		return v.EndpointConfigARN, v.Tags
	})
	out = appendTaggedResources(out, b.endpoints, func(v *Endpoint) (string, map[string]string) {
		return v.EndpointArn, v.Tags
	})
	out = appendTaggedResources(out, b.trainingJobs, func(v *TrainingJob) (string, map[string]string) {
		return v.TrainingJobArn, v.Tags
	})
	out = appendTaggedResources(out, b.notebooks, func(v *NotebookInstance) (string, map[string]string) {
		return v.NotebookInstanceArn, v.Tags
	})
	out = appendTaggedResources(out, b.hpTuningJobs, func(v *HyperParameterTuningJob) (string, map[string]string) {
		return v.HyperParameterTuningJobArn, v.Tags
	})
	out = appendTaggedResources(out, b.actions, func(v *Action) (string, map[string]string) {
		return v.ActionArn, v.Tags
	})
	out = appendTaggedResources(out, b.algorithms, func(v *Algorithm) (string, map[string]string) {
		return v.AlgorithmArn, v.Tags
	})
	out = appendTaggedResources(out, b.clusters, func(v *Cluster) (string, map[string]string) {
		return v.ClusterArn, v.Tags
	})
	out = appendTaggedResources(out, b.modelPackages, func(v *ModelPackage) (string, map[string]string) {
		return v.ModelPackageArn, v.Tags
	})
	out = appendTaggedResources(out, b.processingJobs, func(v *ProcessingJob) (string, map[string]string) {
		return v.ProcessingJobArn, v.Tags
	})
	out = appendTaggedResources(out, b.transformJobs, func(v *TransformJob) (string, map[string]string) {
		return v.TransformJobArn, v.Tags
	})
	out = appendTaggedResources(out, b.domains, func(v *Domain) (string, map[string]string) {
		return v.DomainArn, v.Tags
	})
	out = appendTaggedResources(out, b.featureGroups, func(v *FeatureGroup) (string, map[string]string) {
		return v.FeatureGroupArn, v.Tags
	})
	out = appendTaggedResources(out, b.pipelines, func(v *Pipeline) (string, map[string]string) {
		return v.PipelineArn, v.Tags
	})
	out = appendTaggedResources(out, b.experiments, func(v *Experiment) (string, map[string]string) {
		return v.ExperimentArn, v.Tags
	})
	out = appendTaggedResources(out, b.trials, func(v *Trial) (string, map[string]string) {
		return v.TrialArn, v.Tags
	})
	out = appendTaggedResources(out, b.trialComponents, func(v *TrialComponent) (string, map[string]string) {
		return v.TrialComponentArn, v.Tags
	})

	return out
}
