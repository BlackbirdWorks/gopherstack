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

	tagMap := b.findTagMapLocked(resourceARN, region)
	if tagMap == nil {
		return fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
	}

	*tagMap = mergeTags(*tagMap, tags)

	return nil
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

// tagLookup resolves a resource ARN to a pointer at its Tags field, or nil if
// the ARN belongs to no resource of the kind this lookup covers.
type tagLookup func(b *InMemoryBackend, region, resourceARN string) *map[string]string

// indexedTagLookup builds a tagLookup for a resource kind whose store.Table is
// keyed by name rather than ARN, resolved via a region-scoped ARN->name index.
func indexedTagLookup[V any](
	arnIndex func(b *InMemoryBackend, region string) map[string]string,
	tbl func(b *InMemoryBackend, region string) *store.Table[V],
	tagsOf func(*V) *map[string]string,
) tagLookup {
	return func(b *InMemoryBackend, region, resourceARN string) *map[string]string {
		name, ok := arnIndex(b, region)[resourceARN]
		if !ok {
			return nil
		}

		v := tableGet(tbl(b, region), name)
		if v == nil {
			return nil
		}

		return tagsOf(v)
	}
}

// directTagLookup builds a tagLookup for a resource kind whose store.Table is
// keyed by its own ARN, so no separate ARN index is needed.
func directTagLookup[V any](
	tbl func(b *InMemoryBackend, region string) *store.Table[V],
	tagsOf func(*V) *map[string]string,
) tagLookup {
	return func(b *InMemoryBackend, region, resourceARN string) *map[string]string {
		v := tableGet(tbl(b, region), resourceARN)
		if v == nil {
			return nil
		}

		return tagsOf(v)
	}
}

// scanTagLookup builds a tagLookup for a resource kind with no ARN index,
// resolved by a linear scan comparing each stored value's own ARN field.
func scanTagLookup[V any](
	tbl func(b *InMemoryBackend, region string) *store.Table[V],
	arnOf func(*V) string,
	tagsOf func(*V) *map[string]string,
) tagLookup {
	return func(b *InMemoryBackend, region, resourceARN string) *map[string]string {
		for _, v := range tbl(b, region).All() {
			if arnOf(v) == resourceARN {
				return tagsOf(v)
			}
		}

		return nil
	}
}

// indexedTagLookups covers resource kinds keyed by name in their store.Table
// and resolved to an ARN via a region-scoped ARN index.
func indexedTagLookups() []tagLookup {
	return []tagLookup{
		indexedTagLookup(
			(*InMemoryBackend).modelARNIndexStoreRO, (*InMemoryBackend).modelsStoreRO,
			func(v *Model) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).endpointConfigARNIndexStoreRO, (*InMemoryBackend).endpointConfigsStoreRO,
			func(v *EndpointConfig) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).actionARNIndexStoreRO, (*InMemoryBackend).actionsStoreRO,
			func(v *Action) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).algorithmARNIndexStoreRO, (*InMemoryBackend).algorithmsStoreRO,
			func(v *Algorithm) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).endpointARNIndexStoreRO, (*InMemoryBackend).endpointsStoreRO,
			func(v *Endpoint) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).trainingJobARNIndexStoreRO, (*InMemoryBackend).trainingJobsStoreRO,
			func(v *TrainingJob) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).notebookARNIndexStoreRO, (*InMemoryBackend).notebooksStoreRO,
			func(v *NotebookInstance) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).hpTuningJobARNIndexStoreRO, (*InMemoryBackend).hpTuningJobsStoreRO,
			func(v *HyperParameterTuningJob) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).processingJobARNIndexStoreRO, (*InMemoryBackend).processingJobsStoreRO,
			func(v *ProcessingJob) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).transformJobARNIndexStoreRO, (*InMemoryBackend).transformJobsStoreRO,
			func(v *TransformJob) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).clusterARNIndexStoreRO, (*InMemoryBackend).clustersStoreRO,
			func(v *Cluster) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).contextARNIndexStoreRO, (*InMemoryBackend).contextsStoreRO,
			func(v *Context) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).modelPackageGroupARNIndexStoreRO, (*InMemoryBackend).modelPackageGroupsStoreRO,
			func(v *ModelPackageGroup) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).workteamARNIndexStoreRO, (*InMemoryBackend).workteamsStoreRO,
			func(v *Workteam) *map[string]string { return &v.Tags },
		),
		indexedTagLookup(
			(*InMemoryBackend).workforceARNIndexStoreRO, (*InMemoryBackend).workforcesStoreRO,
			func(v *Workforce) *map[string]string { return &v.Tags },
		),
	}
}

// directTagLookups covers resource kinds whose store.Table is keyed by their
// own ARN already, so ListTags resolves them without a separate ARN index.
func directTagLookups() []tagLookup {
	return []tagLookup{
		directTagLookup(
			(*InMemoryBackend).modelPackagesStoreRO,
			func(v *ModelPackage) *map[string]string { return &v.Tags },
		),
		directTagLookup(
			(*InMemoryBackend).artifactsStoreRO,
			func(v *Artifact) *map[string]string { return &v.Tags },
		),
	}
}

// statefulTagLookups covers resource kinds with no ARN index, resolved by
// scanning each region's store.Table and comparing the stored ARN field.
func statefulTagLookups() []tagLookup {
	return []tagLookup{
		scanTagLookup(
			(*InMemoryBackend).domainsStoreRO,
			func(v *Domain) string { return v.DomainArn },
			func(v *Domain) *map[string]string { return &v.Tags },
		),
		scanTagLookup(
			(*InMemoryBackend).featureGroupsStoreRO,
			func(v *FeatureGroup) string { return v.FeatureGroupArn },
			func(v *FeatureGroup) *map[string]string { return &v.Tags },
		),
		scanTagLookup(
			(*InMemoryBackend).pipelinesStoreRO,
			func(v *Pipeline) string { return v.PipelineArn },
			func(v *Pipeline) *map[string]string { return &v.Tags },
		),
		scanTagLookup(
			(*InMemoryBackend).experimentsStoreRO,
			func(v *Experiment) string { return v.ExperimentArn },
			func(v *Experiment) *map[string]string { return &v.Tags },
		),
		scanTagLookup(
			(*InMemoryBackend).trialsStoreRO,
			func(v *Trial) string { return v.TrialArn },
			func(v *Trial) *map[string]string { return &v.Tags },
		),
		scanTagLookup(
			(*InMemoryBackend).trialComponentsStoreRO,
			func(v *TrialComponent) string { return v.TrialComponentArn },
			func(v *TrialComponent) *map[string]string { return &v.Tags },
		),
	}
}

// findTagMapLocked returns a pointer to the tags map for a resource identified by ARN.
// Must be called with b.mu held. Returns nil if the resource is not found.
//
// Adding a taggable kind means adding one entry to indexedTagLookups/
// directTagLookups/statefulTagLookups above -- never a new branch here, so
// this stays a flat loop regardless of registry size (see gopherstack-qyon).
func (b *InMemoryBackend) findTagMapLocked(resourceARN string, region string) *map[string]string {
	for _, lookups := range [][]tagLookup{indexedTagLookups(), directTagLookups(), statefulTagLookups()} {
		for _, lookup := range lookups {
			if tagMap := lookup(b, region, resourceARN); tagMap != nil {
				return tagMap
			}
		}
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
// extracts that kind's own ARN field and Tags field (every kind the tagLookups above
// reach stores both directly on the value already -- see e.g. Model.ModelARN/
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
// resource kind the tagLookups above support (models, endpoint configs, endpoints,
// training jobs, notebook instances, hyperparameter tuning jobs, actions, algorithms,
// clusters, contexts, model packages, model package groups, artifacts, processing
// jobs, transform jobs, domains, feature groups, pipelines, experiments, trials,
// trial components, work teams, and workforces), that currently has at least one tag.
// Resource kinds not reachable through AddTags/DeleteTags/ListTags (e.g. projects,
// monitoring schedules) are intentionally excluded: they are not taggable through
// this backend's own tagging API either.
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
	out = appendTaggedResources(out, b.contexts, func(v *Context) (string, map[string]string) {
		return v.ContextArn, v.Tags
	})
	out = appendTaggedResources(out, b.modelPackages, func(v *ModelPackage) (string, map[string]string) {
		return v.ModelPackageArn, v.Tags
	})
	out = appendTaggedResources(out, b.modelPackageGroups, func(v *ModelPackageGroup) (string, map[string]string) {
		return v.ModelPackageGroupArn, v.Tags
	})
	out = appendTaggedResources(out, b.artifacts, func(v *Artifact) (string, map[string]string) {
		return v.ArtifactArn, v.Tags
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
	out = appendTaggedResources(out, b.workteams, func(v *Workteam) (string, map[string]string) {
		return v.WorkteamArn, v.Tags
	})
	out = appendTaggedResources(out, b.workforces, func(v *Workforce) (string, map[string]string) {
		return v.WorkforceArn, v.Tags
	})

	return out
}
