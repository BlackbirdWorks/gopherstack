package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Resource-type / property-name string constants shared by Search,
// GetSearchSuggestions and ListModelMetadata below. Defining these avoids
// scattering repeated literals (goconst) across the family's catalogs.
const (
	resourceTrainingJob   = "TrainingJob"
	resourcePipeline      = "Pipeline"
	resourceModel         = "Model"
	frameworkPytorch      = "PYTORCH"
	keyPipelineNameProp   = "PipelineName"
	keyPipelineStatusProp = "PipelineStatus"
)

// ---------------------------------------------------------------------------
// ListCandidatesForAutoMLJob
// ---------------------------------------------------------------------------

// Deterministic candidate-generation tuning constants.
const (
	autoMLCandidateCount      = 3
	autoMLCandidateStepDelay  = 2 * time.Minute
	autoMLBaseObjectiveMetric = 0.95
	autoMLObjectiveMetricStep = 0.03
)

// AutoMLObjectiveMetric mirrors types.FinalAutoMLJobObjectiveMetric.
type AutoMLObjectiveMetric struct {
	MetricName string  `json:"MetricName"`
	Value      float64 `json:"Value"`
}

// AutoMLCandidate mirrors types.AutoMLCandidate.
type AutoMLCandidate struct {
	CreationTime                  time.Time              `json:"CreationTime"`
	LastModifiedTime              time.Time              `json:"LastModifiedTime"`
	FinalAutoMLJobObjectiveMetric *AutoMLObjectiveMetric `json:"FinalAutoMLJobObjectiveMetric,omitempty"`
	CandidateName                 string                 `json:"CandidateName"`
	CandidateStatus               string                 `json:"CandidateStatus"`
	ObjectiveStatus               string                 `json:"ObjectiveStatus"`
	CandidateSteps                []map[string]any       `json:"CandidateSteps"`
}

// generateAutoMLCandidates deterministically derives a set of candidates for
// an AutoML job from its stored state: while the job is in progress the
// leading candidate is Completed and the others remain InProgress; once the
// job has stopped/completed, all candidates are Completed.
func generateAutoMLCandidates(job *AutoMLJob) []*AutoMLCandidate {
	statuses := make([]string, autoMLCandidateCount)
	for i := range statuses {
		statuses[i] = algorithmStatusCompleted
	}

	if job.AutoMLJobStatus == trainingJobStatusInProgress {
		for i := 1; i < autoMLCandidateCount; i++ {
			statuses[i] = trainingJobStatusInProgress
		}
	}

	candidates := make([]*AutoMLCandidate, 0, autoMLCandidateCount)

	for i, status := range statuses {
		created := job.CreationTime.Add(time.Duration(i) * time.Minute)
		modified := created.Add(autoMLCandidateStepDelay)

		c := &AutoMLCandidate{
			CandidateName:    fmt.Sprintf("%s-%03d", job.AutoMLJobName, i+1),
			CandidateStatus:  status,
			CreationTime:     created,
			LastModifiedTime: modified,
			CandidateSteps:   []map[string]any{},
			ObjectiveStatus:  "Pending",
		}

		if status == algorithmStatusCompleted {
			c.ObjectiveStatus = "Succeeded"
			c.FinalAutoMLJobObjectiveMetric = &AutoMLObjectiveMetric{
				MetricName: "validation:accuracy",
				Value:      autoMLBaseObjectiveMetric - float64(i)*autoMLObjectiveMetricStep,
			}
		}

		candidates = append(candidates, c)
	}

	return candidates
}

// ListCandidatesForAutoMLJobParams bundles the filter/sort/page criteria.
type ListCandidatesForAutoMLJobParams struct {
	CandidateNameEquals string
	StatusEquals        string
	SortBy              string
	SortOrder           string
	NextToken           string
	MaxResults          int32
}

func candidateObjectiveValue(c *AutoMLCandidate) float64 {
	if c.FinalAutoMLJobObjectiveMetric == nil {
		return 0
	}

	return c.FinalAutoMLJobObjectiveMetric.Value
}

// ListCandidatesForAutoMLJob returns the (deterministically derived)
// candidates for a stored AutoML job.
//
// api_op_ListCandidatesForAutoMLJob.go:38,41: SortBy's own doc text reads
// "The default is Descending" -- not a valid CandidateSortBy value at all
// (CreationTime/Status/FinalObjectiveMetricValue), so that half of the doc
// is corrupted copy-paste and not trusted; SortOrder's doc ("The default is
// Ascending") is taken at face value since it is internally consistent.
func (b *InMemoryBackend) ListCandidatesForAutoMLJob(
	ctx context.Context,
	jobName string,
	params ListCandidatesForAutoMLJobParams,
) ([]*AutoMLCandidate, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListCandidatesForAutoMLJob")
	defer b.mu.RUnlock()

	job, ok := b.autoMLJobsStoreRO(region).Get(jobName)
	if !ok {
		return nil, "", fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, jobName)
	}

	all := generateAutoMLCandidates(job)
	filtered := make([]*AutoMLCandidate, 0, len(all))

	for _, c := range all {
		if params.CandidateNameEquals != "" && c.CandidateName != params.CandidateNameEquals {
			continue
		}

		if params.StatusEquals != "" && c.CandidateStatus != params.StatusEquals {
			continue
		}

		filtered = append(filtered, c)
	}

	desc := params.SortOrder == sortOrderDescending
	sort.SliceStable(filtered, func(i, k int) bool {
		var less bool

		switch params.SortBy {
		case keyStatus:
			less = filtered[i].CandidateStatus < filtered[k].CandidateStatus
		case "FinalObjectiveMetricValue":
			less = candidateObjectiveValue(filtered[i]) < candidateObjectiveValue(filtered[k])
		default:
			less = filtered[i].CreationTime.Before(filtered[k].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	page, next := paginateSlice(filtered, params.NextToken, params.MaxResults)

	return page, next, nil
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// SearchFilter mirrors types.Filter (a single property comparison).
type SearchFilter struct {
	Name     string `json:"Name"`
	Operator string `json:"Operator"`
	Value    string `json:"Value"`
}

// searchResourceItem pairs a stored resource with a flattened JSON view used
// for filter evaluation.
type searchResourceItem struct {
	raw  any
	flat map[string]any
	key  string
}

func matchesSearchFilter(flat map[string]any, f SearchFilter) bool {
	v, ok := flat[f.Name]

	switch f.Operator {
	case "", "Equals":
		return ok && fmt.Sprintf("%v", v) == f.Value
	case "NotEquals":
		return !ok || fmt.Sprintf("%v", v) != f.Value
	case "Contains":
		return ok && strings.Contains(fmt.Sprintf("%v", v), f.Value)
	case "Exists":
		return ok
	case "NotExists":
		return !ok
	default:
		return true
	}
}

func matchesSearchExpression(flat map[string]any, filters []SearchFilter, boolOp string) bool {
	if len(filters) == 0 {
		return true
	}

	if boolOp == "Or" {
		for _, f := range filters {
			if matchesSearchFilter(flat, f) {
				return true
			}
		}

		return false
	}

	for _, f := range filters {
		if !matchesSearchFilter(flat, f) {
			return false
		}
	}

	return true
}

// searchableResources returns the Search resource-key items for a supported
// ResourceType. Only resources with fully real, stateful backends are
// supported; unsupported (but valid) resource types return an empty result
// set, matching real AWS's "no matches" shape rather than an error.
// trainingJobSearchView/pipelineSearchView build the same epoch-seconds JSON
// shape handleDescribeTrainingJobFull/handleDescribePipeline already emit.
// TrainingJob/Pipeline have no MarshalJSON of their own (unlike
// CompilationJob), so a direct json.Marshal of the raw struct -- which
// toJSONFlatMap did previously, and which searchableResources stored
// directly as the response's "raw" value -- emits CreationTime/
// LastModifiedTime/etc. as Go's default RFC3339 strings. The real awsjson1.1
// protocol expects a JSON number for every Timestamp-shaped member, so a
// real SDK client's Search call failed deserialization outright for either
// of the only two resource types this backend fully supports.
func trainingJobSearchView(tj *TrainingJob) map[string]any {
	resp := map[string]any{
		keyTrainingJobName:       tj.TrainingJobName,
		keyTrainingJobArn:        tj.TrainingJobArn,
		keyTrainingJobStatus:     tj.TrainingJobStatus,
		"SecondaryStatus":        tj.SecondaryStatus,
		keyRoleArn:               tj.RoleArn,
		"AlgorithmSpecification": tj.AlgorithmSpecification,
		"ResourceConfig":         tj.ResourceConfig,
		"StoppingCondition":      tj.StoppingCondition,
		keyCreationTime:          epochSeconds(tj.CreationTime),
		keyLastModifiedTime:      epochSeconds(tj.LastModifiedTime),
	}
	addTrainingJobOptionalFields(resp, tj)

	return resp
}

func pipelineSearchView(p *Pipeline) map[string]any {
	resp := map[string]any{
		"PipelineName":        p.PipelineName,
		keyPipelineArn:        p.PipelineArn,
		"PipelineStatus":      p.PipelineStatus,
		keyPipelineDefinition: p.PipelineDefinition,
		keyRoleArn:            p.RoleArn,
		keyCreationTime:       epochSeconds(p.CreationTime),
		keyLastModifiedTime:   epochSeconds(p.LastModifiedTime),
	}
	if p.PipelineDisplayName != "" {
		resp["PipelineDisplayName"] = p.PipelineDisplayName
	}
	if p.PipelineDescription != "" {
		resp["PipelineDescription"] = p.PipelineDescription
	}
	if p.ParallelismConfiguration != nil {
		resp["ParallelismConfiguration"] = p.ParallelismConfiguration
	}

	return resp
}

func (b *InMemoryBackend) searchableResources(region, resource string) []searchResourceItem {
	switch resource {
	case resourceTrainingJob:
		items := make([]searchResourceItem, 0, b.trainingJobsStoreRO(region).Len())
		for _, tj := range b.trainingJobsStoreRO(region).All() {
			view := trainingJobSearchView(tj)
			items = append(items, searchResourceItem{raw: view, flat: view, key: tj.TrainingJobName})
		}

		return items
	case resourcePipeline:
		items := make([]searchResourceItem, 0, b.pipelinesStoreRO(region).Len())
		for _, p := range b.pipelinesStoreRO(region).All() {
			view := pipelineSearchView(p)
			items = append(items, searchResourceItem{raw: view, flat: view, key: p.PipelineName})
		}

		return items
	default:
		return nil
	}
}

// searchSupportedResourceTypes lists the ResourceType values Search accepts.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var searchSupportedResourceTypes = map[string]bool{
	resourceTrainingJob: true, "Experiment": true, "ExperimentTrial": true,
	"ExperimentTrialComponent": true, "Endpoint": true, resourceModel: true,
	"ModelPackage": true, "ModelPackageGroup": true, resourcePipeline: true,
	"PipelineExecution": true, "FeatureGroup": true, "FeatureMetadata": true,
	"Image": true, "ImageVersion": true, "Project": true,
	"HyperParameterTuningJob": true, "ModelCard": true, "PipelineVersion": true,
}

// SearchParams bundles the filter/sort/page criteria for Search.
type SearchParams struct {
	Resource                 string
	BooleanOperator          string
	NextToken                string
	SortBy                   string
	SortOrder                string
	CrossAccountFilterOption string
	Filters                  []SearchFilter
	MaxResults               int32
}

// crossAccountFilterOptionCrossAccount is the only CrossAccountFilterOption
// value this single-tenant backend can honestly answer: it has no concept
// of another account's resources, so a CrossAccount search always yields
// zero matches, the same as real AWS would return with nothing shared.
const crossAccountFilterOptionCrossAccount = "CrossAccount"

// Search evaluates a SearchExpression's top-level Filters against stored
// resources of the given type.
//
// Previously SortBy/SortOrder were decoded by the handler and then dropped
// before reaching this function -- a real request specifying either had no
// effect at all. api_op_Search.go:56,60: SortBy's default is LastModifiedTime,
// SortOrder's default is Descending.
func (b *InMemoryBackend) Search(
	ctx context.Context,
	params SearchParams,
) ([]map[string]any, int, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("Search")
	defer b.mu.RUnlock()

	if !searchSupportedResourceTypes[params.Resource] {
		return nil, 0, "", fmt.Errorf("%w: unsupported search Resource %q", ErrValidation, params.Resource)
	}

	if params.CrossAccountFilterOption == crossAccountFilterOptionCrossAccount {
		return []map[string]any{}, 0, "", nil
	}

	items := b.searchableResources(region, params.Resource)
	filtered := make([]searchResourceItem, 0, len(items))

	for _, it := range items {
		if matchesSearchExpression(it.flat, params.Filters, params.BooleanOperator) {
			filtered = append(filtered, it)
		}
	}

	sort.Slice(filtered, func(i, j int) bool { return filtered[i].key < filtered[j].key })

	sortBy := params.SortBy
	if sortBy == "" {
		sortBy = keyLastModifiedTime
	}

	desc := params.SortOrder != sortOrderAscending
	sort.SliceStable(filtered, func(i, j int) bool {
		vi := fmt.Sprintf("%v", filtered[i].flat[sortBy])
		vj := fmt.Sprintf("%v", filtered[j].flat[sortBy])

		if desc {
			return vi > vj
		}

		return vi < vj
	})

	page, next := paginateSlice(filtered, params.NextToken, params.MaxResults)

	out := make([]map[string]any, len(page))
	for i, it := range page {
		out[i] = map[string]any{params.Resource: it.raw}
	}

	return out, len(filtered), next, nil
}

// ---------------------------------------------------------------------------
// GetSearchSuggestions
// ---------------------------------------------------------------------------

// searchablePropertiesByResource is a static catalog of the property names
// each Search resource type exposes, used to serve GetSearchSuggestions.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var searchablePropertiesByResource = map[string][]string{
	resourceTrainingJob: {"TrainingJobName", "TrainingJobStatus", keyCreationTime, "LastModifiedTime", keyRoleArn},
	resourcePipeline:    {keyPipelineNameProp, keyPipelineStatusProp, keyPipelineArn, keyCreationTime, keyRoleArn},
	"Experiment":        {"ExperimentName", keyExperimentArn, keyCreationTime},
	"ModelPackage":      {"ModelPackageName", keyModelApprovalStatus, keyModelPackageArn},
	"ModelPackageGroup": {keyModelPackageGroupName, keyModelPackageGroupArn},
	"Endpoint":          {keyEndpointNameField, "EndpointStatus", "EndpointArn"},
	resourceModel:       {"ModelName", "ModelArn", keyCreationTime},
	"FeatureGroup":      {keyFeatureGroupName, keyFeatureGroupStatus, keyFeatureGroupArn},
	"Image":             {"ImageName", "ImageStatus", keyImageArn},
	"ImageVersion":      {keyImageVersionArn, "ImageVersionStatus", "Version"},
	"Project":           {"ProjectName", keyProjectArn, "ProjectStatus"},
}

// GetSearchSuggestions returns candidate property names for resource whose
// name starts with propertyNameHint (case-insensitive), mirroring the
// suggestion behaviour of real AWS's Search property-name autocomplete.
func (b *InMemoryBackend) GetSearchSuggestions(resource, propertyNameHint string) ([]string, error) {
	if !searchSupportedResourceTypes[resource] {
		return nil, fmt.Errorf("%w: unsupported search Resource %q", ErrValidation, resource)
	}

	props := searchablePropertiesByResource[resource]
	hint := strings.ToLower(propertyNameHint)
	out := make([]string, 0, len(props))

	for _, p := range props {
		if hint == "" || strings.HasPrefix(strings.ToLower(p), hint) {
			out = append(out, p)
		}
	}

	return out, nil
}

// ---------------------------------------------------------------------------
// ListModelMetadata
// ---------------------------------------------------------------------------

// ModelMetadataEntry mirrors types.ModelMetadataSummary.
type ModelMetadataEntry struct {
	Domain           string
	Framework        string
	FrameworkVersion string
	Model            string
	Task             string
}

// modelMetadataCatalog is a static catalog of well-known curated models,
// mirroring the built-in model metadata real SageMaker exposes via
// ListModelMetadata (this operation has no corresponding Create* API).
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var modelMetadataCatalog = []ModelMetadataEntry{
	{
		Domain: "NATURAL_LANGUAGE_PROCESSING", Framework: frameworkPytorch,
		FrameworkVersion: "1.13.1", Model: "bert-base-uncased", Task: "FILL_MASK",
	},
	{
		Domain: "NATURAL_LANGUAGE_PROCESSING", Framework: frameworkPytorch,
		FrameworkVersion: "2.0.1", Model: "gpt2", Task: "TEXT_GENERATION",
	},
	{
		Domain: "COMPUTER_VISION", Framework: frameworkPytorch,
		FrameworkVersion: "2.0.1", Model: "resnet50", Task: "IMAGE_CLASSIFICATION",
	},
	{
		Domain: "COMPUTER_VISION", Framework: "TENSORFLOW",
		FrameworkVersion: "2.12.0", Model: "efficientnet-b0", Task: "IMAGE_CLASSIFICATION",
	},
	{
		Domain: "MACHINE_LEARNING", Framework: "XGBOOST",
		FrameworkVersion: "1.7.4", Model: "xgboost-classifier", Task: "CLASSIFICATION",
	},
	{
		Domain: "MACHINE_LEARNING", Framework: "SKLEARN",
		FrameworkVersion: "1.2.2", Model: "sklearn-random-forest", Task: "CLASSIFICATION",
	},
}

// ModelMetadataFilter mirrors types.ModelMetadataFilter.
type ModelMetadataFilter struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

func modelMetadataFieldValue(e ModelMetadataEntry, name string) string {
	switch name {
	case "Domain":
		return e.Domain
	case "Framework":
		return e.Framework
	case "FrameworkVersion":
		return e.FrameworkVersion
	case "Task":
		return e.Task
	default:
		return ""
	}
}

// ListModelMetadata lists curated model metadata, optionally filtered by
// Domain, Framework, FrameworkVersion or Task.
func (b *InMemoryBackend) ListModelMetadata(
	filters []ModelMetadataFilter,
	nextToken string,
	maxResults int32,
) ([]ModelMetadataEntry, string) {
	filtered := make([]ModelMetadataEntry, 0, len(modelMetadataCatalog))

	for _, e := range modelMetadataCatalog {
		matches := true

		for _, f := range filters {
			if modelMetadataFieldValue(e, f.Name) != f.Value {
				matches = false

				break
			}
		}

		if matches {
			filtered = append(filtered, e)
		}
	}

	return paginateSlice(filtered, nextToken, maxResults)
}

// ---------------------------------------------------------------------------
// GetScalingConfigurationRecommendation
// ---------------------------------------------------------------------------

// Deterministic scaling recommendation defaults.
const (
	scalingRecDefaultTargetCPUUtilization = 50
	scalingRecMinCapacity                 = 1
	scalingRecMaxCapacity                 = 4
	scalingRecCooldownSeconds             = 300
	// scalingRecInvocationsPerInstance/scalingRecModelLatencyMillis are
	// synthesized, not measured -- this backend never actually benchmarks
	// an endpoint, the same disclosure already applied to CompilationJob's
	// ModelArtifacts.S3ModelArtifacts.
	scalingRecInvocationsPerInstance = 1000
	scalingRecModelLatencyMillis     = 50
)

// ScalingPolicyObjective mirrors types.ScalingPolicyObjective
// (api_op_GetScalingConfigurationRecommendation.go): purely an echo of
// what the caller sent, not persisted or used in the recommendation math.
type ScalingPolicyObjective struct {
	MinInvocationsPerMinute *int32 `json:"MinInvocationsPerMinute,omitempty"`
	MaxInvocationsPerMinute *int32 `json:"MaxInvocationsPerMinute,omitempty"`
}

// ScalingConfigurationRecommendation mirrors the relevant parts of
// GetScalingConfigurationRecommendationOutput.
type ScalingConfigurationRecommendation struct {
	MinCapacity                 int32
	MaxCapacity                 int32
	ScaleInCooldown             int32
	ScaleOutCooldown            int32
	TargetCPUUtilizationPerCore int32
	InvocationsPerInstance      int32
	ModelLatency                int32
}

// GetScalingConfigurationRecommendation validates that the named inference
// recommendations job exists and returns a deterministic scaling
// recommendation derived from the requested target CPU utilization.
func (b *InMemoryBackend) GetScalingConfigurationRecommendation(
	ctx context.Context,
	jobName string,
	targetCPUUtilizationPerCore int32,
) (*ScalingConfigurationRecommendation, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetScalingConfigurationRecommendation")
	defer b.mu.RUnlock()

	if _, ok := b.inferenceRecommendationsJobsStoreRO(region).Get(jobName); !ok {
		return nil, fmt.Errorf(
			"%w: inference recommendations job %q not found", ErrInferenceRecommendationsJobNotFound, jobName,
		)
	}

	target := targetCPUUtilizationPerCore
	if target <= 0 {
		target = scalingRecDefaultTargetCPUUtilization
	}

	return &ScalingConfigurationRecommendation{
		MinCapacity:                 scalingRecMinCapacity,
		MaxCapacity:                 scalingRecMaxCapacity,
		ScaleInCooldown:             scalingRecCooldownSeconds,
		ScaleOutCooldown:            scalingRecCooldownSeconds,
		TargetCPUUtilizationPerCore: target,
		InvocationsPerInstance:      scalingRecInvocationsPerInstance,
		ModelLatency:                scalingRecModelLatencyMillis,
	}, nil
}
