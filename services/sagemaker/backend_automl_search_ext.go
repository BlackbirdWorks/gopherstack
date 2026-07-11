package sagemaker

import (
	"context"
	"encoding/json"
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
	NextToken           string
	MaxResults          int32
}

// ListCandidatesForAutoMLJob returns the (deterministically derived)
// candidates for a stored AutoML job.
func (b *InMemoryBackend) ListCandidatesForAutoMLJob(
	ctx context.Context,
	jobName string,
	params ListCandidatesForAutoMLJobParams,
) ([]*AutoMLCandidate, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListCandidatesForAutoMLJob")
	defer b.mu.RUnlock()

	job, ok := b.autoMLJobsStore(region).Get(jobName)
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

func toJSONFlatMap(v any) map[string]any {
	raw, marshalErr := json.Marshal(v)
	if marshalErr != nil {
		return map[string]any{}
	}

	var m map[string]any
	if unmarshalErr := json.Unmarshal(raw, &m); unmarshalErr != nil {
		return map[string]any{}
	}

	return m
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
func (b *InMemoryBackend) searchableResources(region, resource string) []searchResourceItem {
	switch resource {
	case resourceTrainingJob:
		items := make([]searchResourceItem, 0, b.trainingJobsStore(region).Len())
		for _, tj := range b.trainingJobsStore(region).All() {
			items = append(items, searchResourceItem{
				raw: tj, flat: toJSONFlatMap(tj), key: tj.TrainingJobName,
			})
		}

		return items
	case resourcePipeline:
		items := make([]searchResourceItem, 0, b.pipelinesStore(region).Len())
		for _, p := range b.pipelinesStore(region).All() {
			items = append(items, searchResourceItem{
				raw: p, flat: toJSONFlatMap(p), key: p.PipelineName,
			})
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
	Resource        string
	BooleanOperator string
	NextToken       string
	Filters         []SearchFilter
	MaxResults      int32
}

// Search evaluates a SearchExpression's top-level Filters against stored
// resources of the given type.
func (b *InMemoryBackend) Search(ctx context.Context, params SearchParams) ([]map[string]any, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("Search")
	defer b.mu.RUnlock()

	if !searchSupportedResourceTypes[params.Resource] {
		return nil, "", fmt.Errorf("%w: unsupported search Resource %q", ErrValidation, params.Resource)
	}

	items := b.searchableResources(region, params.Resource)
	filtered := make([]searchResourceItem, 0, len(items))

	for _, it := range items {
		if matchesSearchExpression(it.flat, params.Filters, params.BooleanOperator) {
			filtered = append(filtered, it)
		}
	}

	sort.Slice(filtered, func(i, j int) bool { return filtered[i].key < filtered[j].key })

	page, next := paginateSlice(filtered, params.NextToken, params.MaxResults)

	results := make([]map[string]any, len(page))
	for i, it := range page {
		results[i] = map[string]any{params.Resource: it.raw}
	}

	return results, next, nil
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
	"ModelPackageGroup": {"ModelPackageGroupName", keyModelPackageGroupArn},
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
)

// ScalingConfigurationRecommendation mirrors the relevant parts of
// GetScalingConfigurationRecommendationOutput.
type ScalingConfigurationRecommendation struct {
	MinCapacity                 int32
	MaxCapacity                 int32
	ScaleInCooldown             int32
	ScaleOutCooldown            int32
	TargetCPUUtilizationPerCore int32
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

	if _, ok := b.inferenceRecommendationsJobsStore(region).Get(jobName); !ok {
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
	}, nil
}
