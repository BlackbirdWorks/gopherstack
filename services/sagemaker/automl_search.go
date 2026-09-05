package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
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

// SearchNestedFilter mirrors types.NestedFilters.
type SearchNestedFilter struct {
	NestedPropertyName string         `json:"NestedPropertyName"`
	Filters            []SearchFilter `json:"Filters"`
}

// SearchExpression mirrors types.SearchExpression.
type SearchExpression struct {
	Operator       string               `json:"Operator"`
	Filters        []SearchFilter       `json:"Filters"`
	NestedFilters  []SearchNestedFilter `json:"NestedFilters"`
	SubExpressions []SearchExpression   `json:"SubExpressions"`
}

// searchResourceItem pairs a stored resource with a flattened JSON view used
// for filter evaluation.
type searchResourceItem struct {
	raw  any
	flat map[string]any
	key  string
}

// timestampSearchFields are the flat-map keys whose value is an
// epoch-seconds float64 (see epochSeconds / trainingJobSearchView /
// pipelineSearchView), even though types.Filter.Value's doc states
// timestamp properties are compared as ISO 8601 strings
// (YYYY-mm-dd'T'HH:MM:SS): a raw string/numeric comparison of the two forms
// would mean a filter built in the documented format could never match
// this API's own emitted CreationTime/LastModifiedTime.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var timestampSearchFields = map[string]bool{
	keyCreationTime: true, keyLastModifiedTime: true,
	"TrainingStartTime": true, "TrainingEndTime": true,
}

func parseFilterTimestamp(value string) (float64, bool) {
	for _, layout := range []string{"2006-01-02T15:04:05", time.RFC3339} {
		if t, err := time.Parse(layout, value); err == nil {
			return float64(t.Unix()), true
		}
	}

	return 0, false
}

func toSearchFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

// searchComparable resolves both sides of a comparison to float64s: via
// epoch-seconds conversion for the documented timestamp fields, or plain
// numeric parsing otherwise. ok is false when the property/value pair
// cannot be compared this way (e.g. a text property), matching the doc's
// "Not supported for text properties" note for the range operators by
// declining to match rather than guessing a lexical order.
func searchComparable(name string, v any, filterValue string) (float64, float64, bool) {
	if timestampSearchFields[name] {
		fv, okF := toSearchFloat(v)
		want, okW := parseFilterTimestamp(filterValue)

		return fv, want, okF && okW
	}

	fv, okF := toSearchFloat(v)
	want, err := strconv.ParseFloat(filterValue, 64)

	return fv, want, okF && err == nil
}

func searchValuesEqual(name string, v any, filterValue string) bool {
	if timestampSearchFields[name] {
		fv, want, ok := searchComparable(name, v, filterValue)

		return ok && fv == want
	}

	return fmt.Sprintf("%v", v) == filterValue
}

// matchesSearchRange evaluates the four documented range operators
// (GreaterThan, GreaterThanOrEqualTo, LessThan, LessThanOrEqualTo).
func matchesSearchRange(name string, v any, op, filterValue string) bool {
	fv, want, canCompare := searchComparable(name, v, filterValue)
	if !canCompare {
		return false
	}

	switch op {
	case "GreaterThan":
		return fv > want
	case "GreaterThanOrEqualTo":
		return fv >= want
	case "LessThan":
		return fv < want
	default: // LessThanOrEqualTo
		return fv <= want
	}
}

func matchesSearchIn(v any, filterValue string) bool {
	return slices.Contains(strings.Split(filterValue, ","), fmt.Sprintf("%v", v))
}

// matchesSearchFilter evaluates a single Filter's documented Operator
// (types.Operator's full enum: Equals, NotEquals, GreaterThan,
// GreaterThanOrEqualTo, LessThan, LessThanOrEqualTo, Contains, Exists,
// NotExists, In). An Operator outside that set does not match anything --
// over-accepting an undocumented operator has been the sharper bug shape
// elsewhere in this campaign (e.g. an SNS numeric operator outside its
// documented set).
func matchesSearchFilter(flat map[string]any, f SearchFilter) bool {
	v, ok := flat[f.Name]

	switch f.Operator {
	case "", "Equals":
		return ok && searchValuesEqual(f.Name, v, f.Value)
	case "NotEquals":
		return !ok || !searchValuesEqual(f.Name, v, f.Value)
	case "Contains":
		return ok && strings.Contains(fmt.Sprintf("%v", v), f.Value)
	case "Exists":
		return ok
	case "NotExists":
		return !ok
	case "GreaterThan", "GreaterThanOrEqualTo", "LessThan", "LessThanOrEqualTo":
		return ok && matchesSearchRange(f.Name, v, f.Operator, f.Value)
	case "In":
		return ok && matchesSearchIn(v, f.Value)
	default:
		return false
	}
}

// toObjectList converts a nested resource field (a []Channel or similar
// typed slice stored in a searchResourceItem's flat map) into a generic
// []map[string]any via a JSON round trip, so its fields can be addressed by
// the JSON dotted path a NestedFilters entry documents.
func toObjectList(raw any) ([]map[string]any, bool) {
	b, marshalErr := json.Marshal(raw)
	if marshalErr != nil {
		return nil, false
	}

	var items []map[string]any
	if unmarshalErr := json.Unmarshal(b, &items); unmarshalErr != nil {
		return nil, false
	}

	return items, true
}

func dottedLookup(m map[string]any, path string) (any, bool) {
	cur := any(m)

	for part := range strings.SplitSeq(path, ".") {
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		v, exists := cm[part]
		if !exists {
			return nil, false
		}

		cur = v
	}

	return cur, true
}

// matchesNestedFilter evaluates a NestedFilters entry: satisfied if a
// SINGLE object in the NestedPropertyName list satisfies every one of its
// Filters (types.NestedFilters' doc). Per the SDK's API_NestedFilters.html
// worked example, each nested Filter's Name carries the FULL dotted path
// including the NestedPropertyName prefix (e.g.
// "InputDataConfig.DataSource.S3DataSource.S3Uri" under NestedPropertyName
// "InputDataConfig"), not a path relative to the nested object.
func matchesNestedFilter(flat map[string]any, nf SearchNestedFilter) bool {
	raw, ok := flat[nf.NestedPropertyName]
	if !ok {
		return false
	}

	items, ok := toObjectList(raw)
	if !ok {
		return false
	}

	prefix := nf.NestedPropertyName + "."

	for _, item := range items {
		allMatch := true

		for _, f := range nf.Filters {
			rel := strings.TrimPrefix(f.Name, prefix)

			v, exists := dottedLookup(item, rel)
			itemFlat := map[string]any{}

			if exists {
				itemFlat[rel] = v
			}

			if !matchesSearchFilter(itemFlat, SearchFilter{Name: rel, Operator: f.Operator, Value: f.Value}) {
				allMatch = false

				break
			}
		}

		if allMatch {
			return true
		}
	}

	return false
}

// matchesSearchExpression evaluates a full SearchExpression: every
// condition across Filters, NestedFilters and SubExpressions is combined
// by the SAME single Operator (types.SearchExpression's doc: "If you want
// every conditional statement in all lists to be satisfied ... specify
// And. If only a single conditional statement needs to be true ...,
// specify Or. The default value is And."), not independently per list.
func matchesSearchExpression(flat map[string]any, expr SearchExpression) bool {
	total := len(expr.Filters) + len(expr.NestedFilters) + len(expr.SubExpressions)
	if total == 0 {
		return true
	}

	conds := make([]bool, 0, total)

	for _, f := range expr.Filters {
		conds = append(conds, matchesSearchFilter(flat, f))
	}

	for _, nf := range expr.NestedFilters {
		conds = append(conds, matchesNestedFilter(flat, nf))
	}

	for _, sub := range expr.SubExpressions {
		conds = append(conds, matchesSearchExpression(flat, sub))
	}

	if expr.Operator == "Or" {
		for _, c := range conds {
			if c {
				return true
			}
		}

		return false
	}

	for _, c := range conds {
		if !c {
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
	NextToken                string
	SortBy                   string
	SortOrder                string
	CrossAccountFilterOption string
	Expression               SearchExpression
	MaxResults               int32
}

// crossAccountFilterOptionCrossAccount is the only CrossAccountFilterOption
// value this single-tenant backend can honestly answer: it has no concept
// of another account's resources, so a CrossAccount search always yields
// zero matches, the same as real AWS would return with nothing shared.
const crossAccountFilterOptionCrossAccount = "CrossAccount"

// Search evaluates a SearchExpression (Filters, NestedFilters and
// SubExpressions, all combined by its single Operator) against stored
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
		if matchesSearchExpression(it.flat, params.Expression) {
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
