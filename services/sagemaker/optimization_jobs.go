package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrOptimizationJobNotFound is returned when an optimization job does not exist.
var ErrOptimizationJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// OptimizationJob
// ---------------------------------------------------------------------------

// optimizationConfigTypeName maps an OptimizationConfig union member's wire
// key (types/types.go:16523-16526, smithy union JSON key == member name) to
// the technique name OptimizationJobSummary.OptimizationTypes /
// ListOptimizationJobsInput.OptimizationContains use ("Quantization" /
// "Compilation" per the op's own doc; "Sharding" / "SpeculativeDecoding"
// inferred from the same Model*Config-stripped naming convention, not
// independently confirmed against a real wire example).
func optimizationConfigTypeName(key string) (string, bool) {
	switch key {
	case "ModelCompilationConfig":
		return "Compilation", true
	case "ModelQuantizationConfig":
		return "Quantization", true
	case "ModelShardingConfig":
		return "Sharding", true
	case "ModelSpeculativeDecodingConfig":
		return "SpeculativeDecoding", true
	default:
		return "", false
	}
}

// optimizationTypesOf derives OptimizationJobSummary's required
// OptimizationTypes from the raw OptimizationConfigs the client sent, by
// checking which union member keys appear in the JSON.
func optimizationTypesOf(rawConfigs json.RawMessage) []string {
	// Every OptimizationConfig union member's wire key, in the same order
	// optimizationConfigTypeName recognizes them.
	wireKeys := [...]string{
		"ModelCompilationConfig", "ModelQuantizationConfig", "ModelShardingConfig", "ModelSpeculativeDecodingConfig",
	}

	types := make([]string, 0, len(wireKeys))

	for _, key := range wireKeys {
		if strings.Contains(string(rawConfigs), `"`+key+`"`) {
			name, _ := optimizationConfigTypeName(key)
			types = append(types, name)
		}
	}

	sort.Strings(types)

	return types
}

// OptimizationJob represents a SageMaker optimization job. ModelSource,
// OutputConfig, and VpcConfig are stored as opaque json.RawMessage
// passthrough (same convention as ai_workload_configs.go): this backend
// never simulates actual model optimization, so only the client-supplied
// fields round-trip, not server-derived results.
type OptimizationJob struct {
	LastModifiedTime        time.Time          `json:"LastModifiedTime"`
	CreationTime            time.Time          `json:"CreationTime"`
	StoppingCondition       *StoppingCondition `json:"StoppingCondition,omitempty"`
	Tags                    map[string]string  `json:"Tags,omitempty"`
	OptimizationEnvironment map[string]string  `json:"OptimizationEnvironment,omitempty"`
	OptimizationJobStatus   string             `json:"OptimizationJobStatus"`
	OptimizationJobName     string             `json:"OptimizationJobName"`
	OptimizationJobArn      string             `json:"OptimizationJobArn"`
	RoleArn                 string             `json:"RoleArn,omitempty"`
	DeploymentInstanceType  string             `json:"DeploymentInstanceType,omitempty"`
	OutputConfig            json.RawMessage    `json:"OutputConfig,omitempty"`
	VpcConfig               json.RawMessage    `json:"VpcConfig,omitempty"`
	OptimizationConfigs     json.RawMessage    `json:"OptimizationConfigs,omitempty"`
	TrainingPlanArns        []string           `json:"TrainingPlanArns,omitempty"`
	ModelSource             json.RawMessage    `json:"ModelSource,omitempty"`
	MaxInstanceCount        int32              `json:"MaxInstanceCount,omitempty"`
}

func cloneOptimizationJob(j *OptimizationJob) *OptimizationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.OptimizationEnvironment = maps.Clone(j.OptimizationEnvironment)
	cp.ModelSource = append(json.RawMessage(nil), j.ModelSource...)
	cp.OptimizationConfigs = append(json.RawMessage(nil), j.OptimizationConfigs...)
	cp.OutputConfig = append(json.RawMessage(nil), j.OutputConfig...)
	cp.VpcConfig = append(json.RawMessage(nil), j.VpcConfig...)
	cp.TrainingPlanArns = append([]string(nil), j.TrainingPlanArns...)

	if j.StoppingCondition != nil {
		sc := *j.StoppingCondition
		cp.StoppingCondition = &sc
	}

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeOptimizationJob.
func (j *OptimizationJob) MarshalJSON() ([]byte, error) {
	type alias OptimizationJob

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(j),
		CreationTime:     epochSeconds(j.CreationTime),
		LastModifiedTime: epochSeconds(j.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [OptimizationJob.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (j *OptimizationJob) UnmarshalJSON(data []byte) error {
	type alias OptimizationJob

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(j)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	j.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	j.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateOptimizationJobOptions holds the parameters for creating an
// optimization job (api_op_CreateOptimizationJob.go:48-114).
type CreateOptimizationJobOptions struct {
	OptimizationEnvironment map[string]string
	Tags                    map[string]string
	StoppingCondition       *StoppingCondition
	Name                    string
	RoleArn                 string
	DeploymentInstanceType  string
	ModelSource             json.RawMessage
	OptimizationConfigs     json.RawMessage
	OutputConfig            json.RawMessage
	VpcConfig               json.RawMessage
	TrainingPlanArns        []string
	MaxInstanceCount        int32
}

// CreateOptimizationJob creates an optimization job.
func (b *InMemoryBackend) CreateOptimizationJob(
	ctx context.Context,
	opts CreateOptimizationJobOptions,
) (*OptimizationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateOptimizationJob")
	defer b.mu.Unlock()

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", ErrValidation)
	}

	if opts.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	if opts.DeploymentInstanceType == "" {
		return nil, fmt.Errorf("%w: DeploymentInstanceType is required", ErrValidation)
	}

	if len(opts.ModelSource) == 0 {
		return nil, fmt.Errorf("%w: ModelSource is required", ErrValidation)
	}

	if len(opts.OptimizationConfigs) == 0 {
		return nil, fmt.Errorf("%w: OptimizationConfigs is required", ErrValidation)
	}

	if len(opts.OutputConfig) == 0 {
		return nil, fmt.Errorf("%w: OutputConfig is required", ErrValidation)
	}

	store := b.optimizationJobsStore(region)

	if _, ok := store.Get(opts.Name); ok {
		return nil, fmt.Errorf("%w: optimization job %q already exists", ErrValidation, opts.Name)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "optimization-job/"+opts.Name)
	now := time.Now()

	j := &OptimizationJob{
		OptimizationJobName:     opts.Name,
		OptimizationJobArn:      jobARN,
		OptimizationJobStatus:   "COMPLETED",
		RoleArn:                 opts.RoleArn,
		DeploymentInstanceType:  opts.DeploymentInstanceType,
		ModelSource:             opts.ModelSource,
		OptimizationConfigs:     opts.OptimizationConfigs,
		OutputConfig:            opts.OutputConfig,
		VpcConfig:               opts.VpcConfig,
		StoppingCondition:       opts.StoppingCondition,
		MaxInstanceCount:        opts.MaxInstanceCount,
		OptimizationEnvironment: opts.OptimizationEnvironment,
		TrainingPlanArns:        opts.TrainingPlanArns,
		Tags:                    mergeTags(nil, opts.Tags),
		CreationTime:            now,
		LastModifiedTime:        now,
	}
	store.Put(j)

	return cloneOptimizationJob(j), nil
}

// DescribeOptimizationJob returns an optimization job by name.
func (b *InMemoryBackend) DescribeOptimizationJob(ctx context.Context, name string) (*OptimizationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeOptimizationJob")
	defer b.mu.RUnlock()

	j, ok := b.optimizationJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	return cloneOptimizationJob(j), nil
}

// DeleteOptimizationJob removes an optimization job by name.
func (b *InMemoryBackend) DeleteOptimizationJob(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteOptimizationJob")
	defer b.mu.Unlock()

	store := b.optimizationJobsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	store.Delete(name)

	return nil
}

// StopOptimizationJob sets an optimization job status to "STOPPED".
func (b *InMemoryBackend) StopOptimizationJob(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopOptimizationJob")
	defer b.mu.Unlock()

	j, ok := b.optimizationJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	j.OptimizationJobStatus = jobStatusStopped
	j.LastModifiedTime = time.Now()

	return nil
}

// ListOptimizationJobsFilter narrows and orders the results of
// ListOptimizationJobs (api_op_ListOptimizationJobs.go:30-72). The op's own
// doc states both real defaults explicitly: SortBy is CreationTime, SortOrder
// is Ascending.
type ListOptimizationJobsFilter struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NameContains           string
	OptimizationContains   string
	StatusEquals           string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// optimizationJobMatchesOptimizationContains reports whether j has at least
// one technique matching filter.OptimizationContains (a no-op true when the
// filter is unset).
func optimizationJobMatchesOptimizationContains(j *OptimizationJob, filter ListOptimizationJobsFilter) bool {
	if filter.OptimizationContains == "" {
		return true
	}

	for _, t := range optimizationTypesOf(j.OptimizationConfigs) {
		if strings.EqualFold(t, filter.OptimizationContains) {
			return true
		}
	}

	return false
}

func optimizationJobMatchesFilter(j *OptimizationJob, filter ListOptimizationJobsFilter) bool {
	if filter.StatusEquals != "" && j.OptimizationJobStatus != filter.StatusEquals {
		return false
	}

	if filter.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.OptimizationJobName), strings.ToLower(filter.NameContains)) {
		return false
	}

	if !optimizationJobMatchesOptimizationContains(j, filter) {
		return false
	}

	if !timeWindowOK(j.CreationTime, filter.CreationTimeAfter, filter.CreationTimeBefore) {
		return false
	}

	return timeWindowOK(j.LastModifiedTime, filter.LastModifiedTimeAfter, filter.LastModifiedTimeBefore)
}

// lessOptimizationJob orders a before b by sortBy (Name/Status/default
// CreationTime, tie-broken by name).
func lessOptimizationJob(a, b *OptimizationJob, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		return a.OptimizationJobName < b.OptimizationJobName
	case keyStatus:
		return a.OptimizationJobStatus < b.OptimizationJobStatus
	default:
		if a.CreationTime.Equal(b.CreationTime) {
			return a.OptimizationJobName < b.OptimizationJobName
		}

		return a.CreationTime.Before(b.CreationTime)
	}
}

// ListOptimizationJobs returns optimization jobs matching filter, sorted by
// filter.SortBy (default CreationTime) / filter.SortOrder (default Ascending).
func (b *InMemoryBackend) ListOptimizationJobs(
	ctx context.Context,
	nextToken string,
	filter ListOptimizationJobsFilter,
) ([]*OptimizationJob, string) {
	b.mu.RLock("ListOptimizationJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*OptimizationJob, 0, b.optimizationJobsStoreRO(region).Len())

	for _, j := range b.optimizationJobsStoreRO(region).All() {
		if optimizationJobMatchesFilter(j, filter) {
			list = append(list, cloneOptimizationJob(j))
		}
	}

	desc := strings.EqualFold(filter.SortOrder, "Descending")
	sort.Slice(list, func(i, k int) bool {
		less := lessOptimizationJob(list[i], list[k], filter.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}
