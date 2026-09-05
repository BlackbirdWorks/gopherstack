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

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob
// ---------------------------------------------------------------------------

var (
	// ErrInferenceRecommendationsJobNotFound is returned when the job does not exist.
	ErrInferenceRecommendationsJobNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)
	// ErrInferenceRecommendationsJobAlreadyExists is returned when the job already exists.
	ErrInferenceRecommendationsJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

const (
	inferenceRecommendationsJobStatusInProgress = "IN_PROGRESS"
	inferenceRecommendationsJobStatusCompleted  = "COMPLETED"
	inferenceRecommendationsJobStatusStopping   = "STOPPING"
	inferenceRecommendationsJobStatusStopped    = "STOPPED"
)

// InferenceRecommendationsJob represents a SageMaker inference
// recommendations job. InputConfig/OutputConfig/StoppingConditions are
// stored as opaque json.RawMessage passthrough (same convention as
// ai_workload_configs.go): this backend never runs an actual recommendation
// job, so only client-supplied fields round-trip. OutputConfig is
// deliberately never echoed back by Describe — real
// DescribeInferenceRecommendationsJobOutput has no OutputConfig member at
// all (api_op_DescribeInferenceRecommendationsJob.go), an asymmetry by
// design, not a gap.
type InferenceRecommendationsJob struct {
	CreationTime       time.Time         `json:"CreationTime"`
	LastModifiedTime   time.Time         `json:"LastModifiedTime"`
	Tags               map[string]string `json:"Tags,omitempty"`
	JobName            string            `json:"JobName"`
	JobArn             string            `json:"JobArn"`
	JobType            string            `json:"JobType,omitempty"`
	JobDescription     string            `json:"JobDescription,omitempty"`
	Status             string            `json:"Status"`
	RoleArn            string            `json:"RoleArn,omitempty"`
	InputConfig        json.RawMessage   `json:"InputConfig,omitempty"`
	OutputConfig       json.RawMessage   `json:"OutputConfig,omitempty"`
	StoppingConditions json.RawMessage   `json:"StoppingConditions,omitempty"`
}

func cloneInferenceRecommendationsJob(j *InferenceRecommendationsJob) *InferenceRecommendationsJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.InputConfig = append(json.RawMessage(nil), j.InputConfig...)
	cp.OutputConfig = append(json.RawMessage(nil), j.OutputConfig...)
	cp.StoppingConditions = append(json.RawMessage(nil), j.StoppingConditions...)

	return &cp
}

// inputConfigModelIdentity is the subset of RecommendationJobInputConfig
// (types/types.go:19251-19291) needed to evaluate
// ListInferenceRecommendationsJobsInput's ModelNameEquals/
// ModelPackageVersionArnEquals filters against the opaque stored InputConfig.
type inputConfigModelIdentity struct {
	ModelName              string `json:"ModelName"`
	ModelPackageVersionArn string `json:"ModelPackageVersionArn"`
}

func decodeInputConfigModelIdentity(raw json.RawMessage) inputConfigModelIdentity {
	var id inputConfigModelIdentity

	_ = json.Unmarshal(raw, &id)

	return id
}

// CreateInferenceRecommendationsJobOptions holds input fields.
type CreateInferenceRecommendationsJobOptions struct {
	Tags               map[string]string
	JobName            string
	JobType            string
	JobDescription     string
	RoleArn            string
	InputConfig        json.RawMessage
	OutputConfig       json.RawMessage
	StoppingConditions json.RawMessage
}

// CreateInferenceRecommendationsJob creates an inference recommendations job.
func (b *InMemoryBackend) CreateInferenceRecommendationsJob(
	ctx context.Context,
	opts CreateInferenceRecommendationsJobOptions,
) (*InferenceRecommendationsJob, error) {
	b.mu.Lock("CreateInferenceRecommendationsJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	if opts.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	if len(opts.InputConfig) == 0 {
		return nil, fmt.Errorf("%w: InputConfig is required", ErrValidation)
	}

	if _, ok := b.inferenceRecommendationsJobsStore(region).Get(opts.JobName); ok {
		return nil, fmt.Errorf(
			"%w: inference recommendations job %q already exists",
			ErrInferenceRecommendationsJobAlreadyExists,
			opts.JobName,
		)
	}

	// JobType's own doc states the real default explicitly: "If left
	// unspecified, ... will run ... (DEFAULT)" — despite being flagged
	// "This member is required" in the generated struct comment.
	jobType := opts.JobType
	if jobType == "" {
		jobType = "Default"
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "inference-recommendations-job/"+opts.JobName)
	now := time.Now()

	j := &InferenceRecommendationsJob{
		JobName:            opts.JobName,
		JobArn:             jobARN,
		JobType:            jobType,
		JobDescription:     opts.JobDescription,
		Status:             inferenceRecommendationsJobStatusInProgress,
		RoleArn:            opts.RoleArn,
		InputConfig:        opts.InputConfig,
		OutputConfig:       opts.OutputConfig,
		StoppingConditions: opts.StoppingConditions,
		Tags:               mergeTags(nil, opts.Tags),
		CreationTime:       now,
		LastModifiedTime:   now,
	}
	b.inferenceRecommendationsJobsStore(region).Put(j)

	b.scheduleInferenceRecommendationsJobCompletion(b.lifecycleCtx, region, opts.JobName)

	return cloneInferenceRecommendationsJob(j), nil
}

// scheduleInferenceRecommendationsJobCompletion drives IN_PROGRESS ->
// COMPLETED after [inferenceRecommendationsJobInProgressToCompleted].
// Nothing previously advanced an IN_PROGRESS job -- no ticker, no later call
// -- so DescribeInferenceRecommendationsJob showed IN_PROGRESS for the
// entire lifetime of every job ever created.
func (b *InMemoryBackend) scheduleInferenceRecommendationsJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, inferenceRecommendationsJobInProgressToCompleted, func() {
		b.mu.Lock("scheduleInferenceRecommendationsJobCompletion.goroutine")
		defer b.mu.Unlock()

		j, ok := b.inferenceRecommendationsJobsStore(region).Get(name)
		if !ok || j.Status != inferenceRecommendationsJobStatusInProgress {
			return
		}

		j.Status = inferenceRecommendationsJobStatusCompleted
		j.LastModifiedTime = time.Now()
	})
}

// DescribeInferenceRecommendationsJob returns an inference recommendations job by name.
func (b *InMemoryBackend) DescribeInferenceRecommendationsJob(
	ctx context.Context,
	name string,
) (*InferenceRecommendationsJob, error) {
	b.mu.RLock("DescribeInferenceRecommendationsJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.inferenceRecommendationsJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: inference recommendations job %q not found",
			ErrInferenceRecommendationsJobNotFound,
			name,
		)
	}

	return cloneInferenceRecommendationsJob(j), nil
}

// StopInferenceRecommendationsJob stops an inference recommendations job.
func (b *InMemoryBackend) StopInferenceRecommendationsJob(ctx context.Context, name string) error {
	b.mu.Lock("StopInferenceRecommendationsJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.inferenceRecommendationsJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf(
			"%w: inference recommendations job %q not found",
			ErrInferenceRecommendationsJobNotFound,
			name,
		)
	}

	j.Status = inferenceRecommendationsJobStatusStopping
	j.LastModifiedTime = time.Now()

	b.runDelayed(b.lifecycleCtx, inferenceRecommendationsJobStoppingToStopped, func() {
		b.mu.Lock("StopInferenceRecommendationsJob.goroutine")
		defer b.mu.Unlock()

		j2, found := b.inferenceRecommendationsJobsStore(region).Get(name)
		if !found || j2.Status != inferenceRecommendationsJobStatusStopping {
			return
		}

		j2.Status = inferenceRecommendationsJobStatusStopped
		j2.LastModifiedTime = time.Now()
	})

	return nil
}

// ListInferenceRecommendationsJobsFilter narrows and orders the results of
// ListInferenceRecommendationsJobs (api_op_ListInferenceRecommendationsJobs.go:30-63).
type ListInferenceRecommendationsJobsFilter struct {
	CreationTimeAfter            *time.Time
	CreationTimeBefore           *time.Time
	LastModifiedTimeAfter        *time.Time
	LastModifiedTimeBefore       *time.Time
	NameContains                 string
	ModelNameEquals              string
	ModelPackageVersionArnEquals string
	StatusEquals                 string
	SortBy                       string
	SortOrder                    string
	MaxResults                   int32
}

// inferenceRecommendationsJobMatchesModelIdentity reports whether j's
// InputConfig matches filter's ModelNameEquals/ModelPackageVersionArnEquals,
// decoding the opaque InputConfig only when either filter is set.
func inferenceRecommendationsJobMatchesModelIdentity(
	j *InferenceRecommendationsJob,
	filter ListInferenceRecommendationsJobsFilter,
) bool {
	if filter.ModelNameEquals == "" && filter.ModelPackageVersionArnEquals == "" {
		return true
	}

	id := decodeInputConfigModelIdentity(j.InputConfig)
	if filter.ModelNameEquals != "" && id.ModelName != filter.ModelNameEquals {
		return false
	}

	return filter.ModelPackageVersionArnEquals == "" ||
		id.ModelPackageVersionArn == filter.ModelPackageVersionArnEquals
}

func inferenceRecommendationsJobMatchesFilter(
	j *InferenceRecommendationsJob,
	filter ListInferenceRecommendationsJobsFilter,
) bool {
	if filter.StatusEquals != "" && j.Status != filter.StatusEquals {
		return false
	}

	if filter.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.JobName), strings.ToLower(filter.NameContains)) {
		return false
	}

	if !inferenceRecommendationsJobMatchesModelIdentity(j, filter) {
		return false
	}

	if !timeWindowOK(j.CreationTime, filter.CreationTimeAfter, filter.CreationTimeBefore) {
		return false
	}

	return timeWindowOK(j.LastModifiedTime, filter.LastModifiedTimeAfter, filter.LastModifiedTimeBefore)
}

// lessInferenceRecommendationsJob orders a before b by sortBy
// (Name/Status/default CreationTime, tie-broken by name).
func lessInferenceRecommendationsJob(a, b *InferenceRecommendationsJob, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		return a.JobName < b.JobName
	case keyStatus:
		return a.Status < b.Status
	default:
		if a.CreationTime.Equal(b.CreationTime) {
			return a.JobName < b.JobName
		}

		return a.CreationTime.Before(b.CreationTime)
	}
}

// ListInferenceRecommendationsJobs returns jobs matching filter, sorted by
// filter.SortBy (default CreationTime) / filter.SortOrder (default Ascending).
func (b *InMemoryBackend) ListInferenceRecommendationsJobs(
	ctx context.Context,
	nextToken string,
	filter ListInferenceRecommendationsJobsFilter,
) ([]*InferenceRecommendationsJob, string) {
	b.mu.RLock("ListInferenceRecommendationsJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*InferenceRecommendationsJob, 0, b.inferenceRecommendationsJobsStoreRO(region).Len())

	for _, j := range b.inferenceRecommendationsJobsStoreRO(region).All() {
		if inferenceRecommendationsJobMatchesFilter(j, filter) {
			list = append(list, cloneInferenceRecommendationsJob(j))
		}
	}

	desc := strings.EqualFold(filter.SortOrder, "Descending")
	sort.Slice(list, func(i, k int) bool {
		less := lessInferenceRecommendationsJob(list[i], list[k], filter.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}
