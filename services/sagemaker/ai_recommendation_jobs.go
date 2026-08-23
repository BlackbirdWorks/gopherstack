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
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// AIRecommendationJob — analyzes a model source against an AIWorkloadConfig
// and produces deployment-configuration recommendations. Distinct from
// InferenceRecommendationsJob (inference_recommendations_jobs.go, an older,
// separately-keyed family with its own JobType/JobDescription shape): this
// family references AIWorkloadConfig, carries ModelSource/PerformanceTarget/
// ComputeSpec, and lives in its own store (b.aiRecommendationJobs).
// ---------------------------------------------------------------------------

// ErrAIRecommendationJobNotFound is returned when an AI recommendation job
// does not exist. Field-diffed against deserializers.go: Describe/Stop/
// Delete AIRecommendationJob all recognize only a "ResourceNotFound" wire
// exception.
var ErrAIRecommendationJobNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)

// ErrAIRecommendationJobAlreadyExists is returned on a duplicate AIRecommendationJobName.
var ErrAIRecommendationJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)

// AIRecommendationJob represents a SageMaker AI recommendation job.
// ModelSource, OutputConfig, PerformanceTarget, ComputeSpec, and
// InferenceSpecification are stored as opaque JSON (the json.RawMessage
// passthrough convention already used by algorithms.go): all are
// union/config shapes whose server-computed sub-fields are optional in
// DescribeAIRecommendationJobOutput, so echoing the client's Create payload
// back verbatim is wire-accurate. Recommendations is intentionally never
// populated by this backend — a real job would fill it with measured
// benchmark results, which this synthetic backend does not fabricate.
type AIRecommendationJob struct {
	CreationTime               time.Time         `json:"CreationTime"`
	StartTime                  *time.Time        `json:"StartTime,omitempty"`
	EndTime                    *time.Time        `json:"EndTime,omitempty"`
	OptimizeModel              *bool             `json:"OptimizeModel,omitempty"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	AIRecommendationJobName    string            `json:"AIRecommendationJobName"`
	AIRecommendationJobArn     string            `json:"AIRecommendationJobArn"`
	AIRecommendationJobStatus  string            `json:"AIRecommendationJobStatus"`
	AIWorkloadConfigIdentifier string            `json:"AIWorkloadConfigIdentifier"`
	RoleArn                    string            `json:"RoleArn"`
	FailureReason              string            `json:"FailureReason,omitempty"`
	ModelSource                json.RawMessage   `json:"ModelSource"`
	OutputConfig               json.RawMessage   `json:"OutputConfig"`
	PerformanceTarget          json.RawMessage   `json:"PerformanceTarget,omitempty"`
	ComputeSpec                json.RawMessage   `json:"ComputeSpec,omitempty"`
	InferenceSpecification     json.RawMessage   `json:"InferenceSpecification,omitempty"`
	AdapterSource              json.RawMessage   `json:"AdapterSource,omitempty"`
}

// MarshalJSON emits CreationTime/StartTime/EndTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings.
func (j *AIRecommendationJob) MarshalJSON() ([]byte, error) {
	type alias AIRecommendationJob

	return json.Marshal(struct {
		*alias
		StartTime    *float64 `json:"StartTime,omitempty"`
		EndTime      *float64 `json:"EndTime,omitempty"`
		CreationTime float64  `json:"CreationTime"`
	}{
		alias:        (*alias)(j),
		CreationTime: epochSeconds(j.CreationTime),
		StartTime:    epochSecondsPtr(j.StartTime),
		EndTime:      epochSecondsPtr(j.EndTime),
	})
}

// UnmarshalJSON is the inverse of [AIRecommendationJob.MarshalJSON], used by
// persistence.go's snapshot restore path.
func (j *AIRecommendationJob) UnmarshalJSON(data []byte) error {
	type alias AIRecommendationJob

	aux := struct {
		*alias
		StartTime    *float64 `json:"StartTime,omitempty"`
		EndTime      *float64 `json:"EndTime,omitempty"`
		CreationTime float64  `json:"CreationTime"`
	}{alias: (*alias)(j)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	j.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	j.StartTime = timeFromEpochSecondsPtr(aux.StartTime)
	j.EndTime = timeFromEpochSecondsPtr(aux.EndTime)

	return nil
}

func cloneAIRecommendationJob(j *AIRecommendationJob) *AIRecommendationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.ModelSource = append(json.RawMessage(nil), j.ModelSource...)
	cp.OutputConfig = append(json.RawMessage(nil), j.OutputConfig...)
	cp.PerformanceTarget = append(json.RawMessage(nil), j.PerformanceTarget...)
	cp.ComputeSpec = append(json.RawMessage(nil), j.ComputeSpec...)
	cp.InferenceSpecification = append(json.RawMessage(nil), j.InferenceSpecification...)
	cp.AdapterSource = append(json.RawMessage(nil), j.AdapterSource...)

	if j.StartTime != nil {
		t := *j.StartTime
		cp.StartTime = &t
	}

	if j.EndTime != nil {
		t := *j.EndTime
		cp.EndTime = &t
	}

	if j.OptimizeModel != nil {
		v := *j.OptimizeModel
		cp.OptimizeModel = &v
	}

	return &cp
}

func (b *InMemoryBackend) aiRecommendationJobsStore(r string) *store.Table[AIRecommendationJob] {
	if b.aiRecommendationJobs[r] == nil {
		b.aiRecommendationJobs[r] = store.Register(
			b.registry,
			"aiRecommendationJobs:"+r,
			store.New(func(v *AIRecommendationJob) string { return v.AIRecommendationJobName }),
		)
	}

	return b.aiRecommendationJobs[r]
}

// aiRecommendationJobsStoreRO returns the region-scoped aiRecommendationJobs
// table for r without mutating the outer map. Safe to call while holding
// only b.mu.RLock(): if the region has not been observed yet, it returns a
// fresh, unregistered, empty view instead of lazily creating (and
// persisting) an entry.
func (b *InMemoryBackend) aiRecommendationJobsStoreRO(r string) *store.Table[AIRecommendationJob] {
	if v := b.aiRecommendationJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *AIRecommendationJob) string { return v.AIRecommendationJobName })
}

// CreateAIRecommendationJobOptions holds input fields for CreateAIRecommendationJob.
type CreateAIRecommendationJobOptions struct {
	OptimizeModel              *bool
	Tags                       map[string]string
	AIRecommendationJobName    string
	AIWorkloadConfigIdentifier string
	RoleArn                    string
	ModelSource                json.RawMessage
	OutputConfig               json.RawMessage
	PerformanceTarget          json.RawMessage
	ComputeSpec                json.RawMessage
	InferenceSpecification     json.RawMessage
	AdapterSource              json.RawMessage
}

// CreateAIRecommendationJob creates an AI recommendation job and schedules
// its InProgress -> Completed transition after [aiJobInProgressToCompleted].
func (b *InMemoryBackend) CreateAIRecommendationJob(
	ctx context.Context,
	opts CreateAIRecommendationJobOptions,
) (*AIRecommendationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAIRecommendationJob")
	defer b.mu.Unlock()

	if err := validateCreateAIRecommendationJobOptions(opts); err != nil {
		return nil, err
	}

	if _, err := b.resolveAIWorkloadConfigLocked(region, opts.AIWorkloadConfigIdentifier); err != nil {
		return nil, err
	}

	tbl := b.aiRecommendationJobsStore(region)
	if _, ok := tbl.Get(opts.AIRecommendationJobName); ok {
		return nil, fmt.Errorf(
			"%w: AI recommendation job %q already exists",
			ErrAIRecommendationJobAlreadyExists,
			opts.AIRecommendationJobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "ai-recommendation-job/"+opts.AIRecommendationJobName)
	now := time.Now()

	j := &AIRecommendationJob{
		AIRecommendationJobName:    opts.AIRecommendationJobName,
		AIRecommendationJobArn:     jobARN,
		AIRecommendationJobStatus:  trainingJobStatusInProgress,
		AIWorkloadConfigIdentifier: opts.AIWorkloadConfigIdentifier,
		RoleArn:                    opts.RoleArn,
		ModelSource:                opts.ModelSource,
		OutputConfig:               opts.OutputConfig,
		PerformanceTarget:          opts.PerformanceTarget,
		ComputeSpec:                opts.ComputeSpec,
		InferenceSpecification:     opts.InferenceSpecification,
		AdapterSource:              opts.AdapterSource,
		OptimizeModel:              opts.OptimizeModel,
		Tags:                       mergeTags(nil, opts.Tags),
		CreationTime:               now,
		StartTime:                  &now,
	}
	tbl.Put(j)

	b.scheduleAIRecommendationJobCompletion(b.lifecycleCtx, region, opts.AIRecommendationJobName)

	return cloneAIRecommendationJob(j), nil
}

func validateCreateAIRecommendationJobOptions(opts CreateAIRecommendationJobOptions) error {
	switch {
	case opts.AIRecommendationJobName == "":
		return fmt.Errorf("%w: AIRecommendationJobName is required", ErrValidation)
	case opts.AIWorkloadConfigIdentifier == "":
		return fmt.Errorf("%w: AIWorkloadConfigIdentifier is required", ErrValidation)
	case opts.RoleArn == "":
		return fmt.Errorf("%w: RoleArn is required", ErrValidation)
	case len(opts.ModelSource) == 0:
		return fmt.Errorf("%w: ModelSource is required", ErrValidation)
	case len(opts.OutputConfig) == 0:
		return fmt.Errorf("%w: OutputConfig is required", ErrValidation)
	case len(opts.PerformanceTarget) == 0:
		return fmt.Errorf("%w: PerformanceTarget is required", ErrValidation)
	default:
		return nil
	}
}

// scheduleAIRecommendationJobCompletion drives InProgress -> Completed after
// [aiJobInProgressToCompleted]. Recommendations is deliberately left empty —
// see [AIRecommendationJob]'s doc comment.
func (b *InMemoryBackend) scheduleAIRecommendationJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, aiJobInProgressToCompleted, func() {
		b.mu.Lock("scheduleAIRecommendationJobCompletion.goroutine")
		defer b.mu.Unlock()

		j, ok := b.aiRecommendationJobsStore(region).Get(name)
		if !ok || j.AIRecommendationJobStatus != trainingJobStatusInProgress {
			return
		}

		now := time.Now()
		j.AIRecommendationJobStatus = algorithmStatusCompleted
		j.EndTime = &now
	})
}

// DescribeAIRecommendationJob returns an AI recommendation job by name.
func (b *InMemoryBackend) DescribeAIRecommendationJob(
	ctx context.Context,
	name string,
) (*AIRecommendationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAIRecommendationJob")
	defer b.mu.RUnlock()

	j, ok := b.aiRecommendationJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: AI recommendation job %q not found", ErrAIRecommendationJobNotFound, name)
	}

	return cloneAIRecommendationJob(j), nil
}

// DeleteAIRecommendationJob removes an AI recommendation job by name,
// returning its ARN.
func (b *InMemoryBackend) DeleteAIRecommendationJob(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAIRecommendationJob")
	defer b.mu.Unlock()

	tbl := b.aiRecommendationJobsStore(region)

	j, ok := tbl.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: AI recommendation job %q not found", ErrAIRecommendationJobNotFound, name)
	}

	tbl.Delete(name)

	return j.AIRecommendationJobArn, nil
}

// StopAIRecommendationJob transitions an in-progress AI recommendation job
// InProgress -> Stopping -> Stopped, returning its ARN. Stopping a job that
// is not currently InProgress is a no-op (idempotent).
func (b *InMemoryBackend) StopAIRecommendationJob(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)
	notFound := fmt.Errorf("%w: AI recommendation job %q not found", ErrAIRecommendationJobNotFound, name)

	return stopSimpleJobFSM(
		b, "StopAIRecommendationJob", region, name,
		b.aiRecommendationJobsStore,
		notFound,
		func(j *AIRecommendationJob) string { return j.AIRecommendationJobStatus },
		func(j *AIRecommendationJob, status string) { j.AIRecommendationJobStatus = status },
		func(j *AIRecommendationJob, t time.Time) { j.EndTime = &t },
		func(j *AIRecommendationJob) string { return j.AIRecommendationJobArn },
	)
}

// ListAIRecommendationJobsParams bundles the filter/sort criteria for ListAIRecommendationJobs.
type ListAIRecommendationJobsParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	NameContains       string
	StatusEquals       string
	SortBy             string
	SortOrder          string
	NextToken          string
	MaxResults         int32
}

// ListAIRecommendationJobs lists AI recommendation jobs, optionally filtered and sorted.
func (b *InMemoryBackend) ListAIRecommendationJobs(
	ctx context.Context,
	params ListAIRecommendationJobsParams,
) ([]*AIRecommendationJob, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAIRecommendationJobs")
	defer b.mu.RUnlock()

	tbl := b.aiRecommendationJobsStoreRO(region)
	list := make([]*AIRecommendationJob, 0, tbl.Len())

	for _, j := range tbl.All() {
		if !aiRecommendationJobMatchesFilter(j, params) {
			continue
		}

		list = append(list, cloneAIRecommendationJob(j))
	}

	sortAIRecommendationJobs(list, params.SortBy, params.SortOrder)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func aiRecommendationJobMatchesFilter(j *AIRecommendationJob, params ListAIRecommendationJobsParams) bool {
	if params.StatusEquals != "" && j.AIRecommendationJobStatus != params.StatusEquals {
		return false
	}

	if params.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.AIRecommendationJobName), strings.ToLower(params.NameContains)) {
		return false
	}

	if params.CreationTimeAfter != nil && !j.CreationTime.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !j.CreationTime.Before(*params.CreationTimeBefore) {
		return false
	}

	return true
}

// sortAIRecommendationJobs orders list by sortBy/sortOrder. The op's own
// doc (api_op_ListAIRecommendationJobs.go:51,55) states real defaults of
// CreationTime/Descending -- the reverse of what an empty sortBy/sortOrder
// would naively fall through to (Name/Ascending), so both defaults are
// made explicit rather than left to a switch's zero-value default case.
func sortAIRecommendationJobs(list []*AIRecommendationJob, sortBy, sortOrder string) {
	desc := sortOrder != sortOrderAscending

	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch sortBy {
		case keyGenericName:
			less = list[i].AIRecommendationJobName < list[j].AIRecommendationJobName
		case keyStatus:
			less = list[i].AIRecommendationJobStatus < list[j].AIRecommendationJobStatus
		default:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})
}
