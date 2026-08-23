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
// AIBenchmarkJob — runs synthetic performance benchmarks against an
// inference endpoint using a named AIWorkloadConfig. Distinct from every
// other SageMaker job type in this service: it is the only family that
// references an AIWorkloadConfig, and it lives entirely in its own store
// (b.aiBenchmarkJobs), never touching TrainingJob/ProcessingJob/etc. state.
// ---------------------------------------------------------------------------

// ErrAIBenchmarkJobNotFound is returned when an AI benchmark job does not
// exist. Field-diffed against deserializers.go: Describe/Stop/Delete
// AIBenchmarkJob all recognize only a "ResourceNotFound" wire exception.
var ErrAIBenchmarkJobNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)

// ErrAIBenchmarkJobAlreadyExists is returned on a duplicate AIBenchmarkJobName.
var ErrAIBenchmarkJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)

// AIBenchmarkJob represents a SageMaker AI benchmark job. BenchmarkTarget,
// OutputConfig, and NetworkConfig are stored as opaque JSON (the
// json.RawMessage passthrough convention already used by algorithms.go):
// BenchmarkTarget and NetworkConfig are union/config shapes whose only
// server-computed sub-fields (e.g. OutputConfig's CloudWatchLogs) are
// optional in DescribeAIBenchmarkJobOutput, so echoing the client's Create
// payload back verbatim is wire-accurate for every field a real client
// would read.
type AIBenchmarkJob struct {
	CreationTime               time.Time         `json:"CreationTime"`
	StartTime                  *time.Time        `json:"StartTime,omitempty"`
	EndTime                    *time.Time        `json:"EndTime,omitempty"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	AIBenchmarkJobName         string            `json:"AIBenchmarkJobName"`
	AIBenchmarkJobArn          string            `json:"AIBenchmarkJobArn"`
	AIBenchmarkJobStatus       string            `json:"AIBenchmarkJobStatus"`
	AIWorkloadConfigIdentifier string            `json:"AIWorkloadConfigIdentifier"`
	RoleArn                    string            `json:"RoleArn"`
	FailureReason              string            `json:"FailureReason,omitempty"`
	BenchmarkTarget            json.RawMessage   `json:"BenchmarkTarget"`
	OutputConfig               json.RawMessage   `json:"OutputConfig"`
	NetworkConfig              json.RawMessage   `json:"NetworkConfig,omitempty"`
}

// MarshalJSON emits CreationTime/StartTime/EndTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings.
func (j *AIBenchmarkJob) MarshalJSON() ([]byte, error) {
	type alias AIBenchmarkJob

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

// UnmarshalJSON is the inverse of [AIBenchmarkJob.MarshalJSON], used by
// persistence.go's snapshot restore path.
func (j *AIBenchmarkJob) UnmarshalJSON(data []byte) error {
	type alias AIBenchmarkJob

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

func cloneAIBenchmarkJob(j *AIBenchmarkJob) *AIBenchmarkJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.BenchmarkTarget = append(json.RawMessage(nil), j.BenchmarkTarget...)
	cp.OutputConfig = append(json.RawMessage(nil), j.OutputConfig...)
	cp.NetworkConfig = append(json.RawMessage(nil), j.NetworkConfig...)

	if j.StartTime != nil {
		t := *j.StartTime
		cp.StartTime = &t
	}

	if j.EndTime != nil {
		t := *j.EndTime
		cp.EndTime = &t
	}

	return &cp
}

func (b *InMemoryBackend) aiBenchmarkJobsStore(r string) *store.Table[AIBenchmarkJob] {
	if b.aiBenchmarkJobs[r] == nil {
		b.aiBenchmarkJobs[r] = store.Register(
			b.registry,
			"aiBenchmarkJobs:"+r,
			store.New(func(v *AIBenchmarkJob) string { return v.AIBenchmarkJobName }),
		)
	}

	return b.aiBenchmarkJobs[r]
}

// aiBenchmarkJobsStoreRO returns the region-scoped aiBenchmarkJobs table for
// r without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a
// fresh, unregistered, empty view instead of lazily creating (and
// persisting) an entry.
func (b *InMemoryBackend) aiBenchmarkJobsStoreRO(r string) *store.Table[AIBenchmarkJob] {
	if v := b.aiBenchmarkJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *AIBenchmarkJob) string { return v.AIBenchmarkJobName })
}

// CreateAIBenchmarkJobOptions holds input fields for CreateAIBenchmarkJob.
type CreateAIBenchmarkJobOptions struct {
	Tags                       map[string]string
	AIBenchmarkJobName         string
	AIWorkloadConfigIdentifier string
	RoleArn                    string
	BenchmarkTarget            json.RawMessage
	OutputConfig               json.RawMessage
	NetworkConfig              json.RawMessage
}

// CreateAIBenchmarkJob creates an AI benchmark job and schedules its
// InProgress -> Completed transition after [aiJobInProgressToCompleted].
func (b *InMemoryBackend) CreateAIBenchmarkJob(
	ctx context.Context,
	opts CreateAIBenchmarkJobOptions,
) (*AIBenchmarkJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAIBenchmarkJob")
	defer b.mu.Unlock()

	if err := validateCreateAIBenchmarkJobOptions(opts); err != nil {
		return nil, err
	}

	if _, err := b.resolveAIWorkloadConfigLocked(region, opts.AIWorkloadConfigIdentifier); err != nil {
		return nil, err
	}

	tbl := b.aiBenchmarkJobsStore(region)
	if _, ok := tbl.Get(opts.AIBenchmarkJobName); ok {
		return nil, fmt.Errorf(
			"%w: AI benchmark job %q already exists",
			ErrAIBenchmarkJobAlreadyExists,
			opts.AIBenchmarkJobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "ai-benchmark-job/"+opts.AIBenchmarkJobName)
	now := time.Now()

	j := &AIBenchmarkJob{
		AIBenchmarkJobName:         opts.AIBenchmarkJobName,
		AIBenchmarkJobArn:          jobARN,
		AIBenchmarkJobStatus:       trainingJobStatusInProgress,
		AIWorkloadConfigIdentifier: opts.AIWorkloadConfigIdentifier,
		RoleArn:                    opts.RoleArn,
		BenchmarkTarget:            opts.BenchmarkTarget,
		OutputConfig:               opts.OutputConfig,
		NetworkConfig:              opts.NetworkConfig,
		Tags:                       mergeTags(nil, opts.Tags),
		CreationTime:               now,
		StartTime:                  &now,
	}
	tbl.Put(j)

	b.scheduleAIBenchmarkJobCompletion(b.lifecycleCtx, region, opts.AIBenchmarkJobName)

	return cloneAIBenchmarkJob(j), nil
}

func validateCreateAIBenchmarkJobOptions(opts CreateAIBenchmarkJobOptions) error {
	switch {
	case opts.AIBenchmarkJobName == "":
		return fmt.Errorf("%w: AIBenchmarkJobName is required", ErrValidation)
	case opts.AIWorkloadConfigIdentifier == "":
		return fmt.Errorf("%w: AIWorkloadConfigIdentifier is required", ErrValidation)
	case opts.RoleArn == "":
		return fmt.Errorf("%w: RoleArn is required", ErrValidation)
	case len(opts.BenchmarkTarget) == 0:
		return fmt.Errorf("%w: BenchmarkTarget is required", ErrValidation)
	case len(opts.OutputConfig) == 0:
		return fmt.Errorf("%w: OutputConfig is required", ErrValidation)
	default:
		return nil
	}
}

// scheduleAIBenchmarkJobCompletion drives InProgress -> Completed after
// [aiJobInProgressToCompleted]. No benchmark metrics are fabricated: the
// job's OutputConfig.S3OutputLocation is where a real AWS benchmark would
// have written measured results, and this backend does not synthesize a
// value there.
func (b *InMemoryBackend) scheduleAIBenchmarkJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, aiJobInProgressToCompleted, func() {
		b.mu.Lock("scheduleAIBenchmarkJobCompletion.goroutine")
		defer b.mu.Unlock()

		j, ok := b.aiBenchmarkJobsStore(region).Get(name)
		if !ok || j.AIBenchmarkJobStatus != trainingJobStatusInProgress {
			return
		}

		now := time.Now()
		j.AIBenchmarkJobStatus = algorithmStatusCompleted
		j.EndTime = &now
	})
}

// DescribeAIBenchmarkJob returns an AI benchmark job by name.
func (b *InMemoryBackend) DescribeAIBenchmarkJob(ctx context.Context, name string) (*AIBenchmarkJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAIBenchmarkJob")
	defer b.mu.RUnlock()

	j, ok := b.aiBenchmarkJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: AI benchmark job %q not found", ErrAIBenchmarkJobNotFound, name)
	}

	return cloneAIBenchmarkJob(j), nil
}

// DeleteAIBenchmarkJob removes an AI benchmark job by name, returning its ARN.
func (b *InMemoryBackend) DeleteAIBenchmarkJob(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAIBenchmarkJob")
	defer b.mu.Unlock()

	tbl := b.aiBenchmarkJobsStore(region)

	j, ok := tbl.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: AI benchmark job %q not found", ErrAIBenchmarkJobNotFound, name)
	}

	tbl.Delete(name)

	return j.AIBenchmarkJobArn, nil
}

// StopAIBenchmarkJob transitions an in-progress AI benchmark job
// InProgress -> Stopping -> Stopped, returning its ARN. Stopping a job that
// is not currently InProgress is a no-op (idempotent), matching the FSM
// convention already used by StopTrainingJobFSM.
func (b *InMemoryBackend) StopAIBenchmarkJob(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)
	notFound := fmt.Errorf("%w: AI benchmark job %q not found", ErrAIBenchmarkJobNotFound, name)

	return stopSimpleJobFSM(
		b, "StopAIBenchmarkJob", region, name,
		b.aiBenchmarkJobsStore,
		notFound,
		func(j *AIBenchmarkJob) string { return j.AIBenchmarkJobStatus },
		func(j *AIBenchmarkJob, status string) { j.AIBenchmarkJobStatus = status },
		func(j *AIBenchmarkJob, t time.Time) { j.EndTime = &t },
		func(j *AIBenchmarkJob) string { return j.AIBenchmarkJobArn },
	)
}

// ListAIBenchmarkJobsParams bundles the filter/sort criteria for ListAIBenchmarkJobs.
type ListAIBenchmarkJobsParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	NameContains       string
	StatusEquals       string
	SortBy             string
	SortOrder          string
	NextToken          string
	MaxResults         int32
}

// ListAIBenchmarkJobs lists AI benchmark jobs, optionally filtered and sorted.
func (b *InMemoryBackend) ListAIBenchmarkJobs(
	ctx context.Context,
	params ListAIBenchmarkJobsParams,
) ([]*AIBenchmarkJob, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAIBenchmarkJobs")
	defer b.mu.RUnlock()

	tbl := b.aiBenchmarkJobsStoreRO(region)
	list := make([]*AIBenchmarkJob, 0, tbl.Len())

	for _, j := range tbl.All() {
		if !aiBenchmarkJobMatchesFilter(j, params) {
			continue
		}

		list = append(list, cloneAIBenchmarkJob(j))
	}

	sortAIBenchmarkJobs(list, params.SortBy, params.SortOrder)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func aiBenchmarkJobMatchesFilter(j *AIBenchmarkJob, params ListAIBenchmarkJobsParams) bool {
	if params.StatusEquals != "" && j.AIBenchmarkJobStatus != params.StatusEquals {
		return false
	}

	if params.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.AIBenchmarkJobName), strings.ToLower(params.NameContains)) {
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

// sortAIBenchmarkJobs orders list by sortBy/sortOrder. The op's own doc
// (api_op_ListAIBenchmarkJobs.go:44,49) states real defaults of
// CreationTime/Descending — the reverse of what an empty sortBy/sortOrder
// would naively fall through to (Name/Ascending), so both defaults are
// made explicit rather than left to a switch's zero-value default case.
func sortAIBenchmarkJobs(list []*AIBenchmarkJob, sortBy, sortOrder string) {
	desc := sortOrder != sortOrderAscending

	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch sortBy {
		case keyGenericName:
			less = list[i].AIBenchmarkJobName < list[j].AIBenchmarkJobName
		case keyStatus:
			less = list[i].AIBenchmarkJobStatus < list[j].AIBenchmarkJobStatus
		default:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})
}

// aiWorkloadConfigNameFromIdentifier derives a bare workload-config name
// from an AIWorkloadConfigIdentifier that may be either a name or an ARN,
// for AIBenchmarkJobSummary.AIWorkloadConfigName (which — unlike the Create
// input and Describe output — is always the bare name, never an ARN).
func aiWorkloadConfigNameFromIdentifier(identifier string) string {
	if idx := strings.LastIndex(identifier, "/"); idx != -1 {
		return identifier[idx+1:]
	}

	return identifier
}
