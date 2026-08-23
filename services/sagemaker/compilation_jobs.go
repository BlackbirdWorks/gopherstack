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

var (
	// ErrCompilationJobNotFound is returned when a compilation job does not exist.
	ErrCompilationJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrCompilationJobNotStoppable is returned when stopping an already-terminal compilation job.
	ErrCompilationJobNotStoppable = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const (
	jobStatusStopped               = "STOPPED"
	compilationJobStatusStopping   = "STOPPING"
	compilationJobStatusInProgress = "INPROGRESS"
)

// ---------------------------------------------------------------------------
// CompilationJob
// ---------------------------------------------------------------------------

// CompilationInputConfig specifies the model source for a Neo compilation job.
type CompilationInputConfig struct {
	S3Uri            string `json:"S3Uri,omitempty"`
	DataInputConfig  string `json:"DataInputConfig,omitempty"`
	Framework        string `json:"Framework,omitempty"`
	FrameworkVersion string `json:"FrameworkVersion,omitempty"`
}

// CompilationOutputConfig specifies the output destination for a Neo compilation job.
type CompilationOutputConfig struct {
	S3OutputLocation string `json:"S3OutputLocation,omitempty"`
	TargetDevice     string `json:"TargetDevice,omitempty"`
	KmsKeyID         string `json:"KmsKeyId,omitempty"`
}

// CompilationJob represents a SageMaker Neo compilation job.
type CompilationJob struct {
	CreationTime           time.Time                `json:"CreationTime"`
	LastModifiedTime       time.Time                `json:"LastModifiedTime"`
	CompilationStartTime   *time.Time               `json:"CompilationStartTime,omitempty"`
	CompilationEndTime     *time.Time               `json:"CompilationEndTime,omitempty"`
	Tags                   map[string]string        `json:"Tags,omitempty"`
	InputConfig            *CompilationInputConfig  `json:"InputConfig,omitempty"`
	OutputConfig           *CompilationOutputConfig `json:"OutputConfig,omitempty"`
	StoppingCondition      *StoppingCondition       `json:"StoppingCondition,omitempty"`
	ModelArtifacts         *ModelArtifacts          `json:"ModelArtifacts,omitempty"`
	CompilationJobName     string                   `json:"CompilationJobName"`
	CompilationJobArn      string                   `json:"CompilationJobArn"`
	CompilationJobStatus   string                   `json:"CompilationJobStatus"`
	RoleArn                string                   `json:"RoleArn,omitempty"`
	FailureReason          string                   `json:"FailureReason,omitempty"`
	ModelPackageVersionArn string                   `json:"ModelPackageVersionArn,omitempty"`
	VpcConfig              json.RawMessage          `json:"VpcConfig,omitempty"`
}

func cloneCompilationJob(j *CompilationJob) *CompilationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	if j.InputConfig != nil {
		ic := *j.InputConfig
		cp.InputConfig = &ic
	}

	if j.OutputConfig != nil {
		oc := *j.OutputConfig
		cp.OutputConfig = &oc
	}

	if j.StoppingCondition != nil {
		sc := *j.StoppingCondition
		cp.StoppingCondition = &sc
	}

	if j.ModelArtifacts != nil {
		ma := *j.ModelArtifacts
		cp.ModelArtifacts = &ma
	}

	cp.VpcConfig = append(json.RawMessage(nil), j.VpcConfig...)

	if j.CompilationStartTime != nil {
		t := *j.CompilationStartTime
		cp.CompilationStartTime = &t
	}

	if j.CompilationEndTime != nil {
		t := *j.CompilationEndTime
		cp.CompilationEndTime = &t
	}

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime/CompilationStartTime/
// CompilationEndTime as AWS awsjson1.1 epoch-seconds numbers rather than
// Go's default RFC3339 strings — this struct is marshaled directly by
// handleDescribeCompilationJob.
func (j *CompilationJob) MarshalJSON() ([]byte, error) {
	type alias CompilationJob

	return json.Marshal(struct {
		*alias
		CompilationStartTime *float64 `json:"CompilationStartTime,omitempty"`
		CompilationEndTime   *float64 `json:"CompilationEndTime,omitempty"`
		CreationTime         float64  `json:"CreationTime"`
		LastModifiedTime     float64  `json:"LastModifiedTime"`
	}{
		alias:                (*alias)(j),
		CreationTime:         epochSeconds(j.CreationTime),
		LastModifiedTime:     epochSeconds(j.LastModifiedTime),
		CompilationStartTime: epochSecondsPtr(j.CompilationStartTime),
		CompilationEndTime:   epochSecondsPtr(j.CompilationEndTime),
	})
}

// UnmarshalJSON is the inverse of [CompilationJob.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (j *CompilationJob) UnmarshalJSON(data []byte) error {
	type alias CompilationJob

	aux := struct {
		*alias
		CompilationStartTime *float64 `json:"CompilationStartTime,omitempty"`
		CompilationEndTime   *float64 `json:"CompilationEndTime,omitempty"`
		CreationTime         float64  `json:"CreationTime"`
		LastModifiedTime     float64  `json:"LastModifiedTime"`
	}{alias: (*alias)(j)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	j.CompilationStartTime = timeFromEpochSecondsPtr(aux.CompilationStartTime)
	j.CompilationEndTime = timeFromEpochSecondsPtr(aux.CompilationEndTime)
	j.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	j.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateCompilationJob creates a compilation job.
func (b *InMemoryBackend) CreateCompilationJob(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*CompilationJob, error) {
	b.mu.Lock("CreateCompilationJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", ErrValidation)
	}

	if _, ok := b.compilationJobsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: compilation job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "compilation-job/"+name)
	now := time.Now()

	j := &CompilationJob{
		CompilationJobName:   name,
		CompilationJobArn:    jobARN,
		CompilationJobStatus: compilationJobStatusInProgress,
		RoleArn:              roleArn,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         now,
		LastModifiedTime:     now,
		CompilationStartTime: &now,
	}
	b.compilationJobsStore(region).Put(j)

	b.scheduleCompilationJobCompletion(b.lifecycleCtx, region, name)

	return cloneCompilationJob(j), nil
}

// scheduleCompilationJobCompletion drives INPROGRESS -> COMPLETED after
// [compilationJobInProgressToCompleted], matching the auto-completion FSM
// already established for AIBenchmarkJob/AIRecommendationJob/TrainingJob.
// Previously nothing ever advanced this status past INPROGRESS unless a
// client called StopCompilationJob -- the fourth instance of this
// campaign's stuck-status bug class, this time "stuck" because no
// completion path existed at all rather than a Stopping transition that
// never resolved.
func (b *InMemoryBackend) scheduleCompilationJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, compilationJobInProgressToCompleted, func() {
		b.mu.Lock("scheduleCompilationJobCompletion.goroutine")
		defer b.mu.Unlock()

		j, ok := b.compilationJobsStore(region).Get(name)
		if !ok || j.CompilationJobStatus != compilationJobStatusInProgress {
			return
		}

		now := time.Now()
		j.CompilationJobStatus = "COMPLETED"
		j.CompilationEndTime = &now
		j.LastModifiedTime = now

		if j.OutputConfig != nil && j.OutputConfig.S3OutputLocation != "" {
			j.ModelArtifacts = &ModelArtifacts{
				S3ModelArtifacts: strings.TrimSuffix(j.OutputConfig.S3OutputLocation, "/") + "/model.tar.gz",
			}
		}
	})
}

// DescribeCompilationJob returns a compilation job by name.
func (b *InMemoryBackend) DescribeCompilationJob(ctx context.Context, name string) (*CompilationJob, error) {
	b.mu.RLock("DescribeCompilationJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.compilationJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	return cloneCompilationJob(j), nil
}

// DeleteCompilationJob removes a compilation job by name.
func (b *InMemoryBackend) DeleteCompilationJob(ctx context.Context, name string) error {
	b.mu.Lock("DeleteCompilationJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.compilationJobsStore(region).Get(name); !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	store := b.compilationJobsStore(region)
	store.Delete(name)

	return nil
}

// StopCompilationJob transitions a running compilation job
// INPROGRESS -> STOPPING -> STOPPED (api_op_StopCompilationJob.go:16-19:
// "Amazon SageMaker AI changes the CompilationJobStatus of the job to
// Stopping. After Amazon SageMaker stops the job, it sets the
// CompilationJobStatus to Stopped."). Previously this set STOPPED directly
// with no intermediate STOPPING state at all, contradicting the op's own
// doc -- a client polling Describe right after Stop would never observe
// STOPPING, only ever see the terminal state a call later than real AWS.
func (b *InMemoryBackend) StopCompilationJob(ctx context.Context, name string) error {
	b.mu.Lock("StopCompilationJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.compilationJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	// AWS rejects stopping a job that is not currently running.
	if j.CompilationJobStatus != compilationJobStatusInProgress {
		return fmt.Errorf("%w: compilation job %q is not running (status: %s)",
			ErrCompilationJobNotStoppable, name, j.CompilationJobStatus)
	}

	j.CompilationJobStatus = compilationJobStatusStopping
	j.LastModifiedTime = time.Now()

	b.runDelayed(b.lifecycleCtx, compilationJobStoppingToStopped, func() {
		b.mu.Lock("StopCompilationJob.goroutine")
		defer b.mu.Unlock()

		j2, ok2 := b.compilationJobsStore(region).Get(name)
		if !ok2 || j2.CompilationJobStatus != compilationJobStatusStopping {
			return
		}

		now := time.Now()
		j2.CompilationJobStatus = jobStatusStopped
		j2.CompilationEndTime = &now
		j2.LastModifiedTime = now
	})

	return nil
}

// ListCompilationJobsFilter bundles the filter/sort criteria for
// ListCompilationJobs (api_op_ListCompilationJobs.go:33-73).
type ListCompilationJobsFilter struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NameContains           string
	StatusEquals           string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// ListCompilationJobs lists compilation jobs, optionally filtered and
// sorted. Previously this decoded only NextToken and dropped every filter
// and sort control the op's own request shape declares.
func (b *InMemoryBackend) ListCompilationJobs(
	ctx context.Context,
	nextToken string,
	f ListCompilationJobsFilter,
) ([]*CompilationJob, string) {
	b.mu.RLock("ListCompilationJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*CompilationJob, 0, b.compilationJobsStoreRO(region).Len())

	for _, j := range b.compilationJobsStoreRO(region).All() {
		if compilationJobMatchesFilter(j, f) {
			list = append(list, cloneCompilationJob(j))
		}
	}

	// api_op_ListCompilationJobs.go:64,68: real defaults are
	// CreationTime/Ascending -- the reverse SortOrder default from
	// ListAIBenchmarkJobs, confirmed per-op rather than generalized.
	desc := f.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, k int) bool {
		var less bool

		switch f.SortBy {
		case keyGenericName:
			less = list[i].CompilationJobName < list[k].CompilationJobName
		case keyStatus:
			less = list[i].CompilationJobStatus < list[k].CompilationJobStatus
		default:
			less = list[i].CreationTime.Before(list[k].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, f.MaxResults)
}

func compilationJobMatchesFilter(j *CompilationJob, f ListCompilationJobsFilter) bool {
	if f.StatusEquals != "" && j.CompilationJobStatus != f.StatusEquals {
		return false
	}

	if f.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.CompilationJobName), strings.ToLower(f.NameContains)) {
		return false
	}

	if !timeWindowOK(j.CreationTime, f.CreationTimeAfter, f.CreationTimeBefore) {
		return false
	}

	return timeWindowOK(j.LastModifiedTime, f.LastModifiedTimeAfter, f.LastModifiedTimeBefore)
}

// SetCompilationJobExtras sets optional configuration fields on an existing compilation job
// that were not included in the original CreateCompilationJob signature.
func (b *InMemoryBackend) SetCompilationJobExtras(
	ctx context.Context,
	name string,
	inputConfig *CompilationInputConfig,
	outputConfig *CompilationOutputConfig,
	stoppingCondition *StoppingCondition,
	modelPackageVersionArn string,
	vpcConfig json.RawMessage,
) error {
	b.mu.Lock("SetCompilationJobExtras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.compilationJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	if inputConfig != nil {
		ic := *inputConfig
		j.InputConfig = &ic
	}

	if outputConfig != nil {
		oc := *outputConfig
		j.OutputConfig = &oc
	}

	if stoppingCondition != nil {
		sc := *stoppingCondition
		j.StoppingCondition = &sc
	}

	if modelPackageVersionArn != "" {
		j.ModelPackageVersionArn = modelPackageVersionArn
	}

	if len(vpcConfig) > 0 {
		j.VpcConfig = vpcConfig
	}

	j.LastModifiedTime = time.Now()

	return nil
}
