package sagemaker

import (
	"context"
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
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrLabelingJobNotFound is returned when a labeling job does not exist.
	ErrLabelingJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrLabelingJobAlreadyExists is returned when a labeling job already exists.
	ErrLabelingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrSubscribedWorkteamNotFound is returned for DescribeSubscribedWorkteam,
	// since no Amazon Web Services Marketplace vendor subscriptions are modeled.
	ErrSubscribedWorkteamNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrWorkteamInUse is returned when deleting a workforce that still has work teams.
	ErrWorkteamInUse = awserr.New("ResourceInUse", awserr.ErrConflict)
)

const (
	labelingJobCompletionDelay = 300 * time.Millisecond
	labelingJobStopDelay       = 150 * time.Millisecond

	labelingJobStatusInitializing = "Initializing"
	labelingJobStatusInProgress   = "InProgress"
	labelingJobStatusCompleted    = "Completed"
	labelingJobStatusStopping     = "Stopping"
	labelingJobStatusStopped      = "Stopped"
)

// ---------------------------------------------------------------------------
// Labeling job configuration types
// ---------------------------------------------------------------------------

// LabelingJobS3DataSource locates the input manifest file in Amazon S3.
type LabelingJobS3DataSource struct {
	ManifestS3Uri string `json:"ManifestS3Uri"`
}

// LabelingJobSnsDataSource is an SNS topic used for streaming labeling jobs.
type LabelingJobSnsDataSource struct {
	SnsTopicArn string `json:"SnsTopicArn"`
}

// LabelingJobDataSource locates a labeling job's input data objects.
type LabelingJobDataSource struct {
	S3DataSource  *LabelingJobS3DataSource  `json:"S3DataSource,omitempty"`
	SnsDataSource *LabelingJobSnsDataSource `json:"SnsDataSource,omitempty"`
}

// LabelingJobDataAttributes describes customer-declared properties of the input data.
type LabelingJobDataAttributes struct {
	ContentClassifiers []string `json:"ContentClassifiers,omitempty"`
}

// LabelingJobInputConfig is the input configuration for a labeling job.
type LabelingJobInputConfig struct {
	DataSource     LabelingJobDataSource      `json:"DataSource"`
	DataAttributes *LabelingJobDataAttributes `json:"DataAttributes,omitempty"`
}

// LabelingJobOutputConfig is the output configuration for a labeling job.
type LabelingJobOutputConfig struct {
	S3OutputPath string `json:"S3OutputPath"`
	KmsKeyID     string `json:"KmsKeyId,omitempty"`
	SnsTopicArn  string `json:"SnsTopicArn,omitempty"`
}

// LabelingJobOutput locates the output produced by a labeling job.
type LabelingJobOutput struct {
	OutputDatasetS3Uri          string `json:"OutputDatasetS3Uri"`
	FinalActiveLearningModelArn string `json:"FinalActiveLearningModelArn,omitempty"`
}

// LabelingJobStoppingConditions bound the cost of a labeling job.
type LabelingJobStoppingConditions struct {
	MaxHumanLabeledObjectCount         int32 `json:"MaxHumanLabeledObjectCount,omitempty"`
	MaxPercentageOfInputDatasetLabeled int32 `json:"MaxPercentageOfInputDatasetLabeled,omitempty"`
}

// LabelingJobResourceConfig configures encryption of automated data labeling storage.
type LabelingJobResourceConfig struct {
	VpcConfig      *VpcConfig `json:"VpcConfig,omitempty"`
	VolumeKmsKeyID string     `json:"VolumeKmsKeyId,omitempty"`
}

// LabelingJobAlgorithmsConfig configures automated data labeling.
type LabelingJobAlgorithmsConfig struct {
	LabelingJobResourceConfig            *LabelingJobResourceConfig `json:"LabelingJobResourceConfig,omitempty"`
	LabelingJobAlgorithmSpecificationArn string                     `json:"LabelingJobAlgorithmSpecificationArn"`
	InitialActiveLearningModelArn        string                     `json:"InitialActiveLearningModelArn,omitempty"`
}

// UIConfigInfo describes the worker UI used to render labeling tasks.
type UIConfigInfo struct {
	UITemplateS3Uri string `json:"UiTemplateS3Uri,omitempty"`
	HumanTaskUIArn  string `json:"HumanTaskUiArn,omitempty"`
}

// HumanTaskConfig configures how a labeling task is presented to human workers.
type HumanTaskConfig struct {
	UIConfig                          UIConfigInfo `json:"UiConfig"`
	WorkteamArn                       string       `json:"WorkteamArn"`
	PreHumanTaskLambdaArn             string       `json:"PreHumanTaskLambdaArn,omitempty"`
	TaskTitle                         string       `json:"TaskTitle"`
	TaskDescription                   string       `json:"TaskDescription"`
	AnnotationConsolidationLambdaArn  string       `json:"AnnotationConsolidationLambdaArn,omitempty"`
	TaskKeywords                      []string     `json:"TaskKeywords,omitempty"`
	NumberOfHumanWorkersPerDataObject int32        `json:"NumberOfHumanWorkersPerDataObject"`
	TaskTimeLimitInSeconds            int32        `json:"TaskTimeLimitInSeconds"`
	TaskAvailabilityLifetimeInSeconds int32        `json:"TaskAvailabilityLifetimeInSeconds,omitempty"`
	MaxConcurrentTaskCount            int32        `json:"MaxConcurrentTaskCount,omitempty"`
}

// LabelCounters gives a breakdown of a labeling job's progress.
type LabelCounters struct {
	TotalLabeled            int32 `json:"TotalLabeled"`
	HumanLabeled            int32 `json:"HumanLabeled"`
	MachineLabeled          int32 `json:"MachineLabeled"`
	FailedNonRetryableError int32 `json:"FailedNonRetryableError"`
	Unlabeled               int32 `json:"Unlabeled"`
}

// LabelCountersForWorkteam gives a breakdown of human-labeled tasks for a workteam.
type LabelCountersForWorkteam struct {
	HumanLabeled int32 `json:"HumanLabeled"`
	PendingHuman int32 `json:"PendingHuman"`
	Total        int32 `json:"Total"`
}

// LabelingJob represents a SageMaker Ground Truth labeling job.
type LabelingJob struct {
	CreationTime                time.Time                      `json:"CreationTime"`
	LastModifiedTime            time.Time                      `json:"LastModifiedTime"`
	InputConfig                 LabelingJobInputConfig         `json:"InputConfig"`
	LabelingJobOutput           *LabelingJobOutput             `json:"LabelingJobOutput,omitempty"`
	StoppingConditions          *LabelingJobStoppingConditions `json:"StoppingConditions,omitempty"`
	LabelingJobAlgorithmsConfig *LabelingJobAlgorithmsConfig   `json:"LabelingJobAlgorithmsConfig,omitempty"`
	Tags                        map[string]string              `json:"Tags,omitempty"`
	OutputConfig                LabelingJobOutputConfig        `json:"OutputConfig"`
	LabelingJobName             string                         `json:"LabelingJobName"`
	LabelingJobArn              string                         `json:"LabelingJobArn"`
	LabelingJobStatus           string                         `json:"LabelingJobStatus"`
	JobReferenceCode            string                         `json:"JobReferenceCode"`
	RoleArn                     string                         `json:"RoleArn"`
	LabelAttributeName          string                         `json:"LabelAttributeName,omitempty"`
	LabelCategoryConfigS3Uri    string                         `json:"LabelCategoryConfigS3Uri,omitempty"`
	FailureReason               string                         `json:"FailureReason,omitempty"`
	HumanTaskConfig             HumanTaskConfig                `json:"HumanTaskConfig"`
	LabelCounters               LabelCounters                  `json:"LabelCounters"`
}

func cloneLabelingJob(j *LabelingJob) *LabelingJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.InputConfig.DataSource = j.InputConfig.DataSource

	if j.InputConfig.DataAttributes != nil {
		da := *j.InputConfig.DataAttributes
		da.ContentClassifiers = append([]string(nil), j.InputConfig.DataAttributes.ContentClassifiers...)
		cp.InputConfig.DataAttributes = &da
	}

	cp.HumanTaskConfig.TaskKeywords = append([]string(nil), j.HumanTaskConfig.TaskKeywords...)

	if j.LabelingJobOutput != nil {
		out := *j.LabelingJobOutput
		cp.LabelingJobOutput = &out
	}

	if j.StoppingConditions != nil {
		sc := *j.StoppingConditions
		cp.StoppingConditions = &sc
	}

	if j.LabelingJobAlgorithmsConfig != nil {
		ac := *j.LabelingJobAlgorithmsConfig
		cp.LabelingJobAlgorithmsConfig = &ac
	}

	return &cp
}

// CreateLabelingJobOptions holds the parameters for creating a labeling job.
type CreateLabelingJobOptions struct {
	StoppingConditions          *LabelingJobStoppingConditions
	LabelingJobAlgorithmsConfig *LabelingJobAlgorithmsConfig
	Tags                        map[string]string
	LabelingJobName             string
	LabelAttributeName          string
	RoleArn                     string
	LabelCategoryConfigS3Uri    string
	InputConfig                 LabelingJobInputConfig
	OutputConfig                LabelingJobOutputConfig
	HumanTaskConfig             HumanTaskConfig
}

func (b *InMemoryBackend) labelingJobsStore(r string) *store.Table[LabelingJob] {
	if b.labelingJobs[r] == nil {
		b.labelingJobs[r] = store.Register(
			b.registry,
			"labelingJobs:"+r,
			store.New(func(v *LabelingJob) string { return v.LabelingJobName }),
		)
	}

	return b.labelingJobs[r]
}

// labelingJobsStoreRO returns the region-scoped labelingJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) labelingJobsStoreRO(r string) *store.Table[LabelingJob] {
	if v := b.labelingJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *LabelingJob) string { return v.LabelingJobName })
}

// CreateLabelingJob creates and schedules a Ground Truth labeling job.
func (b *InMemoryBackend) CreateLabelingJob(
	ctx context.Context,
	opts CreateLabelingJobOptions,
) (*LabelingJob, error) {
	b.mu.Lock("CreateLabelingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.LabelingJobName == "" {
		return nil, fmt.Errorf("%w: LabelingJobName is required", ErrValidation)
	}

	store := b.labelingJobsStore(region)
	if _, ok := store.Get(opts.LabelingJobName); ok {
		return nil, fmt.Errorf(
			"%w: labeling job %q already exists", ErrLabelingJobAlreadyExists, opts.LabelingJobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "labeling-job/"+opts.LabelingJobName)
	now := time.Now()

	j := &LabelingJob{
		LabelingJobName:             opts.LabelingJobName,
		LabelingJobArn:              jobARN,
		LabelingJobStatus:           labelingJobStatusInitializing,
		JobReferenceCode:            generateID(),
		RoleArn:                     opts.RoleArn,
		LabelAttributeName:          opts.LabelAttributeName,
		LabelCategoryConfigS3Uri:    opts.LabelCategoryConfigS3Uri,
		InputConfig:                 opts.InputConfig,
		OutputConfig:                opts.OutputConfig,
		HumanTaskConfig:             opts.HumanTaskConfig,
		StoppingConditions:          opts.StoppingConditions,
		LabelingJobAlgorithmsConfig: opts.LabelingJobAlgorithmsConfig,
		Tags:                        mergeTags(nil, opts.Tags),
		CreationTime:                now,
		LastModifiedTime:            now,
	}
	store.Put(j)

	b.scheduleLabelingJobCompletion(b.lifecycleCtx, region, opts.LabelingJobName)

	return cloneLabelingJob(j), nil
}

// scheduleLabelingJobCompletion transitions a labeling job Initializing ->
// InProgress -> Completed. ctx must be b.lifecycleCtx captured by the caller
// while holding b.mu.
func (b *InMemoryBackend) scheduleLabelingJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, labelingJobStopDelay, func() {
		b.mu.Lock("scheduleLabelingJobCompletion.toInProgress")
		defer b.mu.Unlock()

		j, ok := b.labelingJobsStore(region).Get(name)
		if ok && j.LabelingJobStatus == labelingJobStatusInitializing {
			j.LabelingJobStatus = labelingJobStatusInProgress
			j.LastModifiedTime = time.Now()
		}
	})

	b.runDelayed(ctx, labelingJobCompletionDelay, func() {
		b.mu.Lock("scheduleLabelingJobCompletion.toCompleted")
		defer b.mu.Unlock()

		j, ok := b.labelingJobsStore(region).Get(name)
		if !ok || j.LabelingJobStatus != labelingJobStatusInProgress {
			return
		}

		now := time.Now()
		j.LabelingJobStatus = labelingJobStatusCompleted
		j.LastModifiedTime = now
		j.LabelCounters = LabelCounters{TotalLabeled: 1, HumanLabeled: 1}
		j.LabelingJobOutput = &LabelingJobOutput{
			OutputDatasetS3Uri: j.OutputConfig.S3OutputPath + j.LabelingJobName + "/manifests/output/output.manifest",
		}
	})
}

// DescribeLabelingJob returns a labeling job by name.
func (b *InMemoryBackend) DescribeLabelingJob(ctx context.Context, name string) (*LabelingJob, error) {
	b.mu.RLock("DescribeLabelingJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.labelingJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: labeling job %q not found", ErrLabelingJobNotFound, name)
	}

	return cloneLabelingJob(j), nil
}

// StopLabelingJob transitions a labeling job to Stopping then Stopped.
func (b *InMemoryBackend) StopLabelingJob(ctx context.Context, name string) error {
	b.mu.Lock("StopLabelingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.labelingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: labeling job %q not found", ErrLabelingJobNotFound, name)
	}

	if j.LabelingJobStatus == labelingJobStatusCompleted || j.LabelingJobStatus == labelingJobStatusStopped {
		return fmt.Errorf(
			"%w: labeling job %q is in status %s and cannot be stopped",
			ErrValidation, name, j.LabelingJobStatus,
		)
	}

	j.LabelingJobStatus = labelingJobStatusStopping
	j.LastModifiedTime = time.Now()

	b.runDelayed(b.lifecycleCtx, labelingJobStopDelay, func() {
		b.mu.Lock("StopLabelingJob.goroutine")
		defer b.mu.Unlock()

		if j2, ok2 := b.labelingJobsStore(region).Get(name); ok2 && j2.LabelingJobStatus == labelingJobStatusStopping {
			j2.LabelingJobStatus = labelingJobStatusStopped
			j2.LastModifiedTime = time.Now()
		}
	})

	return nil
}

// ListLabelingJobsFilter narrows the results of ListLabelingJobs
// (api_op_ListLabelingJobs.go:30-71). SortBy defaults to CreationTime,
// SortOrder to Ascending — both documented defaults for this op.
type ListLabelingJobsFilter struct {
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

// labelingJobMatchesFilter reports whether j satisfies every set field of filter.
func labelingJobMatchesFilter(j *LabelingJob, filter ListLabelingJobsFilter) bool {
	if filter.StatusEquals != "" && j.LabelingJobStatus != filter.StatusEquals {
		return false
	}

	if filter.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.LabelingJobName), strings.ToLower(filter.NameContains)) {
		return false
	}

	if filter.CreationTimeAfter != nil && !j.CreationTime.After(*filter.CreationTimeAfter) {
		return false
	}

	if filter.CreationTimeBefore != nil && !j.CreationTime.Before(*filter.CreationTimeBefore) {
		return false
	}

	if filter.LastModifiedTimeAfter != nil && !j.LastModifiedTime.After(*filter.LastModifiedTimeAfter) {
		return false
	}

	if filter.LastModifiedTimeBefore != nil && !j.LastModifiedTime.Before(*filter.LastModifiedTimeBefore) {
		return false
	}

	return true
}

// lessLabelingJob orders a before b by sortBy (Name/Status/default CreationTime,
// tie-broken by name).
func lessLabelingJob(a, b *LabelingJob, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		return a.LabelingJobName < b.LabelingJobName
	case keyStatus:
		return a.LabelingJobStatus < b.LabelingJobStatus
	default:
		if a.CreationTime.Equal(b.CreationTime) {
			return a.LabelingJobName < b.LabelingJobName
		}

		return a.CreationTime.Before(b.CreationTime)
	}
}

// ListLabelingJobs returns labeling jobs matching filter, sorted by
// filter.SortBy (default CreationTime) / filter.SortOrder (default Ascending).
func (b *InMemoryBackend) ListLabelingJobs(
	ctx context.Context,
	nextToken string,
	filter ListLabelingJobsFilter,
) ([]*LabelingJob, string) {
	b.mu.RLock("ListLabelingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*LabelingJob, 0, b.labelingJobsStoreRO(region).Len())

	for _, j := range b.labelingJobsStoreRO(region).All() {
		if labelingJobMatchesFilter(j, filter) {
			list = append(list, cloneLabelingJob(j))
		}
	}

	desc := strings.EqualFold(filter.SortOrder, "Descending")
	sort.Slice(list, func(i, k int) bool {
		less := lessLabelingJob(list[i], list[k], filter.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}

// ListLabelingJobsForWorkteamFilter narrows the results of
// ListLabelingJobsForWorkteam (api_op_ListLabelingJobsForWorkteam.go:30-64).
// SortBy has exactly one real value (CreationTime,
// types.ListLabelingJobsForWorkteamSortByOptions), so it is decoded for
// wire-shape fidelity but not threaded into the sort — CreationTime order is
// this op's only possible order regardless of the value sent.
type ListLabelingJobsForWorkteamFilter struct {
	CreationTimeAfter        *time.Time
	CreationTimeBefore       *time.Time
	JobReferenceCodeContains string
	SortOrder                string
	MaxResults               int32
}

// ListLabelingJobsForWorkteam returns labeling jobs assigned to a workteam.
func (b *InMemoryBackend) ListLabelingJobsForWorkteam(
	ctx context.Context,
	workteamArn, nextToken string,
	filter ListLabelingJobsForWorkteamFilter,
) ([]*LabelingJob, string) {
	b.mu.RLock("ListLabelingJobsForWorkteam")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*LabelingJob, 0, b.labelingJobsStoreRO(region).Len())

	for _, j := range b.labelingJobsStoreRO(region).All() {
		if j.HumanTaskConfig.WorkteamArn != workteamArn {
			continue
		}

		if filter.JobReferenceCodeContains != "" &&
			!strings.Contains(j.JobReferenceCode, filter.JobReferenceCodeContains) {
			continue
		}

		if filter.CreationTimeAfter != nil && !j.CreationTime.After(*filter.CreationTimeAfter) {
			continue
		}

		if filter.CreationTimeBefore != nil && !j.CreationTime.Before(*filter.CreationTimeBefore) {
			continue
		}

		list = append(list, cloneLabelingJob(j))
	}

	desc := strings.EqualFold(filter.SortOrder, "Descending")
	sort.Slice(list, func(i, k int) bool {
		var less bool
		if list[i].CreationTime.Equal(list[k].CreationTime) {
			less = list[i].LabelingJobName < list[k].LabelingJobName
		} else {
			less = list[i].CreationTime.Before(list[k].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}
