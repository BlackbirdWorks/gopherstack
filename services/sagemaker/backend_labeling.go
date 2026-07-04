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
	Tags                        map[string]string              `json:"-"`
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

func (b *InMemoryBackend) labelingJobsStore(r string) map[string]*LabelingJob {
	if b.labelingJobs[r] == nil {
		b.labelingJobs[r] = make(map[string]*LabelingJob)
	}

	return b.labelingJobs[r]
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
	if _, ok := store[opts.LabelingJobName]; ok {
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
	store[opts.LabelingJobName] = j

	b.scheduleLabelingJobCompletion(b.lifecycleCtx, region, opts.LabelingJobName)

	return cloneLabelingJob(j), nil
}

// scheduleLabelingJobCompletion transitions a labeling job Initializing ->
// InProgress -> Completed. ctx must be b.lifecycleCtx captured by the caller
// while holding b.mu.
func (b *InMemoryBackend) scheduleLabelingJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, labelingJobStopDelay, func() {
		b.mu.Lock("scheduleLabelingJobCompletion.toInProgress")
		j, ok := b.labelingJobsStore(region)[name]
		if ok && j.LabelingJobStatus == labelingJobStatusInitializing {
			j.LabelingJobStatus = labelingJobStatusInProgress
			j.LastModifiedTime = time.Now()
		}
		b.mu.Unlock()
	})

	b.runDelayed(ctx, labelingJobCompletionDelay, func() {
		b.mu.Lock("scheduleLabelingJobCompletion.toCompleted")
		defer b.mu.Unlock()

		j, ok := b.labelingJobsStore(region)[name]
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

	j, ok := b.labelingJobsStore(region)[name]
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

	j, ok := b.labelingJobsStore(region)[name]
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

		if j2, ok2 := b.labelingJobsStore(region)[name]; ok2 && j2.LabelingJobStatus == labelingJobStatusStopping {
			j2.LabelingJobStatus = labelingJobStatusStopped
			j2.LastModifiedTime = time.Now()
		}
	})

	return nil
}

// ListLabelingJobsFilter narrows the results of ListLabelingJobs.
type ListLabelingJobsFilter struct {
	NameContains string
	StatusEquals string
	MaxResults   int32
}

// ListLabelingJobs returns labeling jobs sorted by creation time.
func (b *InMemoryBackend) ListLabelingJobs(
	ctx context.Context,
	nextToken string,
	filter ListLabelingJobsFilter,
) ([]*LabelingJob, string) {
	b.mu.RLock("ListLabelingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*LabelingJob, 0, len(b.labelingJobsStore(region)))

	for _, j := range b.labelingJobsStore(region) {
		if filter.StatusEquals != "" && j.LabelingJobStatus != filter.StatusEquals {
			continue
		}

		if filter.NameContains != "" &&
			!strings.Contains(strings.ToLower(j.LabelingJobName), strings.ToLower(filter.NameContains)) {
			continue
		}

		list = append(list, cloneLabelingJob(j))
	}

	sort.Slice(list, func(i, k int) bool {
		if list[i].CreationTime.Equal(list[k].CreationTime) {
			return list[i].LabelingJobName < list[k].LabelingJobName
		}

		return list[i].CreationTime.Before(list[k].CreationTime)
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}

// ListLabelingJobsForWorkteam returns labeling jobs assigned to a workteam.
func (b *InMemoryBackend) ListLabelingJobsForWorkteam(
	ctx context.Context,
	workteamArn, nextToken string,
	maxResults int32,
) ([]*LabelingJob, string) {
	b.mu.RLock("ListLabelingJobsForWorkteam")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*LabelingJob, 0, len(b.labelingJobsStore(region)))

	for _, j := range b.labelingJobsStore(region) {
		if j.HumanTaskConfig.WorkteamArn == workteamArn {
			list = append(list, cloneLabelingJob(j))
		}
	}

	sort.Slice(list, func(i, k int) bool {
		if list[i].CreationTime.Equal(list[k].CreationTime) {
			return list[i].LabelingJobName < list[k].LabelingJobName
		}

		return list[i].CreationTime.Before(list[k].CreationTime)
	})

	return paginateSlice(list, nextToken, maxResults)
}
