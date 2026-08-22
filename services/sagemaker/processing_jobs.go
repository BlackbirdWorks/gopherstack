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

var (
	// ErrProcessingJobNotFound is returned when a processing job does not exist.
	ErrProcessingJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrProcessingJobAlreadyExists is returned when a processing job already exists.
	ErrProcessingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ---------------------------------------------------------------------------
// ProcessingJob (#12, partial)
// ---------------------------------------------------------------------------

// ProcessingInput specifies input data for a processing job.
type ProcessingInput struct {
	S3Input           *ProcessingS3Input `json:"S3Input,omitempty"`
	DatasetDefinition *DatasetDefinition `json:"DatasetDefinition,omitempty"`
	InputName         string             `json:"InputName"`
	AppManaged        bool               `json:"AppManaged,omitempty"`
}

// ProcessingS3Input references S3 input data.
type ProcessingS3Input struct {
	S3Uri                  string `json:"S3Uri"`
	LocalPath              string `json:"LocalPath"`
	S3DataType             string `json:"S3DataType,omitempty"`
	S3InputMode            string `json:"S3InputMode,omitempty"`
	S3DataDistributionType string `json:"S3DataDistributionType,omitempty"`
	S3CompressionType      string `json:"S3CompressionType,omitempty"`
}

// DatasetDefinition is an alternative to S3Input for Athena/Redshift.
type DatasetDefinition struct {
	DataDistributionType string `json:"DataDistributionType,omitempty"`
	InputMode            string `json:"InputMode,omitempty"`
}

// ProcessingOutput specifies output data for a processing job.
type ProcessingOutput struct {
	S3Output   *ProcessingS3Output `json:"S3Output,omitempty"`
	OutputName string              `json:"OutputName"`
	AppManaged bool                `json:"AppManaged,omitempty"`
}

// ProcessingS3Output specifies S3 output for a processing step.
type ProcessingS3Output struct {
	S3Uri        string `json:"S3Uri"`
	LocalPath    string `json:"LocalPath"`
	S3UploadMode string `json:"S3UploadMode,omitempty"`
}

// ProcessingOutputConfig wraps outputs plus optional KMS key.
type ProcessingOutputConfig struct {
	KmsKeyID string             `json:"KmsKeyId,omitempty"`
	Outputs  []ProcessingOutput `json:"Outputs,omitempty"`
}

// ProcessingResources specifies compute for a processing job.
type ProcessingResources struct {
	ClusterConfig ProcessingClusterConfig `json:"ClusterConfig"`
}

// ProcessingClusterConfig is the compute config for a processing job.
type ProcessingClusterConfig struct {
	InstanceType   string `json:"InstanceType"`
	VolumeKmsKeyID string `json:"VolumeKmsKeyId,omitempty"`
	InstanceCount  int32  `json:"InstanceCount"`
	VolumeSizeInGB int32  `json:"VolumeSizeInGB"`
}

// ProcessingAppSpec identifies the container image for a processing job.
type ProcessingAppSpec struct {
	ImageURI            string   `json:"ImageUri"`
	ContainerArguments  []string `json:"ContainerArguments,omitempty"`
	ContainerEntrypoint []string `json:"ContainerEntrypoint,omitempty"`
}

// ProcessingNetworkConfig mirrors types.NetworkConfig
// (api_op_CreateProcessingJob.go's NetworkConfig field). The request's VPC
// settings nest under NetworkConfig.VpcConfig -- there is no top-level
// VpcConfig on CreateProcessingJobInput at all.
type ProcessingNetworkConfig struct {
	VpcConfig                             *VpcConfig `json:"VpcConfig,omitempty"`
	EnableInterContainerTrafficEncryption *bool      `json:"EnableInterContainerTrafficEncryption,omitempty"`
	EnableNetworkIsolation                *bool      `json:"EnableNetworkIsolation,omitempty"`
}

// ProcessingExperimentConfig mirrors types.ExperimentConfig
// (types/types.go — ExperimentName/RunName/TrialComponentDisplayName/TrialName).
type ProcessingExperimentConfig struct {
	ExperimentName            string `json:"ExperimentName,omitempty"`
	RunName                   string `json:"RunName,omitempty"`
	TrialComponentDisplayName string `json:"TrialComponentDisplayName,omitempty"`
	TrialName                 string `json:"TrialName,omitempty"`
}

// ProcessingStoppingCondition mirrors types.ProcessingStoppingCondition
// (MaxRuntimeInSeconds, "This member is required" on the type itself, but the
// type as a whole is an optional CreateProcessingJobInput field).
type ProcessingStoppingCondition struct {
	MaxRuntimeInSeconds int32 `json:"MaxRuntimeInSeconds"`
}

// ProcessingJob represents a SageMaker processing job.
type ProcessingJob struct {
	CreationTime           time.Time                    `json:"CreationTime"`
	LastModifiedTime       time.Time                    `json:"LastModifiedTime"`
	ProcessingStartTime    *time.Time                   `json:"ProcessingStartTime,omitempty"`
	ProcessingEndTime      *time.Time                   `json:"ProcessingEndTime,omitempty"`
	Tags                   map[string]string            `json:"Tags,omitempty"`
	Environment            map[string]string            `json:"Environment,omitempty"`
	NetworkConfig          *ProcessingNetworkConfig     `json:"NetworkConfig,omitempty"`
	ExperimentConfig       *ProcessingExperimentConfig  `json:"ExperimentConfig,omitempty"`
	StoppingCondition      *ProcessingStoppingCondition `json:"StoppingCondition,omitempty"`
	ProcessingResources    ProcessingResources          `json:"ProcessingResources"`
	ProcessingOutputConfig ProcessingOutputConfig       `json:"ProcessingOutputConfig"`
	ProcessingJobName      string                       `json:"ProcessingJobName"`
	ProcessingJobArn       string                       `json:"ProcessingJobArn"`
	ProcessingJobStatus    string                       `json:"ProcessingJobStatus"`
	RoleArn                string                       `json:"RoleArn,omitempty"`
	FailureReason          string                       `json:"FailureReason,omitempty"`
	AppSpecification       ProcessingAppSpec            `json:"AppSpecification"`
	ProcessingInputs       []ProcessingInput            `json:"ProcessingInputs,omitempty"`
}

// cloneProcessingJob returns a deep copy of pj.
func cloneProcessingJob(pj *ProcessingJob) *ProcessingJob {
	cp := *pj
	cp.Tags = maps.Clone(pj.Tags)
	cp.Environment = maps.Clone(pj.Environment)
	cp.ProcessingInputs = make([]ProcessingInput, len(pj.ProcessingInputs))
	for i, inp := range pj.ProcessingInputs {
		pi := inp
		if inp.S3Input != nil {
			s3 := *inp.S3Input
			pi.S3Input = &s3
		}
		if inp.DatasetDefinition != nil {
			dd := *inp.DatasetDefinition
			pi.DatasetDefinition = &dd
		}
		cp.ProcessingInputs[i] = pi
	}
	cp.ProcessingOutputConfig.Outputs = make(
		[]ProcessingOutput,
		len(pj.ProcessingOutputConfig.Outputs),
	)
	for i, out := range pj.ProcessingOutputConfig.Outputs {
		po := out
		if out.S3Output != nil {
			s3 := *out.S3Output
			po.S3Output = &s3
		}
		cp.ProcessingOutputConfig.Outputs[i] = po
	}

	return &cp
}

// CreateProcessingJob creates and schedules a processing job.
func (b *InMemoryBackend) CreateProcessingJob(ctx context.Context, opts ProcessingJob) (*ProcessingJob, error) {
	b.mu.Lock("CreateProcessingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.processingJobsStore(region).Get(opts.ProcessingJobName); ok {
		return nil, fmt.Errorf(
			"%w: processing job %s already exists",
			ErrProcessingJobAlreadyExists,
			opts.ProcessingJobName,
		)
	}

	pjARN := arn.Build("sagemaker", region, b.accountID, "processing-job/"+opts.ProcessingJobName)
	now := time.Now()
	pj := &ProcessingJob{
		ProcessingJobName:      opts.ProcessingJobName,
		ProcessingJobArn:       pjARN,
		ProcessingJobStatus:    trainingJobStatusInProgress,
		RoleArn:                opts.RoleArn,
		AppSpecification:       opts.AppSpecification,
		ProcessingInputs:       opts.ProcessingInputs,
		ProcessingOutputConfig: opts.ProcessingOutputConfig,
		ProcessingResources:    opts.ProcessingResources,
		NetworkConfig:          opts.NetworkConfig,
		ExperimentConfig:       opts.ExperimentConfig,
		StoppingCondition:      opts.StoppingCondition,
		Environment:            maps.Clone(opts.Environment),
		CreationTime:           now,
		LastModifiedTime:       now,
		ProcessingStartTime:    &now,
		Tags:                   mergeTags(nil, opts.Tags),
	}
	b.processingJobsStore(region).Put(pj)
	b.processingJobARNIndexStore(region)[pjARN] = opts.ProcessingJobName

	b.scheduleProcessingCompletion(b.lifecycleCtx, region, opts.ProcessingJobName)

	return cloneProcessingJob(pj), nil
}

// scheduleProcessingCompletion transitions a processing job to Completed.
// ctx must be b.lifecycleCtx captured by the caller while holding b.mu.
// region must be captured by the caller before the lock is released.
func (b *InMemoryBackend) scheduleProcessingCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, processingJobCompletionDelay, func() {
		b.mu.Lock("scheduleProcessingCompletion.goroutine")
		defer b.mu.Unlock()

		pj, ok := b.processingJobsStore(region).Get(name)
		if !ok || pj.ProcessingJobStatus != "InProgress" {
			return
		}
		now := time.Now()
		pj.ProcessingJobStatus = algorithmStatusCompleted
		pj.ProcessingEndTime = &now
		pj.LastModifiedTime = now
	})
}

// DescribeProcessingJob returns a processing job by name.
func (b *InMemoryBackend) DescribeProcessingJob(ctx context.Context, name string) (*ProcessingJob, error) {
	b.mu.RLock("DescribeProcessingJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	pj, ok := b.processingJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: processing job %q not found", ErrProcessingJobNotFound, name)
	}

	return cloneProcessingJob(pj), nil
}

// StopProcessingJob transitions a processing job to Stopping then Stopped.
func (b *InMemoryBackend) StopProcessingJob(ctx context.Context, name string) error {
	b.mu.Lock("StopProcessingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	pj, ok := b.processingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: processing job %q not found", ErrProcessingJobNotFound, name)
	}

	pj.ProcessingJobStatus = "Stopping"
	pj.LastModifiedTime = time.Now()

	b.runDelayed(b.lifecycleCtx, processingJobStopDelay, func() {
		b.mu.Lock("StopProcessingJob.goroutine")
		defer b.mu.Unlock()

		pj2, ok2 := b.processingJobsStore(region).Get(name)
		if ok2 && pj2.ProcessingJobStatus == notebookStatusStopping {
			pj2.ProcessingJobStatus = pipelineStatusStopped
			pj2.LastModifiedTime = time.Now()
		}
	})

	return nil
}

// DeleteProcessingJob removes a processing job's record. Only terminal
// (non-InProgress, non-Stopping) jobs may be deleted.
func (b *InMemoryBackend) DeleteProcessingJob(ctx context.Context, name string) error {
	b.mu.Lock("DeleteProcessingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	pj, ok := b.processingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: processing job %q not found", ErrProcessingJobNotFound, name)
	}

	if pj.ProcessingJobStatus == trainingJobStatusInProgress || pj.ProcessingJobStatus == notebookStatusStopping {
		return fmt.Errorf(
			"%w: processing job %q cannot be deleted while status is %q",
			ErrValidation, name, pj.ProcessingJobStatus,
		)
	}

	b.processingJobsStore(region).Delete(name)
	delete(b.processingJobARNIndexStore(region), pj.ProcessingJobArn)

	return nil
}

// ListProcessingJobsFilter holds optional filters for ListProcessingJobs,
// mirroring ListProcessingJobsInput (api_op_ListProcessingJobs.go). SortBy
// defaults to CreationTime, SortOrder defaults to Ascending per that op's own
// doc.
type ListProcessingJobsFilter struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	StatusEquals           string
	NameContains           string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// ListProcessingJobs returns processing jobs matching filter.
func (b *InMemoryBackend) ListProcessingJobs(
	ctx context.Context,
	nextToken string,
	filter ListProcessingJobsFilter,
) ([]*ProcessingJob, string) {
	b.mu.RLock("ListProcessingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*ProcessingJob, 0, b.processingJobsStoreRO(region).Len())
	for _, pj := range b.processingJobsStoreRO(region).All() {
		if filter.StatusEquals != "" && !strings.EqualFold(pj.ProcessingJobStatus, filter.StatusEquals) {
			continue
		}

		if filter.NameContains != "" && !strings.Contains(pj.ProcessingJobName, filter.NameContains) {
			continue
		}

		if !timeWindowOK(pj.CreationTime, filter.CreationTimeAfter, filter.CreationTimeBefore) {
			continue
		}

		if !timeWindowOK(pj.LastModifiedTime, filter.LastModifiedTimeAfter, filter.LastModifiedTimeBefore) {
			continue
		}

		list = append(list, cloneProcessingJob(pj))
	}

	desc := filter.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, k int) bool {
		var less bool

		switch filter.SortBy {
		case keyGenericName:
			less = list[i].ProcessingJobName < list[k].ProcessingJobName
		case sortByStatus:
			less = list[i].ProcessingJobStatus < list[k].ProcessingJobStatus
		default:
			less = list[i].CreationTime.Before(list[k].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}
