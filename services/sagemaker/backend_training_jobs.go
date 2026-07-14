package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrTrainingJobNotFound is returned when a training job does not exist.
	ErrTrainingJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrTrainingJobAlreadyExists is returned when a training job already exists.
	ErrTrainingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

type TrainingJob struct {
	LastModifiedTime                      time.Time                   `json:"LastModifiedTime"`
	CreationTime                          time.Time                   `json:"CreationTime"`
	VpcConfig                             *VpcConfig                  `json:"VpcConfig,omitempty"`
	TrainingStartTime                     *time.Time                  `json:"TrainingStartTime,omitempty"`
	TrainingEndTime                       *time.Time                  `json:"TrainingEndTime,omitempty"`
	Tags                                  map[string]string           `json:"Tags,omitempty"`
	HyperParameters                       map[string]string           `json:"HyperParameters,omitempty"`
	Environment                           map[string]string           `json:"Environment,omitempty"`
	ModelArtifacts                        *ModelArtifacts             `json:"ModelArtifacts,omitempty"`
	CheckpointConfig                      *CheckpointConfig           `json:"CheckpointConfig,omitempty"`
	OutputDataConfig                      OutputDataConfig            `json:"OutputDataConfig"`
	SecondaryStatus                       string                      `json:"SecondaryStatus,omitempty"`
	FailureReason                         string                      `json:"FailureReason,omitempty"`
	TrainingJobName                       string                      `json:"TrainingJobName"`
	TrainingJobArn                        string                      `json:"TrainingJobArn"`
	RoleArn                               string                      `json:"RoleArn,omitempty"`
	TrainingJobStatus                     string                      `json:"TrainingJobStatus"`
	InputDataConfig                       []Channel                   `json:"InputDataConfig,omitempty"`
	SecondaryStatusTransitions            []SecondaryStatusTransition `json:"SecondaryStatusTransitions,omitempty"`
	AlgorithmSpecification                AlgorithmSpecification      `json:"AlgorithmSpecification"`
	ResourceConfig                        ResourceConfig              `json:"ResourceConfig"`
	StoppingCondition                     StoppingCondition           `json:"StoppingCondition"`
	BillableTimeInSeconds                 int32                       `json:"BillableTimeInSeconds,omitempty"`
	TrainingTimeInSeconds                 int32                       `json:"TrainingTimeInSeconds,omitempty"`
	EnableNetworkIsolation                bool                        `json:"EnableNetworkIsolation,omitempty"`
	EnableManagedSpotTraining             bool                        `json:"EnableManagedSpotTraining,omitempty"`
	EnableInterContainerTrafficEncryption bool                        `json:"EnableInterContainerTrafficEncryption,omitempty"` //nolint:lll // AWS API field name exceeds 120 chars; cannot be shortened
}

// cloneTrainingJob returns a deep copy of tj.
func cloneTrainingJob(tj *TrainingJob) *TrainingJob {
	cp := *tj
	cp.Tags = maps.Clone(tj.Tags)
	cp.HyperParameters = maps.Clone(tj.HyperParameters)
	cp.Environment = maps.Clone(tj.Environment)
	cp.InputDataConfig = make([]Channel, len(tj.InputDataConfig))
	copy(cp.InputDataConfig, tj.InputDataConfig)
	cp.SecondaryStatusTransitions = make(
		[]SecondaryStatusTransition,
		len(tj.SecondaryStatusTransitions),
	)
	copy(cp.SecondaryStatusTransitions, tj.SecondaryStatusTransitions)
	if tj.VpcConfig != nil {
		vpc := *tj.VpcConfig
		vpc.SecurityGroupIDs = append([]string(nil), tj.VpcConfig.SecurityGroupIDs...)
		vpc.Subnets = append([]string(nil), tj.VpcConfig.Subnets...)
		cp.VpcConfig = &vpc
	}
	if tj.CheckpointConfig != nil {
		cc := *tj.CheckpointConfig
		cp.CheckpointConfig = &cc
	}
	if tj.ModelArtifacts != nil {
		ma := *tj.ModelArtifacts
		cp.ModelArtifacts = &ma
	}

	return &cp
}

// NotebookInstance represents a SageMaker notebook instance.

// ---------------------------------------------------------------------------
// TrainingJob
// ---------------------------------------------------------------------------

// CreateTrainingJob creates a new training job (legacy signature, kept for compatibility).
func (b *InMemoryBackend) CreateTrainingJob(
	ctx context.Context,
	name, roleArn string,
	algorithmSpec map[string]string,
	tags map[string]string,
) (*TrainingJob, error) {
	spec := AlgorithmSpecification{
		TrainingImage:     algorithmSpec["TrainingImage"],
		AlgorithmName:     algorithmSpec["AlgorithmName"],
		TrainingInputMode: algorithmSpec["TrainingInputMode"],
	}

	return b.CreateTrainingJobFull(ctx, TrainingJobOptions{
		TrainingJobName:        name,
		RoleArn:                roleArn,
		AlgorithmSpecification: spec,
		Tags:                   tags,
	})
}

// DescribeTrainingJob returns a training job by name.
func (b *InMemoryBackend) DescribeTrainingJob(ctx context.Context, name string) (*TrainingJob, error) {
	b.mu.RLock("DescribeTrainingJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tj, ok := b.trainingJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: training job %q not found", ErrTrainingJobNotFound, name)
	}

	return cloneTrainingJob(tj), nil
}

// ListTrainingJobs returns training jobs sorted by name with optional pagination.
func (b *InMemoryBackend) ListTrainingJobs(ctx context.Context, nextToken string) ([]*TrainingJob, string) {
	b.mu.RLock("ListTrainingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.trainingJobsStoreRO(region), nextToken, cloneTrainingJob,
		func(a, b *TrainingJob) bool { return a.TrainingJobName < b.TrainingJobName })
}

// StopTrainingJob marks a training job as Stopping.
func (b *InMemoryBackend) StopTrainingJob(ctx context.Context, name string) error {
	b.mu.Lock("StopTrainingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tj, ok := b.trainingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: training job %q not found", ErrTrainingJobNotFound, name)
	}

	tj.TrainingJobStatus = pipelineStatusStopping
	tj.LastModifiedTime = time.Now()

	return nil
}

// DeleteTrainingJob removes a training job from the backend.
func (b *InMemoryBackend) DeleteTrainingJob(ctx context.Context, name string) error {
	b.mu.Lock("DeleteTrainingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tj, ok := b.trainingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: training job %q not found", ErrTrainingJobNotFound, name)
	}

	arnIdx := b.trainingJobARNIndexStore(region)
	delete(arnIdx, tj.TrainingJobArn)
	store := b.trainingJobsStore(region)
	store.Delete(name)

	return nil
}

// ---------------------------------------------------------------------------
// Training job lifecycle FSM + expanded struct (#4, #5, #6)
// ---------------------------------------------------------------------------

// AlgorithmSpecification is the typed algorithm spec for a training job.
type AlgorithmSpecification struct {
	TrainingImage                    string             `json:"TrainingImage,omitempty"`
	AlgorithmName                    string             `json:"AlgorithmName,omitempty"`
	TrainingInputMode                string             `json:"TrainingInputMode,omitempty"`
	MetricDefinitions                []MetricDefinition `json:"MetricDefinitions,omitempty"`
	ContainerEntrypoint              []string           `json:"ContainerEntrypoint,omitempty"`
	ContainerArguments               []string           `json:"ContainerArguments,omitempty"`
	EnableSageMakerMetricsTimeSeries bool               `json:"EnableSageMakerMetricsTimeSeries,omitempty"`
}

// MetricDefinition maps a metric name to a regex.
type MetricDefinition struct {
	Name  string `json:"Name"`
	Regex string `json:"Regex,omitempty"`
}

// ChannelDataSource holds either an S3 or file system data source.
type ChannelDataSource struct {
	S3DataSource *S3DataSource `json:"S3DataSource,omitempty"`
}

// S3DataSource references an S3 location for training data.
type S3DataSource struct {
	S3Uri                  string `json:"S3Uri"`
	S3DataType             string `json:"S3DataType,omitempty"`
	S3DataDistributionType string `json:"S3DataDistributionType,omitempty"`
}

// Channel is one input data channel for a training job.
type Channel struct {
	DataSource        ChannelDataSource `json:"DataSource"`
	ChannelName       string            `json:"ChannelName"`
	ContentType       string            `json:"ContentType,omitempty"`
	CompressionType   string            `json:"CompressionType,omitempty"`
	RecordWrapperType string            `json:"RecordWrapperType,omitempty"`
	InputMode         string            `json:"InputMode,omitempty"`
}

// OutputDataConfig specifies where training output is stored.
type OutputDataConfig struct {
	S3OutputPath    string `json:"S3OutputPath"`
	KmsKeyID        string `json:"KmsKeyId,omitempty"`
	CompressionType string `json:"CompressionType,omitempty"`
}

// ResourceConfig specifies compute resources for a training job.
type ResourceConfig struct {
	InstanceType             string          `json:"InstanceType"`
	VolumeKmsKeyID           string          `json:"VolumeKmsKeyId,omitempty"`
	InstanceGroups           []InstanceGroup `json:"InstanceGroups,omitempty"`
	InstanceCount            int32           `json:"InstanceCount"`
	VolumeSizeInGB           int32           `json:"VolumeSizeInGB"`
	KeepAlivePeriodInSeconds int32           `json:"KeepAlivePeriodInSeconds,omitempty"`
}

// InstanceGroup is a heterogeneous instance group in a training job.
type InstanceGroup struct {
	InstanceGroupName string `json:"InstanceGroupName"`
	InstanceType      string `json:"InstanceType"`
	InstanceCount     int32  `json:"InstanceCount"`
}

// StoppingCondition defines the maximum run time for a training job.
type StoppingCondition struct {
	MaxRuntimeInSeconds     int32 `json:"MaxRuntimeInSeconds,omitempty"`
	MaxWaitTimeInSeconds    int32 `json:"MaxWaitTimeInSeconds,omitempty"`
	MaxPendingTimeInSeconds int32 `json:"MaxPendingTimeInSeconds,omitempty"`
}

// VpcConfig specifies the VPC subnets and security groups.
type VpcConfig struct {
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
	Subnets          []string `json:"Subnets,omitempty"`
}

// CheckpointConfig stores checkpoint location for managed spot.
type CheckpointConfig struct {
	S3Uri     string `json:"S3Uri"`
	LocalPath string `json:"LocalPath,omitempty"`
}

// ModelArtifacts references the S3 model output of a training job.
type ModelArtifacts struct {
	S3ModelArtifacts string `json:"S3ModelArtifacts"`
}

// SecondaryStatusTransition records a FSM step in a training job.
type SecondaryStatusTransition struct {
	StartTime     time.Time  `json:"StartTime"`
	EndTime       *time.Time `json:"EndTime,omitempty"`
	Status        string     `json:"Status"`
	StatusMessage string     `json:"StatusMessage,omitempty"`
}

// TrainingJobOptions holds all fields for CreateTrainingJob.
type TrainingJobOptions struct {
	Tags                                  map[string]string      `json:"Tags,omitempty"`
	Environment                           map[string]string      `json:"Environment,omitempty"`
	HyperParameters                       map[string]string      `json:"HyperParameters,omitempty"`
	CheckpointConfig                      *CheckpointConfig      `json:"CheckpointConfig,omitempty"`
	VpcConfig                             *VpcConfig             `json:"VpcConfig,omitempty"`
	OutputDataConfig                      OutputDataConfig       `json:"OutputDataConfig"`
	TrainingJobName                       string                 `json:"TrainingJobName"`
	RoleArn                               string                 `json:"RoleArn"`
	InputDataConfig                       []Channel              `json:"InputDataConfig,omitempty"`
	AlgorithmSpecification                AlgorithmSpecification `json:"AlgorithmSpecification"`
	ResourceConfig                        ResourceConfig         `json:"ResourceConfig"`
	StoppingCondition                     StoppingCondition      `json:"StoppingCondition"`
	EnableNetworkIsolation                bool                   `json:"EnableNetworkIsolation,omitempty"`
	EnableManagedSpotTraining             bool                   `json:"EnableManagedSpotTraining,omitempty"`
	EnableInterContainerTrafficEncryption bool                   `json:"EnableInterContainerTrafficEncryption,omitempty"`
}

// CreateTrainingJobFull creates a training job from a full options struct
// and schedules InProgress → Completed after a short delay.
func (b *InMemoryBackend) CreateTrainingJobFull(ctx context.Context, opts TrainingJobOptions) (*TrainingJob, error) {
	b.mu.Lock("CreateTrainingJobFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.trainingJobsStore(region).Get(opts.TrainingJobName); ok {
		return nil, fmt.Errorf(
			"%w: training job %s already exists",
			ErrTrainingJobAlreadyExists,
			opts.TrainingJobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "training-job/"+opts.TrainingJobName)
	now := time.Now()

	tj := &TrainingJob{
		TrainingJobName:                       opts.TrainingJobName,
		TrainingJobArn:                        jobARN,
		TrainingJobStatus:                     trainingJobStatusInProgress,
		SecondaryStatus:                       "Starting",
		RoleArn:                               opts.RoleArn,
		AlgorithmSpecification:                opts.AlgorithmSpecification,
		InputDataConfig:                       opts.InputDataConfig,
		OutputDataConfig:                      opts.OutputDataConfig,
		ResourceConfig:                        opts.ResourceConfig,
		StoppingCondition:                     opts.StoppingCondition,
		VpcConfig:                             opts.VpcConfig,
		CheckpointConfig:                      opts.CheckpointConfig,
		HyperParameters:                       maps.Clone(opts.HyperParameters),
		Environment:                           maps.Clone(opts.Environment),
		CreationTime:                          now,
		LastModifiedTime:                      now,
		TrainingStartTime:                     &now,
		Tags:                                  mergeTags(nil, opts.Tags),
		EnableNetworkIsolation:                opts.EnableNetworkIsolation,
		EnableManagedSpotTraining:             opts.EnableManagedSpotTraining,
		EnableInterContainerTrafficEncryption: opts.EnableInterContainerTrafficEncryption,
		SecondaryStatusTransitions: []SecondaryStatusTransition{
			{StartTime: now, Status: "Starting", StatusMessage: "Launching requested ML instances"},
		},
	}
	b.trainingJobsStore(region).Put(tj)
	b.trainingJobARNIndexStore(region)[jobARN] = opts.TrainingJobName

	b.scheduleTrainingCompletion(b.lifecycleCtx, region, opts.TrainingJobName)

	return cloneTrainingJob(tj), nil
}

// scheduleTrainingCompletion drives InProgress → Completed after delay.
// ctx must be b.lifecycleCtx captured by the caller while holding b.mu.
// region must be captured by the caller before the lock is released.
func (b *InMemoryBackend) scheduleTrainingCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, trainingInProgressToCompleted, func() {
		b.mu.Lock("scheduleTrainingCompletion.goroutine")
		defer b.mu.Unlock()

		tj, ok := b.trainingJobsStore(region).Get(name)
		if !ok {
			return
		}

		if tj.TrainingJobStatus != trainingJobStatusInProgress {
			return
		}

		now := time.Now()
		tj.TrainingJobStatus = algorithmStatusCompleted
		tj.SecondaryStatus = algorithmStatusCompleted
		tj.TrainingEndTime = &now
		tj.LastModifiedTime = now
		billable := max(int32(trainingInProgressToCompleted.Seconds()), 1)
		tj.BillableTimeInSeconds = billable
		tj.TrainingTimeInSeconds = billable
		tj.ModelArtifacts = &ModelArtifacts{
			S3ModelArtifacts: "s3://" + name + "-output/output/model.tar.gz",
		}
		if tj.OutputDataConfig.S3OutputPath != "" {
			tj.ModelArtifacts.S3ModelArtifacts = tj.OutputDataConfig.S3OutputPath + "/output/model.tar.gz"
		}

		tj.SecondaryStatusTransitions = append(
			tj.SecondaryStatusTransitions,
			SecondaryStatusTransition{StartTime: now, EndTime: &now, Status: algorithmStatusCompleted},
		)
	})
}

// StopTrainingJobFSM transitions InProgress → Stopping → Stopped.
func (b *InMemoryBackend) StopTrainingJobFSM(ctx context.Context, name string) error {
	b.mu.Lock("StopTrainingJobFSM")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tj, ok := b.trainingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: training job %q not found", ErrTrainingJobNotFound, name)
	}

	tj.TrainingJobStatus = pipelineStatusStopping
	tj.LastModifiedTime = time.Now()

	b.runDelayed(b.lifecycleCtx, trainingStoppingToStopped, func() {
		b.mu.Lock("StopTrainingJobFSM.goroutine")
		defer b.mu.Unlock()

		if tj2, ok2 := b.trainingJobsStore(region).Get(name); ok2 &&
			tj2.TrainingJobStatus == pipelineStatusStopping {
			tj2.TrainingJobStatus = pipelineStatusStopped
			tj2.LastModifiedTime = time.Now()
		}
	})

	return nil
}

// ListTrainingJobsFilter narrows ListTrainingJobs results.
type ListTrainingJobsFilter struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	StatusEquals       string
	NameContains       string
	MaxResults         int32
}

// ListTrainingJobsFiltered returns training jobs matching filter.
func (b *InMemoryBackend) ListTrainingJobsFiltered(
	ctx context.Context,
	nextToken string,
	f ListTrainingJobsFilter,
) ([]*TrainingJob, string) {
	b.mu.RLock("ListTrainingJobsFiltered")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*TrainingJob, 0, b.trainingJobsStoreRO(region).Len())
	for _, tj := range b.trainingJobsStoreRO(region).All() {
		if f.StatusEquals != "" && !strings.EqualFold(tj.TrainingJobStatus, f.StatusEquals) {
			continue
		}
		if f.NameContains != "" &&
			!strings.Contains(
				strings.ToLower(tj.TrainingJobName),
				strings.ToLower(f.NameContains),
			) {
			continue
		}
		if f.CreationTimeAfter != nil && !tj.CreationTime.After(*f.CreationTimeAfter) {
			continue
		}
		if f.CreationTimeBefore != nil && !tj.CreationTime.Before(*f.CreationTimeBefore) {
			continue
		}
		list = append(list, cloneTrainingJob(tj))
	}
	sort.Slice(
		list,
		func(i, j int) bool { return list[i].TrainingJobName < list[j].TrainingJobName },
	)

	pageSize := sagemakerDefaultPageSize
	if f.MaxResults > 0 && int(f.MaxResults) < pageSize {
		pageSize = int(f.MaxResults)
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*TrainingJob{}, ""
	}
	end := startIdx + pageSize
	var outToken string
	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}
