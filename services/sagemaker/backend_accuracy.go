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

// ---------------------------------------------------------------------------
// Lifecycle simulator delays
// ---------------------------------------------------------------------------

const (
	notebookPendingToInServiceDelay = 250 * time.Millisecond
	notebookStoppingToStoppedDelay  = 150 * time.Millisecond
	notebookUpdatingToInService     = 200 * time.Millisecond
	trainingInProgressToCompleted   = 300 * time.Millisecond
	trainingStoppingToStopped       = 150 * time.Millisecond
	endpointCreatingToInService     = 300 * time.Millisecond
	endpointUpdatingToInService     = 250 * time.Millisecond
	processingJobCompletionDelay    = 300 * time.Millisecond
	processingJobStopDelay          = 150 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Status constants
// ---------------------------------------------------------------------------

const (
	notebookStatusStopped       = "Stopped"
	notebookStatusPending       = "Pending"
	notebookStatusStopping      = "Stopping"
	trainingJobStatusInProgress = "InProgress"
	keyNotebookInstanceArn      = "NotebookInstanceArn"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrNotebookLifecycleConfigNotFound is returned when a lifecycle config does not exist.
	ErrNotebookLifecycleConfigNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrNotebookLifecycleConfigAlreadyExists is returned when a lifecycle config already exists.
	ErrNotebookLifecycleConfigAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrProcessingJobNotFound is returned when a processing job does not exist.
	ErrProcessingJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrProcessingJobAlreadyExists is returned when a processing job already exists.
	ErrProcessingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ---------------------------------------------------------------------------
// NotebookInstanceLifecycleConfig (#3)
// ---------------------------------------------------------------------------

// NotebookLifecycleHook is a single lifecycle script entry.
type NotebookLifecycleHook struct {
	Content string `json:"Content,omitempty"` // base64-encoded shell script
}

// NotebookInstanceLifecycleConfig stores Create/Start lifecycle scripts.
type NotebookInstanceLifecycleConfig struct {
	CreationTime     time.Time               `json:"CreationTime"`
	LastModifiedTime time.Time               `json:"LastModifiedTime"`
	Name             string                  `json:"NotebookInstanceLifecycleConfigName"`
	ARN              string                  `json:"NotebookInstanceLifecycleConfigArn"`
	OnCreate         []NotebookLifecycleHook `json:"OnCreate,omitempty"`
	OnStart          []NotebookLifecycleHook `json:"OnStart,omitempty"`
}

// cloneNotebookLifecycleConfig returns a deep copy.
func cloneNotebookLifecycleConfig(
	lc *NotebookInstanceLifecycleConfig,
) *NotebookInstanceLifecycleConfig {
	cp := *lc
	cp.OnCreate = make([]NotebookLifecycleHook, len(lc.OnCreate))
	copy(cp.OnCreate, lc.OnCreate)
	cp.OnStart = make([]NotebookLifecycleHook, len(lc.OnStart))
	copy(cp.OnStart, lc.OnStart)

	return &cp
}

// CreateNotebookInstanceLifecycleConfig creates a new lifecycle config.
func (b *InMemoryBackend) CreateNotebookInstanceLifecycleConfig(
	ctx context.Context,
	name string,
	onCreate, onStart []NotebookLifecycleHook,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.Lock("CreateNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.notebookLifecycleConfigsStore(region).Get(name); ok {
		return nil, fmt.Errorf(
			"%w: notebook lifecycle config %s already exists",
			ErrNotebookLifecycleConfigAlreadyExists,
			name,
		)
	}

	lcARN := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		"notebook-instance-lifecycle-config/"+name,
	)
	now := time.Now()
	lc := &NotebookInstanceLifecycleConfig{
		Name:             name,
		ARN:              lcARN,
		OnCreate:         onCreate,
		OnStart:          onStart,
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.notebookLifecycleConfigsStore(region).Put(lc)

	return cloneNotebookLifecycleConfig(lc), nil
}

// DescribeNotebookInstanceLifecycleConfig returns a lifecycle config by name.
func (b *InMemoryBackend) DescribeNotebookInstanceLifecycleConfig(
	ctx context.Context,
	name string,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.RLock("DescribeNotebookInstanceLifecycleConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	lc, ok := b.notebookLifecycleConfigsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: notebook lifecycle config %q not found",
			ErrNotebookLifecycleConfigNotFound,
			name,
		)
	}

	return cloneNotebookLifecycleConfig(lc), nil
}

// UpdateNotebookInstanceLifecycleConfig replaces onCreate/onStart scripts.
func (b *InMemoryBackend) UpdateNotebookInstanceLifecycleConfig(
	ctx context.Context,
	name string,
	onCreate, onStart []NotebookLifecycleHook,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.Lock("UpdateNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	lc, ok := b.notebookLifecycleConfigsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: notebook lifecycle config %q not found",
			ErrNotebookLifecycleConfigNotFound,
			name,
		)
	}

	if onCreate != nil {
		lc.OnCreate = onCreate
	}
	if onStart != nil {
		lc.OnStart = onStart
	}
	lc.LastModifiedTime = time.Now()

	return cloneNotebookLifecycleConfig(lc), nil
}

// DeleteNotebookInstanceLifecycleConfig removes a lifecycle config.
func (b *InMemoryBackend) DeleteNotebookInstanceLifecycleConfig(ctx context.Context, name string) error {
	b.mu.Lock("DeleteNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.notebookLifecycleConfigsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf(
			"%w: notebook lifecycle config %q not found",
			ErrNotebookLifecycleConfigNotFound,
			name,
		)
	}

	store.Delete(name)

	return nil
}

// ListNotebookInstanceLifecycleConfigs returns lifecycle configs sorted by name.
func (b *InMemoryBackend) ListNotebookInstanceLifecycleConfigs(
	ctx context.Context,
	nextToken string,
) ([]*NotebookInstanceLifecycleConfig, string) {
	b.mu.RLock("ListNotebookInstanceLifecycleConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.notebookLifecycleConfigsStore(region), nextToken, cloneNotebookLifecycleConfig,
		func(a, b *NotebookInstanceLifecycleConfig) bool { return a.Name < b.Name })
}

// ---------------------------------------------------------------------------
// Notebook lifecycle FSM simulator (#2)
// ---------------------------------------------------------------------------

// scheduleNotebookTransition asynchronously transitions a notebook to nextStatus after delay.
// Must be called while holding b.mu (runDelayed captures the lifecycle context).
func (b *InMemoryBackend) scheduleNotebookTransition(
	ctx context.Context,
	name, nextStatus string,
	delay time.Duration,
) {
	region := getRegion(ctx, b.region)
	b.runDelayed(ctx, delay, func() {
		b.mu.Lock("scheduleNotebookTransition.goroutine")
		defer b.mu.Unlock()

		if nb, ok := b.notebooksStore(region).Get(name); ok {
			nb.NotebookInstanceStatus = nextStatus
			nb.LastModifiedTime = time.Now()
		}
	})
}

// StartNotebookInstanceFSM transitions: Stopped → Pending, then Pending → InService.
func (b *InMemoryBackend) StartNotebookInstanceFSM(ctx context.Context, name string) error {
	b.mu.Lock("StartNotebookInstanceFSM")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if nb.NotebookInstanceStatus != notebookStatusStopped {
		return fmt.Errorf(
			"%w: notebook %q is not Stopped (status=%s)",
			ErrValidation,
			name,
			nb.NotebookInstanceStatus,
		)
	}

	nb.NotebookInstanceStatus = notebookStatusPending
	nb.LastModifiedTime = time.Now()
	b.scheduleNotebookTransition(
		b.lifecycleCtx,
		name,
		statusInService,
		notebookPendingToInServiceDelay,
	)

	return nil
}

// StopNotebookInstanceFSM transitions: InService → Stopping, then Stopping → Stopped.
func (b *InMemoryBackend) StopNotebookInstanceFSM(ctx context.Context, name string) error {
	b.mu.Lock("StopNotebookInstanceFSM")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if nb.NotebookInstanceStatus != statusInService {
		return fmt.Errorf(
			"%w: notebook %q is not InService (status=%s)",
			ErrValidation,
			name,
			nb.NotebookInstanceStatus,
		)
	}

	nb.NotebookInstanceStatus = notebookStatusStopping
	nb.LastModifiedTime = time.Now()
	b.scheduleNotebookTransition(b.lifecycleCtx, name, notebookStatusStopped, notebookStoppingToStoppedDelay)

	return nil
}

// CreateNotebookInstanceFSM creates a notebook and immediately schedules Pending → InService.
func (b *InMemoryBackend) CreateNotebookInstanceFSM(
	ctx context.Context,
	opts NotebookInstanceOptions,
) (*NotebookInstance, error) {
	b.mu.RLock("CreateNotebookInstanceFSM.ctx")
	lifecycleCtx := b.lifecycleCtx
	b.mu.RUnlock()

	nb, err := b.CreateNotebookInstanceFull(ctx, opts)
	if err != nil {
		return nil, err
	}
	b.scheduleNotebookTransition(lifecycleCtx, opts.Name, statusInService, notebookPendingToInServiceDelay)

	return nb, nil
}

// UpdateNotebookInstanceFull updates all mutable fields on a notebook.
func (b *InMemoryBackend) UpdateNotebookInstanceFull(
	ctx context.Context,
	name string,
	opts NotebookUpdateOptions,
) error {
	b.mu.Lock("UpdateNotebookInstanceFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if nb.NotebookInstanceStatus != notebookStatusStopped {
		return fmt.Errorf(
			"%w: notebook instance %q is in %s status and cannot be updated",
			ErrValidation, name, nb.NotebookInstanceStatus,
		)
	}

	if opts.InstanceType != "" {
		nb.InstanceType = opts.InstanceType
	}
	if opts.RoleArn != "" {
		nb.RoleArn = opts.RoleArn
	}
	if opts.LifecycleConfigName != "" {
		nb.LifecycleConfigName = opts.LifecycleConfigName
	}
	if opts.DisassociateLifecycleConfig {
		nb.LifecycleConfigName = ""
	}
	if opts.VolumeSizeInGB > 0 {
		nb.VolumeSizeInGB = opts.VolumeSizeInGB
	}
	if opts.DefaultCodeRepository != "" {
		nb.DefaultCodeRepository = opts.DefaultCodeRepository
	}
	if opts.DisassociateDefaultCodeRepository {
		nb.DefaultCodeRepository = ""
	}
	if opts.AdditionalCodeRepositories != nil {
		nb.AdditionalCodeRepositories = opts.AdditionalCodeRepositories
	}
	if opts.DisassociateAdditionalCodeRepositories {
		nb.AdditionalCodeRepositories = nil
	}

	nb.LastModifiedTime = time.Now()

	return nil
}

// NotebookUpdateOptions holds mutable fields for UpdateNotebookInstance.
type NotebookUpdateOptions struct {
	InstanceType                           string
	RoleArn                                string
	LifecycleConfigName                    string
	DefaultCodeRepository                  string
	AdditionalCodeRepositories             []string
	VolumeSizeInGB                         int32
	DisassociateLifecycleConfig            bool
	DisassociateDefaultCodeRepository      bool
	DisassociateAdditionalCodeRepositories bool
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

	list := make([]*TrainingJob, 0, b.trainingJobsStore(region).Len())
	for _, tj := range b.trainingJobsStore(region).All() {
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

// ---------------------------------------------------------------------------
// Endpoint lifecycle FSM (#9)
// ---------------------------------------------------------------------------

// scheduleEndpointTransition drives an endpoint to nextStatus after delay.
// ctx must be b.lifecycleCtx captured by the caller while holding b.mu.
// region must be captured by the caller before the lock is released.
func (b *InMemoryBackend) scheduleEndpointTransition(
	ctx context.Context,
	region, name, nextStatus string,
	delay time.Duration,
) {
	b.runDelayed(ctx, delay, func() {
		b.mu.Lock("scheduleEndpointTransition.goroutine")
		defer b.mu.Unlock()

		if ep, ok := b.endpointsStore(region).Get(name); ok {
			ep.EndpointStatus = nextStatus
			ep.LastModifiedTime = time.Now()
		}
	})
}

// CreateEndpointFSM creates an endpoint and schedules Creating → InService.
func (b *InMemoryBackend) CreateEndpointFSM(
	ctx context.Context,
	name, endpointConfigName string,
	tags map[string]string,
) (*Endpoint, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CreateEndpointFSM.ctx")
	lifecycleCtx := b.lifecycleCtx
	b.mu.RUnlock()

	ep, err := b.CreateEndpoint(ctx, name, endpointConfigName, tags)
	if err != nil {
		return nil, err
	}
	b.scheduleEndpointTransition(lifecycleCtx, region, name, statusInService, endpointCreatingToInService)

	return ep, nil
}

// UpdateEndpointFSM updates config and drives InService → Updating → InService.
func (b *InMemoryBackend) UpdateEndpointFSM(ctx context.Context, name, endpointConfigName string) (*Endpoint, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("UpdateEndpointFSM.ctx")
	lifecycleCtx := b.lifecycleCtx
	b.mu.RUnlock()

	ep, err := b.UpdateEndpoint(ctx, name, endpointConfigName)
	if err != nil {
		return nil, err
	}
	b.scheduleEndpointTransition(lifecycleCtx, region, name, statusInService, endpointUpdatingToInService)

	return ep, nil
}

// UpdateEndpointWeightsAndCapacitiesFull applies weight/capacity changes and drives Updating → InService.
func (b *InMemoryBackend) UpdateEndpointWeightsAndCapacitiesFull(
	ctx context.Context,
	name string,
	changes []DesiredWeightAndCapacity,
) (*Endpoint, error) {
	b.mu.Lock("UpdateEndpointWeightsAndCapacitiesFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	// Apply weight/capacity changes to the endpoint's variant snapshots.
	for _, change := range changes {
		found := false
		for i := range ep.ProductionVariants {
			if ep.ProductionVariants[i].VariantName == change.VariantName {
				if change.DesiredWeight != nil {
					ep.ProductionVariants[i].InitialVariantWeight = *change.DesiredWeight
				}
				if change.DesiredInstanceCount != nil {
					ep.ProductionVariants[i].InitialInstanceCount = *change.DesiredInstanceCount
				}
				found = true

				break
			}
		}
		if !found {
			return nil, fmt.Errorf(
				"%w: variant %q not found in endpoint %q",
				ErrValidation,
				change.VariantName,
				name,
			)
		}
	}

	ep.EndpointStatus = "Updating"
	ep.LastModifiedTime = time.Now()

	cp := cloneEndpoint(ep)
	b.scheduleEndpointTransition(b.lifecycleCtx, region, name, statusInService, endpointUpdatingToInService)

	return cp, nil
}

// DesiredWeightAndCapacity is one entry in UpdateEndpointWeightsAndCapacities.
type DesiredWeightAndCapacity struct {
	DesiredWeight        *float64 `json:"DesiredWeight,omitempty"`
	DesiredInstanceCount *int32   `json:"DesiredInstanceCount,omitempty"`
	VariantName          string   `json:"VariantName"`
}

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

// ProcessingJob represents a SageMaker processing job.
type ProcessingJob struct {
	CreationTime           time.Time              `json:"CreationTime"`
	LastModifiedTime       time.Time              `json:"LastModifiedTime"`
	ProcessingStartTime    *time.Time             `json:"ProcessingStartTime,omitempty"`
	ProcessingEndTime      *time.Time             `json:"ProcessingEndTime,omitempty"`
	Tags                   map[string]string      `json:"Tags,omitempty"`
	Environment            map[string]string      `json:"Environment,omitempty"`
	VpcConfig              *VpcConfig             `json:"VpcConfig,omitempty"`
	ProcessingResources    ProcessingResources    `json:"ProcessingResources"`
	ProcessingOutputConfig ProcessingOutputConfig `json:"ProcessingOutputConfig"`
	ProcessingJobName      string                 `json:"ProcessingJobName"`
	ProcessingJobArn       string                 `json:"ProcessingJobArn"`
	ProcessingJobStatus    string                 `json:"ProcessingJobStatus"`
	RoleArn                string                 `json:"RoleArn,omitempty"`
	FailureReason          string                 `json:"FailureReason,omitempty"`
	AppSpecification       ProcessingAppSpec      `json:"AppSpecification"`
	ProcessingInputs       []ProcessingInput      `json:"ProcessingInputs,omitempty"`
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
		VpcConfig:              opts.VpcConfig,
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

	pj, ok := b.processingJobsStore(region).Get(name)
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

		if pj2, ok2 := b.processingJobsStore(region).Get(name); ok2 && pj2.ProcessingJobStatus == "Stopping" {
			pj2.ProcessingJobStatus = "Stopped"
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

// ListProcessingJobs returns processing jobs sorted by name.
func (b *InMemoryBackend) ListProcessingJobs(
	ctx context.Context,
	nextToken, statusEquals string,
	maxResults int32,
) ([]*ProcessingJob, string) {
	b.mu.RLock("ListProcessingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*ProcessingJob, 0, b.processingJobsStore(region).Len())
	for _, pj := range b.processingJobsStore(region).All() {
		if statusEquals != "" && !strings.EqualFold(pj.ProcessingJobStatus, statusEquals) {
			continue
		}
		list = append(list, cloneProcessingJob(pj))
	}
	sort.Slice(
		list,
		func(i, j int) bool { return list[i].ProcessingJobName < list[j].ProcessingJobName },
	)

	pageSize := sagemakerDefaultPageSize
	if maxResults > 0 && int(maxResults) < pageSize {
		pageSize = int(maxResults)
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*ProcessingJob{}, ""
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

// ---------------------------------------------------------------------------
// Lifecycle context management — per-backend context cancelled by Reset()
// ---------------------------------------------------------------------------

// resetLifecycleContext replaces the backend's lifecycle context with a fresh one,
// cancelling all pending goroutines from the previous context.
// Must be called with no lock held.
func (b *InMemoryBackend) resetLifecycleContext() {
	if b.lifecycleCancel != nil {
		b.lifecycleCancel()
	}

	parent := b.lifecycleParent
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	b.lifecycleCtx = ctx
	b.lifecycleCancel = cancel
}

// runDelayed runs fn after delay unless ctx is cancelled first (via Reset,
// Restore, or Shutdown, all of which cancel the lifecycle context). The goroutine
// is tracked by b.wg so Shutdown can wait for in-flight transitions to drain. fn is
// responsible for taking any locks it needs and re-checking resource existence.
//
// ctx must be the backend's lifecycle context (b.lifecycleCtx) captured by the
// caller — typically while holding b.mu — so a concurrent Reset that swaps
// b.lifecycleCtx cannot race this goroutine's select.
func (b *InMemoryBackend) runDelayed(ctx context.Context, delay time.Duration, fn func()) {
	b.wg.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		fn()
	})
}

// Shutdown cancels all pending lifecycle-transition goroutines and waits for the
// in-flight ones to finish, bounded by ctx. It implements the shutdown half of the
// service.Shutdowner contract (wired through the Handler).
func (b *InMemoryBackend) Shutdown(ctx context.Context) {
	b.mu.Lock("Shutdown")
	if b.lifecycleCancel != nil {
		b.lifecycleCancel()
	}
	b.mu.Unlock()

	done := make(chan struct{})

	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// ---------------------------------------------------------------------------
// NotebookInstanceOptions for gap #1 (full field set)
// ---------------------------------------------------------------------------

// NotebookInstanceOptions holds all CreateNotebookInstance request fields.
type NotebookInstanceOptions struct {
	Tags                       map[string]string `json:"Tags,omitempty"`
	SubnetID                   string            `json:"SubnetId,omitempty"`
	LifecycleConfigName        string            `json:"LifecycleConfigName,omitempty"`
	Name                       string            `json:"NotebookInstanceName"`
	InstanceType               string            `json:"InstanceType"`
	RoleArn                    string            `json:"RoleArn"`
	RootAccess                 string            `json:"RootAccess,omitempty"`
	KmsKeyID                   string            `json:"KmsKeyId,omitempty"`
	DirectInternetAccess       string            `json:"DirectInternetAccess,omitempty"`
	DefaultCodeRepository      string            `json:"DefaultCodeRepository,omitempty"`
	PlatformIdentifier         string            `json:"PlatformIdentifier,omitempty"`
	AcceleratorTypes           []string          `json:"AcceleratorTypes,omitempty"`
	AdditionalCodeRepositories []string          `json:"AdditionalCodeRepositories,omitempty"`
	SecurityGroupIDs           []string          `json:"SecurityGroupIds,omitempty"`
	VolumeSizeInGB             int32             `json:"VolumeSizeInGB,omitempty"`
}

// CreateNotebookInstanceFull persists all NotebookInstanceOptions fields.
func (b *InMemoryBackend) CreateNotebookInstanceFull(
	ctx context.Context,
	opts NotebookInstanceOptions,
) (*NotebookInstance, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", ErrValidation)
	}
	if opts.InstanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrValidation)
	}
	if opts.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	b.mu.Lock("CreateNotebookInstanceFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.notebooksStore(region).Get(opts.Name); ok {
		return nil, fmt.Errorf(
			"%w: notebook instance %s already exists",
			ErrNotebookAlreadyExists,
			opts.Name,
		)
	}

	nbARN := arn.Build("sagemaker", region, b.accountID, "notebook-instance/"+opts.Name)
	now := time.Now()
	nb := &NotebookInstance{
		NotebookInstanceName:       opts.Name,
		NotebookInstanceArn:        nbARN,
		NotebookInstanceStatus:     "Pending",
		InstanceType:               opts.InstanceType,
		RoleArn:                    opts.RoleArn,
		SubnetID:                   opts.SubnetID,
		SecurityGroupIDs:           append([]string(nil), opts.SecurityGroupIDs...),
		KmsKeyID:                   opts.KmsKeyID,
		LifecycleConfigName:        opts.LifecycleConfigName,
		DirectInternetAccess:       opts.DirectInternetAccess,
		RootAccess:                 opts.RootAccess,
		AcceleratorTypes:           append([]string(nil), opts.AcceleratorTypes...),
		AdditionalCodeRepositories: append([]string(nil), opts.AdditionalCodeRepositories...),
		DefaultCodeRepository:      opts.DefaultCodeRepository,
		VolumeSizeInGB:             opts.VolumeSizeInGB,
		PlatformIdentifier:         opts.PlatformIdentifier,
		CreationTime:               now,
		LastModifiedTime:           now,
		Tags:                       mergeTags(nil, opts.Tags),
	}
	b.notebooksStore(region).Put(nb)
	b.notebookARNIndexStore(region)[nbARN] = opts.Name

	return cloneNotebook(nb), nil
}
