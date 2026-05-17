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
)

// ---------------------------------------------------------------------------
// Status constants
// ---------------------------------------------------------------------------

const (
	notebookStatusStopped       = "Stopped"
	notebookStatusPending       = "Pending"
	notebookStatusStopping      = "Stopping"
	trainingJobStatusInProgress = "InProgress"
	trainingJobStatusCompleted  = "Completed"
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
	OnCreate         []NotebookLifecycleHook `json:"OnCreate,omitempty"`
	OnStart          []NotebookLifecycleHook `json:"OnStart,omitempty"`
	Name             string                  `json:"NotebookInstanceLifecycleConfigName"`
	ARN              string                  `json:"NotebookInstanceLifecycleConfigArn"`
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
	name string,
	onCreate, onStart []NotebookLifecycleHook,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.Lock("CreateNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	if _, ok := b.notebookLifecycleConfigs[name]; ok {
		return nil, fmt.Errorf(
			"%w: notebook lifecycle config %s already exists",
			ErrNotebookLifecycleConfigAlreadyExists,
			name,
		)
	}

	lcARN := arn.Build(
		"sagemaker",
		b.region,
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
	b.notebookLifecycleConfigs[name] = lc

	return cloneNotebookLifecycleConfig(lc), nil
}

// DescribeNotebookInstanceLifecycleConfig returns a lifecycle config by name.
func (b *InMemoryBackend) DescribeNotebookInstanceLifecycleConfig(
	name string,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.RLock("DescribeNotebookInstanceLifecycleConfig")
	defer b.mu.RUnlock()

	lc, ok := b.notebookLifecycleConfigs[name]
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
	name string,
	onCreate, onStart []NotebookLifecycleHook,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.Lock("UpdateNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	lc, ok := b.notebookLifecycleConfigs[name]
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
func (b *InMemoryBackend) DeleteNotebookInstanceLifecycleConfig(name string) error {
	b.mu.Lock("DeleteNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	if _, ok := b.notebookLifecycleConfigs[name]; !ok {
		return fmt.Errorf(
			"%w: notebook lifecycle config %q not found",
			ErrNotebookLifecycleConfigNotFound,
			name,
		)
	}

	delete(b.notebookLifecycleConfigs, name)

	return nil
}

// ListNotebookInstanceLifecycleConfigs returns lifecycle configs sorted by name.
func (b *InMemoryBackend) ListNotebookInstanceLifecycleConfigs(
	nextToken string,
) ([]*NotebookInstanceLifecycleConfig, string) {
	b.mu.RLock("ListNotebookInstanceLifecycleConfigs")
	defer b.mu.RUnlock()

	list := make([]*NotebookInstanceLifecycleConfig, 0, len(b.notebookLifecycleConfigs))
	for _, lc := range b.notebookLifecycleConfigs {
		cp := *lc
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*NotebookInstanceLifecycleConfig{}, ""
	}
	end := startIdx + sagemakerDefaultPageSize
	var outToken string
	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// ---------------------------------------------------------------------------
// Notebook lifecycle FSM simulator (#2)
// ---------------------------------------------------------------------------

// scheduleNotebookTransition asynchronously transitions a notebook to nextStatus after delay.
// ctx must be b.lifecycleCtx captured by the caller while holding b.mu.
func (b *InMemoryBackend) scheduleNotebookTransition(
	ctx context.Context,
	name, nextStatus string,
	delay time.Duration,
) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		b.mu.Lock("scheduleNotebookTransition.goroutine")
		defer b.mu.Unlock()

		if nb, ok := b.notebooks[name]; ok {
			nb.NotebookInstanceStatus = nextStatus
			nb.LastModifiedTime = time.Now()
		}
	}()
}

// StartNotebookInstanceFSM transitions: Stopped → Pending, then Pending → InService.
func (b *InMemoryBackend) StartNotebookInstanceFSM(name string) error {
	b.mu.Lock("StartNotebookInstanceFSM")
	defer b.mu.Unlock()

	nb, ok := b.notebooks[name]
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
func (b *InMemoryBackend) StopNotebookInstanceFSM(name string) error {
	b.mu.Lock("StopNotebookInstanceFSM")
	defer b.mu.Unlock()

	nb, ok := b.notebooks[name]
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
	opts NotebookInstanceOptions,
) (*NotebookInstance, error) {
	b.mu.Lock("CreateNotebookInstanceFSM.lock")
	ctx := b.lifecycleCtx
	b.mu.Unlock()

	nb, err := b.CreateNotebookInstanceFull(opts)
	if err != nil {
		return nil, err
	}
	b.scheduleNotebookTransition(ctx, opts.Name, statusInService, notebookPendingToInServiceDelay)
	return nb, nil
}

// UpdateNotebookInstanceFull updates all mutable fields on a notebook.
func (b *InMemoryBackend) UpdateNotebookInstanceFull(
	name string,
	opts NotebookUpdateOptions,
) error {
	b.mu.Lock("UpdateNotebookInstanceFull")
	defer b.mu.Unlock()

	nb, ok := b.notebooks[name]
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
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
	AdditionalCodeRepositories             []string
	InstanceType                           string
	RoleArn                                string
	LifecycleConfigName                    string
	DefaultCodeRepository                  string
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
	MetricDefinitions                []MetricDefinition `json:"MetricDefinitions,omitempty"`
	ContainerEntrypoint              []string           `json:"ContainerEntrypoint,omitempty"`
	ContainerArguments               []string           `json:"ContainerArguments,omitempty"`
	TrainingImage                    string             `json:"TrainingImage,omitempty"`
	AlgorithmName                    string             `json:"AlgorithmName,omitempty"`
	TrainingInputMode                string             `json:"TrainingInputMode,omitempty"`
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
	KmsKeyId        string `json:"KmsKeyId,omitempty"`
	CompressionType string `json:"CompressionType,omitempty"`
}

// ResourceConfig specifies compute resources for a training job.
type ResourceConfig struct {
	InstanceGroups           []InstanceGroup `json:"InstanceGroups,omitempty"`
	InstanceType             string          `json:"InstanceType"`
	InstanceCount            int32           `json:"InstanceCount"`
	VolumeSizeInGB           int32           `json:"VolumeSizeInGB"`
	VolumeKmsKeyId           string          `json:"VolumeKmsKeyId,omitempty"`
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
	SecurityGroupIds []string `json:"SecurityGroupIds,omitempty"`
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
	AlgorithmSpecification                AlgorithmSpecification `json:"AlgorithmSpecification"`
	InputDataConfig                       []Channel              `json:"InputDataConfig,omitempty"`
	OutputDataConfig                      OutputDataConfig       `json:"OutputDataConfig"`
	ResourceConfig                        ResourceConfig         `json:"ResourceConfig"`
	StoppingCondition                     StoppingCondition      `json:"StoppingCondition"`
	VpcConfig                             *VpcConfig             `json:"VpcConfig,omitempty"`
	CheckpointConfig                      *CheckpointConfig      `json:"CheckpointConfig,omitempty"`
	HyperParameters                       map[string]string      `json:"HyperParameters,omitempty"`
	Environment                           map[string]string      `json:"Environment,omitempty"`
	Tags                                  map[string]string      `json:"Tags,omitempty"`
	TrainingJobName                       string                 `json:"TrainingJobName"`
	RoleArn                               string                 `json:"RoleArn"`
	EnableNetworkIsolation                bool                   `json:"EnableNetworkIsolation,omitempty"`
	EnableManagedSpotTraining             bool                   `json:"EnableManagedSpotTraining,omitempty"`
	EnableInterContainerTrafficEncryption bool                   `json:"EnableInterContainerTrafficEncryption,omitempty"`
}

// CreateTrainingJobFull creates a training job from a full options struct
// and schedules InProgress → Completed after a short delay.
func (b *InMemoryBackend) CreateTrainingJobFull(opts TrainingJobOptions) (*TrainingJob, error) {
	b.mu.Lock("CreateTrainingJobFull")
	defer b.mu.Unlock()

	if _, ok := b.trainingJobs[opts.TrainingJobName]; ok {
		return nil, fmt.Errorf(
			"%w: training job %s already exists",
			ErrTrainingJobAlreadyExists,
			opts.TrainingJobName,
		)
	}

	jobARN := arn.Build("sagemaker", b.region, b.accountID, "training-job/"+opts.TrainingJobName)
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
	b.trainingJobs[opts.TrainingJobName] = tj
	b.trainingJobARNIndex[jobARN] = opts.TrainingJobName

	b.scheduleTrainingCompletion(b.lifecycleCtx, opts.TrainingJobName)

	return cloneTrainingJob(tj), nil
}

// scheduleTrainingCompletion drives InProgress → Completed after delay.
// ctx must be b.lifecycleCtx captured by the caller while holding b.mu.
func (b *InMemoryBackend) scheduleTrainingCompletion(ctx context.Context, name string) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(trainingInProgressToCompleted):
		}

		b.mu.Lock("scheduleTrainingCompletion.goroutine")
		defer b.mu.Unlock()

		tj, ok := b.trainingJobs[name]
		if !ok {
			return
		}

		if tj.TrainingJobStatus != "InProgress" {
			return
		}

		now := time.Now()
		tj.TrainingJobStatus = "Completed"
		tj.SecondaryStatus = "Completed"
		tj.TrainingEndTime = &now
		tj.LastModifiedTime = now
		tj.BillableTimeInSeconds = int32(trainingInProgressToCompleted.Seconds())
		tj.TrainingTimeInSeconds = int32(trainingInProgressToCompleted.Seconds())
		tj.ModelArtifacts = &ModelArtifacts{
			S3ModelArtifacts: "s3://" + name + "-output/output/model.tar.gz",
		}
		if tj.OutputDataConfig.S3OutputPath != "" {
			tj.ModelArtifacts.S3ModelArtifacts = tj.OutputDataConfig.S3OutputPath + "/output/model.tar.gz"
		}

		tj.SecondaryStatusTransitions = append(tj.SecondaryStatusTransitions,
			SecondaryStatusTransition{StartTime: now, EndTime: &now, Status: "Completed"},
		)
	}()
}

// StopTrainingJobFSM transitions InProgress → Stopping → Stopped.
func (b *InMemoryBackend) StopTrainingJobFSM(name string) error {
	b.mu.Lock("StopTrainingJobFSM")
	defer b.mu.Unlock()

	tj, ok := b.trainingJobs[name]
	if !ok {
		return fmt.Errorf("%w: training job %q not found", ErrTrainingJobNotFound, name)
	}

	tj.TrainingJobStatus = pipelineStatusStopping
	tj.LastModifiedTime = time.Now()

	ctx := b.lifecycleCtx
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(trainingStoppingToStopped):
		}

		b.mu.Lock("StopTrainingJobFSM.goroutine")
		defer b.mu.Unlock()

		if tj2, ok2 := b.trainingJobs[name]; ok2 &&
			tj2.TrainingJobStatus == pipelineStatusStopping {
			tj2.TrainingJobStatus = pipelineStatusStopped
			tj2.LastModifiedTime = time.Now()
		}
	}()

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
	nextToken string,
	f ListTrainingJobsFilter,
) ([]*TrainingJob, string) {
	b.mu.RLock("ListTrainingJobsFiltered")
	defer b.mu.RUnlock()

	list := make([]*TrainingJob, 0, len(b.trainingJobs))
	for _, tj := range b.trainingJobs {
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
		cp := *tj
		list = append(list, &cp)
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
func (b *InMemoryBackend) scheduleEndpointTransition(
	ctx context.Context,
	name, nextStatus string,
	delay time.Duration,
) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		b.mu.Lock("scheduleEndpointTransition.goroutine")
		defer b.mu.Unlock()

		if ep, ok := b.endpoints[name]; ok {
			ep.EndpointStatus = nextStatus
			ep.LastModifiedTime = time.Now()
		}
	}()
}

// CreateEndpointFSM creates an endpoint and schedules Creating → InService.
func (b *InMemoryBackend) CreateEndpointFSM(
	name, endpointConfigName string,
	tags map[string]string,
) (*Endpoint, error) {
	b.mu.Lock("CreateEndpointFSM.ctx")
	ctx := b.lifecycleCtx
	b.mu.Unlock()

	ep, err := b.CreateEndpoint(name, endpointConfigName, tags)
	if err != nil {
		return nil, err
	}
	b.scheduleEndpointTransition(ctx, name, statusInService, endpointCreatingToInService)
	return ep, nil
}

// UpdateEndpointFSM updates config and drives InService → Updating → InService.
func (b *InMemoryBackend) UpdateEndpointFSM(name, endpointConfigName string) (*Endpoint, error) {
	b.mu.Lock("UpdateEndpointFSM.ctx")
	ctx := b.lifecycleCtx
	b.mu.Unlock()

	ep, err := b.UpdateEndpoint(name, endpointConfigName)
	if err != nil {
		return nil, err
	}
	b.scheduleEndpointTransition(ctx, name, statusInService, endpointUpdatingToInService)
	return ep, nil
}

// UpdateEndpointWeightsAndCapacitiesFull applies weight/capacity changes and drives Updating → InService.
func (b *InMemoryBackend) UpdateEndpointWeightsAndCapacitiesFull(
	name string,
	changes []DesiredWeightAndCapacity,
) (*Endpoint, error) {
	b.mu.Lock("UpdateEndpointWeightsAndCapacitiesFull")
	defer b.mu.Unlock()

	ep, ok := b.endpoints[name]
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
	b.scheduleEndpointTransition(b.lifecycleCtx, name, statusInService, endpointUpdatingToInService)

	return cp, nil
}

// DesiredWeightAndCapacity is one entry in UpdateEndpointWeightsAndCapacities.
type DesiredWeightAndCapacity struct {
	VariantName          string   `json:"VariantName"`
	DesiredWeight        *float64 `json:"DesiredWeight,omitempty"`
	DesiredInstanceCount *int32   `json:"DesiredInstanceCount,omitempty"`
}

// ---------------------------------------------------------------------------
// ProcessingJob (#12, partial)
// ---------------------------------------------------------------------------

// ProcessingInput specifies input data for a processing job.
type ProcessingInput struct {
	InputName         string             `json:"InputName"`
	S3Input           *ProcessingS3Input `json:"S3Input,omitempty"`
	DatasetDefinition *DatasetDefinition `json:"DatasetDefinition,omitempty"`
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
	OutputName string              `json:"OutputName"`
	S3Output   *ProcessingS3Output `json:"S3Output,omitempty"`
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
	Outputs  []ProcessingOutput `json:"Outputs,omitempty"`
	KmsKeyId string             `json:"KmsKeyId,omitempty"`
}

// ProcessingResources specifies compute for a processing job.
type ProcessingResources struct {
	ClusterConfig ProcessingClusterConfig `json:"ClusterConfig"`
}

// ProcessingClusterConfig is the compute config for a processing job.
type ProcessingClusterConfig struct {
	InstanceType   string `json:"InstanceType"`
	InstanceCount  int32  `json:"InstanceCount"`
	VolumeSizeInGB int32  `json:"VolumeSizeInGB"`
	VolumeKmsKeyId string `json:"VolumeKmsKeyId,omitempty"`
}

// ProcessingAppSpec identifies the container image for a processing job.
type ProcessingAppSpec struct {
	ImageUri            string   `json:"ImageUri"`
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
	ProcessingInputs       []ProcessingInput      `json:"ProcessingInputs,omitempty"`
	ProcessingOutputConfig ProcessingOutputConfig `json:"ProcessingOutputConfig"`
	ProcessingResources    ProcessingResources    `json:"ProcessingResources"`
	AppSpecification       ProcessingAppSpec      `json:"AppSpecification"`
	VpcConfig              *VpcConfig             `json:"VpcConfig,omitempty"`
	ProcessingJobName      string                 `json:"ProcessingJobName"`
	ProcessingJobArn       string                 `json:"ProcessingJobArn"`
	ProcessingJobStatus    string                 `json:"ProcessingJobStatus"`
	RoleArn                string                 `json:"RoleArn,omitempty"`
	FailureReason          string                 `json:"FailureReason,omitempty"`
}

// cloneProcessingJob returns a deep copy of pj.
func cloneProcessingJob(pj *ProcessingJob) *ProcessingJob {
	cp := *pj
	cp.Tags = maps.Clone(pj.Tags)
	cp.Environment = maps.Clone(pj.Environment)
	cp.ProcessingInputs = make([]ProcessingInput, len(pj.ProcessingInputs))
	copy(cp.ProcessingInputs, pj.ProcessingInputs)
	cp.ProcessingOutputConfig.Outputs = make(
		[]ProcessingOutput,
		len(pj.ProcessingOutputConfig.Outputs),
	)
	copy(cp.ProcessingOutputConfig.Outputs, pj.ProcessingOutputConfig.Outputs)
	return &cp
}

// CreateProcessingJob creates and schedules a processing job.
func (b *InMemoryBackend) CreateProcessingJob(opts ProcessingJob) (*ProcessingJob, error) {
	b.mu.Lock("CreateProcessingJob")
	defer b.mu.Unlock()

	if _, ok := b.processingJobs[opts.ProcessingJobName]; ok {
		return nil, fmt.Errorf(
			"%w: processing job %s already exists",
			ErrProcessingJobAlreadyExists,
			opts.ProcessingJobName,
		)
	}

	pjARN := arn.Build("sagemaker", b.region, b.accountID, "processing-job/"+opts.ProcessingJobName)
	now := time.Now()
	pj := &ProcessingJob{
		ProcessingJobName:      opts.ProcessingJobName,
		ProcessingJobArn:       pjARN,
		ProcessingJobStatus:    "InProgress",
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
	b.processingJobs[opts.ProcessingJobName] = pj
	b.processingJobARNIndex[pjARN] = opts.ProcessingJobName

	b.scheduleProcessingCompletion(b.lifecycleCtx, opts.ProcessingJobName)

	return cloneProcessingJob(pj), nil
}

// scheduleProcessingCompletion transitions a processing job to Completed.
// ctx must be b.lifecycleCtx captured by the caller while holding b.mu.
func (b *InMemoryBackend) scheduleProcessingCompletion(ctx context.Context, name string) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}

		b.mu.Lock("scheduleProcessingCompletion.goroutine")
		defer b.mu.Unlock()

		pj, ok := b.processingJobs[name]
		if !ok || pj.ProcessingJobStatus != "InProgress" {
			return
		}
		now := time.Now()
		pj.ProcessingJobStatus = "Completed"
		pj.ProcessingEndTime = &now
		pj.LastModifiedTime = now
	}()
}

// DescribeProcessingJob returns a processing job by name.
func (b *InMemoryBackend) DescribeProcessingJob(name string) (*ProcessingJob, error) {
	b.mu.RLock("DescribeProcessingJob")
	defer b.mu.RUnlock()

	pj, ok := b.processingJobs[name]
	if !ok {
		return nil, fmt.Errorf("%w: processing job %q not found", ErrProcessingJobNotFound, name)
	}

	return cloneProcessingJob(pj), nil
}

// StopProcessingJob transitions a processing job to Stopping then Stopped.
func (b *InMemoryBackend) StopProcessingJob(name string) error {
	b.mu.Lock("StopProcessingJob")
	defer b.mu.Unlock()

	pj, ok := b.processingJobs[name]
	if !ok {
		return fmt.Errorf("%w: processing job %q not found", ErrProcessingJobNotFound, name)
	}

	pj.ProcessingJobStatus = "Stopping"
	pj.LastModifiedTime = time.Now()

	ctx := b.lifecycleCtx
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(150 * time.Millisecond):
		}

		b.mu.Lock("StopProcessingJob.goroutine")
		defer b.mu.Unlock()

		if pj2, ok2 := b.processingJobs[name]; ok2 && pj2.ProcessingJobStatus == "Stopping" {
			pj2.ProcessingJobStatus = "Stopped"
			pj2.LastModifiedTime = time.Now()
		}
	}()

	return nil
}

// ListProcessingJobs returns processing jobs sorted by name.
func (b *InMemoryBackend) ListProcessingJobs(
	nextToken, statusEquals string,
	maxResults int32,
) ([]*ProcessingJob, string) {
	b.mu.RLock("ListProcessingJobs")
	defer b.mu.RUnlock()

	list := make([]*ProcessingJob, 0, len(b.processingJobs))
	for _, pj := range b.processingJobs {
		if statusEquals != "" && !strings.EqualFold(pj.ProcessingJobStatus, statusEquals) {
			continue
		}
		cp := *pj
		list = append(list, &cp)
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

	ctx, cancel := context.WithCancel(context.Background())
	b.lifecycleCtx = ctx
	b.lifecycleCancel = cancel
}

// ---------------------------------------------------------------------------
// NotebookInstanceOptions for gap #1 (full field set)
// ---------------------------------------------------------------------------

// NotebookInstanceOptions holds all CreateNotebookInstance request fields.
type NotebookInstanceOptions struct {
	AdditionalCodeRepositories []string          `json:"AdditionalCodeRepositories,omitempty"`
	AcceleratorTypes           []string          `json:"AcceleratorTypes,omitempty"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	Name                       string            `json:"NotebookInstanceName"`
	InstanceType               string            `json:"InstanceType"`
	RoleArn                    string            `json:"RoleArn"`
	SubnetId                   string            `json:"SubnetId,omitempty"`
	KmsKeyId                   string            `json:"KmsKeyId,omitempty"`
	LifecycleConfigName        string            `json:"LifecycleConfigName,omitempty"`
	DefaultCodeRepository      string            `json:"DefaultCodeRepository,omitempty"`
	PlatformIdentifier         string            `json:"PlatformIdentifier,omitempty"`
	DirectInternetAccess       string            `json:"DirectInternetAccess,omitempty"`
	RootAccess                 string            `json:"RootAccess,omitempty"`
	VolumeSizeInGB             int32             `json:"VolumeSizeInGB,omitempty"`
	SecurityGroupIds           []string          `json:"SecurityGroupIds,omitempty"`
}

// CreateNotebookInstanceFull persists all NotebookInstanceOptions fields.
func (b *InMemoryBackend) CreateNotebookInstanceFull(
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

	if _, ok := b.notebooks[opts.Name]; ok {
		return nil, fmt.Errorf(
			"%w: notebook instance %s already exists",
			ErrNotebookAlreadyExists,
			opts.Name,
		)
	}

	nbARN := arn.Build("sagemaker", b.region, b.accountID, "notebook-instance/"+opts.Name)
	now := time.Now()
	nb := &NotebookInstance{
		NotebookInstanceName:       opts.Name,
		NotebookInstanceArn:        nbARN,
		NotebookInstanceStatus:     "Pending",
		InstanceType:               opts.InstanceType,
		RoleArn:                    opts.RoleArn,
		SubnetId:                   opts.SubnetId,
		SecurityGroupIds:           opts.SecurityGroupIds,
		KmsKeyId:                   opts.KmsKeyId,
		LifecycleConfigName:        opts.LifecycleConfigName,
		DirectInternetAccess:       opts.DirectInternetAccess,
		RootAccess:                 opts.RootAccess,
		AcceleratorTypes:           opts.AcceleratorTypes,
		AdditionalCodeRepositories: opts.AdditionalCodeRepositories,
		DefaultCodeRepository:      opts.DefaultCodeRepository,
		VolumeSizeInGB:             opts.VolumeSizeInGB,
		PlatformIdentifier:         opts.PlatformIdentifier,
		CreationTime:               now,
		LastModifiedTime:           now,
		Tags:                       mergeTags(nil, opts.Tags),
	}
	b.notebooks[opts.Name] = nb
	b.notebookARNIndex[nbARN] = opts.Name

	return cloneNotebook(nb), nil
}
