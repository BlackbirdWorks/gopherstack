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
// Endpoint
// ---------------------------------------------------------------------------

var (
	// ErrEndpointNotFound is returned when an endpoint does not exist.
	ErrEndpointNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrEndpointAlreadyExists is returned when an endpoint already exists.
	ErrEndpointAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrTrainingJobNotFound is returned when a training job does not exist.
	ErrTrainingJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrTrainingJobAlreadyExists is returned when a training job already exists.
	ErrTrainingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrNotebookNotFound is returned when a notebook instance does not exist.
	ErrNotebookNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrNotebookAlreadyExists is returned when a notebook instance already exists.
	ErrNotebookAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrHPTuningJobNotFound is returned when an HP tuning job does not exist.
	ErrHPTuningJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrHPTuningJobAlreadyExists is returned when an HP tuning job already exists.
	ErrHPTuningJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// Endpoint represents a SageMaker inference endpoint.
type Endpoint struct {
	CreationTime       time.Time           `json:"CreationTime"`
	LastModifiedTime   time.Time           `json:"LastModifiedTime"`
	Tags               map[string]string   `json:"Tags,omitempty"`
	EndpointName       string              `json:"EndpointName"`
	EndpointArn        string              `json:"EndpointArn"`
	EndpointConfigName string              `json:"EndpointConfigName"`
	EndpointStatus     string              `json:"EndpointStatus"`
	FailureReason      string              `json:"FailureReason,omitempty"`
	ProductionVariants []ProductionVariant `json:"ProductionVariants,omitempty"`
}

// cloneEndpoint returns a deep copy of ep.
func cloneEndpoint(ep *Endpoint) *Endpoint {
	cp := *ep
	cp.Tags = maps.Clone(ep.Tags)
	cp.ProductionVariants = make([]ProductionVariant, len(ep.ProductionVariants))
	for i, pv := range ep.ProductionVariants {
		cp.ProductionVariants[i] = cloneProductionVariant(pv)
	}

	return &cp
}

// TrainingJob represents a SageMaker training job.
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
type NotebookInstance struct {
	CreationTime               time.Time         `json:"CreationTime"`
	LastModifiedTime           time.Time         `json:"LastModifiedTime"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	RootAccess                 string            `json:"RootAccess,omitempty"`
	KmsKeyID                   string            `json:"KmsKeyId,omitempty"`
	URL                        string            `json:"Url,omitempty"`
	NotebookInstanceName       string            `json:"NotebookInstanceName"`
	NotebookInstanceArn        string            `json:"NotebookInstanceArn"`
	NotebookInstanceStatus     string            `json:"NotebookInstanceStatus"`
	InstanceType               string            `json:"InstanceType,omitempty"`
	RoleArn                    string            `json:"RoleArn,omitempty"`
	SubnetID                   string            `json:"SubnetId,omitempty"`
	PlatformIdentifier         string            `json:"PlatformIdentifier,omitempty"`
	LifecycleConfigName        string            `json:"NotebookInstanceLifecycleConfigName,omitempty"`
	DirectInternetAccess       string            `json:"DirectInternetAccess,omitempty"`
	DefaultCodeRepository      string            `json:"DefaultCodeRepository,omitempty"`
	SecurityGroupIDs           []string          `json:"SecurityGroupIds,omitempty"`
	AcceleratorTypes           []string          `json:"AcceleratorTypes,omitempty"`
	AdditionalCodeRepositories []string          `json:"AdditionalCodeRepositories,omitempty"`
	VolumeSizeInGB             int32             `json:"VolumeSizeInGB,omitempty"`
}

// cloneNotebook returns a deep copy of nb.
func cloneNotebook(nb *NotebookInstance) *NotebookInstance {
	cp := *nb
	cp.Tags = maps.Clone(nb.Tags)
	cp.SecurityGroupIDs = append([]string(nil), nb.SecurityGroupIDs...)
	cp.AcceleratorTypes = append([]string(nil), nb.AcceleratorTypes...)
	cp.AdditionalCodeRepositories = append([]string(nil), nb.AdditionalCodeRepositories...)

	return &cp
}

// HyperParameterTuningJob represents a SageMaker hyperparameter tuning job.
type HyperParameterTuningJob struct {
	CreationTime                  time.Time         `json:"CreationTime"`
	LastModifiedTime              time.Time         `json:"LastModifiedTime"`
	Tags                          map[string]string `json:"Tags,omitempty"`
	HyperParameterTuningJobName   string            `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobArn    string            `json:"HyperParameterTuningJobArn"`
	HyperParameterTuningJobStatus string            `json:"HyperParameterTuningJobStatus"`
	Strategy                      string            `json:"Strategy,omitempty"`
}

// cloneHPTuningJob returns a deep copy of j.
func cloneHPTuningJob(j *HyperParameterTuningJob) *HyperParameterTuningJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// ---------------------------------------------------------------------------
// Backend field additions via a separate map — we extend InMemoryBackend with
// a parallel struct to avoid modifying the original large file.
// ---------------------------------------------------------------------------

// extendedState holds the new resource maps added in this file.
// It is embedded into InMemoryBackend via the ext field initialised in
// NewInMemoryBackendExtended (called from NewInMemoryBackend after init).
// We keep it simple: InMemoryBackend itself is extended with four new fields.

// CreateEndpoint creates a new SageMaker endpoint.
func (b *InMemoryBackend) CreateEndpoint(
	ctx context.Context,
	name, endpointConfigName string,
	tags map[string]string,
) (*Endpoint, error) {
	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.endpointsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrEndpointAlreadyExists, name)
	}

	epARN := arn.Build("sagemaker", region, b.accountID, "endpoint/"+name)
	now := time.Now()
	ep := &Endpoint{
		EndpointName:       name,
		EndpointArn:        epARN,
		EndpointConfigName: endpointConfigName,
		EndpointStatus:     "Creating",
		CreationTime:       now,
		LastModifiedTime:   now,
		Tags:               mergeTags(nil, tags),
	}
	b.endpointsStore(region).Put(ep)
	b.endpointARNIndexStore(region)[epARN] = name

	return cloneEndpoint(ep), nil
}

// DescribeEndpoint returns an endpoint by name.
func (b *InMemoryBackend) DescribeEndpoint(ctx context.Context, name string) (*Endpoint, error) {
	b.mu.RLock("DescribeEndpoint")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	return cloneEndpoint(ep), nil
}

// ListEndpoints returns endpoints sorted by name with optional pagination.
func (b *InMemoryBackend) ListEndpoints(ctx context.Context, nextToken string) ([]*Endpoint, string) {
	b.mu.RLock("ListEndpoints")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.endpointsStore(region), nextToken, cloneEndpoint,
		func(a, b *Endpoint) bool { return a.EndpointName < b.EndpointName })
}

// DeleteEndpoint deletes an endpoint by name.
func (b *InMemoryBackend) DeleteEndpoint(ctx context.Context, name string) error {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	arnIdx := b.endpointARNIndexStore(region)
	delete(arnIdx, ep.EndpointArn)
	endpoints := b.endpointsStore(region)
	endpoints.Delete(name)

	return nil
}

// UpdateEndpoint updates the endpoint config for an existing endpoint.
func (b *InMemoryBackend) UpdateEndpoint(ctx context.Context, name, endpointConfigName string) (*Endpoint, error) {
	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	ep.EndpointConfigName = endpointConfigName
	ep.EndpointStatus = "Updating"
	ep.LastModifiedTime = time.Now()

	return cloneEndpoint(ep), nil
}

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

	tj, ok := b.trainingJobsStore(region).Get(name)
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

	return sagemakerListPaged(b.trainingJobsStore(region), nextToken, cloneTrainingJob,
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
// NotebookInstance
// ---------------------------------------------------------------------------

// CreateNotebookInstance creates a new notebook instance.
func (b *InMemoryBackend) CreateNotebookInstance(
	ctx context.Context,
	name, instanceType, roleArn string,
	tags map[string]string,
) (*NotebookInstance, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", ErrValidation)
	}

	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrValidation)
	}

	if roleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	b.mu.Lock("CreateNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.notebooksStore(region).Get(name); ok {
		return nil, fmt.Errorf(
			"%w: notebook instance %s already exists",
			ErrNotebookAlreadyExists,
			name,
		)
	}

	nbARN := arn.Build("sagemaker", region, b.accountID, "notebook-instance/"+name)
	now := time.Now()
	nb := &NotebookInstance{
		NotebookInstanceName:   name,
		NotebookInstanceArn:    nbARN,
		NotebookInstanceStatus: notebookStatusPending,
		InstanceType:           instanceType,
		RoleArn:                roleArn,
		CreationTime:           now,
		LastModifiedTime:       now,
		Tags:                   mergeTags(nil, tags),
	}
	b.notebooksStore(region).Put(nb)
	b.notebookARNIndexStore(region)[nbARN] = name

	return cloneNotebook(nb), nil
}

// DescribeNotebookInstance returns a notebook instance by name.
func (b *InMemoryBackend) DescribeNotebookInstance(ctx context.Context, name string) (*NotebookInstance, error) {
	b.mu.RLock("DescribeNotebookInstance")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	return cloneNotebook(nb), nil
}

// ListNotebookInstancesFilter narrows ListNotebookInstances results.
// Empty fields are treated as wildcards.
type ListNotebookInstancesFilter struct {
	StatusEquals string
	NameContains string
}

// ListNotebookInstances returns notebook instances sorted by name with optional pagination
// and AWS-style filters: StatusEquals (exact, case-insensitive) and NameContains
// (substring, case-insensitive).
func (b *InMemoryBackend) ListNotebookInstances(
	ctx context.Context,
	nextToken string,
	filter ListNotebookInstancesFilter,
) ([]*NotebookInstance, string) {
	b.mu.RLock("ListNotebookInstances")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	store := b.notebooksStore(region)
	list := make([]*NotebookInstance, 0, store.Len())
	for _, nb := range store.All() {
		if !matchesNotebookFilter(nb, filter) {
			continue
		}

		list = append(list, cloneNotebook(nb))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].NotebookInstanceName < list[j].NotebookInstanceName
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*NotebookInstance{}, ""
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

// matchesNotebookFilter reports whether nb satisfies the provided filter.
func matchesNotebookFilter(nb *NotebookInstance, f ListNotebookInstancesFilter) bool {
	if f.StatusEquals != "" && !strings.EqualFold(nb.NotebookInstanceStatus, f.StatusEquals) {
		return false
	}

	if f.NameContains != "" &&
		!strings.Contains(
			strings.ToLower(nb.NotebookInstanceName),
			strings.ToLower(f.NameContains),
		) {
		return false
	}

	return true
}

// DeleteNotebookInstance removes a notebook instance from the backend.
func (b *InMemoryBackend) DeleteNotebookInstance(ctx context.Context, name string) error {
	b.mu.Lock("DeleteNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	arnIdx := b.notebookARNIndexStore(region)
	delete(arnIdx, nb.NotebookInstanceArn)
	store := b.notebooksStore(region)
	store.Delete(name)

	return nil
}

// StartNotebookInstance transitions a notebook instance to InService.
func (b *InMemoryBackend) StartNotebookInstance(ctx context.Context, name string) error {
	b.mu.Lock("StartNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	nb.NotebookInstanceStatus = statusInService
	nb.LastModifiedTime = time.Now()

	return nil
}

// StopNotebookInstance transitions a notebook instance to Stopped.
func (b *InMemoryBackend) StopNotebookInstance(ctx context.Context, name string) error {
	b.mu.Lock("StopNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	nb.NotebookInstanceStatus = notebookStatusStopped
	nb.LastModifiedTime = time.Now()

	return nil
}

// UpdateNotebookInstance updates a notebook instance's instance type.
func (b *InMemoryBackend) UpdateNotebookInstance(ctx context.Context, name, instanceType string) error {
	b.mu.Lock("UpdateNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if instanceType != "" {
		nb.InstanceType = instanceType
	}
	nb.LastModifiedTime = time.Now()

	return nil
}

// CreatePresignedNotebookInstanceURL returns a presigned URL for a notebook instance.
func (b *InMemoryBackend) CreatePresignedNotebookInstanceURL(ctx context.Context, name string) (string, error) {
	b.mu.RLock("CreatePresignedNotebookInstanceURL")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return "", fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	url := "https://" + nb.NotebookInstanceArn + ".notebook.sagemaker.aws/lab"

	return url, nil
}

// ---------------------------------------------------------------------------
// HyperParameterTuningJob
// ---------------------------------------------------------------------------

// CreateHyperParameterTuningJob creates a new HPO job.
func (b *InMemoryBackend) CreateHyperParameterTuningJob(
	ctx context.Context,
	name, strategy string,
	tags map[string]string,
) (*HyperParameterTuningJob, error) {
	b.mu.Lock("CreateHyperParameterTuningJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.hpTuningJobsStore(region).Get(name); ok {
		return nil, fmt.Errorf(
			"%w: HP tuning job %s already exists",
			ErrHPTuningJobAlreadyExists,
			name,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "hyper-parameter-tuning-job/"+name)
	now := time.Now()
	j := &HyperParameterTuningJob{
		HyperParameterTuningJobName:   name,
		HyperParameterTuningJobArn:    jobARN,
		HyperParameterTuningJobStatus: trainingJobStatusInProgress,
		Strategy:                      strategy,
		CreationTime:                  now,
		LastModifiedTime:              now,
		Tags:                          mergeTags(nil, tags),
	}
	b.hpTuningJobsStore(region).Put(j)
	b.hpTuningJobARNIndexStore(region)[jobARN] = name

	return cloneHPTuningJob(j), nil
}

// DescribeHyperParameterTuningJob returns an HP tuning job by name.
func (b *InMemoryBackend) DescribeHyperParameterTuningJob(
	ctx context.Context,
	name string,
) (*HyperParameterTuningJob, error) {
	b.mu.RLock("DescribeHyperParameterTuningJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.hpTuningJobsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: HP tuning job %q not found", ErrHPTuningJobNotFound, name)
	}

	return cloneHPTuningJob(j), nil
}

// ListHyperParameterTuningJobs returns HP tuning jobs sorted by name.
func (b *InMemoryBackend) ListHyperParameterTuningJobs(
	ctx context.Context,
	nextToken string,
) ([]*HyperParameterTuningJob, string) {
	b.mu.RLock("ListHyperParameterTuningJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.hpTuningJobsStore(region), nextToken, cloneHPTuningJob,
		func(a, b *HyperParameterTuningJob) bool {
			return a.HyperParameterTuningJobName < b.HyperParameterTuningJobName
		})
}

// StopHyperParameterTuningJob marks an HP tuning job as Stopping.
func (b *InMemoryBackend) StopHyperParameterTuningJob(ctx context.Context, name string) error {
	b.mu.Lock("StopHyperParameterTuningJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.hpTuningJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: HP tuning job %q not found", ErrHPTuningJobNotFound, name)
	}

	j.HyperParameterTuningJobStatus = pipelineStatusStopping
	j.LastModifiedTime = time.Now()

	return nil
}

// DeleteHyperParameterTuningJob removes an HP tuning job from the backend.
func (b *InMemoryBackend) DeleteHyperParameterTuningJob(ctx context.Context, name string) error {
	b.mu.Lock("DeleteHyperParameterTuningJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.hpTuningJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: HP tuning job %q not found", ErrHPTuningJobNotFound, name)
	}

	arnIdx := b.hpTuningJobARNIndexStore(region)
	delete(arnIdx, j.HyperParameterTuningJobArn)
	store := b.hpTuningJobsStore(region)
	store.Delete(name)

	return nil
}
