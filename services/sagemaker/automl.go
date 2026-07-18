package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrAutoMLJobNotFound is returned when an AutoML job does not exist.
	ErrAutoMLJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAutoMLJobNotStoppable is returned when stopping an already-terminal AutoML job.
	ErrAutoMLJobNotStoppable = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// ---------------------------------------------------------------------------
// AutoMLJob
// ---------------------------------------------------------------------------

// AutoMLOutputDataConfig specifies the S3 output location for an AutoML job.
type AutoMLOutputDataConfig struct {
	S3OutputPath string `json:"S3OutputPath,omitempty"`
	KmsKeyID     string `json:"KmsKeyId,omitempty"`
}

// AutoMLJobObjective specifies the optimization metric for an AutoML job.
type AutoMLJobObjective struct {
	MetricName string `json:"MetricName"`
}

// AutoMLJob represents a SageMaker AutoML job.
type AutoMLJob struct {
	CreationTime       time.Time               `json:"CreationTime"`
	Tags               map[string]string       `json:"Tags,omitempty"`
	OutputDataConfig   *AutoMLOutputDataConfig `json:"OutputDataConfig,omitempty"`
	AutoMLJobObjective *AutoMLJobObjective     `json:"AutoMLJobObjective,omitempty"`
	AutoMLJobName      string                  `json:"AutoMLJobName"`
	AutoMLJobArn       string                  `json:"AutoMLJobArn"`
	AutoMLJobStatus    string                  `json:"AutoMLJobStatus"`
	RoleArn            string                  `json:"RoleArn,omitempty"`
}

func cloneAutoMLJob(j *AutoMLJob) *AutoMLJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	if j.OutputDataConfig != nil {
		odc := *j.OutputDataConfig
		cp.OutputDataConfig = &odc
	}

	if j.AutoMLJobObjective != nil {
		obj := *j.AutoMLJobObjective
		cp.AutoMLJobObjective = &obj
	}

	return &cp
}

// CreateAutoMLJob creates an AutoML job.
func (b *InMemoryBackend) CreateAutoMLJob(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*AutoMLJob, error) {
	b.mu.Lock("CreateAutoMLJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", ErrValidation)
	}

	if _, ok := b.autoMLJobsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: AutoML job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "automl-job/"+name)

	j := &AutoMLJob{
		AutoMLJobName:   name,
		AutoMLJobArn:    jobARN,
		AutoMLJobStatus: trainingJobStatusInProgress,
		RoleArn:         roleArn,
		Tags:            mergeTags(nil, tags),
		CreationTime:    time.Now(),
	}
	b.autoMLJobsStore(region).Put(j)

	return cloneAutoMLJob(j), nil
}

// DescribeAutoMLJob returns an AutoML job by name.
func (b *InMemoryBackend) DescribeAutoMLJob(ctx context.Context, name string) (*AutoMLJob, error) {
	b.mu.RLock("DescribeAutoMLJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.autoMLJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	return cloneAutoMLJob(j), nil
}

// StopAutoMLJob sets an AutoML job status to "Stopped".
func (b *InMemoryBackend) StopAutoMLJob(ctx context.Context, name string) error {
	b.mu.Lock("StopAutoMLJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.autoMLJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	// AWS rejects stopping a job that is already in a terminal state.
	if j.AutoMLJobStatus == algorithmStatusCompleted || j.AutoMLJobStatus == pipelineStatusStopped {
		return fmt.Errorf("%w: AutoML job %q cannot be stopped (status: %s)",
			ErrAutoMLJobNotStoppable, name, j.AutoMLJobStatus)
	}

	j.AutoMLJobStatus = pipelineStatusStopped

	return nil
}

// ListAutoMLJobs returns all AutoML jobs sorted by name.
func (b *InMemoryBackend) ListAutoMLJobs(ctx context.Context, nextToken string) ([]*AutoMLJob, string) {
	b.mu.RLock("ListAutoMLJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.autoMLJobsStoreRO(region),
		nextToken,
		cloneAutoMLJob,
		func(v *AutoMLJob) string { return v.AutoMLJobName },
	)
}

// SetAutoMLJobExtras sets optional configuration fields on an existing AutoML job
// that were not included in the original CreateAutoMLJob signature.
func (b *InMemoryBackend) SetAutoMLJobExtras(
	ctx context.Context,
	name string,
	outputDataConfig *AutoMLOutputDataConfig,
	objective *AutoMLJobObjective,
) error {
	b.mu.Lock("SetAutoMLJobExtras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.autoMLJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	if outputDataConfig != nil {
		odc := *outputDataConfig
		j.OutputDataConfig = &odc
	}

	if objective != nil {
		obj := *objective
		j.AutoMLJobObjective = &obj
	}

	return nil
}
