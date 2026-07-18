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
	// ErrCompilationJobNotFound is returned when a compilation job does not exist.
	ErrCompilationJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrCompilationJobNotStoppable is returned when stopping an already-terminal compilation job.
	ErrCompilationJobNotStoppable = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const jobStatusStopped = "STOPPED"

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
	CreationTime         time.Time                `json:"CreationTime"`
	LastModifiedTime     time.Time                `json:"LastModifiedTime"`
	Tags                 map[string]string        `json:"Tags,omitempty"`
	InputConfig          *CompilationInputConfig  `json:"InputConfig,omitempty"`
	OutputConfig         *CompilationOutputConfig `json:"OutputConfig,omitempty"`
	StoppingCondition    *StoppingCondition       `json:"StoppingCondition,omitempty"`
	CompilationJobName   string                   `json:"CompilationJobName"`
	CompilationJobArn    string                   `json:"CompilationJobArn"`
	CompilationJobStatus string                   `json:"CompilationJobStatus"`
	RoleArn              string                   `json:"RoleArn,omitempty"`
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

	return &cp
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
		CompilationJobStatus: "INPROGRESS",
		RoleArn:              roleArn,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         now,
		LastModifiedTime:     now,
	}
	b.compilationJobsStore(region).Put(j)

	return cloneCompilationJob(j), nil
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

// StopCompilationJob sets a compilation job status to "STOPPED".
func (b *InMemoryBackend) StopCompilationJob(ctx context.Context, name string) error {
	b.mu.Lock("StopCompilationJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.compilationJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	// AWS rejects stopping a job that is already in a terminal state.
	if j.CompilationJobStatus == "COMPLETED" || j.CompilationJobStatus == jobStatusStopped {
		return fmt.Errorf("%w: compilation job %q is not running (status: %s)",
			ErrCompilationJobNotStoppable, name, j.CompilationJobStatus)
	}

	j.CompilationJobStatus = jobStatusStopped
	j.LastModifiedTime = time.Now()

	return nil
}

// ListCompilationJobs returns all compilation jobs sorted by name.
func (b *InMemoryBackend) ListCompilationJobs(ctx context.Context, nextToken string) ([]*CompilationJob, string) {
	b.mu.RLock("ListCompilationJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.compilationJobsStoreRO(region),
		nextToken,
		cloneCompilationJob,
		func(v *CompilationJob) string { return v.CompilationJobName },
	)
}

// SetCompilationJobExtras sets optional configuration fields on an existing compilation job
// that were not included in the original CreateCompilationJob signature.
func (b *InMemoryBackend) SetCompilationJobExtras(
	ctx context.Context,
	name string,
	inputConfig *CompilationInputConfig,
	outputConfig *CompilationOutputConfig,
	stoppingCondition *StoppingCondition,
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

	j.LastModifiedTime = time.Now()

	return nil
}
