package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrOptimizationJobNotFound is returned when an optimization job does not exist.
var ErrOptimizationJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// OptimizationJob
// ---------------------------------------------------------------------------

// OptimizationJob represents a SageMaker optimization job.
type OptimizationJob struct {
	CreationTime          time.Time         `json:"CreationTime"`
	LastModifiedTime      time.Time         `json:"LastModifiedTime"`
	Tags                  map[string]string `json:"Tags,omitempty"`
	OptimizationJobName   string            `json:"OptimizationJobName"`
	OptimizationJobArn    string            `json:"OptimizationJobArn"`
	OptimizationJobStatus string            `json:"OptimizationJobStatus"`
	RoleArn               string            `json:"RoleArn,omitempty"`
}

func cloneOptimizationJob(j *OptimizationJob) *OptimizationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateOptimizationJob creates an optimization job.
func (b *InMemoryBackend) CreateOptimizationJob(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*OptimizationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateOptimizationJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", ErrValidation)
	}

	store := b.optimizationJobsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: optimization job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "optimization-job/"+name)
	now := time.Now()

	j := &OptimizationJob{
		OptimizationJobName:   name,
		OptimizationJobArn:    jobARN,
		OptimizationJobStatus: "COMPLETED",
		RoleArn:               roleArn,
		Tags:                  mergeTags(nil, tags),
		CreationTime:          now,
		LastModifiedTime:      now,
	}
	store.Put(j)

	return cloneOptimizationJob(j), nil
}

// DescribeOptimizationJob returns an optimization job by name.
func (b *InMemoryBackend) DescribeOptimizationJob(ctx context.Context, name string) (*OptimizationJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeOptimizationJob")
	defer b.mu.RUnlock()

	j, ok := b.optimizationJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	return cloneOptimizationJob(j), nil
}

// DeleteOptimizationJob removes an optimization job by name.
func (b *InMemoryBackend) DeleteOptimizationJob(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteOptimizationJob")
	defer b.mu.Unlock()

	store := b.optimizationJobsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	store.Delete(name)

	return nil
}

// StopOptimizationJob sets an optimization job status to "STOPPED".
func (b *InMemoryBackend) StopOptimizationJob(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopOptimizationJob")
	defer b.mu.Unlock()

	j, ok := b.optimizationJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	j.OptimizationJobStatus = jobStatusStopped
	j.LastModifiedTime = time.Now()

	return nil
}

// ListOptimizationJobs returns all optimization jobs.
func (b *InMemoryBackend) ListOptimizationJobs(ctx context.Context, nextToken string) ([]*OptimizationJob, string) {
	b.mu.RLock("ListOptimizationJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.optimizationJobsStoreRO(region),
		nextToken,
		cloneOptimizationJob,
		func(v *OptimizationJob) string { return v.OptimizationJobName },
	)
}
