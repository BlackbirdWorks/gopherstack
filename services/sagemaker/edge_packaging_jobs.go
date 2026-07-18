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
// EdgePackagingJob
// ---------------------------------------------------------------------------

var (
	// ErrEdgePackagingJobNotFound is returned when an edge packaging job does not exist.
	ErrEdgePackagingJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrEdgePackagingJobAlreadyExists is returned when an edge packaging job already exists.
	ErrEdgePackagingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// EdgePackagingJob represents a SageMaker edge packaging job.
type EdgePackagingJob struct {
	CreationTime           time.Time         `json:"CreationTime"`
	LastModifiedTime       time.Time         `json:"LastModifiedTime"`
	Tags                   map[string]string `json:"Tags,omitempty"`
	EdgePackagingJobName   string            `json:"EdgePackagingJobName"`
	EdgePackagingJobArn    string            `json:"EdgePackagingJobArn"`
	EdgePackagingJobStatus string            `json:"EdgePackagingJobStatus"`
	ModelName              string            `json:"ModelName,omitempty"`
	ModelVersion           string            `json:"ModelVersion,omitempty"`
	RoleArn                string            `json:"RoleArn,omitempty"`
	CompilationJobName     string            `json:"CompilationJobName,omitempty"`
	FailureReason          string            `json:"FailureReason,omitempty"`
}

func cloneEdgePackagingJob(j *EdgePackagingJob) *EdgePackagingJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateEdgePackagingJobOptions holds input fields for CreateEdgePackagingJob.
type CreateEdgePackagingJobOptions struct {
	Tags                 map[string]string
	EdgePackagingJobName string
	ModelName            string
	ModelVersion         string
	RoleArn              string
	CompilationJobName   string
}

// CreateEdgePackagingJob creates a SageMaker edge packaging job.
func (b *InMemoryBackend) CreateEdgePackagingJob(
	ctx context.Context,
	opts CreateEdgePackagingJobOptions,
) (*EdgePackagingJob, error) {
	b.mu.Lock("CreateEdgePackagingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", ErrValidation)
	}

	if _, ok := b.edgePackagingJobsStore(region).Get(opts.EdgePackagingJobName); ok {
		return nil, fmt.Errorf(
			"%w: edge packaging job %q already exists",
			ErrEdgePackagingJobAlreadyExists,
			opts.EdgePackagingJobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "edge-packaging-job/"+opts.EdgePackagingJobName)
	now := time.Now()

	j := &EdgePackagingJob{
		EdgePackagingJobName:   opts.EdgePackagingJobName,
		EdgePackagingJobArn:    jobARN,
		EdgePackagingJobStatus: "STARTING",
		ModelName:              opts.ModelName,
		ModelVersion:           opts.ModelVersion,
		RoleArn:                opts.RoleArn,
		CompilationJobName:     opts.CompilationJobName,
		Tags:                   mergeTags(nil, opts.Tags),
		CreationTime:           now,
		LastModifiedTime:       now,
	}
	b.edgePackagingJobsStore(region).Put(j)

	return cloneEdgePackagingJob(j), nil
}

// DescribeEdgePackagingJob returns an edge packaging job by name.
func (b *InMemoryBackend) DescribeEdgePackagingJob(ctx context.Context, name string) (*EdgePackagingJob, error) {
	b.mu.RLock("DescribeEdgePackagingJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.edgePackagingJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: edge packaging job %q not found", ErrEdgePackagingJobNotFound, name)
	}

	return cloneEdgePackagingJob(j), nil
}

// StopEdgePackagingJob stops an edge packaging job.
func (b *InMemoryBackend) StopEdgePackagingJob(ctx context.Context, name string) error {
	b.mu.Lock("StopEdgePackagingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.edgePackagingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: edge packaging job %q not found", ErrEdgePackagingJobNotFound, name)
	}

	j.EdgePackagingJobStatus = "STOPPING"
	j.LastModifiedTime = time.Now()

	return nil
}

// ListEdgePackagingJobsFilter holds optional filters for ListEdgePackagingJobs.
type ListEdgePackagingJobsFilter struct {
	StatusEquals string
	NameContains string
}

// ListEdgePackagingJobs returns edge packaging jobs with optional filters.
func (b *InMemoryBackend) ListEdgePackagingJobs(
	ctx context.Context,
	nextToken string,
	filter ListEdgePackagingJobsFilter,
) ([]*EdgePackagingJob, string) {
	b.mu.RLock("ListEdgePackagingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var keys []string

	for _, j := range b.edgePackagingJobsStoreRO(region).All() {
		if filter.StatusEquals != "" && j.EdgePackagingJobStatus != filter.StatusEquals {
			continue
		}

		if filter.NameContains != "" && !strings.Contains(j.EdgePackagingJobName, filter.NameContains) {
			continue
		}

		keys = append(keys, j.EdgePackagingJobName)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	store := b.edgePackagingJobsStoreRO(region)

	out := make([]*EdgePackagingJob, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneEdgePackagingJob(tableGet(store, k)))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}
