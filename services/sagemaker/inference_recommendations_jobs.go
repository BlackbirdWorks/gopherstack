package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob
// ---------------------------------------------------------------------------

var (
	// ErrInferenceRecommendationsJobNotFound is returned when the job does not exist.
	ErrInferenceRecommendationsJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrInferenceRecommendationsJobAlreadyExists is returned when the job already exists.
	ErrInferenceRecommendationsJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// InferenceRecommendationsJob represents a SageMaker inference recommendations job.
type InferenceRecommendationsJob struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	JobName          string            `json:"JobName"`
	JobArn           string            `json:"JobArn"`
	JobType          string            `json:"JobType,omitempty"`
	JobDescription   string            `json:"JobDescription,omitempty"`
	Status           string            `json:"Status"`
	RoleArn          string            `json:"RoleArn,omitempty"`
	InputConfig      json.RawMessage   `json:"InputConfig,omitempty"`
}

func cloneInferenceRecommendationsJob(j *InferenceRecommendationsJob) *InferenceRecommendationsJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.InputConfig = append(json.RawMessage(nil), j.InputConfig...)

	return &cp
}

// CreateInferenceRecommendationsJobOptions holds input fields.
type CreateInferenceRecommendationsJobOptions struct {
	Tags           map[string]string
	JobName        string
	JobType        string
	JobDescription string
	RoleArn        string
	InputConfig    json.RawMessage
}

// CreateInferenceRecommendationsJob creates an inference recommendations job.
func (b *InMemoryBackend) CreateInferenceRecommendationsJob(
	ctx context.Context,
	opts CreateInferenceRecommendationsJobOptions,
) (*InferenceRecommendationsJob, error) {
	b.mu.Lock("CreateInferenceRecommendationsJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	if _, ok := b.inferenceRecommendationsJobsStore(region).Get(opts.JobName); ok {
		return nil, fmt.Errorf(
			"%w: inference recommendations job %q already exists",
			ErrInferenceRecommendationsJobAlreadyExists,
			opts.JobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "inference-recommendations-job/"+opts.JobName)
	now := time.Now()

	j := &InferenceRecommendationsJob{
		JobName:          opts.JobName,
		JobArn:           jobARN,
		JobType:          opts.JobType,
		JobDescription:   opts.JobDescription,
		Status:           "IN_PROGRESS",
		RoleArn:          opts.RoleArn,
		InputConfig:      opts.InputConfig,
		Tags:             mergeTags(nil, opts.Tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.inferenceRecommendationsJobsStore(region).Put(j)

	return cloneInferenceRecommendationsJob(j), nil
}

// DescribeInferenceRecommendationsJob returns an inference recommendations job by name.
func (b *InMemoryBackend) DescribeInferenceRecommendationsJob(
	ctx context.Context,
	name string,
) (*InferenceRecommendationsJob, error) {
	b.mu.RLock("DescribeInferenceRecommendationsJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.inferenceRecommendationsJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: inference recommendations job %q not found",
			ErrInferenceRecommendationsJobNotFound,
			name,
		)
	}

	return cloneInferenceRecommendationsJob(j), nil
}

// StopInferenceRecommendationsJob stops an inference recommendations job.
func (b *InMemoryBackend) StopInferenceRecommendationsJob(ctx context.Context, name string) error {
	b.mu.Lock("StopInferenceRecommendationsJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.inferenceRecommendationsJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf(
			"%w: inference recommendations job %q not found",
			ErrInferenceRecommendationsJobNotFound,
			name,
		)
	}

	j.Status = "STOPPING"
	j.LastModifiedTime = time.Now()

	return nil
}

// ListInferenceRecommendationsJobs returns inference recommendations jobs.
func (b *InMemoryBackend) ListInferenceRecommendationsJobs(
	ctx context.Context,
	nextToken string,
) ([]*InferenceRecommendationsJob, string) {
	b.mu.RLock("ListInferenceRecommendationsJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.inferenceRecommendationsJobsStoreRO(region),
		nextToken,
		cloneInferenceRecommendationsJob,
		func(v *InferenceRecommendationsJob) string { return v.JobName },
	)
}
