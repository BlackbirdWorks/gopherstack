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
	// ErrHPTuningJobNotFound is returned when an HP tuning job does not exist.
	ErrHPTuningJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrHPTuningJobAlreadyExists is returned when an HP tuning job already exists.
	ErrHPTuningJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// HPResourceLimits mirrors AWS's ResourceLimits: the maximum number of
// training jobs (total and concurrent) a tuning job may launch.
type HPResourceLimits struct {
	MaxParallelTrainingJobs int32 `json:"MaxParallelTrainingJobs"`
	MaxNumberOfTrainingJobs int32 `json:"MaxNumberOfTrainingJobs,omitempty"`
	MaxRuntimeInSeconds     int32 `json:"MaxRuntimeInSeconds,omitempty"`
}

// HPObjectiveStatusCounters mirrors AWS's ObjectiveStatusCounters: counts of
// child training jobs by objective-metric-evaluation status. This emulator
// does not launch child training jobs, so these always read zero.
type HPObjectiveStatusCounters struct {
	Succeeded int32 `json:"Succeeded"`
	Pending   int32 `json:"Pending"`
	Failed    int32 `json:"Failed"`
}

// HPTrainingJobStatusCounters mirrors AWS's TrainingJobStatusCounters: counts
// of child training jobs by status. This emulator does not launch child
// training jobs, so these always read zero.
type HPTrainingJobStatusCounters struct {
	Completed         int32 `json:"Completed"`
	InProgress        int32 `json:"InProgress"`
	NonRetryableError int32 `json:"NonRetryableError"`
	RetryableError    int32 `json:"RetryableError"`
	Stopped           int32 `json:"Stopped"`
}

type HyperParameterTuningJob struct {
	CreationTime                  time.Time                   `json:"CreationTime"`
	LastModifiedTime              time.Time                   `json:"LastModifiedTime"`
	Tags                          map[string]string           `json:"Tags,omitempty"`
	HyperParameterTuningJobName   string                      `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobArn    string                      `json:"HyperParameterTuningJobArn"`
	HyperParameterTuningJobStatus string                      `json:"HyperParameterTuningJobStatus"`
	Strategy                      string                      `json:"Strategy,omitempty"`
	ResourceLimits                HPResourceLimits            `json:"ResourceLimits"`
	ObjectiveStatusCounters       HPObjectiveStatusCounters   `json:"ObjectiveStatusCounters"`
	TrainingJobStatusCounters     HPTrainingJobStatusCounters `json:"TrainingJobStatusCounters"`
}

// cloneHPTuningJob returns a deep copy of j.
func cloneHPTuningJob(j *HyperParameterTuningJob) *HyperParameterTuningJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// ---------------------------------------------------------------------------
// HyperParameterTuningJob
// ---------------------------------------------------------------------------

// CreateHyperParameterTuningJob creates a new HPO job.
func (b *InMemoryBackend) CreateHyperParameterTuningJob(
	ctx context.Context,
	name, strategy string,
	limits HPResourceLimits,
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
		ResourceLimits:                limits,
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

	j, ok := b.hpTuningJobsStoreRO(region).Get(name)
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

	return sagemakerListPaged(b.hpTuningJobsStoreRO(region), nextToken, cloneHPTuningJob,
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

// ListTrainingJobsForHyperParameterTuningJob returns training jobs for an HP tuning job.
// Since this emulator does not launch training jobs automatically, it always returns empty.
func (b *InMemoryBackend) ListTrainingJobsForHyperParameterTuningJob(
	ctx context.Context,
	jobName, _ string,
) ([]*TrainingJob, string, error) {
	b.mu.RLock("ListTrainingJobsForHyperParameterTuningJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.hpTuningJobsStoreRO(region).Get(jobName); !ok {
		return nil, "", fmt.Errorf("%w: HP tuning job %q not found", ErrHPTuningJobNotFound, jobName)
	}

	return []*TrainingJob{}, "", nil
}
