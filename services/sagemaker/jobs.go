package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// Job — the generic "model customization job" family added by CreateJob/
// DescribeJob/DeleteJob/StopJob/ListJobs. This is NOT another name for
// TrainingJob/ProcessingJob/TransformJob/AutoMLJob/CompilationJob/etc: those
// job types are each keyed purely by their own *JobName and carry
// domain-specific config (AlgorithmSpecification, ResourceConfig, ...).
// The generic Job here is keyed by JobName (unique per AWS's own doc:
// "The name must be unique within your account and ... Region") and is
// scoped by an opaque JobCategory ("AgentRFT", "AgentRFTEvaluation", ...
// per types.JobCategory — expected to grow) plus an opaque
// JobConfigDocument JSON string validated only against JobConfigSchemaVersion
// (see DescribeJobSchemaVersion/ListJobSchemaVersions below). It lives in
// its own store (b.jobs) and never reads or writes any of the other job
// families' state.
// ---------------------------------------------------------------------------

// ErrJobNotFound is returned when a Job does not exist (or exists under a
// different JobCategory than requested — see resolveJobLocked). Field-diffed
// against deserializers.go: Describe/Stop/Delete/CreateJob all recognize a
// "ResourceNotFound" wire exception.
var ErrJobNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)

// ErrJobAlreadyExists is returned on a duplicate JobName.
var ErrJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)

// ErrJobRunning is returned by DeleteJob when the job is still InProgress —
// real AWS requires StopJob first ("If the job is currently running, you
// must stop it before deleting it by calling StopJob", confirmed against
// DeleteJob's doc comment and its "ResourceInUse" error branch).
var ErrJobRunning = awserr.New("ResourceInUse", awserr.ErrConflict)

// JobSecondaryStatusTransition records a FSM step in a generic Job. Kept as
// its own type (rather than reusing training_jobs.go's
// SecondaryStatusTransition) so the generic Job family's state never aliases
// TrainingJob's, even though the wire shape happens to match.
type JobSecondaryStatusTransition struct {
	StartTime     time.Time  `json:"StartTime"`
	EndTime       *time.Time `json:"EndTime,omitempty"`
	Status        string     `json:"Status"`
	StatusMessage string     `json:"StatusMessage,omitempty"`
}

// Job represents a SageMaker generic model-customization job.
// JobConfigDocument is stored verbatim as the opaque JSON string the client
// submitted — CreateJob only validates it conforms to a known
// JobConfigSchemaVersion (see [InMemoryBackend.CreateJob]), it never parses
// the document's contents.
type Job struct {
	CreationTime               time.Time                      `json:"CreationTime"`
	LastModifiedTime           time.Time                      `json:"LastModifiedTime"`
	EndTime                    *time.Time                     `json:"EndTime,omitempty"`
	Tags                       map[string]string              `json:"Tags,omitempty"`
	JobArn                     string                         `json:"JobArn"`
	JobName                    string                         `json:"JobName"`
	JobCategory                string                         `json:"JobCategory"`
	JobConfigSchemaVersion     string                         `json:"JobConfigSchemaVersion"`
	JobConfigDocument          string                         `json:"JobConfigDocument,omitempty"`
	JobStatus                  string                         `json:"JobStatus"`
	SecondaryStatus            string                         `json:"SecondaryStatus"`
	RoleArn                    string                         `json:"RoleArn"`
	FailureReason              string                         `json:"FailureReason,omitempty"`
	SecondaryStatusTransitions []JobSecondaryStatusTransition `json:"SecondaryStatusTransitions"`
}

// MarshalJSON emits CreationTime/LastModifiedTime/EndTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings.
func (j *Job) MarshalJSON() ([]byte, error) {
	type alias Job

	return json.Marshal(struct {
		*alias
		EndTime          *float64 `json:"EndTime,omitempty"`
		CreationTime     float64  `json:"CreationTime"`
		LastModifiedTime float64  `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(j),
		CreationTime:     epochSeconds(j.CreationTime),
		LastModifiedTime: epochSeconds(j.LastModifiedTime),
		EndTime:          epochSecondsPtr(j.EndTime),
	})
}

// UnmarshalJSON is the inverse of [Job.MarshalJSON], used by persistence.go's
// snapshot restore path.
func (j *Job) UnmarshalJSON(data []byte) error {
	type alias Job

	aux := struct {
		*alias
		EndTime          *float64 `json:"EndTime,omitempty"`
		CreationTime     float64  `json:"CreationTime"`
		LastModifiedTime float64  `json:"LastModifiedTime"`
	}{alias: (*alias)(j)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	j.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	j.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)
	j.EndTime = timeFromEpochSecondsPtr(aux.EndTime)

	return nil
}

func cloneJob(j *Job) *Job {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.SecondaryStatusTransitions = append(
		[]JobSecondaryStatusTransition(nil),
		j.SecondaryStatusTransitions...,
	)

	if j.EndTime != nil {
		t := *j.EndTime
		cp.EndTime = &t
	}

	return &cp
}

func (b *InMemoryBackend) jobsStore(r string) *store.Table[Job] {
	if b.jobs[r] == nil {
		b.jobs[r] = store.Register(b.registry, "jobs:"+r, store.New(func(v *Job) string { return v.JobName }))
	}

	return b.jobs[r]
}

// jobsStoreRO returns the region-scoped jobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the
// region has not been observed yet, it returns a fresh, unregistered, empty
// view instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) jobsStoreRO(r string) *store.Table[Job] {
	if v := b.jobs[r]; v != nil {
		return v
	}

	return store.New(func(v *Job) string { return v.JobName })
}

// resolveJobLocked looks up a Job by name, additionally verifying it was
// created under the given category (Describe/Delete/Stop/CreateJob all take
// JobCategory alongside JobName even though JobName alone is the unique
// key — see [Job]'s doc comment). A category mismatch is treated the same
// as not-found, matching real APIs that scope resource visibility by a
// caller-supplied partition key.
func (b *InMemoryBackend) resolveJobLocked(region, category, name string) (*Job, error) {
	j, ok := b.jobsStoreRO(region).Get(name)
	if !ok || j.JobCategory != category {
		return nil, fmt.Errorf("%w: job %q not found in category %q", ErrJobNotFound, name, category)
	}

	return j, nil
}

// defaultJobConfigSchemaVersion is the only schema version this backend
// knows about. AWS does not ship per-category schema definitions in the
// SDK — DescribeJobSchemaVersion's JobConfigSchema is an opaque
// server-generated JSON-schema string — so every JobCategory is served the
// same single, generic (not category-specific) permissive schema.
const defaultJobConfigSchemaVersion = "1.0"

// jobConfigSchemaVersionsForCategory returns the schema versions known for
// category. Every category currently maps to the same single version; this
// is still routed through a real function (rather than inlined in the
// handler) so DescribeJobSchemaVersion/ListJobSchemaVersions/CreateJob all
// consult one source of truth.
func jobConfigSchemaVersionsForCategory(category string) []string {
	if category == "" {
		return nil
	}

	return []string{defaultJobConfigSchemaVersion}
}

// jobConfigSchemaDocument returns the JSON-schema document text for
// category/version. Deliberately generic (not pretending to model AWS's
// actual, unpublished AgentRFT/AgentRFTEvaluation config semantics) —
// title-tagged by category so two Describe calls for different categories
// are still distinguishable.
func jobConfigSchemaDocument(category string) string {
	return fmt.Sprintf(
		`{"$schema":"http://json-schema.org/draft-07/schema#","title":%q,"type":"object","additionalProperties":true}`,
		category+"JobConfig",
	)
}

// CreateJobOptions holds input fields for CreateJob.
type CreateJobOptions struct {
	Tags                   map[string]string
	JobCategory            string
	JobConfigDocument      string
	JobConfigSchemaVersion string
	JobName                string
	RoleArn                string
}

// CreateJob creates a generic model-customization job and schedules its
// InProgress -> Completed transition after [aiJobInProgressToCompleted].
func (b *InMemoryBackend) CreateJob(ctx context.Context, opts CreateJobOptions) (*Job, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	if err := validateCreateJobOptions(opts); err != nil {
		return nil, err
	}

	versions := jobConfigSchemaVersionsForCategory(opts.JobCategory)
	if !slicesContains(versions, opts.JobConfigSchemaVersion) {
		return nil, fmt.Errorf(
			"%w: unknown JobConfigSchemaVersion %q for category %q",
			ErrJobNotFound,
			opts.JobConfigSchemaVersion,
			opts.JobCategory,
		)
	}

	tbl := b.jobsStore(region)
	if _, ok := tbl.Get(opts.JobName); ok {
		return nil, fmt.Errorf("%w: job %q already exists", ErrJobAlreadyExists, opts.JobName)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "job/"+opts.JobName)
	now := time.Now()

	j := &Job{
		JobName:                opts.JobName,
		JobArn:                 jobARN,
		JobCategory:            opts.JobCategory,
		JobConfigDocument:      opts.JobConfigDocument,
		JobConfigSchemaVersion: opts.JobConfigSchemaVersion,
		JobStatus:              trainingJobStatusInProgress,
		SecondaryStatus:        secondaryStatusStarting,
		RoleArn:                opts.RoleArn,
		Tags:                   mergeTags(nil, opts.Tags),
		CreationTime:           now,
		LastModifiedTime:       now,
		SecondaryStatusTransitions: []JobSecondaryStatusTransition{
			{StartTime: now, Status: secondaryStatusStarting},
		},
	}
	tbl.Put(j)

	b.scheduleJobCompletion(b.lifecycleCtx, region, opts.JobName)

	return cloneJob(j), nil
}

func validateCreateJobOptions(opts CreateJobOptions) error {
	switch {
	case opts.JobName == "":
		return fmt.Errorf("%w: JobName is required", ErrValidation)
	case opts.JobCategory == "":
		return fmt.Errorf("%w: JobCategory is required", ErrValidation)
	case opts.JobConfigDocument == "":
		return fmt.Errorf("%w: JobConfigDocument is required", ErrValidation)
	case opts.JobConfigSchemaVersion == "":
		return fmt.Errorf("%w: JobConfigSchemaVersion is required", ErrValidation)
	case opts.RoleArn == "":
		return fmt.Errorf("%w: RoleArn is required", ErrValidation)
	default:
		return nil
	}
}

// scheduleJobCompletion drives InProgress -> Completed after
// [aiJobInProgressToCompleted]. No customization-job output metrics are
// fabricated: real AWS would attach an output-model reference here, which
// this synthetic backend does not invent.
func (b *InMemoryBackend) scheduleJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, aiJobInProgressToCompleted, func() {
		b.mu.Lock("scheduleJobCompletion.goroutine")
		defer b.mu.Unlock()

		j, ok := b.jobsStore(region).Get(name)
		if !ok || j.JobStatus != trainingJobStatusInProgress {
			return
		}

		now := time.Now()
		j.JobStatus = algorithmStatusCompleted
		j.SecondaryStatus = algorithmStatusCompleted
		j.LastModifiedTime = now
		j.EndTime = &now
		j.SecondaryStatusTransitions = append(
			j.SecondaryStatusTransitions,
			JobSecondaryStatusTransition{StartTime: now, EndTime: &now, Status: algorithmStatusCompleted},
		)
	})
}

// DescribeJob returns a Job by category and name.
func (b *InMemoryBackend) DescribeJob(ctx context.Context, category, name string) (*Job, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeJob")
	defer b.mu.RUnlock()

	j, err := b.resolveJobLocked(region, category, name)
	if err != nil {
		return nil, err
	}

	return cloneJob(j), nil
}

// DeleteJob removes a Job by category and name. It rejects deletion of a
// still-InProgress job with [ErrJobRunning] (real AWS requires StopJob
// first — see [ErrJobRunning]'s doc comment).
func (b *InMemoryBackend) DeleteJob(ctx context.Context, category, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	j, err := b.resolveJobLocked(region, category, name)
	if err != nil {
		return err
	}

	if j.JobStatus == trainingJobStatusInProgress {
		return fmt.Errorf("%w: job %q is still running; call StopJob first", ErrJobRunning, name)
	}

	b.jobsStore(region).Delete(name)

	return nil
}

// StopJob transitions an in-progress Job InProgress -> Stopping -> Stopped.
// Stopping a job that is not currently InProgress is a no-op (idempotent).
func (b *InMemoryBackend) StopJob(ctx context.Context, category, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopJob")
	defer b.mu.Unlock()

	j, err := b.resolveJobLocked(region, category, name)
	if err != nil {
		return err
	}

	if j.JobStatus != trainingJobStatusInProgress {
		return nil
	}

	now := time.Now()
	j.JobStatus = pipelineStatusStopping
	j.SecondaryStatus = pipelineStatusStopping
	j.LastModifiedTime = now
	j.SecondaryStatusTransitions = append(
		j.SecondaryStatusTransitions,
		JobSecondaryStatusTransition{StartTime: now, Status: pipelineStatusStopping},
	)

	b.runDelayed(b.lifecycleCtx, aiJobStoppingToStopped, func() {
		b.mu.Lock("StopJob.goroutine")
		defer b.mu.Unlock()

		if j2, ok2 := b.jobsStore(region).Get(name); ok2 && j2.JobStatus == pipelineStatusStopping {
			stoppedAt := time.Now()
			j2.JobStatus = pipelineStatusStopped
			j2.SecondaryStatus = pipelineStatusStopped
			j2.LastModifiedTime = stoppedAt
			j2.EndTime = &stoppedAt
			j2.SecondaryStatusTransitions = append(
				j2.SecondaryStatusTransitions,
				JobSecondaryStatusTransition{StartTime: stoppedAt, EndTime: &stoppedAt, Status: pipelineStatusStopped},
			)
		}
	})

	return nil
}

// ListJobsParams bundles the filter/sort criteria for ListJobs.
type ListJobsParams struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	JobCategory            string
	NameContains           string
	StatusEquals           string
	SortBy                 string
	SortOrder              string
	NextToken              string
	MaxResults             int32
}

// ListJobs lists Jobs within a single JobCategory (required by the real
// API), optionally further filtered and sorted.
func (b *InMemoryBackend) ListJobs(ctx context.Context, params ListJobsParams) ([]*Job, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	tbl := b.jobsStoreRO(region)
	list := make([]*Job, 0, tbl.Len())

	for _, j := range tbl.All() {
		if j.JobCategory != params.JobCategory {
			continue
		}

		if !jobMatchesFilter(j, params) {
			continue
		}

		list = append(list, cloneJob(j))
	}

	sortJobs(list, params.SortBy, params.SortOrder)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func jobMatchesFilter(j *Job, params ListJobsParams) bool {
	if params.StatusEquals != "" && j.JobStatus != params.StatusEquals {
		return false
	}

	if params.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.JobName), strings.ToLower(params.NameContains)) {
		return false
	}

	if params.CreationTimeAfter != nil && !j.CreationTime.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !j.CreationTime.Before(*params.CreationTimeBefore) {
		return false
	}

	if params.LastModifiedTimeAfter != nil && !j.LastModifiedTime.After(*params.LastModifiedTimeAfter) {
		return false
	}

	if params.LastModifiedTimeBefore != nil && !j.LastModifiedTime.Before(*params.LastModifiedTimeBefore) {
		return false
	}

	return true
}

func sortJobs(list []*Job, sortBy, sortOrder string) {
	desc := sortOrder == sortOrderDescending

	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch sortBy {
		case keyStatus:
			less = list[i].JobStatus < list[j].JobStatus
		case keyCreationTime:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		default:
			less = list[i].JobName < list[j].JobName
		}

		if desc {
			return !less
		}

		return less
	})
}

// slicesContains reports whether s contains v. A tiny local helper so this
// file doesn't need to import the generic "slices" package solely for one
// call site.
func slicesContains(s []string, v string) bool {
	return slices.Contains(s, v)
}

// ---------------------------------------------------------------------------
// JobSchemaVersion — read-only lookups over jobConfigSchemaVersionsForCategory.
// ---------------------------------------------------------------------------

// JobConfigSchemaVersionInfo bundles a single job-config schema version and
// its schema document, as returned by DescribeJobSchemaVersion.
type JobConfigSchemaVersionInfo struct {
	JobConfigSchemaVersion string
	JobConfigSchema        string
}

// DescribeJobSchemaVersion returns the schema document for category/version.
// If version is empty, the latest known version for category is used.
func (b *InMemoryBackend) DescribeJobSchemaVersion(
	_ context.Context,
	category, version string,
) (*JobConfigSchemaVersionInfo, error) {
	if category == "" {
		return nil, fmt.Errorf("%w: JobCategory is required", ErrValidation)
	}

	versions := jobConfigSchemaVersionsForCategory(category)

	if version == "" {
		version = versions[len(versions)-1]
	} else if !slicesContains(versions, version) {
		return nil, fmt.Errorf(
			"%w: schema version %q not found for category %q",
			ErrJobNotFound,
			version,
			category,
		)
	}

	return &JobConfigSchemaVersionInfo{
		JobConfigSchemaVersion: version,
		JobConfigSchema:        jobConfigSchemaDocument(category),
	}, nil
}

// ListJobSchemaVersions returns the known schema versions for category.
func (b *InMemoryBackend) ListJobSchemaVersions(
	_ context.Context,
	category, nextToken string,
	maxResults int32,
) ([]string, string, error) {
	if category == "" {
		return nil, "", fmt.Errorf("%w: JobCategory is required", ErrValidation)
	}

	versions, token := paginateSlice(jobConfigSchemaVersionsForCategory(category), nextToken, maxResults)

	return versions, token, nil
}
