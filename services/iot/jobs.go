package iot

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AssociateTargetsWithJob associates targets with a continuous job. Real AWS
// IoT returns ResourceNotFoundException for an unknown job ID.
func (b *InMemoryBackend) AssociateTargetsWithJob(
	input *AssociateTargetsWithJobInput,
) (*AssociateTargetsWithJobOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.jobs.Has(input.JobID) {
		return nil, fmt.Errorf("job %q not found: %w", input.JobID, ErrResourceNotFound)
	}

	b.jobTargets[input.JobID] = append(b.jobTargets[input.JobID], input.Targets...)

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("job/%s", input.JobID))

	return &AssociateTargetsWithJobOutput{
		JobID:  input.JobID,
		JobArn: arn,
	}, nil
}

// ListJobExecutionsForJob returns summaries of all executions for a job.
func (b *InMemoryBackend) ListJobExecutionsForJob(jobID string) []*JobExecution {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*JobExecution
	for _, exec := range b.jobExecutions.All() {
		if exec.JobID == jobID {
			cp := *exec
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ThingName < out[j].ThingName })

	return out
}

// ListJobExecutionsForThing returns summaries of all executions for a thing.
func (b *InMemoryBackend) ListJobExecutionsForThing(thingName string) []*JobExecution {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*JobExecution
	for _, exec := range b.jobExecutions.All() {
		if exec.ThingName == thingName {
			cp := *exec
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })

	return out
}

// JobStatus represents the status of a job.
type JobStatus string

const (
	JobStatusInProgress JobStatus = "IN_PROGRESS"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
	JobStatusCanceled   JobStatus = "CANCELED"
	JobStatusDeletion   JobStatus = "DELETION_IN_PROGRESS"
)

// JobExecutionStatus represents the status of a job execution on a thing.
type JobExecutionStatus string

const (
	JobExecQueued     JobExecutionStatus = "QUEUED"
	JobExecInProgress JobExecutionStatus = "IN_PROGRESS"
	JobExecSucceeded  JobExecutionStatus = "SUCCEEDED"
	JobExecFailed     JobExecutionStatus = "FAILED"
	JobExecCanceled   JobExecutionStatus = "CANCELED"
	JobExecRejected   JobExecutionStatus = "REJECTED"
	JobExecRemoved    JobExecutionStatus = "REMOVED"
)

// AbortConfig holds abort criteria for a job.
type AbortConfig struct {
	CriteriaList []AbortCriteria `json:"criteriaList,omitempty"`
}

// AbortCriteria is a single abort criterion.
type AbortCriteria struct {
	Action                    string  `json:"action,omitempty"`
	FailureType               string  `json:"failureType,omitempty"`
	MinNumberOfExecutedThings int     `json:"minNumberOfExecutedThings,omitempty"`
	ThresholdPercentage       float64 `json:"thresholdPercentage,omitempty"`
}

// JobExecutionsRolloutConfig holds rollout config for a job.
type JobExecutionsRolloutConfig struct {
	MaximumPerMinute int `json:"maximumPerMinute,omitempty"`
}

// TimeoutConfig holds timeout settings for a job.
type TimeoutConfig struct {
	InProgressTimeoutInMinutes int64 `json:"inProgressTimeoutInMinutes,omitempty"`
}

// Job represents an IoT job.
//
// Tags, Document, and DocumentSource are internal-only (json:"-"): real AWS
// IoT's Job shape (aws-sdk-go-v2/service/iot/types.Job, verified against
// awsRestjson1_deserializeDocumentJob in v1.76.0) has none of these three
// fields -- tags are a separate ListTagsForResource concept, the job
// document is only returned via GetJobDocument, and documentSource is a
// top-level DescribeJobOutput field, not part of the nested Job object. They
// stay on this struct purely as backend storage (Document for
// GetJobDocument, DocumentSource for the DescribeJobOutput top-level field,
// Tags for future TagResource wiring) but must never leak into a JSON
// response that embeds the whole Job struct.
type Job struct {
	Tags                       map[string]string           `json:"-"`
	DocumentParameters         map[string]string           `json:"documentParameters,omitempty"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobID                      string                      `json:"jobId"`
	JobARN                     string                      `json:"jobArn"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"-"`
	DocumentSource             string                      `json:"-"`
	JobTemplateARN             string                      `json:"jobTemplateArn,omitempty"`
	Status                     JobStatus                   `json:"status"`
	TargetSelection            string                      `json:"targetSelection,omitempty"`
	Targets                    []string                    `json:"targets,omitempty"`
	CreatedAt                  float64                     `json:"createdAt,omitempty"`
	LastUpdatedAt              float64                     `json:"lastUpdatedAt,omitempty"`
	CompletedAt                float64                     `json:"completedAt,omitempty"`
}

// JobExecutionStatusDetails holds free-form status detail name/value pairs
// for a job execution (aws-sdk-go-v2/service/iot/types.
// JobExecutionStatusDetails).
type JobExecutionStatusDetails struct {
	DetailsMap map[string]string `json:"detailsMap,omitempty"`
}

// JobExecution represents a single job execution on a thing.
//
// ThingName is internal-only storage used for lookups (jobExecKey,
// ListJobExecutionsForThing) -- real AWS's JobExecution wire shape has no
// "thingName" field, only "thingArn" (confirmed against
// awsRestjson1_deserializeDocumentJobExecution in
// aws-sdk-go-v2/service/iot@v1.76.0, which has no "thingName" case at all).
// Wire-response builders (handler_jobs.go) must compute ThingArn from
// ThingName via [InMemoryBackend.ThingARN] rather than ever serializing this
// struct directly, since ThingName still needs a normal json tag for
// Snapshot/Restore persistence to round-trip it.
type JobExecution struct {
	StatusDetails                    *JobExecutionStatusDetails `json:"statusDetails,omitempty"`
	JobID                            string                     `json:"jobId"`
	ThingName                        string                     `json:"thingName"`
	Status                           JobExecutionStatus         `json:"status"`
	ExecutionNumber                  int64                      `json:"executionNumber,omitempty"`
	QueuedAt                         float64                    `json:"queuedAt,omitempty"`
	StartedAt                        float64                    `json:"startedAt,omitempty"`
	LastUpdatedAt                    float64                    `json:"lastUpdatedAt,omitempty"`
	ApproximateSecondsBeforeTimedOut int64                      `json:"approximateSecondsBeforeTimedOut,omitempty"`
	VersionNumber                    int64                      `json:"versionNumber,omitempty"`
	ForceCanceled                    bool                       `json:"forceCanceled,omitempty"`
}

func cloneJob(j *Job) *Job {
	cp := *j
	cp.Targets = append([]string(nil), j.Targets...)
	if j.Tags != nil {
		cp.Tags = maps.Clone(j.Tags)
	}

	return &cp
}

func (b *InMemoryBackend) jobARN(jobID string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("job/%s", jobID))
}

// ThingARN builds a Thing's ARN from its name, matching the format computed
// elsewhere for real Things (see store.go). Used to derive JobExecution's
// real wire field "thingArn" from the internally-stored ThingName -- this
// does not require the Thing to still exist, matching real AWS's behavior
// of continuing to report the (deterministic) ARN for a job execution even
// after its target Thing has since been deleted.
func (b *InMemoryBackend) ThingARN(thingName string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("thing/%s", thingName))
}

// CreateJobInput holds input for CreateJob.
type CreateJobInput struct {
	Tags                       map[string]string           `json:"tags,omitempty"`
	DocumentParameters         map[string]string           `json:"documentParameters,omitempty"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobID                      string                      `json:"jobId"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"document,omitempty"`
	DocumentSource             string                      `json:"documentSource,omitempty"`
	JobTemplateARN             string                      `json:"jobTemplateArn,omitempty"`
	TargetSelection            string                      `json:"targetSelection,omitempty"`
	Targets                    []string                    `json:"targets"`
}

func (b *InMemoryBackend) CreateJob(input *CreateJobInput) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.jobs.Has(input.JobID) {
		return nil, fmt.Errorf("job %q already exists: %w", input.JobID, ErrAlreadyExists)
	}
	now := float64(time.Now().Unix())
	j := &Job{
		JobID:                      input.JobID,
		JobARN:                     b.jobARN(input.JobID),
		Description:                input.Description,
		Document:                   input.Document,
		DocumentSource:             input.DocumentSource,
		JobTemplateARN:             input.JobTemplateARN,
		TargetSelection:            input.TargetSelection,
		Targets:                    append([]string(nil), input.Targets...),
		AbortConfig:                input.AbortConfig,
		JobExecutionsRolloutConfig: input.JobExecutionsRolloutConfig,
		TimeoutConfig:              input.TimeoutConfig,
		Tags:                       input.Tags,
		DocumentParameters:         input.DocumentParameters,
		Status:                     JobStatusInProgress,
		CreatedAt:                  now,
		LastUpdatedAt:              now,
	}
	b.jobs.Put(j)

	return cloneJob(j), nil
}

func (b *InMemoryBackend) DescribeJob(jobID string) (*Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	j, ok := b.jobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}

	return cloneJob(j), nil
}

func (b *InMemoryBackend) ListJobs() []*Job {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Job, 0, b.jobs.Len())
	for _, v := range b.jobs.Snapshot() {
		out = append(out, cloneJob(v))
	}

	return out
}

func (b *InMemoryBackend) UpdateJob(jobID, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(jobID)
	if !ok {
		return fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	if description != "" {
		j.Description = description
	}
	j.LastUpdatedAt = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) CancelJob(jobID, _ string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	j.Status = JobStatusCanceled
	j.LastUpdatedAt = float64(time.Now().Unix())

	return cloneJob(j), nil
}

func (b *InMemoryBackend) DeleteJob(jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.jobs.Has(jobID) {
		return fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}
	b.jobs.Delete(jobID)
	// Remove executions. Preserves the original key-prefix check (including
	// its edge case: a JobExecution with an empty ThingName is NOT deleted,
	// since its key jobID+"|" has length exactly len(jobID)+1) byte-for-byte.
	for _, exec := range b.jobExecutions.All() {
		k := jobExecKey(exec.JobID, exec.ThingName)
		if len(k) > len(jobID)+1 && k[:len(jobID)] == jobID {
			b.jobExecutions.Delete(k)
		}
	}

	return nil
}

func (b *InMemoryBackend) GetJobDocument(jobID string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	j, ok := b.jobs.Get(jobID)
	if !ok {
		return "", fmt.Errorf("job %q not found: %w", jobID, ErrResourceNotFound)
	}

	return j.Document, nil
}

func jobExecKey(jobID, thingName string) string {
	return jobID + "|" + thingName
}

// AddJobExecutionInternal seeds a job execution directly into the backend
// for testing (mirrors AddAuditTaskInternal/AddServerlessCacheInternal):
// lets tests exercise states (e.g. IN_PROGRESS) that CancelJobExecution's
// own create-on-miss fallback can never produce, since it always creates in
// CANCELED state.
func (b *InMemoryBackend) AddJobExecutionInternal(e *JobExecution) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *e
	b.jobExecutions.Put(&cp)
}

func (b *InMemoryBackend) DescribeJobExecution(jobID, thingName string) (*JobExecution, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := jobExecKey(jobID, thingName)
	exec, ok := b.jobExecutions.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"job execution for job %q / thing %q not found: %w",
			jobID,
			thingName,
			ErrResourceNotFound,
		)
	}
	cp := *exec

	return &cp, nil
}

// CancelJobExecutionOptions carries CancelJobExecution's optional fields,
// matching real CancelJobExecutionInput's force/statusDetails/
// expectedVersion members.
type CancelJobExecutionOptions struct {
	StatusDetails   map[string]string
	Force           bool
	ExpectedVersion int64 // 0 means "not specified"
}

// CancelJobExecution cancels a job execution. Real AWS IoT rejects
// canceling an IN_PROGRESS execution unless force=true
// (InvalidStateTransitionException), and rejects a mismatched
// expectedVersion (VersionConflictException) -- both verified against
// api_op_CancelJobExecution.go's doc comments and the real error-deserializer
// switch (awsRestjson1_deserializeOpErrorCancelJobExecution recognizes
// exactly InvalidRequestException/InvalidStateTransitionException/
// ResourceNotFoundException/VersionConflictException).
//
// If no execution exists yet for this (jobID, thingName) pair, one is
// created directly in CANCELED state: this emulator does not fan out a
// QUEUED JobExecution per target at CreateJob time (a larger, separate gap
// tracked in PARITY.md's job_and_jobtemplate deferred list), so without this
// fallback CancelJobExecution could never succeed at all for a freshly
// created job.
func (b *InMemoryBackend) CancelJobExecution(jobID, thingName string, opts CancelJobExecutionOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := float64(time.Now().Unix())
	key := jobExecKey(jobID, thingName)

	exec, ok := b.jobExecutions.Get(key)
	if !ok {
		b.jobExecutions.Put(&JobExecution{
			JobID:           jobID,
			ThingName:       thingName,
			Status:          JobExecCanceled,
			ExecutionNumber: 1,
			VersionNumber:   1,
			ForceCanceled:   opts.Force,
			StatusDetails:   statusDetailsFromMap(opts.StatusDetails),
			QueuedAt:        now,
			LastUpdatedAt:   now,
		})

		return nil
	}

	if opts.ExpectedVersion != 0 && opts.ExpectedVersion != exec.VersionNumber {
		return fmt.Errorf(
			"%w: job execution for job %q / thing %q is at version %d, expected %d",
			ErrVersionConflict, jobID, thingName, exec.VersionNumber, opts.ExpectedVersion,
		)
	}

	if exec.Status == JobExecInProgress && !opts.Force {
		return fmt.Errorf(
			"%w: job execution for job %q / thing %q is IN_PROGRESS, set force=true to cancel it",
			ErrInvalidStateTransition, jobID, thingName,
		)
	}

	exec.Status = JobExecCanceled
	exec.ForceCanceled = opts.Force
	exec.LastUpdatedAt = now
	exec.VersionNumber++

	if len(opts.StatusDetails) > 0 {
		if exec.StatusDetails == nil {
			exec.StatusDetails = &JobExecutionStatusDetails{DetailsMap: map[string]string{}}
		}

		maps.Copy(exec.StatusDetails.DetailsMap, opts.StatusDetails)
	}

	return nil
}

// statusDetailsFromMap wraps a possibly-nil map into a
// *JobExecutionStatusDetails, or nil when the map is empty (matching real
// AWS's "statusDetails absent when never set" behavior).
func statusDetailsFromMap(m map[string]string) *JobExecutionStatusDetails {
	if len(m) == 0 {
		return nil
	}

	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return &JobExecutionStatusDetails{DetailsMap: cp}
}

// isTerminalJobExecutionStatus reports whether status is one of the
// statuses real AWS IoT considers terminal for DeleteJobExecution's
// force-required guard (SUCCEEDED, FAILED, REJECTED, REMOVED, CANCELED --
// everything except QUEUED/IN_PROGRESS, per api_op_DeleteJobExecution.go's
// doc comment).
func isTerminalJobExecutionStatus(status JobExecutionStatus) bool {
	switch status {
	case JobExecSucceeded, JobExecFailed, JobExecRejected, JobExecRemoved, JobExecCanceled:
		return true
	case JobExecQueued, JobExecInProgress:
		return false
	default:
		return false
	}
}

// DeleteJobExecution deletes a job execution. Real AWS IoT rejects deleting
// a non-terminal (QUEUED/IN_PROGRESS) execution unless force=true
// (InvalidStateTransitionException); deleting an execution that does not
// exist is idempotent (matches real AWS, which also returns success for an
// already-absent execution rather than ResourceNotFoundException, since
// deletion is the natural end state).
func (b *InMemoryBackend) DeleteJobExecution(jobID, thingName string, force bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := jobExecKey(jobID, thingName)

	exec, ok := b.jobExecutions.Get(key)
	if !ok {
		return nil
	}

	if !force && !isTerminalJobExecutionStatus(exec.Status) {
		return fmt.Errorf(
			"%w: job execution for job %q / thing %q is %s, set force=true to delete it",
			ErrInvalidStateTransition, jobID, thingName, exec.Status,
		)
	}

	b.jobExecutions.Delete(key)

	return nil
}

// JobTemplate represents an IoT job template.
//
// Tags is internal-only (json:"-"): DescribeJobTemplateOutput (verified
// against awsRestjson1_deserializeOpDocumentDescribeJobTemplateOutput in
// aws-sdk-go-v2/service/iot@v1.76.0) has no "tags" field -- tags are a
// separate ListTagsForResource concept.
type JobTemplate struct {
	Tags                       map[string]string           `json:"-"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobTemplateID              string                      `json:"jobTemplateId"`
	JobTemplateARN             string                      `json:"jobTemplateArn"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"document,omitempty"`
	DocumentSource             string                      `json:"documentSource,omitempty"`
	CreatedAt                  float64                     `json:"createdAt,omitempty"`
}

func cloneJobTemplate(jt *JobTemplate) *JobTemplate {
	cp := *jt

	return &cp
}

func (b *InMemoryBackend) jobTemplateARN(id string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("jobtemplate/%s", id))
}

// CreateJobTemplateInput holds input for CreateJobTemplate.
type CreateJobTemplateInput struct {
	Tags                       map[string]string           `json:"tags,omitempty"`
	AbortConfig                *AbortConfig                `json:"abortConfig,omitempty"`
	JobExecutionsRolloutConfig *JobExecutionsRolloutConfig `json:"jobExecutionsRolloutConfig,omitempty"`
	TimeoutConfig              *TimeoutConfig              `json:"timeoutConfig,omitempty"`
	JobTemplateID              string                      `json:"jobTemplateId"`
	Description                string                      `json:"description,omitempty"`
	Document                   string                      `json:"document,omitempty"`
	DocumentSource             string                      `json:"documentSource,omitempty"`
	JobARN                     string                      `json:"jobArn,omitempty"`
}

func (b *InMemoryBackend) CreateJobTemplate(input *CreateJobTemplateInput) (*JobTemplate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.jobTemplates.Has(input.JobTemplateID) {
		return nil, fmt.Errorf(
			"job template %q already exists: %w",
			input.JobTemplateID,
			ErrAlreadyExists,
		)
	}
	jt := &JobTemplate{
		JobTemplateID:              input.JobTemplateID,
		JobTemplateARN:             b.jobTemplateARN(input.JobTemplateID),
		Description:                input.Description,
		Document:                   input.Document,
		DocumentSource:             input.DocumentSource,
		AbortConfig:                input.AbortConfig,
		JobExecutionsRolloutConfig: input.JobExecutionsRolloutConfig,
		TimeoutConfig:              input.TimeoutConfig,
		Tags:                       input.Tags,
		CreatedAt:                  float64(time.Now().Unix()),
	}
	b.jobTemplates.Put(jt)

	return cloneJobTemplate(jt), nil
}

func (b *InMemoryBackend) DescribeJobTemplate(id string) (*JobTemplate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	jt, ok := b.jobTemplates.Get(id)
	if !ok {
		return nil, fmt.Errorf("job template %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneJobTemplate(jt), nil
}

func (b *InMemoryBackend) ListJobTemplates() []*JobTemplate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*JobTemplate, 0, b.jobTemplates.Len())
	for _, v := range b.jobTemplates.Snapshot() {
		out = append(out, cloneJobTemplate(v))
	}

	return out
}

func (b *InMemoryBackend) DeleteJobTemplate(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.jobTemplates.Has(id) {
		return fmt.Errorf("job template %q not found: %w", id, ErrResourceNotFound)
	}
	b.jobTemplates.Delete(id)

	return nil
}
