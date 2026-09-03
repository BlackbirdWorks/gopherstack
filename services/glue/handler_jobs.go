package glue

import (
	"context"
	"fmt"
)

type createJobInput struct {
	Tags                 map[string]string    `json:"Tags,omitempty"`
	DefaultArguments     map[string]string    `json:"DefaultArguments,omitempty"`
	Command              JobCommand           `json:"Command,omitzero"`
	WorkerType           string               `json:"WorkerType,omitempty"`
	Role                 string               `json:"Role,omitempty"`
	GlueVersion          string               `json:"GlueVersion,omitempty"`
	Name                 string               `json:"Name"`
	Description          string               `json:"Description,omitempty"`
	Connections          ConnectionsList      `json:"Connections,omitzero"`
	NotificationProperty NotificationProperty `json:"NotificationProperty,omitzero"`
	NumberOfWorkers      int                  `json:"NumberOfWorkers,omitempty"`
	MaxRetries           int                  `json:"MaxRetries,omitempty"`
	Timeout              int                  `json:"Timeout,omitempty"`
	MaxCapacity          float64              `json:"MaxCapacity,omitempty"`
	ExecutionProperty    ExecutionProperty    `json:"ExecutionProperty,omitzero"`
}

type createJobOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateJob(_ context.Context, in *createJobInput) (*createJobOutput, error) {
	j, err := h.Backend.CreateJob(Job{
		Name:                 in.Name,
		Description:          in.Description,
		Role:                 in.Role,
		Command:              in.Command,
		DefaultArguments:     in.DefaultArguments,
		GlueVersion:          in.GlueVersion,
		WorkerType:           in.WorkerType,
		NumberOfWorkers:      in.NumberOfWorkers,
		MaxCapacity:          in.MaxCapacity,
		MaxRetries:           in.MaxRetries,
		Timeout:              in.Timeout,
		Tags:                 in.Tags,
		ExecutionProperty:    in.ExecutionProperty,
		Connections:          in.Connections,
		NotificationProperty: in.NotificationProperty,
	})
	if err != nil {
		return nil, err
	}

	return &createJobOutput{Name: j.Name}, nil
}

type getJobInput struct {
	JobName string `json:"JobName"`
}

type getJobOutput struct {
	Job *Job `json:"Job"`
}

func (h *Handler) handleGetJob(_ context.Context, in *getJobInput) (*getJobOutput, error) {
	j, err := h.Backend.GetJob(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobOutput{Job: j}, nil
}

// defaultGetJobsLimit is used when GetJobsInput.MaxResults is unset.
const defaultGetJobsLimit = 100

type getJobsInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

type getJobsOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Jobs      []*Job `json:"Jobs"`
}

func (h *Handler) handleGetJobs(_ context.Context, in *getJobsInput) (*getJobsOutput, error) {
	jobs := h.Backend.GetJobs()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetJobsLimit
	}

	page, next := paginateSlice(jobs, in.NextToken, limit)

	return &getJobsOutput{Jobs: page, NextToken: next}, nil
}

// jobUpdatePayload models the allowed fields for Glue's JobUpdate shape.
// It intentionally omits create-only fields such as Name and Tags.
type jobUpdatePayload struct {
	DefaultArguments     map[string]string    `json:"DefaultArguments,omitempty"`
	Command              JobCommand           `json:"Command,omitzero"`
	WorkerType           string               `json:"WorkerType,omitempty"`
	Role                 string               `json:"Role,omitempty"`
	GlueVersion          string               `json:"GlueVersion,omitempty"`
	Description          string               `json:"Description,omitempty"`
	Connections          ConnectionsList      `json:"Connections,omitzero"`
	NotificationProperty NotificationProperty `json:"NotificationProperty,omitzero"`
	NumberOfWorkers      int                  `json:"NumberOfWorkers,omitempty"`
	MaxRetries           int                  `json:"MaxRetries,omitempty"`
	Timeout              int                  `json:"Timeout,omitempty"`
	MaxCapacity          float64              `json:"MaxCapacity,omitempty"`
	ExecutionProperty    ExecutionProperty    `json:"ExecutionProperty,omitzero"`
}

type updateJobInput struct {
	JobName   string           `json:"JobName"`
	JobUpdate jobUpdatePayload `json:"JobUpdate"`
}

type updateJobOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleUpdateJob(_ context.Context, in *updateJobInput) (*updateJobOutput, error) {
	if err := h.Backend.UpdateJob(in.JobName, Job{
		Description:          in.JobUpdate.Description,
		Role:                 in.JobUpdate.Role,
		Command:              in.JobUpdate.Command,
		DefaultArguments:     in.JobUpdate.DefaultArguments,
		GlueVersion:          in.JobUpdate.GlueVersion,
		WorkerType:           in.JobUpdate.WorkerType,
		NumberOfWorkers:      in.JobUpdate.NumberOfWorkers,
		MaxCapacity:          in.JobUpdate.MaxCapacity,
		MaxRetries:           in.JobUpdate.MaxRetries,
		Timeout:              in.JobUpdate.Timeout,
		ExecutionProperty:    in.JobUpdate.ExecutionProperty,
		Connections:          in.JobUpdate.Connections,
		NotificationProperty: in.JobUpdate.NotificationProperty,
	}); err != nil {
		return nil, err
	}

	return &updateJobOutput{JobName: in.JobName}, nil
}

type deleteJobInput struct {
	JobName string `json:"JobName"`
}

type deleteJobOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleDeleteJob(_ context.Context, in *deleteJobInput) (*deleteJobOutput, error) {
	if err := h.Backend.DeleteJob(in.JobName); err != nil {
		return nil, err
	}

	return &deleteJobOutput{JobName: in.JobName}, nil
}

type startJobRunInput struct {
	Arguments             map[string]string     `json:"Arguments,omitempty"`
	NotificationProperty  *NotificationProperty `json:"NotificationProperty,omitempty"`
	JobName               string                `json:"JobName"`
	WorkerType            string                `json:"WorkerType,omitempty"`
	SecurityConfiguration string                `json:"SecurityConfiguration,omitempty"`
	NumberOfWorkers       int                   `json:"NumberOfWorkers,omitempty"`
	MaxCapacity           float64               `json:"MaxCapacity,omitempty"`
	Timeout               int                   `json:"Timeout,omitempty"`
}

type startJobRunOutput struct {
	JobRunID string `json:"JobRunId"`
}

func (h *Handler) handleStartJobRun(_ context.Context, in *startJobRunInput) (*startJobRunOutput, error) {
	run, err := h.Backend.StartJobRunWithOptions(in.JobName, in.Arguments, StartJobRunOptions{
		WorkerType:            in.WorkerType,
		SecurityConfiguration: in.SecurityConfiguration,
		NotificationProperty:  in.NotificationProperty,
		NumberOfWorkers:       in.NumberOfWorkers,
		MaxCapacity:           in.MaxCapacity,
		Timeout:               in.Timeout,
	})
	if err != nil {
		return nil, err
	}

	return &startJobRunOutput{JobRunID: run.ID}, nil
}

type getJobRunInput struct {
	JobName string `json:"JobName"`
	RunID   string `json:"RunId"`
}

type getJobRunOutput struct {
	JobRun *JobRun `json:"JobRun"`
}

func (h *Handler) handleGetJobRun(_ context.Context, in *getJobRunInput) (*getJobRunOutput, error) {
	run, err := h.Backend.GetJobRun(in.JobName, in.RunID)
	if err != nil {
		return nil, err
	}

	return &getJobRunOutput{JobRun: run}, nil
}

type getJobRunsInput struct {
	JobName string `json:"JobName"`
}

type getJobRunsOutput struct {
	JobRuns []*JobRun `json:"JobRuns"`
}

func (h *Handler) handleGetJobRuns(_ context.Context, in *getJobRunsInput) (*getJobRunsOutput, error) {
	runs, err := h.Backend.GetJobRuns(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobRunsOutput{JobRuns: runs}, nil
}

type batchStopJobRunInput struct {
	JobName   string   `json:"JobName"`
	JobRunIDs []string `json:"JobRunIds"`
}

type batchStopJobRunOutput struct {
	Errors                []BatchStopJobRunError                `json:"Errors"`
	SuccessfulSubmissions []BatchStopJobRunSuccessfulSubmission `json:"SuccessfulSubmissions"`
}

func (h *Handler) handleBatchStopJobRun(_ context.Context, in *batchStopJobRunInput) (*batchStopJobRunOutput, error) {
	successes, errs := h.Backend.BatchStopJobRun(in.JobName, in.JobRunIDs)

	return &batchStopJobRunOutput{SuccessfulSubmissions: successes, Errors: errs}, nil
}

type getJobBookmarkInput struct {
	JobName string `json:"JobName"`
}

type getJobBookmarkOutput struct {
	JobBookmarkEntry *JobBookmark `json:"JobBookmarkEntry"`
}

func (h *Handler) handleGetJobBookmark(_ context.Context, in *getJobBookmarkInput) (*getJobBookmarkOutput, error) {
	bm, err := h.Backend.GetJobBookmark(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobBookmarkOutput{JobBookmarkEntry: bm}, nil
}

type resetJobBookmarkInput struct {
	JobName string `json:"JobName"`
}

type resetJobBookmarkOutput struct {
	JobBookmarkEntry *JobBookmark `json:"JobBookmarkEntry"`
}

func (h *Handler) handleResetJobBookmark(
	_ context.Context,
	in *resetJobBookmarkInput,
) (*resetJobBookmarkOutput, error) {
	bm, err := h.Backend.ResetJobBookmarkWithResult(in.JobName)
	if err != nil {
		return nil, err
	}

	return &resetJobBookmarkOutput{JobBookmarkEntry: bm}, nil
}

// batchGetJobsInput holds input for BatchGetJobs.
type batchGetJobsInput struct {
	JobNames []string `json:"JobNames"`
}

// batchGetJobsOutput holds the result for BatchGetJobs.
type batchGetJobsOutput struct {
	Jobs         []*Job   `json:"Jobs"`
	JobsNotFound []string `json:"JobsNotFound"`
}

func (h *Handler) handleBatchGetJobs(
	_ context.Context,
	in *batchGetJobsInput,
) (*batchGetJobsOutput, error) {
	var found []*Job
	var missing []string

	for _, name := range in.JobNames {
		j, err := h.Backend.GetJob(name)
		if err != nil {
			missing = append(missing, name)
		} else {
			found = append(found, j)
		}
	}

	return &batchGetJobsOutput{Jobs: found, JobsNotFound: missing}, nil
}

// defaultListJobsLimit is used when ListJobsInput.MaxResults is unset.
const defaultListJobsLimit = 100

// listJobsInput holds input for ListJobs.
type listJobsInput struct {
	Tags       map[string]string `json:"Tags,omitempty"`
	NextToken  string            `json:"NextToken,omitempty"`
	MaxResults int32             `json:"MaxResults,omitempty"`
}

// listJobsOutput holds the result for ListJobs.
type listJobsOutput struct {
	NextToken string   `json:"NextToken,omitempty"`
	JobNames  []string `json:"JobNames"`
}

func (h *Handler) handleListJobs(_ context.Context, in *listJobsInput) (*listJobsOutput, error) {
	jobs := h.Backend.GetJobs()

	if len(in.Tags) > 0 {
		filtered := make([]*Job, 0, len(jobs))

		for _, j := range jobs {
			if matchesTagFilter(j.Tags, in.Tags) {
				filtered = append(filtered, j)
			}
		}

		jobs = filtered
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListJobsLimit
	}

	page, next := paginateSlice(jobs, in.NextToken, limit)

	names := make([]string, 0, len(page))
	for _, j := range page {
		names = append(names, j.Name)
	}

	return &listJobsOutput{JobNames: names, NextToken: next}, nil
}

// jobSourceControlInput holds the shared input shape for
// UpdateJobFromSourceControl and UpdateSourceControlFromJob.
type jobSourceControlInput struct {
	AuthStrategy    string `json:"AuthStrategy,omitempty"`
	AuthToken       string `json:"AuthToken,omitempty"`
	BranchName      string `json:"BranchName,omitempty"`
	CommitID        string `json:"CommitId,omitempty"`
	Folder          string `json:"Folder,omitempty"`
	JobName         string `json:"JobName"`
	Provider        string `json:"Provider,omitempty"`
	RepositoryName  string `json:"RepositoryName,omitempty"`
	RepositoryOwner string `json:"RepositoryOwner,omitempty"`
}

func (in *jobSourceControlInput) toSourceControlDetails() SourceControlDetails {
	return SourceControlDetails{
		AuthStrategy: in.AuthStrategy,
		AuthToken:    in.AuthToken,
		Branch:       in.BranchName,
		Folder:       in.Folder,
		LastCommitID: in.CommitID,
		Owner:        in.RepositoryOwner,
		Provider:     in.Provider,
		Repository:   in.RepositoryName,
	}
}

// updateJobFromSourceControlInput holds input for UpdateJobFromSourceControl.
type updateJobFromSourceControlInput = jobSourceControlInput

// updateJobFromSourceControlOutput holds the result for UpdateJobFromSourceControl.
type updateJobFromSourceControlOutput struct {
	JobName string `json:"JobName"`
}

// handleUpdateJobFromSourceControl syncs a job's definition from its linked
// remote repository. AWS pulls the job definition into Glue; this emulator has
// no real repository to pull from, so it validates the job exists and records
// the source-control linkage as real state on the job.
func (h *Handler) handleUpdateJobFromSourceControl(
	_ context.Context,
	in *updateJobFromSourceControlInput,
) (*updateJobFromSourceControlOutput, error) {
	if in.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	if err := h.Backend.UpdateJobFromSourceControl(in.JobName, in.toSourceControlDetails()); err != nil {
		return nil, err
	}

	return &updateJobFromSourceControlOutput{JobName: in.JobName}, nil
}

// updateSourceControlFromJobInput holds input for UpdateSourceControlFromJob.
type updateSourceControlFromJobInput = jobSourceControlInput

// updateSourceControlFromJobOutput holds the result for UpdateSourceControlFromJob.
type updateSourceControlFromJobOutput struct {
	JobName string `json:"JobName"`
}

// handleUpdateSourceControlFromJob pushes a job's current definition to its
// linked remote repository. As with UpdateJobFromSourceControl, there is no
// real repository here, so it validates the job exists and records the same
// source-control linkage as real state on the job.
func (h *Handler) handleUpdateSourceControlFromJob(
	_ context.Context,
	in *updateSourceControlFromJobInput,
) (*updateSourceControlFromJobOutput, error) {
	if in.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	if err := h.Backend.UpdateSourceControlFromJob(in.JobName, in.toSourceControlDetails()); err != nil {
		return nil, err
	}

	return &updateSourceControlFromJobOutput{JobName: in.JobName}, nil
}
