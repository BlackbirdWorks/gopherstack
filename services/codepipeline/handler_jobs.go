package codepipeline

import (
	"context"
	"fmt"
)

const (
	// keyNonce is the JSON key for job nonce values.
	keyNonce = "nonce"
	// keyJobID is the JSON key for job IDs.
	keyJobID = "id"
	// keyActionExecutionID is the JSON key for action execution IDs.
	keyActionExecutionID = "actionExecutionId"

	// maxJobsPerPoll caps the number of jobs returned by a single PollForJobs
	// or PollForThirdPartyJobs call.
	maxJobsPerPoll = 10
)

type acknowledgeJobInput struct {
	JobID string `json:"jobId"`
	Nonce string `json:"nonce"`
}

type acknowledgeJobOutput struct {
	Status string `json:"status"`
}

func (h *Handler) handleAcknowledgeJob(
	ctx context.Context,
	in *acknowledgeJobInput,
) (*acknowledgeJobOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	if in.Nonce == "" {
		return nil, fmt.Errorf("%w: nonce is required", errInvalidRequest)
	}

	status, err := h.Backend.AcknowledgeJob(ctx, in.JobID, in.Nonce)
	if err != nil {
		return nil, err
	}

	return &acknowledgeJobOutput{Status: status}, nil
}

// jobData renders types.JobData for job: its action type, and for a job a
// pipeline run created, the action configuration, pipeline context and any
// continuation token. Artifact credentials and artifacts are not modeled.
func (b *InMemoryBackend) jobData(job *Job) map[string]any {
	data := map[string]any{"actionTypeId": job.ActionTypeID}

	if job.PipelineName == "" {
		return data
	}

	data["actionConfiguration"] = map[string]any{"configuration": job.Configuration}
	data["pipelineContext"] = map[string]any{
		"pipelineName":        job.PipelineName,
		"pipelineArn":         b.buildPipelineARN(job.region, job.PipelineName),
		"pipelineExecutionId": job.ExecutionID,
		"stage":               map[string]any{"name": job.StageName},
		"action":              map[string]any{"name": job.ActionName, keyActionExecutionID: job.ActionExecutionID},
	}

	if job.ContinuationToken != "" {
		data["continuationToken"] = job.ContinuationToken
	}

	return data
}

type jobDetailsResponse struct {
	Data      map[string]any `json:"data"`
	AccountID string         `json:"accountId"`
	ID        string         `json:"id"`
}

type getJobDetailsInput struct {
	JobID string `json:"jobId"`
}

type getJobDetailsOutput struct {
	JobDetails jobDetailsResponse `json:"jobDetails"`
}

func (h *Handler) handleGetJobDetails(
	ctx context.Context,
	in *getJobDetailsInput,
) (*getJobDetailsOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	job, err := h.Backend.GetJobDetails(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	return &getJobDetailsOutput{
		JobDetails: jobDetailsResponse{
			ID:        job.ID,
			AccountID: h.Backend.accountID,
			Data:      h.Backend.jobData(job),
		},
	}, nil
}

type pollForJobsInput struct {
	QueryParam   map[string]string `json:"queryParam"`
	ActionTypeID struct {
		Category string `json:"category"`
		Owner    string `json:"owner"`
		Provider string `json:"provider"`
		Version  string `json:"version"`
	} `json:"actionTypeId"`
	MaxBatchSize int32 `json:"maxBatchSize"`
}

type pollForJobsOutput struct {
	Jobs []map[string]any `json:"jobs"`
}

func (h *Handler) handlePollForJobs(
	ctx context.Context,
	in *pollForJobsInput,
) (*pollForJobsOutput, error) {
	jobs, err := h.Backend.PollForJobsQuery(
		ctx, in.ActionTypeID.Category, in.ActionTypeID.Owner,
		in.ActionTypeID.Provider, in.ActionTypeID.Version, in.QueryParam,
	)
	if err != nil {
		return nil, err
	}

	limit := in.MaxBatchSize
	if limit <= 0 || limit > maxJobsPerPoll {
		limit = maxJobsPerPoll
	}
	if int(limit) < len(jobs) {
		jobs = jobs[:limit]
	}

	items := make([]map[string]any, len(jobs))
	for i, j := range jobs {
		items[i] = map[string]any{
			keyJobID:    j.ID,
			keyNonce:    j.Nonce,
			"accountId": h.Backend.accountID,
			"data":      h.Backend.jobData(j),
		}
	}

	return &pollForJobsOutput{Jobs: items}, nil
}

type executionDetailsInput struct {
	Summary             string `json:"summary"`
	ExternalExecutionID string `json:"externalExecutionId"`
	PercentComplete     int32  `json:"percentComplete"`
}

type currentRevisionInput struct {
	Revision         string  `json:"revision"`
	ChangeIdentifier string  `json:"changeIdentifier"`
	Created          float64 `json:"created"`
}

type putJobSuccessResultInput struct {
	CurrentRevision   *currentRevisionInput  `json:"currentRevision"`
	ExecutionDetails  *executionDetailsInput `json:"executionDetails"`
	OutputVariables   map[string]string      `json:"outputVariables"`
	JobID             string                 `json:"jobId"`
	ContinuationToken string                 `json:"continuationToken"`
}

func (in putJobSuccessResultInput) result() JobSuccess {
	return buildJobSuccess(in.CurrentRevision, in.ExecutionDetails, in.OutputVariables, in.ContinuationToken)
}

func buildJobSuccess(
	rev *currentRevisionInput, details *executionDetailsInput, vars map[string]string, token string,
) JobSuccess {
	res := JobSuccess{OutputVariables: vars, ContinuationToken: token}

	if rev != nil {
		res.CurrentRevision = &JobCurrentRevision{
			Revision: rev.Revision, ChangeIdentifier: rev.ChangeIdentifier, Created: rev.Created,
		}
	}

	if details != nil {
		res.ExecutionDetails = &JobExecutionDetails{
			ExternalExecutionID: details.ExternalExecutionID,
			Summary:             details.Summary,
			PercentComplete:     details.PercentComplete,
		}
	}

	return res
}

func (h *Handler) handlePutJobSuccessResult(
	ctx context.Context,
	in *putJobSuccessResultInput,
) (*emptyOut, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	return &emptyOut{}, h.Backend.PutJobSuccessResultWith(ctx, in.JobID, in.result())
}

type putJobFailureResultInput struct {
	JobID          string `json:"jobId"`
	FailureDetails struct {
		Message             string `json:"message"`
		Type                string `json:"type"`
		ExternalExecutionID string `json:"externalExecutionId"`
	} `json:"failureDetails"`
}

func (h *Handler) handlePutJobFailureResult(
	ctx context.Context,
	in *putJobFailureResultInput,
) (*emptyOut, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	return &emptyOut{}, h.Backend.PutJobFailureResultWith(ctx, in.JobID, JobFailure{
		Message: in.FailureDetails.Message, Type: in.FailureDetails.Type,
		ExternalExecutionID: in.FailureDetails.ExternalExecutionID,
	})
}
