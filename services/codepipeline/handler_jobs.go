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

type jobDataResponse struct {
	ActionTypeID ActionTypeID `json:"actionTypeId"`
}

type jobDetailsResponse struct {
	Data      jobDataResponse `json:"data"`
	AccountID string          `json:"accountId"`
	ID        string          `json:"id"`
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
			Data:      jobDataResponse{ActionTypeID: job.ActionTypeID},
		},
	}, nil
}

type pollForJobsInput struct {
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
	jobs, err := h.Backend.PollForJobs(
		ctx, in.ActionTypeID.Category, in.ActionTypeID.Owner,
		in.ActionTypeID.Provider, in.ActionTypeID.Version,
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
		items[i] = map[string]any{keyJobID: j.ID, keyNonce: j.Nonce}
	}

	return &pollForJobsOutput{Jobs: items}, nil
}

type putJobSuccessResultInput struct {
	JobID string `json:"jobId"`
}

func (h *Handler) handlePutJobSuccessResult(
	ctx context.Context,
	in *putJobSuccessResultInput,
) (*emptyOut, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	return &emptyOut{}, h.Backend.PutJobSuccessResult(ctx, in.JobID)
}

type putJobFailureResultInput struct {
	JobID          string `json:"jobId"`
	FailureDetails struct {
		Message string `json:"message"`
	} `json:"failureDetails"`
}

func (h *Handler) handlePutJobFailureResult(
	ctx context.Context,
	in *putJobFailureResultInput,
) (*emptyOut, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", errInvalidRequest)
	}

	return &emptyOut{}, h.Backend.PutJobFailureResult(ctx, in.JobID, in.FailureDetails.Message)
}
