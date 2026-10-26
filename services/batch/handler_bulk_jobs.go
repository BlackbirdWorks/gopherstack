package batch

import (
	"context"
	"errors"
	"fmt"
)

const maxBulkJobs = 50

type bulkJobsInput struct {
	Reason string   `json:"reason"`
	Jobs   []string `json:"jobs"`
}

type bulkJobError struct {
	Code    string `json:"code"`
	Job     string `json:"job"`
	Message string `json:"message"`
}

type bulkJobsOutput struct {
	Errors     []bulkJobError `json:"errors"`
	Successful []string       `json:"successful"`
}

// runBulkJobs applies op to each job; unknown jobs count as successful, per
// the CancelJobs/TerminateJobs/TerminateServiceJobs docs.
func runBulkJobs(
	ctx context.Context,
	in *bulkJobsInput,
	op func(ctx context.Context, jobID, reason string) error,
) (*bulkJobsOutput, error) {
	if len(in.Jobs) == 0 || len(in.Jobs) > maxBulkJobs {
		return nil, fmt.Errorf("%w: jobs must contain between 1 and %d job IDs", ErrValidation, maxBulkJobs)
	}

	out := &bulkJobsOutput{Errors: []bulkJobError{}, Successful: []string{}}

	for _, id := range in.Jobs {
		err := op(ctx, id, in.Reason)

		switch {
		case err == nil, errors.Is(err, ErrNotFound):
			out.Successful = append(out.Successful, id)
		default:
			out.Errors = append(out.Errors, bulkJobError{Code: "ClientException", Job: id, Message: err.Error()})
		}
	}

	return out, nil
}

func (h *Handler) handleCancelJobs(ctx context.Context, in *bulkJobsInput) (*bulkJobsOutput, error) {
	return runBulkJobs(ctx, in, h.Backend.CancelJob)
}

func (h *Handler) handleTerminateJobs(ctx context.Context, in *bulkJobsInput) (*bulkJobsOutput, error) {
	return runBulkJobs(ctx, in, h.Backend.TerminateJob)
}

func (h *Handler) handleTerminateServiceJobs(ctx context.Context, in *bulkJobsInput) (*bulkJobsOutput, error) {
	return runBulkJobs(ctx, in, h.Backend.TerminateServiceJob)
}
