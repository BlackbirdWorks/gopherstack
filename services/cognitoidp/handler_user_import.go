package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleCreateUserImportJob(
	_ context.Context,
	in *createUserImportJobInput,
) (*createUserImportJobOutput, error) {
	job, err := h.Backend.CreateUserImportJob(in.UserPoolID, in.JobName)
	if err != nil {
		return nil, err
	}

	return &createUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

func (h *Handler) handleDescribeUserImportJob(
	_ context.Context,
	in *describeUserImportJobInput,
) (*describeUserImportJobOutput, error) {
	job, err := h.Backend.DescribeUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &describeUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

func (h *Handler) handleListUserImportJobs(
	_ context.Context,
	in *listUserImportJobsInput,
) (*listUserImportJobsOutput, error) {
	jobs, err := h.Backend.ListUserImportJobs(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]userImportJobType, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		})
	}

	return &listUserImportJobsOutput{UserImportJobs: out}, nil
}

func (h *Handler) handleStartUserImportJob(
	_ context.Context,
	in *startUserImportJobInput,
) (*startUserImportJobOutput, error) {
	job, err := h.Backend.StartUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &startUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

func (h *Handler) handleStopUserImportJob(
	_ context.Context,
	in *stopUserImportJobInput,
) (*stopUserImportJobOutput, error) {
	job, err := h.Backend.StopUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &stopUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

func (h *Handler) handleGetCSVHeader(_ context.Context, in *getCSVHeaderInput) (*getCSVHeaderOutput, error) {
	return &getCSVHeaderOutput{
		UserPoolID: in.UserPoolID,
		CSVHeader:  []string{"cognito:username", "name", "given_name", "family_name", "email", "email_verified"},
	}, nil
}

func (h *Handler) userImportOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateUserImportJob":   service.WrapOp(h.handleCreateUserImportJob),
		"DescribeUserImportJob": service.WrapOp(h.handleDescribeUserImportJob),
		"GetCSVHeader":          service.WrapOp(h.handleGetCSVHeader),
		"ListUserImportJobs":    service.WrapOp(h.handleListUserImportJobs),
		"StartUserImportJob":    service.WrapOp(h.handleStartUserImportJob),
		"StopUserImportJob":     service.WrapOp(h.handleStopUserImportJob),
	}
}
