package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func toUserImportJobType(job *UserImportJob) *userImportJobType {
	return &userImportJobType{
		JobID:                    job.JobID,
		JobName:                  job.JobName,
		UserPoolID:               job.UserPoolID,
		Status:                   job.Status,
		CloudWatchLogsRoleArn:    job.CloudWatchLogsRoleArn,
		PasswordHashingAlgorithm: job.PasswordHashingAlgorithm,
		PreSignedURL:             job.PreSignedURL,
		CreationDate:             awstime.Epoch(job.CreatedAt),
		StartDate:                awstime.Epoch(job.StartedAt),
		CompletionDate:           awstime.Epoch(job.CompletedAt),
	}
}

func (h *Handler) handleCreateUserImportJob(
	_ context.Context,
	in *createUserImportJobInput,
) (*createUserImportJobOutput, error) {
	job, err := h.Backend.CreateUserImportJob(
		in.UserPoolID, in.JobName, in.CloudWatchLogsRoleArn, in.PasswordHashingAlgorithm,
	)
	if err != nil {
		return nil, err
	}

	return &createUserImportJobOutput{UserImportJob: toUserImportJobType(job)}, nil
}

func (h *Handler) handleDescribeUserImportJob(
	_ context.Context,
	in *describeUserImportJobInput,
) (*describeUserImportJobOutput, error) {
	job, err := h.Backend.DescribeUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &describeUserImportJobOutput{UserImportJob: toUserImportJobType(job)}, nil
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
		out = append(out, *toUserImportJobType(job))
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

	return &startUserImportJobOutput{UserImportJob: toUserImportJobType(job)}, nil
}

func (h *Handler) handleStopUserImportJob(
	_ context.Context,
	in *stopUserImportJobInput,
) (*stopUserImportJobOutput, error) {
	job, err := h.Backend.StopUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &stopUserImportJobOutput{UserImportJob: toUserImportJobType(job)}, nil
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
