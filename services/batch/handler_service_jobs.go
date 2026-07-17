package batch

import (
	"context"
	"fmt"
)

// --- ServiceJob handlers ---

type submitServiceJobInput struct {
	Tags               map[string]string `json:"tags"`
	ServiceJobName     string            `json:"serviceJobName"`
	ServiceEnvironment string            `json:"serviceEnvironment"`
}

type submitServiceJobOutput struct {
	ServiceJobArn  string `json:"serviceJobArn"`
	ServiceJobName string `json:"serviceJobName"`
}

func (h *Handler) handleSubmitServiceJob(
	ctx context.Context,
	in *submitServiceJobInput,
) (*submitServiceJobOutput, error) {
	if in.ServiceJobName == "" {
		return nil, fmt.Errorf("%w: serviceJobName is required", ErrValidation)
	}

	sj, err := h.Backend.SubmitServiceJob(ctx, in.ServiceJobName, in.ServiceEnvironment, in.Tags)
	if err != nil {
		return nil, err
	}

	return &submitServiceJobOutput{
		ServiceJobArn:  sj.ServiceJobArn,
		ServiceJobName: sj.ServiceJobName,
	}, nil
}

type describeServiceJobInput struct {
	ServiceJob string `json:"serviceJob"`
}

type describeServiceJobOutput struct {
	Tags               map[string]string `json:"tags"`
	StartedAt          *int64            `json:"startedAt,omitempty"`
	StoppedAt          *int64            `json:"stoppedAt,omitempty"`
	ServiceJobID       string            `json:"serviceJobId"`
	ServiceJobArn      string            `json:"serviceJobArn"`
	ServiceJobName     string            `json:"serviceJobName"`
	ServiceEnvironment string            `json:"serviceEnvironment"`
	Status             string            `json:"status"`
	StatusReason       string            `json:"statusReason,omitempty"`
	CreatedAt          int64             `json:"createdAt"`
}

func (h *Handler) handleDescribeServiceJob(
	ctx context.Context,
	in *describeServiceJobInput,
) (*describeServiceJobOutput, error) {
	if in.ServiceJob == "" {
		return nil, fmt.Errorf("%w: serviceJob is required", ErrValidation)
	}

	sj, err := h.Backend.DescribeServiceJob(ctx, in.ServiceJob)
	if err != nil {
		return nil, err
	}

	return &describeServiceJobOutput{
		ServiceJobID:       sj.ServiceJobID,
		ServiceJobArn:      sj.ServiceJobArn,
		ServiceJobName:     sj.ServiceJobName,
		ServiceEnvironment: sj.ServiceEnvironment,
		Status:             sj.Status,
		StatusReason:       sj.StatusReason,
		CreatedAt:          sj.CreatedAt,
		StartedAt:          sj.StartedAt,
		StoppedAt:          sj.StoppedAt,
		Tags:               tagsOrEmpty(sj.Tags),
	}, nil
}

type listServiceJobsInput struct {
	ServiceEnvironment string `json:"serviceEnvironment,omitempty"`
}

type listServiceJobsOutput struct {
	ServiceJobs []*ServiceJob `json:"serviceJobs"`
}

func (h *Handler) handleListServiceJobs(ctx context.Context, in *listServiceJobsInput) (*listServiceJobsOutput, error) {
	list, err := h.Backend.ListServiceJobs(ctx, in.ServiceEnvironment)
	if err != nil {
		return nil, err
	}

	return &listServiceJobsOutput{ServiceJobs: list}, nil
}

type terminateServiceJobInput struct {
	ServiceJob string `json:"serviceJob"`
	Reason     string `json:"reason"`
}

func (h *Handler) handleTerminateServiceJob(ctx context.Context, in *terminateServiceJobInput) (*emptyOutput, error) {
	if in.ServiceJob == "" {
		return nil, fmt.Errorf("%w: serviceJob is required", ErrValidation)
	}

	if err := h.Backend.TerminateServiceJob(ctx, in.ServiceJob, in.Reason); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
