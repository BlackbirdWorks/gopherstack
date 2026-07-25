package batch

import (
	"context"
	"fmt"
)

// --- ServiceJob handlers ---

// submitServiceJobInput mirrors aws-sdk-go-v2/service/batch's
// SubmitServiceJobInput exactly. Real AWS Batch submits service jobs to a
// job queue (jobQueue) -- there is no "serviceEnvironment" parameter; the
// service environment association lives on the JobQueue's
// ServiceEnvironmentOrder instead.
type submitServiceJobInput struct {
	Tags                  map[string]string        `json:"tags"`
	RetryStrategy         *ServiceJobRetryStrategy `json:"retryStrategy,omitempty"`
	TimeoutConfig         *ServiceJobTimeout       `json:"timeoutConfig,omitempty"`
	SchedulingPriority    *int32                   `json:"schedulingPriority,omitempty"`
	JobName               string                   `json:"jobName"`
	JobQueue              string                   `json:"jobQueue"`
	ServiceJobType        string                   `json:"serviceJobType"`
	ServiceRequestPayload string                   `json:"serviceRequestPayload"`
	ShareIdentifier       string                   `json:"shareIdentifier,omitempty"`
}

type submitServiceJobOutput struct {
	JobID   string `json:"jobId"`
	JobArn  string `json:"jobArn,omitempty"`
	JobName string `json:"jobName"`
}

func (h *Handler) handleSubmitServiceJob(
	ctx context.Context,
	in *submitServiceJobInput,
) (*submitServiceJobOutput, error) {
	var schedulingPriority int32
	if in.SchedulingPriority != nil {
		schedulingPriority = *in.SchedulingPriority
	}

	sj, err := h.Backend.SubmitServiceJob(
		ctx,
		in.JobName,
		in.JobQueue,
		in.ServiceJobType,
		in.ServiceRequestPayload,
		in.Tags,
		in.RetryStrategy,
		in.TimeoutConfig,
		schedulingPriority,
		in.ShareIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return &submitServiceJobOutput{
		JobID:   sj.JobID,
		JobArn:  sj.JobArn,
		JobName: sj.JobName,
	}, nil
}

type describeServiceJobInput struct {
	JobID string `json:"jobId"`
}

// describeServiceJobOutput mirrors aws-sdk-go-v2/service/batch's
// DescribeServiceJobOutput field names exactly (see deserializers.go's
// awsRestjson1_deserializeOpDocumentDescribeServiceJobOutput case list).
// Attempts/CapacityUsage/LatestAttempt aren't modeled -- this emulator
// doesn't simulate SageMaker Training job execution/capacity details.
type describeServiceJobOutput struct {
	Tags                  map[string]string        `json:"tags"`
	RetryStrategy         *ServiceJobRetryStrategy `json:"retryStrategy,omitempty"`
	TimeoutConfig         *ServiceJobTimeout       `json:"timeoutConfig,omitempty"`
	StartedAt             *int64                   `json:"startedAt,omitempty"`
	StoppedAt             *int64                   `json:"stoppedAt,omitempty"`
	ScheduledAt           *int64                   `json:"scheduledAt,omitempty"`
	JobID                 string                   `json:"jobId"`
	JobArn                string                   `json:"jobArn,omitempty"`
	JobName               string                   `json:"jobName"`
	JobQueue              string                   `json:"jobQueue"`
	ServiceJobType        string                   `json:"serviceJobType"`
	Status                string                   `json:"status"`
	StatusReason          string                   `json:"statusReason,omitempty"`
	ServiceRequestPayload string                   `json:"serviceRequestPayload,omitempty"`
	ShareIdentifier       string                   `json:"shareIdentifier,omitempty"`
	CreatedAt             int64                    `json:"createdAt"`
	SchedulingPriority    int32                    `json:"schedulingPriority,omitempty"`
	IsTerminated          bool                     `json:"isTerminated"`
}

func (h *Handler) handleDescribeServiceJob(
	ctx context.Context,
	in *describeServiceJobInput,
) (*describeServiceJobOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", ErrValidation)
	}

	sj, err := h.Backend.DescribeServiceJob(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	return &describeServiceJobOutput{
		JobID:                 sj.JobID,
		JobArn:                sj.JobArn,
		JobName:               sj.JobName,
		JobQueue:              sj.JobQueue,
		ServiceJobType:        sj.ServiceJobType,
		Status:                sj.Status,
		StatusReason:          sj.StatusReason,
		ServiceRequestPayload: sj.ServiceRequestPayload,
		ShareIdentifier:       sj.ShareIdentifier,
		CreatedAt:             sj.CreatedAt,
		StartedAt:             sj.StartedAt,
		StoppedAt:             sj.StoppedAt,
		ScheduledAt:           sj.ScheduledAt,
		SchedulingPriority:    sj.SchedulingPriority,
		RetryStrategy:         sj.RetryStrategy,
		TimeoutConfig:         sj.TimeoutConfig,
		IsTerminated:          sj.IsTerminated,
		Tags:                  tagsOrEmpty(sj.Tags),
	}, nil
}

// serviceJobSummary mirrors aws-sdk-go-v2/service/batch/types.
// ServiceJobSummary -- a narrower shape than DescribeServiceJob's output (no
// serviceRequestPayload/retryStrategy/timeoutConfig/tags).
type serviceJobSummary struct {
	StartedAt       *int64 `json:"startedAt,omitempty"`
	StoppedAt       *int64 `json:"stoppedAt,omitempty"`
	ScheduledAt     *int64 `json:"scheduledAt,omitempty"`
	JobID           string `json:"jobId"`
	JobArn          string `json:"jobArn,omitempty"`
	JobName         string `json:"jobName"`
	ServiceJobType  string `json:"serviceJobType"`
	Status          string `json:"status,omitempty"`
	StatusReason    string `json:"statusReason,omitempty"`
	ShareIdentifier string `json:"shareIdentifier,omitempty"`
	CreatedAt       int64  `json:"createdAt,omitempty"`
}

type listServiceJobsInput struct {
	JobQueue  string `json:"jobQueue"`
	JobStatus string `json:"jobStatus,omitempty"`
}

type listServiceJobsOutput struct {
	JobSummaryList []serviceJobSummary `json:"jobSummaryList"`
}

func (h *Handler) handleListServiceJobs(ctx context.Context, in *listServiceJobsInput) (*listServiceJobsOutput, error) {
	list, err := h.Backend.ListServiceJobs(ctx, in.JobQueue, in.JobStatus)
	if err != nil {
		return nil, err
	}

	summaries := make([]serviceJobSummary, 0, len(list))
	for _, sj := range list {
		summaries = append(summaries, serviceJobSummary{
			JobID:           sj.JobID,
			JobArn:          sj.JobArn,
			JobName:         sj.JobName,
			ServiceJobType:  sj.ServiceJobType,
			Status:          sj.Status,
			StatusReason:    sj.StatusReason,
			ShareIdentifier: sj.ShareIdentifier,
			CreatedAt:       sj.CreatedAt,
			StartedAt:       sj.StartedAt,
			StoppedAt:       sj.StoppedAt,
			ScheduledAt:     sj.ScheduledAt,
		})
	}

	return &listServiceJobsOutput{JobSummaryList: summaries}, nil
}

type terminateServiceJobInput struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

func (h *Handler) handleTerminateServiceJob(ctx context.Context, in *terminateServiceJobInput) (*emptyOutput, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: jobId is required", ErrValidation)
	}

	if err := h.Backend.TerminateServiceJob(ctx, in.JobID, in.Reason); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
