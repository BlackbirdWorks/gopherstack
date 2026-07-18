package batch

import (
	"context"
	"fmt"
)

type jobStateTimeLimitActionInput struct {
	Reason         string `json:"reason"`
	State          string `json:"state"`
	Action         string `json:"action"`
	MaxTimeSeconds int32  `json:"maxTimeSeconds"`
}

type createJobQueueInput struct {
	Tags                     map[string]string              `json:"tags"`
	JobQueueName             string                         `json:"jobQueueName"`
	State                    string                         `json:"state"`
	SchedulingPolicyArn      string                         `json:"schedulingPolicyArn,omitempty"`
	ComputeEnvironmentOrder  []ComputeEnvironmentOrder      `json:"computeEnvironmentOrder"`
	JobStateTimeLimitActions []jobStateTimeLimitActionInput `json:"jobStateTimeLimitActions,omitempty"`
	Priority                 int32                          `json:"priority"`
}

type createJobQueueOutput struct {
	JobQueueArn  string `json:"jobQueueArn"`
	JobQueueName string `json:"jobQueueName"`
}

func jobStateTimeLimitActionsFromInput(in []jobStateTimeLimitActionInput) []JobStateTimeLimitAction {
	if len(in) == 0 {
		return nil
	}

	out := make([]JobStateTimeLimitAction, len(in))
	for i, a := range in {
		out[i] = JobStateTimeLimitAction(a)
	}

	return out
}

func (h *Handler) handleCreateJobQueue(
	ctx context.Context,
	in *createJobQueueInput,
) (*createJobQueueOutput, error) {
	state := in.State
	if state == "" {
		state = stateEnabled
	}

	jq, err := h.Backend.CreateJobQueue(
		ctx,
		in.JobQueueName,
		in.Priority,
		state,
		in.ComputeEnvironmentOrder,
		in.Tags,
		in.SchedulingPolicyArn,
		jobStateTimeLimitActionsFromInput(in.JobStateTimeLimitActions),
	)
	if err != nil {
		return nil, err
	}

	return &createJobQueueOutput{
		JobQueueArn:  jq.JobQueueArn,
		JobQueueName: jq.JobQueueName,
	}, nil
}

type describeJobQueuesInput struct {
	MaxResults *int32   `json:"maxResults,omitempty"`
	NextToken  *string  `json:"nextToken,omitempty"`
	JobQueues  []string `json:"jobQueues"`
}

type describeJobQueuesOutput struct {
	NextToken *string     `json:"nextToken,omitempty"`
	JobQueues []*JobQueue `json:"jobQueues"`
}

func (h *Handler) handleDescribeJobQueues(
	ctx context.Context,
	in *describeJobQueuesInput,
) (*describeJobQueuesOutput, error) {
	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	jqs, outToken := h.Backend.DescribeJobQueues(ctx, in.JobQueues, maxResults, nextToken)
	out := &describeJobQueuesOutput{JobQueues: jqs}

	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type updateJobQueueInput struct {
	Priority                 *int32                         `json:"priority,omitempty"`
	JobQueue                 string                         `json:"jobQueue"`
	State                    string                         `json:"state"`
	SchedulingPolicyArn      string                         `json:"schedulingPolicyArn,omitempty"`
	ComputeEnvironmentOrder  []ComputeEnvironmentOrder      `json:"computeEnvironmentOrder,omitempty"`
	JobStateTimeLimitActions []jobStateTimeLimitActionInput `json:"jobStateTimeLimitActions,omitempty"`
}

type updateJobQueueOutput struct {
	JobQueueArn  string `json:"jobQueueArn"`
	JobQueueName string `json:"jobQueueName"`
}

func (h *Handler) handleUpdateJobQueue(
	ctx context.Context,
	in *updateJobQueueInput,
) (*updateJobQueueOutput, error) {
	jq, err := h.Backend.UpdateJobQueue(
		ctx,
		in.JobQueue, in.Priority, in.State, in.ComputeEnvironmentOrder,
		jobStateTimeLimitActionsFromInput(in.JobStateTimeLimitActions),
	)
	if err != nil {
		return nil, err
	}

	return &updateJobQueueOutput{
		JobQueueArn:  jq.JobQueueArn,
		JobQueueName: jq.JobQueueName,
	}, nil
}

type deleteJobQueueInput struct {
	JobQueue string `json:"jobQueue"`
}

func (h *Handler) handleDeleteJobQueue(
	ctx context.Context,
	in *deleteJobQueueInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteJobQueue(ctx, in.JobQueue); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getJobQueueSnapshotInput struct {
	JobQueue string `json:"jobQueue"`
}

func (h *Handler) handleGetJobQueueSnapshot(
	ctx context.Context,
	in *getJobQueueSnapshotInput,
) (*JobQueueSnapshot, error) {
	if in.JobQueue == "" {
		return nil, fmt.Errorf("%w: jobQueue is required", ErrValidation)
	}

	return h.Backend.GetJobQueueSnapshot(ctx, in.JobQueue)
}
