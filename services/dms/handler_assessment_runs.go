package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type cancelReplicationTaskAssessmentRunInput struct {
	ReplicationTaskAssessmentRunArn *string `json:"ReplicationTaskAssessmentRunArn"`
}

type cancelReplicationTaskAssessmentRunOutput struct {
	ReplicationTaskAssessmentRun map[string]any `json:"ReplicationTaskAssessmentRun"`
}

func (h *Handler) handleCancelReplicationTaskAssessmentRun(
	ctx context.Context, in *cancelReplicationTaskAssessmentRunInput,
) (*cancelReplicationTaskAssessmentRunOutput, error) {
	if err := h.Backend.CancelReplicationTaskAssessmentRun(
		ctx,
		ptrconv.String(in.ReplicationTaskAssessmentRunArn),
	); err != nil {
		return nil, err
	}

	return &cancelReplicationTaskAssessmentRunOutput{
		ReplicationTaskAssessmentRun: map[string]any{
			keyAssessmentRunArn: ptrconv.String(in.ReplicationTaskAssessmentRunArn),
			keyStatus:           "cancelling",
		},
	}, nil
}

type deleteReplicationTaskAssessmentRunInput struct {
	ReplicationTaskAssessmentRunArn *string `json:"ReplicationTaskAssessmentRunArn"`
}

type deleteReplicationTaskAssessmentRunOutput struct {
	ReplicationTaskAssessmentRun map[string]any `json:"ReplicationTaskAssessmentRun"`
}

func (h *Handler) handleDeleteReplicationTaskAssessmentRun(
	ctx context.Context, in *deleteReplicationTaskAssessmentRunInput,
) (*deleteReplicationTaskAssessmentRunOutput, error) {
	run, err := h.Backend.DeleteAssessmentRun(ctx, ptrconv.String(in.ReplicationTaskAssessmentRunArn))
	if err != nil {
		return nil, err
	}

	return &deleteReplicationTaskAssessmentRunOutput{
		ReplicationTaskAssessmentRun: map[string]any{
			keyAssessmentRunArn:  run.ReplicationTaskAssessmentRunArn,
			keyAssessmentTaskArn: run.ReplicationTaskArn,
			keyAssessmentRunName: run.AssessmentRunName,
			keyStatus:            run.Status,
		},
	}, nil
}

type describeApplicableIndividualAssessmentsInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeApplicableIndividualAssessmentsOutput struct {
	Marker                    *string  `json:"Marker,omitempty"`
	IndividualAssessmentNames []string `json:"IndividualAssessmentNames"`
}

func (h *Handler) handleDescribeApplicableIndividualAssessments(
	_ context.Context, _ *describeApplicableIndividualAssessmentsInput,
) (*describeApplicableIndividualAssessmentsOutput, error) {
	return &describeApplicableIndividualAssessmentsOutput{
		IndividualAssessmentNames: []string{},
	}, nil
}

type describeReplicationTaskAssessmentRunsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationTaskAssessmentRunsOutput struct {
	Marker                        *string          `json:"Marker,omitempty"`
	ReplicationTaskAssessmentRuns []map[string]any `json:"ReplicationTaskAssessmentRuns"`
}

func (h *Handler) handleDescribeReplicationTaskAssessmentRuns(
	ctx context.Context, in *describeReplicationTaskAssessmentRunsInput,
) (*describeReplicationTaskAssessmentRunsOutput, error) {
	taskArn := extractFilterValue(in.Filters, "replication-task-arn")

	runs, err := h.Backend.DescribeAssessmentRuns(ctx, taskArn)
	if err != nil {
		return nil, err
	}

	list := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		list = append(list, map[string]any{
			keyAssessmentRunArn:  run.ReplicationTaskAssessmentRunArn,
			keyAssessmentTaskArn: run.ReplicationTaskArn,
			keyAssessmentRunName: run.AssessmentRunName,
			keyStatus:            run.Status,
		})
	}

	return &describeReplicationTaskAssessmentRunsOutput{
		ReplicationTaskAssessmentRuns: list,
	}, nil
}

type describeReplicationTaskIndividualAssessmentsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationTaskIndividualAssessmentsOutput struct {
	Marker                               *string          `json:"Marker,omitempty"`
	ReplicationTaskIndividualAssessments []map[string]any `json:"ReplicationTaskIndividualAssessments"`
}

func (h *Handler) handleDescribeReplicationTaskIndividualAssessments(
	_ context.Context, _ *describeReplicationTaskIndividualAssessmentsInput,
) (*describeReplicationTaskIndividualAssessmentsOutput, error) {
	return &describeReplicationTaskIndividualAssessmentsOutput{
		ReplicationTaskIndividualAssessments: []map[string]any{},
	}, nil
}

func (h *Handler) handleStartReplicationTaskAssessment(
	ctx context.Context, in *startReplicationTaskAssessmentInput,
) (*startReplicationTaskAssessmentOutput, error) {
	taskArn := ptrconv.String(in.ReplicationTaskArn)

	tasks, err := h.Backend.DescribeReplicationTasks(ctx, taskArn)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, taskArn)
	}

	return &startReplicationTaskAssessmentOutput{ReplicationTask: rtToJSON(tasks[0])}, nil
}

type startReplicationTaskAssessmentRunInput struct {
	ReplicationTaskArn   *string  `json:"ReplicationTaskArn"`
	ServiceAccessRoleArn *string  `json:"ServiceAccessRoleArn"`
	ResultLocationBucket *string  `json:"ResultLocationBucket"`
	AssessmentRunName    *string  `json:"AssessmentRunName"`
	IncludeOnly          []string `json:"IncludeOnly"`
	Exclude              []string `json:"Exclude"`
}

type startReplicationTaskAssessmentRunOutput struct {
	ReplicationTaskAssessmentRun map[string]any `json:"ReplicationTaskAssessmentRun"`
}

func (h *Handler) handleStartReplicationTaskAssessmentRun(
	ctx context.Context, in *startReplicationTaskAssessmentRunInput,
) (*startReplicationTaskAssessmentRunOutput, error) {
	run, err := h.Backend.StartAssessmentRun(
		ctx,
		ptrconv.String(in.ReplicationTaskArn),
		ptrconv.String(in.ServiceAccessRoleArn),
		ptrconv.String(in.ResultLocationBucket),
		ptrconv.String(in.AssessmentRunName),
	)
	if err != nil {
		return nil, err
	}

	return &startReplicationTaskAssessmentRunOutput{
		ReplicationTaskAssessmentRun: map[string]any{
			keyAssessmentRunArn:  run.ReplicationTaskAssessmentRunArn,
			keyAssessmentTaskArn: run.ReplicationTaskArn,
			keyAssessmentRunName: run.AssessmentRunName,
			keyStatus:            run.Status,
		},
	}, nil
}

// opsAssessmentRuns returns the dispatch-table entries for the assessment_runs operation family.
func (h *Handler) opsAssessmentRuns() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CancelReplicationTaskAssessmentRun": service.WrapOp(
			h.handleCancelReplicationTaskAssessmentRun,
		),
		opDeleteReplicationTaskAssessmentRun: service.WrapOp(
			h.handleDeleteReplicationTaskAssessmentRun,
		),
		opDescribeApplicableIndividualAssessments: service.WrapOp(
			h.handleDescribeApplicableIndividualAssessments,
		),
		opDescribeReplicationTaskAssessmentRuns: service.WrapOp(
			h.handleDescribeReplicationTaskAssessmentRuns,
		),
		opDescribeReplicationTaskIndividualAssessments: service.WrapOp(
			h.handleDescribeReplicationTaskIndividualAssessments,
		),
		opStartReplicationTaskAssessment: service.WrapOp(
			h.handleStartReplicationTaskAssessment,
		),
		opStartReplicationTaskAssessmentRun: service.WrapOp(
			h.handleStartReplicationTaskAssessmentRun,
		),
	}
}
