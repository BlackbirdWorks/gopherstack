package glue

import (
	"context"
	"fmt"
)

// cancelMLTaskRunInput holds input for CancelMLTaskRun.
type cancelMLTaskRunInput struct {
	TransformID string `json:"TransformId"`
	TaskRunID   string `json:"TaskRunId"`
}

func (h *Handler) handleCancelMLTaskRun(
	_ context.Context,
	in *cancelMLTaskRunInput,
) (*emptyOutput, error) {
	if in.TransformID == "" || in.TaskRunID == "" {
		return nil, fmt.Errorf("%w: TransformId and TaskRunId are required", ErrValidation)
	}

	return &emptyOutput{}, h.Backend.CancelMLTaskRun(in.TransformID, in.TaskRunID)
}

// createMLTransformInput holds input for CreateMLTransform.
type createMLTransformInput struct {
	Parameters        MLTransformParameter `json:"Parameters,omitzero"`
	Tags              map[string]string    `json:"Tags,omitempty"`
	Name              string               `json:"Name"`
	Description       string               `json:"Description,omitempty"`
	Role              string               `json:"Role,omitempty"`
	InputRecordTables []GlueTable          `json:"InputRecordTables,omitempty"`
}

// createMLTransformOutput holds the result for CreateMLTransform.
type createMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleCreateMLTransform(
	_ context.Context,
	in *createMLTransformInput,
) (*createMLTransformOutput, error) {
	m, err := h.Backend.CreateMLTransform(
		in.Name,
		in.Description,
		in.Role,
		in.InputRecordTables,
		in.Parameters,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createMLTransformOutput{TransformID: m.TransformID}, nil
}

// deleteMLTransformInput holds input for DeleteMLTransform.
type deleteMLTransformInput struct {
	TransformID string `json:"TransformId"`
}

// deleteMLTransformOutput holds the result for DeleteMLTransform.
type deleteMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleDeleteMLTransform(
	_ context.Context,
	in *deleteMLTransformInput,
) (*deleteMLTransformOutput, error) {
	if err := h.Backend.DeleteMLTransform(in.TransformID); err != nil {
		return nil, err
	}

	return &deleteMLTransformOutput{TransformID: in.TransformID}, nil
}

// getMLTaskRunInput holds input for GetMLTaskRun.
type getMLTaskRunInput struct {
	TransformID string `json:"TransformId"`
	TaskRunID   string `json:"TaskRunId"`
}

// getMLTaskRunOutput holds the result for GetMLTaskRun.
type getMLTaskRunOutput struct {
	TransformID string `json:"TransformId"`
	TaskRunID   string `json:"TaskRunId"`
	Status      string `json:"Status"`
}

func (h *Handler) handleGetMLTaskRun(
	_ context.Context,
	in *getMLTaskRunInput,
) (*getMLTaskRunOutput, error) {
	if in.TransformID == "" || in.TaskRunID == "" {
		return &getMLTaskRunOutput{Status: stateSucceeded}, nil
	}

	run, err := h.Backend.GetMLTaskRun(in.TransformID, in.TaskRunID)
	if err != nil {
		return nil, err
	}

	return &getMLTaskRunOutput{
		TransformID: run.TransformID,
		TaskRunID:   run.TaskRunID,
		Status:      run.Status,
	}, nil
}

// getMLTaskRunsInput holds input for GetMLTaskRuns.
type getMLTaskRunsInput struct {
	TransformID string `json:"TransformId"`
}

// getMLTaskRunsOutput holds the result for GetMLTaskRuns.
type getMLTaskRunsOutput struct {
	TaskRuns []any `json:"TaskRuns"`
}

func (h *Handler) handleGetMLTaskRuns(
	_ context.Context,
	in *getMLTaskRunsInput,
) (*getMLTaskRunsOutput, error) {
	if in.TransformID == "" {
		return &getMLTaskRunsOutput{TaskRuns: []any{}}, nil
	}

	runs, err := h.Backend.GetMLTaskRuns(in.TransformID)
	if err != nil {
		return nil, err
	}

	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &getMLTaskRunsOutput{TaskRuns: result}, nil
}

// getMLTransformInput holds input for GetMLTransform.
type getMLTransformInput struct {
	TransformID string `json:"TransformId"`
}

// getMLTransformOutput holds the result for GetMLTransform.
type getMLTransformOutput struct {
	*MLTransform
}

func (h *Handler) handleGetMLTransform(
	_ context.Context,
	in *getMLTransformInput,
) (*getMLTransformOutput, error) {
	m, err := h.Backend.GetMLTransform(in.TransformID)
	if err != nil {
		return nil, err
	}

	return &getMLTransformOutput{MLTransform: m}, nil
}

// getMLTransformsInput holds input for GetMLTransforms.
type getMLTransformsInput struct{}

// getMLTransformsOutput holds the result for GetMLTransforms.
type getMLTransformsOutput struct {
	Transforms []*MLTransform `json:"Transforms"`
}

func (h *Handler) handleGetMLTransforms(
	_ context.Context,
	_ *getMLTransformsInput,
) (*getMLTransformsOutput, error) {
	transforms := h.Backend.GetMLTransforms()
	if transforms == nil {
		transforms = []*MLTransform{}
	}

	return &getMLTransformsOutput{Transforms: transforms}, nil
}

// listMLTransformsInput holds input for ListMLTransforms.
type listMLTransformsInput struct{}

// listMLTransformsOutput holds the result for ListMLTransforms.
type listMLTransformsOutput struct {
	TransformIDs []string `json:"TransformIds"`
}

func (h *Handler) handleListMLTransforms(
	_ context.Context,
	_ *listMLTransformsInput,
) (*listMLTransformsOutput, error) {
	transforms := h.Backend.GetMLTransforms()
	ids := make([]string, 0, len(transforms))

	for _, m := range transforms {
		ids = append(ids, m.TransformID)
	}

	return &listMLTransformsOutput{TransformIDs: ids}, nil
}

// startExportLabelsTaskRunInput holds input for StartExportLabelsTaskRun.
type startExportLabelsTaskRunInput struct {
	TransformID  string `json:"TransformId"`
	OutputS3Path string `json:"OutputS3Path,omitempty"`
}

// startExportLabelsTaskRunOutput holds the result for StartExportLabelsTaskRun.
type startExportLabelsTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartExportLabelsTaskRun(
	_ context.Context,
	in *startExportLabelsTaskRunInput,
) (*startExportLabelsTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startExportLabelsTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartExportLabelsTaskRun(in.TransformID, in.OutputS3Path)
	if err != nil {
		return nil, err
	}

	return &startExportLabelsTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// startImportLabelsTaskRunInput holds input for StartImportLabelsTaskRun.
type startImportLabelsTaskRunInput struct {
	TransformID string `json:"TransformId"`
	InputS3Path string `json:"InputS3Path,omitempty"`
}

// startImportLabelsTaskRunOutput holds the result for StartImportLabelsTaskRun.
type startImportLabelsTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartImportLabelsTaskRun(
	_ context.Context,
	in *startImportLabelsTaskRunInput,
) (*startImportLabelsTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startImportLabelsTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartImportLabelsTaskRun(in.TransformID, in.InputS3Path)
	if err != nil {
		return nil, err
	}

	return &startImportLabelsTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// startMLEvaluationTaskRunInput holds input for StartMLEvaluationTaskRun.
type startMLEvaluationTaskRunInput struct {
	TransformID string `json:"TransformId"`
}

// startMLEvaluationTaskRunOutput holds the result for StartMLEvaluationTaskRun.
type startMLEvaluationTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartMLEvaluationTaskRun(
	_ context.Context,
	in *startMLEvaluationTaskRunInput,
) (*startMLEvaluationTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startMLEvaluationTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartMLEvaluationTaskRun(in.TransformID)
	if err != nil {
		return nil, err
	}

	return &startMLEvaluationTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// startMLLabelingSetGenerationTaskRunInput holds input for StartMLLabelingSetGenerationTaskRun.
type startMLLabelingSetGenerationTaskRunInput struct {
	TransformID string `json:"TransformId"`
}

// startMLLabelingSetGenerationTaskRunOutput holds the result for StartMLLabelingSetGenerationTaskRun.
type startMLLabelingSetGenerationTaskRunOutput struct {
	TaskRunID string `json:"TaskRunId"`
}

func (h *Handler) handleStartMLLabelingSetGenerationTaskRun(
	_ context.Context,
	in *startMLLabelingSetGenerationTaskRunInput,
) (*startMLLabelingSetGenerationTaskRunOutput, error) {
	if in.TransformID == "" {
		return &startMLLabelingSetGenerationTaskRunOutput{TaskRunID: ""}, nil
	}

	run, err := h.Backend.StartMLLabelingSetGenerationTaskRun(in.TransformID)
	if err != nil {
		return nil, err
	}

	return &startMLLabelingSetGenerationTaskRunOutput{TaskRunID: run.TaskRunID}, nil
}

// updateMLTransformInput holds input for UpdateMLTransform.
type updateMLTransformInput struct {
	Parameters        MLTransformParameter `json:"Parameters,omitzero"`
	TransformID       string               `json:"TransformId"`
	Description       string               `json:"Description,omitempty"`
	Role              string               `json:"Role,omitempty"`
	Name              string               `json:"Name,omitempty"`
	InputRecordTables []GlueTable          `json:"InputRecordTables,omitempty"`
}

// updateMLTransformOutput holds the result for UpdateMLTransform.
type updateMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleUpdateMLTransform(
	_ context.Context,
	in *updateMLTransformInput,
) (*updateMLTransformOutput, error) {
	update := MLTransform{
		Name:              in.Name,
		Description:       in.Description,
		Role:              in.Role,
		InputRecordTables: in.InputRecordTables,
		Parameters:        in.Parameters,
	}
	if err := h.Backend.UpdateMLTransform(in.TransformID, update); err != nil {
		return nil, err
	}

	return &updateMLTransformOutput{TransformID: in.TransformID}, nil
}
