package glue

import (
	"context"
	"fmt"
	"slices"
	"sort"
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
	Parameters          MLTransformParameter `json:"Parameters,omitzero"`
	Tags                map[string]string    `json:"Tags,omitempty"`
	TransformEncryption *TransformEncryption `json:"TransformEncryption,omitempty"`
	Name                string               `json:"Name"`
	Description         string               `json:"Description,omitempty"`
	Role                string               `json:"Role,omitempty"`
	GlueVersion         string               `json:"GlueVersion,omitempty"`
	WorkerType          string               `json:"WorkerType,omitempty"`
	InputRecordTables   []GlueTable          `json:"InputRecordTables,omitempty"`
	Schema              []SchemaColumnEntry  `json:"Schema,omitempty"`
	MaxCapacity         float64              `json:"MaxCapacity,omitempty"`
	NumberOfWorkers     int32                `json:"NumberOfWorkers,omitempty"`
	MaxRetries          int                  `json:"MaxRetries,omitempty"`
	Timeout             int                  `json:"Timeout,omitempty"`
}

// createMLTransformOutput holds the result for CreateMLTransform.
type createMLTransformOutput struct {
	TransformID string `json:"TransformId"`
}

func (h *Handler) handleCreateMLTransform(
	_ context.Context,
	in *createMLTransformInput,
) (*createMLTransformOutput, error) {
	m, err := h.Backend.CreateMLTransformWithOptions(
		in.Name,
		in.Description,
		in.Role,
		in.InputRecordTables,
		in.Parameters,
		in.Tags,
		MLTransformOptions{
			GlueVersion:         in.GlueVersion,
			WorkerType:          in.WorkerType,
			NumberOfWorkers:     in.NumberOfWorkers,
			MaxCapacity:         in.MaxCapacity,
			MaxRetries:          in.MaxRetries,
			Timeout:             in.Timeout,
			Schema:              in.Schema,
			TransformEncryption: in.TransformEncryption,
		},
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

// getMLTaskRunOutput holds the result for GetMLTaskRun. StartedOn/CompletedOn/
// ExecutionTime/ErrorString/LogGroupName are real GetMLTaskRunOutput members
// (api_op_GetMLTaskRun.go) backed by real state already tracked on MLTaskRun
// (models.go) -- previously dropped entirely by this narrower response struct.
type getMLTaskRunOutput struct {
	TransformID   string  `json:"TransformId"`
	TaskRunID     string  `json:"TaskRunId"`
	Status        string  `json:"Status"`
	ErrorString   string  `json:"ErrorString,omitempty"`
	LogGroupName  string  `json:"LogGroupName,omitempty"`
	StartedOn     float64 `json:"StartedOn,omitempty"`
	CompletedOn   float64 `json:"CompletedOn,omitempty"`
	ExecutionTime int     `json:"ExecutionTime,omitempty"`
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
		TransformID:   run.TransformID,
		TaskRunID:     run.TaskRunID,
		Status:        run.Status,
		ErrorString:   run.ErrorString,
		LogGroupName:  run.LogGroupName,
		StartedOn:     run.StartedOn,
		CompletedOn:   run.CompletedOn,
		ExecutionTime: run.ExecutionTime,
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

// defaultGetMLTransformsLimit is used when MaxResults is unset on
// GetMLTransformsInput/ListMLTransformsInput.
const defaultGetMLTransformsLimit = 100

// transformFilterCriteria mirrors
// aws-sdk-go-v2/service/glue/types.TransformFilterCriteria. TransformType has
// no honest backing: MLTransform (models.go) has no TransformType field (this
// backend only ever models the one real transform kind, FIND_MATCHES) --
// accepted on the wire and otherwise inert. Every other member is backed by a
// real MLTransform field.
type transformFilterCriteria struct {
	Name               string              `json:"Name,omitempty"`
	GlueVersion        string              `json:"GlueVersion,omitempty"`
	Status             string              `json:"Status,omitempty"`
	TransformType      string              `json:"TransformType,omitempty"`
	Schema             []SchemaColumnEntry `json:"Schema,omitempty"`
	CreatedBefore      float64             `json:"CreatedBefore,omitempty"`
	CreatedAfter       float64             `json:"CreatedAfter,omitempty"`
	LastModifiedBefore float64             `json:"LastModifiedBefore,omitempty"`
	LastModifiedAfter  float64             `json:"LastModifiedAfter,omitempty"`
}

// transformSortCriteria mirrors
// aws-sdk-go-v2/service/glue/types.TransformSortCriteria. Column
// "TRANSFORM_TYPE" has no honest backing (see transformFilterCriteria) and
// falls back to leaving the list in its existing (name-sorted) order.
type transformSortCriteria struct {
	Column        string `json:"Column,omitempty"`
	SortDirection string `json:"SortDirection,omitempty"`
}

// matchesTransformBasics checks the non-time-window scalar members of
// transformFilterCriteria, split out of matchesTransformFilter to keep its
// cyclomatic complexity down.
func matchesTransformBasics(m *MLTransform, f *transformFilterCriteria) bool {
	if f.Name != "" && m.Name != f.Name {
		return false
	}

	if f.GlueVersion != "" && m.GlueVersion != f.GlueVersion {
		return false
	}

	if f.Status != "" && m.Status != f.Status {
		return false
	}

	return len(f.Schema) == 0 || slices.Equal(f.Schema, m.Schema)
}

func matchesTransformFilter(m *MLTransform, f *transformFilterCriteria) bool {
	if f == nil {
		return true
	}

	if !matchesTransformBasics(m, f) {
		return false
	}

	if !matchesTimeWindow(m.CreatedOn, f.CreatedAfter, f.CreatedBefore) {
		return false
	}

	return matchesTimeWindow(m.LastModifiedOn, f.LastModifiedAfter, f.LastModifiedBefore)
}

func sortTransforms(transforms []*MLTransform, sortBy *transformSortCriteria) {
	if sortBy == nil {
		return
	}

	var less func(a, b *MLTransform) bool

	switch sortBy.Column {
	case "NAME":
		less = func(a, b *MLTransform) bool { return a.Name < b.Name }
	case "STATUS":
		less = func(a, b *MLTransform) bool { return a.Status < b.Status }
	case "CREATED":
		less = func(a, b *MLTransform) bool { return a.CreatedOn < b.CreatedOn }
	case "LAST_MODIFIED":
		less = func(a, b *MLTransform) bool { return a.LastModifiedOn < b.LastModifiedOn }
	default:
		return
	}

	sort.SliceStable(transforms, func(i, j int) bool {
		if sortBy.SortDirection == "DESCENDING" {
			return less(transforms[j], transforms[i])
		}

		return less(transforms[i], transforms[j])
	})
}

// getMLTransformsInput holds input for GetMLTransforms.
type getMLTransformsInput struct {
	Filter     *transformFilterCriteria `json:"Filter,omitempty"`
	Sort       *transformSortCriteria   `json:"Sort,omitempty"`
	NextToken  string                   `json:"NextToken,omitempty"`
	MaxResults int32                    `json:"MaxResults,omitempty"`
}

// getMLTransformsOutput holds the result for GetMLTransforms.
type getMLTransformsOutput struct {
	NextToken  string         `json:"NextToken,omitempty"`
	Transforms []*MLTransform `json:"Transforms"`
}

func (h *Handler) handleGetMLTransforms(
	_ context.Context,
	in *getMLTransformsInput,
) (*getMLTransformsOutput, error) {
	transforms := h.Backend.GetMLTransforms()

	filtered := make([]*MLTransform, 0, len(transforms))

	for _, m := range transforms {
		if matchesTransformFilter(m, in.Filter) {
			filtered = append(filtered, m)
		}
	}

	sortTransforms(filtered, in.Sort)

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetMLTransformsLimit
	}

	page, next := paginateSlice(filtered, in.NextToken, limit)
	if page == nil {
		page = []*MLTransform{}
	}

	return &getMLTransformsOutput{Transforms: page, NextToken: next}, nil
}

// listMLTransformsInput holds input for ListMLTransforms.
type listMLTransformsInput struct {
	Filter     *transformFilterCriteria `json:"Filter,omitempty"`
	Sort       *transformSortCriteria   `json:"Sort,omitempty"`
	Tags       map[string]string        `json:"Tags,omitempty"`
	NextToken  string                   `json:"NextToken,omitempty"`
	MaxResults int32                    `json:"MaxResults,omitempty"`
}

// listMLTransformsOutput holds the result for ListMLTransforms.
type listMLTransformsOutput struct {
	NextToken    string   `json:"NextToken,omitempty"`
	TransformIDs []string `json:"TransformIds"`
}

func (h *Handler) handleListMLTransforms(
	_ context.Context,
	in *listMLTransformsInput,
) (*listMLTransformsOutput, error) {
	transforms := h.Backend.GetMLTransforms()

	filtered := make([]*MLTransform, 0, len(transforms))

	for _, m := range transforms {
		if !matchesTransformFilter(m, in.Filter) {
			continue
		}

		if len(in.Tags) > 0 && !matchesTagFilter(m.Tags, in.Tags) {
			continue
		}

		filtered = append(filtered, m)
	}

	sortTransforms(filtered, in.Sort)

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetMLTransformsLimit
	}

	page, next := paginateSlice(filtered, in.NextToken, limit)

	ids := make([]string, 0, len(page))
	for _, m := range page {
		ids = append(ids, m.TransformID)
	}

	return &listMLTransformsOutput{TransformIDs: ids, NextToken: next}, nil
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
	Parameters          MLTransformParameter `json:"Parameters,omitzero"`
	TransformEncryption *TransformEncryption `json:"TransformEncryption,omitempty"`
	TransformID         string               `json:"TransformId"`
	Description         string               `json:"Description,omitempty"`
	Role                string               `json:"Role,omitempty"`
	Name                string               `json:"Name,omitempty"`
	GlueVersion         string               `json:"GlueVersion,omitempty"`
	WorkerType          string               `json:"WorkerType,omitempty"`
	InputRecordTables   []GlueTable          `json:"InputRecordTables,omitempty"`
	MaxCapacity         float64              `json:"MaxCapacity,omitempty"`
	NumberOfWorkers     int32                `json:"NumberOfWorkers,omitempty"`
	MaxRetries          int                  `json:"MaxRetries,omitempty"`
	Timeout             int                  `json:"Timeout,omitempty"`
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
		Name:                in.Name,
		Description:         in.Description,
		Role:                in.Role,
		InputRecordTables:   in.InputRecordTables,
		Parameters:          in.Parameters,
		GlueVersion:         in.GlueVersion,
		WorkerType:          in.WorkerType,
		NumberOfWorkers:     in.NumberOfWorkers,
		MaxCapacity:         in.MaxCapacity,
		MaxRetries:          in.MaxRetries,
		Timeout:             in.Timeout,
		TransformEncryption: in.TransformEncryption,
	}
	if err := h.Backend.UpdateMLTransform(in.TransformID, update); err != nil {
		return nil, err
	}

	return &updateMLTransformOutput{TransformID: in.TransformID}, nil
}
