package glue

import (
	"context"
)

type createDataQualityRulesetInput struct {
	Tags                             map[string]string       `json:"Tags,omitempty"`
	TargetTable                      *DataQualityTargetTable `json:"TargetTable,omitempty"`
	Name                             string                  `json:"Name"`
	Ruleset                          string                  `json:"Ruleset,omitempty"`
	Description                      string                  `json:"Description,omitempty"`
	DataQualitySecurityConfiguration string                  `json:"DataQualitySecurityConfiguration,omitempty"`
}

type createDataQualityRulesetOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateDataQualityRuleset(
	_ context.Context,
	in *createDataQualityRulesetInput,
) (*createDataQualityRulesetOutput, error) {
	r, err := h.Backend.CreateDataQualityRulesetWithOptions(in.Name, in.Ruleset, in.Tags, DataQualityRulesetOptions{
		Description:                      in.Description,
		DataQualitySecurityConfiguration: in.DataQualitySecurityConfiguration,
		TargetTable:                      in.TargetTable,
	})
	if err != nil {
		return nil, err
	}

	return &createDataQualityRulesetOutput{Name: r.Name}, nil
}

type getDataQualityRulesetInput struct {
	Name string `json:"Name"`
}

type getDataQualityRulesetOutput struct {
	TargetTable                      *DataQualityTargetTable `json:"TargetTable,omitempty"`
	Name                             string                  `json:"Name"`
	Ruleset                          string                  `json:"Ruleset,omitempty"`
	Description                      string                  `json:"Description,omitempty"`
	ARN                              string                  `json:"Arn,omitempty"`
	DataQualitySecurityConfiguration string                  `json:"DataQualitySecurityConfiguration,omitempty"`
	CreatedOn                        float64                 `json:"CreatedOn,omitempty"`
	LastModifiedOn                   float64                 `json:"LastModifiedOn,omitempty"`
}

func (h *Handler) handleGetDataQualityRuleset(
	_ context.Context,
	in *getDataQualityRulesetInput,
) (*getDataQualityRulesetOutput, error) {
	r, err := h.Backend.GetDataQualityRuleset(in.Name)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRulesetOutput{
		Name:                             r.Name,
		Ruleset:                          r.Ruleset,
		Description:                      r.Description,
		ARN:                              r.ARN,
		DataQualitySecurityConfiguration: r.DataQualitySecurityConfiguration,
		TargetTable:                      r.TargetTable,
		CreatedOn:                        r.CreatedOn,
		LastModifiedOn:                   r.LastModifiedOn,
	}, nil
}

type deleteDataQualityRulesetInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteDataQualityRuleset(
	_ context.Context,
	in *deleteDataQualityRulesetInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteDataQualityRuleset(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type updateDataQualityRulesetInput struct {
	Name        string `json:"Name"`
	Ruleset     string `json:"Ruleset,omitempty"`
	Description string `json:"Description,omitempty"`
}

func (h *Handler) handleUpdateDataQualityRuleset(
	_ context.Context,
	in *updateDataQualityRulesetInput,
) (*emptyOutput, error) {
	if err := h.Backend.UpdateDataQualityRuleset(in.Name, in.Ruleset, in.Description); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listDataQualityRulesetsInput struct{}

type listDataQualityRulesetsOutput struct {
	Rulesets []*DataQualityRuleset `json:"Rulesets"`
}

func (h *Handler) handleListDataQualityRulesets(
	_ context.Context,
	_ *listDataQualityRulesetsInput,
) (*listDataQualityRulesetsOutput, error) {
	rulesets := h.Backend.ListDataQualityRulesets()

	return &listDataQualityRulesetsOutput{Rulesets: rulesets}, nil
}

type startDataQualityRulesetEvaluationRunInput struct {
	RulesetNames []string `json:"RulesetNames"`
}

type startDataQualityRulesetEvaluationRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *startDataQualityRulesetEvaluationRunInput,
) (*startDataQualityRulesetEvaluationRunOutput, error) {
	run, err := h.Backend.StartDataQualityRulesetEvaluationRun(in.RulesetNames)
	if err != nil {
		return nil, err
	}

	return &startDataQualityRulesetEvaluationRunOutput{RunID: run.RunID}, nil
}

type getDataQualityRulesetEvaluationRunInput struct {
	RunID string `json:"RunId"`
}

type getDataQualityRulesetEvaluationRunOutput struct {
	DataQualityEvaluationRun *DataQualityEvaluationRun `json:"DataQualityEvaluationRun"`
}

func (h *Handler) handleGetDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *getDataQualityRulesetEvaluationRunInput,
) (*getDataQualityRulesetEvaluationRunOutput, error) {
	run, err := h.Backend.GetDataQualityRulesetEvaluationRun(in.RunID)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRulesetEvaluationRunOutput{DataQualityEvaluationRun: run}, nil
}

// batchGetDataQualityRulesetEvaluationRunInput holds input for
// BatchGetDataQualityRulesetEvaluationRun.
type batchGetDataQualityRulesetEvaluationRunInput struct {
	RunIDs []string `json:"RunIds"`
}

// batchGetDataQualityRulesetEvaluationRunOutput holds the result for
// BatchGetDataQualityRulesetEvaluationRun.
type batchGetDataQualityRulesetEvaluationRunOutput struct {
	Runs         []*DataQualityEvaluationRun `json:"Runs"`
	RunsNotFound []string                    `json:"RunsNotFound"`
}

func (h *Handler) handleBatchGetDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *batchGetDataQualityRulesetEvaluationRunInput,
) (*batchGetDataQualityRulesetEvaluationRunOutput, error) {
	found, missing := h.Backend.BatchGetDataQualityRulesetEvaluationRun(in.RunIDs)

	return &batchGetDataQualityRulesetEvaluationRunOutput{Runs: found, RunsNotFound: missing}, nil
}

type cancelDataQualityRulesetEvaluationRunInput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleCancelDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *cancelDataQualityRulesetEvaluationRunInput,
) (*emptyOutput, error) {
	if err := h.Backend.CancelDataQualityRulesetEvaluationRun(in.RunID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// cancelDataQualityRuleRecommendationRunInput holds input for CancelDataQualityRuleRecommendationRun.
type cancelDataQualityRuleRecommendationRunInput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleCancelDataQualityRuleRecommendationRun(
	_ context.Context,
	in *cancelDataQualityRuleRecommendationRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CancelDataQualityRuleRecommendationRun(in.RunID)
}

// getDataQualityRuleRecommendationRunInput holds input for GetDataQualityRuleRecommendationRun.
type getDataQualityRuleRecommendationRunInput struct {
	RunID string `json:"RunId"`
}

// getDataQualityRuleRecommendationRunOutput holds the result for GetDataQualityRuleRecommendationRun.
type getDataQualityRuleRecommendationRunOutput struct {
	RunID  string `json:"RunId"`
	Status string `json:"Status"`
}

func (h *Handler) handleGetDataQualityRuleRecommendationRun(
	_ context.Context,
	in *getDataQualityRuleRecommendationRunInput,
) (*getDataQualityRuleRecommendationRunOutput, error) {
	if in.RunID == "" {
		return &getDataQualityRuleRecommendationRunOutput{Status: stateSucceeded}, nil
	}

	run, err := h.Backend.GetDataQualityRuleRecommendationRun(in.RunID)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRuleRecommendationRunOutput{
		RunID:  run.RecommendationRunID,
		Status: run.Status,
	}, nil
}

// listDataQualityRuleRecommendationRunsInput holds input for ListDataQualityRuleRecommendationRuns.
type listDataQualityRuleRecommendationRunsInput struct{}

// listDataQualityRuleRecommendationRunsOutput holds the result for ListDataQualityRuleRecommendationRuns.
type listDataQualityRuleRecommendationRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleListDataQualityRuleRecommendationRuns(
	_ context.Context,
	_ *listDataQualityRuleRecommendationRunsInput,
) (*listDataQualityRuleRecommendationRunsOutput, error) {
	runs := h.Backend.ListDataQualityRuleRecommendationRuns()
	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &listDataQualityRuleRecommendationRunsOutput{Runs: result}, nil
}

// listDataQualityRulesetEvaluationRunsInput holds input for ListDataQualityRulesetEvaluationRuns.
type listDataQualityRulesetEvaluationRunsInput struct{}

// listDataQualityRulesetEvaluationRunsOutput holds the result for ListDataQualityRulesetEvaluationRuns.
type listDataQualityRulesetEvaluationRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleListDataQualityRulesetEvaluationRuns(
	_ context.Context,
	_ *listDataQualityRulesetEvaluationRunsInput,
) (*listDataQualityRulesetEvaluationRunsOutput, error) {
	runs := h.Backend.ListDataQualityEvaluationRuns()
	result := make([]any, 0, len(runs))

	for _, r := range runs {
		result = append(result, r)
	}

	return &listDataQualityRulesetEvaluationRunsOutput{Runs: result}, nil
}

// startDataQualityRuleRecommendationRunInput holds input for StartDataQualityRuleRecommendationRun.
type startDataQualityRuleRecommendationRunInput struct {
	DataSource struct {
		GlueTable *GlueTable `json:"GlueTable,omitempty"`
	} `json:"DataSource,omitzero"`
	OutputS3Path string `json:"OutputS3Path,omitempty"`
	Role         string `json:"Role,omitempty"`
}

// startDataQualityRuleRecommendationRunOutput holds the result for StartDataQualityRuleRecommendationRun.
type startDataQualityRuleRecommendationRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartDataQualityRuleRecommendationRun(
	_ context.Context,
	in *startDataQualityRuleRecommendationRunInput,
) (*startDataQualityRuleRecommendationRunOutput, error) {
	run, err := h.Backend.StartDataQualityRuleRecommendationRun(in.OutputS3Path)
	if err != nil {
		return nil, err
	}

	return &startDataQualityRuleRecommendationRunOutput{RunID: run.RecommendationRunID}, nil
}
