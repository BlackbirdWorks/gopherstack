package glue

import (
	"context"
	"slices"
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

// defaultListDataQualityRulesetsLimit is used when
// ListDataQualityRulesetsInput.MaxResults is unset.
const defaultListDataQualityRulesetsLimit = 100

// dataQualityRulesetFilterCriteria mirrors
// aws-sdk-go-v2/service/glue/types.DataQualityRulesetFilterCriteria. Every
// member is backed by a real DataQualityRuleset field (models.go).
type dataQualityRulesetFilterCriteria struct {
	TargetTable        *DataQualityTargetTable `json:"TargetTable,omitempty"`
	Name               string                  `json:"Name,omitempty"`
	Description        string                  `json:"Description,omitempty"`
	CreatedBefore      float64                 `json:"CreatedBefore,omitempty"`
	CreatedAfter       float64                 `json:"CreatedAfter,omitempty"`
	LastModifiedBefore float64                 `json:"LastModifiedBefore,omitempty"`
	LastModifiedAfter  float64                 `json:"LastModifiedAfter,omitempty"`
}

type listDataQualityRulesetsInput struct {
	Filter     *dataQualityRulesetFilterCriteria `json:"Filter,omitempty"`
	Tags       map[string]string                 `json:"Tags,omitempty"`
	NextToken  string                            `json:"NextToken,omitempty"`
	MaxResults int32                             `json:"MaxResults,omitempty"`
}

type listDataQualityRulesetsOutput struct {
	NextToken string                `json:"NextToken,omitempty"`
	Rulesets  []*DataQualityRuleset `json:"Rulesets"`
}

// matchesTimeWindow reports whether value falls strictly after after (when
// after > 0) and strictly before before (when before > 0). A zero bound is
// treated as unset, matching how these *FilterCriteria members are optional
// on the wire.
func matchesTimeWindow(value, after, before float64) bool {
	if after > 0 && value <= after {
		return false
	}

	if before > 0 && value >= before {
		return false
	}

	return true
}

func matchesDataQualityRulesetFilter(r *DataQualityRuleset, f *dataQualityRulesetFilterCriteria) bool {
	if f == nil {
		return true
	}

	if f.Name != "" && r.Name != f.Name {
		return false
	}

	if f.Description != "" && r.Description != f.Description {
		return false
	}

	if !matchesTimeWindow(r.CreatedOn, f.CreatedAfter, f.CreatedBefore) {
		return false
	}

	if !matchesTimeWindow(r.LastModifiedOn, f.LastModifiedAfter, f.LastModifiedBefore) {
		return false
	}

	return f.TargetTable == nil || (r.TargetTable != nil && *r.TargetTable == *f.TargetTable)
}

func (h *Handler) handleListDataQualityRulesets(
	_ context.Context,
	in *listDataQualityRulesetsInput,
) (*listDataQualityRulesetsOutput, error) {
	rulesets := h.Backend.ListDataQualityRulesets()

	filtered := make([]*DataQualityRuleset, 0, len(rulesets))

	for _, r := range rulesets {
		if !matchesDataQualityRulesetFilter(r, in.Filter) {
			continue
		}

		if len(in.Tags) > 0 && !matchesTagFilter(r.Tags, in.Tags) {
			continue
		}

		filtered = append(filtered, r)
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListDataQualityRulesetsLimit
	}

	page, next := paginateSlice(filtered, in.NextToken, limit)

	return &listDataQualityRulesetsOutput{Rulesets: page, NextToken: next}, nil
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

// defaultListDataQualityRuleRecommendationRunsLimit is used when
// ListDataQualityRuleRecommendationRunsInput.MaxResults is unset.
const defaultListDataQualityRuleRecommendationRunsLimit = 100

// dataQualityRuleRecommendationRunFilter mirrors
// aws-sdk-go-v2/service/glue/types.DataQualityRuleRecommendationRunFilter.
// DataSource is required on the real op but has no honest backing here:
// DQRuleRecommendationRun (models.go) records only a flat DataSourceS3Path
// string, never the structured types.DataSource{GlueTable: ...} a real
// filter would compare against -- accepted on the wire and left inert rather
// than fabricating a match against data this backend never stored.
// StartedAfter/StartedBefore are real: they compare against StartedOn, which
// DQRuleRecommendationRun does store.
type dataQualityRuleRecommendationRunFilter struct {
	DataSource    any     `json:"DataSource,omitempty"`
	StartedAfter  float64 `json:"StartedAfter,omitempty"`
	StartedBefore float64 `json:"StartedBefore,omitempty"`
}

// listDataQualityRuleRecommendationRunsInput holds input for
// ListDataQualityRuleRecommendationRuns.
//
// Tags is not modeled: DQRuleRecommendationRun is never routed through
// tags.go's tag dispatch, so it is accepted on the wire and otherwise inert.
type listDataQualityRuleRecommendationRunsInput struct {
	Filter     *dataQualityRuleRecommendationRunFilter `json:"Filter,omitempty"`
	Tags       map[string]string                       `json:"Tags,omitempty"`
	NextToken  string                                  `json:"NextToken,omitempty"`
	MaxResults int32                                   `json:"MaxResults,omitempty"`
}

// listDataQualityRuleRecommendationRunsOutput holds the result for ListDataQualityRuleRecommendationRuns.
type listDataQualityRuleRecommendationRunsOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Runs      []any  `json:"Runs"`
}

func (h *Handler) handleListDataQualityRuleRecommendationRuns(
	_ context.Context,
	in *listDataQualityRuleRecommendationRunsInput,
) (*listDataQualityRuleRecommendationRunsOutput, error) {
	all := h.Backend.ListDataQualityRuleRecommendationRuns()

	matching := make([]*DQRuleRecommendationRun, 0, len(all))

	for _, r := range all {
		if in.Filter != nil {
			if in.Filter.StartedAfter > 0 && r.StartedOn <= in.Filter.StartedAfter {
				continue
			}

			if in.Filter.StartedBefore > 0 && r.StartedOn >= in.Filter.StartedBefore {
				continue
			}
		}

		matching = append(matching, r)
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListDataQualityRuleRecommendationRunsLimit
	}

	page, next := paginateSlice(matching, in.NextToken, limit)

	result := make([]any, 0, len(page))
	for _, r := range page {
		result = append(result, r)
	}

	return &listDataQualityRuleRecommendationRunsOutput{Runs: result, NextToken: next}, nil
}

// defaultListDataQualityRulesetEvaluationRunsLimit is used when
// ListDataQualityRulesetEvaluationRunsInput.MaxResults is unset.
const defaultListDataQualityRulesetEvaluationRunsLimit = 100

// dataQualityRulesetEvaluationRunFilter mirrors
// aws-sdk-go-v2/service/glue/types.DataQualityRulesetEvaluationRunFilter.
// DataSource is required on the real op but has no honest backing:
// DataQualityEvaluationRun (models.go) records no data-source/table link at
// all -- accepted on the wire and left inert. RulesetName and
// StartedAfter/StartedBefore are real: they compare against RulesetNames and
// StartedOn, which DataQualityEvaluationRun does store.
type dataQualityRulesetEvaluationRunFilter struct {
	DataSource    any     `json:"DataSource,omitempty"`
	RulesetName   string  `json:"RulesetName,omitempty"`
	StartedAfter  float64 `json:"StartedAfter,omitempty"`
	StartedBefore float64 `json:"StartedBefore,omitempty"`
}

// listDataQualityRulesetEvaluationRunsInput holds input for ListDataQualityRulesetEvaluationRuns.
type listDataQualityRulesetEvaluationRunsInput struct {
	Filter     *dataQualityRulesetEvaluationRunFilter `json:"Filter,omitempty"`
	NextToken  string                                 `json:"NextToken,omitempty"`
	MaxResults int32                                  `json:"MaxResults,omitempty"`
}

// listDataQualityRulesetEvaluationRunsOutput holds the result for ListDataQualityRulesetEvaluationRuns.
type listDataQualityRulesetEvaluationRunsOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Runs      []any  `json:"Runs"`
}

func evaluationRunHasRuleset(r *DataQualityEvaluationRun, name string) bool {
	return slices.Contains(r.RulesetNames, name)
}

func (h *Handler) handleListDataQualityRulesetEvaluationRuns(
	_ context.Context,
	in *listDataQualityRulesetEvaluationRunsInput,
) (*listDataQualityRulesetEvaluationRunsOutput, error) {
	all := h.Backend.ListDataQualityEvaluationRuns()

	matching := make([]*DataQualityEvaluationRun, 0, len(all))

	for _, r := range all {
		if in.Filter != nil {
			if in.Filter.RulesetName != "" && !evaluationRunHasRuleset(r, in.Filter.RulesetName) {
				continue
			}

			if in.Filter.StartedAfter > 0 && r.StartedOn <= in.Filter.StartedAfter {
				continue
			}

			if in.Filter.StartedBefore > 0 && r.StartedOn >= in.Filter.StartedBefore {
				continue
			}
		}

		matching = append(matching, r)
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListDataQualityRulesetEvaluationRunsLimit
	}

	page, next := paginateSlice(matching, in.NextToken, limit)

	result := make([]any, 0, len(page))

	for _, r := range page {
		result = append(result, r)
	}

	return &listDataQualityRulesetEvaluationRunsOutput{Runs: result, NextToken: next}, nil
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
