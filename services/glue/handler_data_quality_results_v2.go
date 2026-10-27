package glue

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type publishDataQualityResultV2Input struct {
	Score                *float64             `json:"Score"`
	EvaluationContext    *string              `json:"EvaluationContext"`
	Dimensions           map[string]string    `json:"Dimensions"`
	RuleResults          []DQRuleResult       `json:"RuleResults"`
	AggregatedMetrics    *DQAggregatedMetrics `json:"AggregatedMetrics"`
	ExecutionStartedOn   *awstime.EpochTime   `json:"ExecutionStartedOn"`
	ExecutionCompletedOn *awstime.EpochTime   `json:"ExecutionCompletedOn"`
	Descriptors          map[string]string    `json:"Descriptors"`
	Tags                 map[string]string    `json:"Tags"`
	RulesetName          string               `json:"RulesetName"`
	ClientToken          string               `json:"ClientToken"`
	AnalyzerResults      []DQAnalyzerResult   `json:"AnalyzerResults"`
	Observations         []DQObservation      `json:"Observations"`
}

type publishDataQualityResultV2Output struct {
	ResultID string `json:"ResultId"`
}

func (h *Handler) handlePublishDataQualityResultV2(
	_ context.Context,
	in *publishDataQualityResultV2Input,
) (*publishDataQualityResultV2Output, error) {
	if in.Score == nil || in.EvaluationContext == nil || in.Dimensions == nil || in.RuleResults == nil {
		return nil, fmt.Errorf("%w: Dimensions, Score, RuleResults and EvaluationContext are required", ErrValidation)
	}

	id, err := h.Backend.PublishDataQualityResultV2(DataQualityResultV2{
		Score:                *in.Score,
		EvaluationContext:    *in.EvaluationContext,
		Dimensions:           in.Dimensions,
		RuleResults:          in.RuleResults,
		AggregatedMetrics:    in.AggregatedMetrics,
		ExecutionStartedOn:   in.ExecutionStartedOn,
		ExecutionCompletedOn: in.ExecutionCompletedOn,
		Descriptors:          in.Descriptors,
		Tags:                 in.Tags,
		RulesetName:          in.RulesetName,
		ClientToken:          in.ClientToken,
		AnalyzerResults:      in.AnalyzerResults,
		Observations:         in.Observations,
	})
	if err != nil {
		return nil, err
	}

	return &publishDataQualityResultV2Output{ResultID: id}, nil
}

func wireDQResultV2(r *DataQualityResultV2) DataQualityResultV2 {
	out := *r
	out.ClientToken = ""

	return out
}

type batchGetDataQualityResultV2Input struct {
	ResultIDs []string `json:"ResultIds"`
}

type batchGetDataQualityResultV2Output struct {
	Results         []DataQualityResultV2 `json:"Results"`
	ResultsNotFound []string              `json:"ResultsNotFound"`
}

func (h *Handler) handleBatchGetDataQualityResultV2(
	_ context.Context,
	in *batchGetDataQualityResultV2Input,
) (*batchGetDataQualityResultV2Output, error) {
	if in.ResultIDs == nil {
		return nil, fmt.Errorf("%w: ResultIds is required", ErrValidation)
	}

	found, notFound := h.Backend.BatchGetDataQualityResultV2(in.ResultIDs)
	results := make([]DataQualityResultV2, 0, len(found))

	for _, r := range found {
		results = append(results, wireDQResultV2(r))
	}

	return &batchGetDataQualityResultV2Output{Results: results, ResultsNotFound: notFound}, nil
}

type getDataQualityResultV2Input struct {
	ResultID *string `json:"ResultId"`
}

func (h *Handler) handleGetDataQualityResultV2(
	_ context.Context,
	in *getDataQualityResultV2Input,
) (*DataQualityResultV2, error) {
	if in.ResultID == nil || *in.ResultID == "" {
		return nil, fmt.Errorf("%w: ResultId is required", ErrValidation)
	}

	found, _ := h.Backend.BatchGetDataQualityResultV2([]string{*in.ResultID})
	if len(found) == 0 {
		return nil, awserr.New(errEntityNotFoundCode, awserr.ErrNotFound)
	}

	out := wireDQResultV2(found[0])

	return &out, nil
}

type dataQualityResultsV2FilterInput struct {
	DataSource        map[string]any     `json:"DataSource"`
	Dimensions        map[string]string  `json:"Dimensions"`
	PublishedAfter    *awstime.EpochTime `json:"PublishedAfter"`
	PublishedBefore   *awstime.EpochTime `json:"PublishedBefore"`
	EvaluationContext string             `json:"EvaluationContext"`
}

type listDataQualityResultsV2Input struct {
	Filter     *dataQualityResultsV2FilterInput `json:"Filter"`
	NextToken  string                           `json:"NextToken"`
	MaxResults int32                            `json:"MaxResults"`
}

type dataQualityResultsV2Description struct {
	ResultPublishedOn *awstime.EpochTime `json:"ResultPublishedOn,omitempty"`
	Descriptors       map[string]string  `json:"Descriptors,omitempty"`
	Dimensions        map[string]string  `json:"Dimensions,omitempty"`
	ResultID          string             `json:"ResultId"`
	EvaluationContext string             `json:"EvaluationContext,omitempty"`
}

type listDataQualityResultsV2Output struct {
	NextToken string                            `json:"NextToken,omitempty"`
	Results   []dataQualityResultsV2Description `json:"Results"`
}

func (f *dataQualityResultsV2FilterInput) toFilter() *DataQualityResultV2Filter {
	if f == nil {
		return nil
	}

	out := &DataQualityResultV2Filter{
		Dimensions:        f.Dimensions,
		EvaluationContext: f.EvaluationContext,
		HasDataSource:     len(f.DataSource) > 0,
	}

	if f.PublishedAfter != nil {
		t := f.PublishedAfter.Time
		out.PublishedAfter = &t
	}

	if f.PublishedBefore != nil {
		t := f.PublishedBefore.Time
		out.PublishedBefore = &t
	}

	return out
}

func (h *Handler) handleListDataQualityResultsV2(
	_ context.Context,
	in *listDataQualityResultsV2Input,
) (*listDataQualityResultsV2Output, error) {
	all := h.Backend.ListDataQualityResultsV2(in.Filter.toFilter())

	page, next, err := pagedSlice(all, in.NextToken, in.MaxResults, defaultListDataQualityResultsLimit)
	if err != nil {
		return nil, err
	}

	list := make([]dataQualityResultsV2Description, 0, len(page))

	for _, r := range page {
		list = append(list, dataQualityResultsV2Description{
			ResultID:          r.ResultID,
			ResultPublishedOn: r.ResultPublishedOn,
			Descriptors:       r.Descriptors,
			Dimensions:        r.Dimensions,
			EvaluationContext: r.EvaluationContext,
		})
	}

	return &listDataQualityResultsV2Output{Results: list, NextToken: next}, nil
}
