package glue

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

type batchGetDataQualityResultInput struct {
	ResultIDs []string `json:"ResultIds"`
}

type batchGetDataQualityResultOutput struct {
	Results         []DataQualityResult `json:"Results"`
	ResultsNotFound []string            `json:"ResultsNotFound"`
}

func (h *Handler) handleBatchGetDataQualityResult(
	_ context.Context,
	in *batchGetDataQualityResultInput,
) (*batchGetDataQualityResultOutput, error) {
	found, notFound := h.Backend.BatchGetDataQualityResult(in.ResultIDs)
	results := make([]DataQualityResult, 0, len(found))

	for _, r := range found {
		results = append(results, *r)
	}

	return &batchGetDataQualityResultOutput{Results: results, ResultsNotFound: notFound}, nil
}

// datapointInclusionAnnotation holds one annotation entry for
// BatchPutDataQualityStatisticAnnotation.
type datapointInclusionAnnotation struct {
	InclusionAnnotation string `json:"InclusionAnnotation,omitempty"`
	ProfileID           string `json:"ProfileId"`
	StatisticID         string `json:"StatisticId"`
}

// batchPutDataQualityStatisticAnnotationInput holds input for BatchPutDataQualityStatisticAnnotation.
type batchPutDataQualityStatisticAnnotationInput struct {
	ClientToken          string                         `json:"ClientToken,omitempty"`
	InclusionAnnotations []datapointInclusionAnnotation `json:"InclusionAnnotations"`
}

// annotationError holds a single failed annotation entry.
type annotationError struct {
	FailureReason string `json:"FailureReason,omitempty"`
	ProfileID     string `json:"ProfileId,omitempty"`
	StatisticID   string `json:"StatisticId,omitempty"`
}

// batchPutDataQualityStatisticAnnotationOutput holds the result for BatchPutDataQualityStatisticAnnotation.
type batchPutDataQualityStatisticAnnotationOutput struct {
	FailedInclusionAnnotations []annotationError `json:"FailedInclusionAnnotations"`
}

func (h *Handler) handleBatchPutDataQualityStatisticAnnotation(
	_ context.Context,
	in *batchPutDataQualityStatisticAnnotationInput,
) (*batchPutDataQualityStatisticAnnotationOutput, error) {
	if len(in.InclusionAnnotations) == 0 {
		return nil, fmt.Errorf("%w: InclusionAnnotations is required", ErrValidation)
	}

	failed := make([]annotationError, 0)

	for _, ann := range in.InclusionAnnotations {
		if ann.ProfileID == "" || ann.StatisticID == "" {
			failed = append(failed, annotationError{
				FailureReason: "ProfileId and StatisticId are required",
				ProfileID:     ann.ProfileID,
				StatisticID:   ann.StatisticID,
			})

			continue
		}

		h.Backend.PutDataQualityStatisticAnnotation(ann.ProfileID, ann.StatisticID, ann.InclusionAnnotation)
	}

	return &batchPutDataQualityStatisticAnnotationOutput{FailedInclusionAnnotations: failed}, nil
}

// getDataQualityModelInput holds input for GetDataQualityModel.
type getDataQualityModelInput struct {
	ProfileID   string `json:"ProfileId"`
	StatisticID string `json:"StatisticId,omitempty"`
}

// getDataQualityModelOutput holds the result for GetDataQualityModel.
type getDataQualityModelOutput struct {
	FailureReason string  `json:"FailureReason,omitempty"`
	Status        string  `json:"Status,omitempty"`
	StartedOn     float64 `json:"StartedOn,omitempty"`
	CompletedOn   float64 `json:"CompletedOn,omitempty"`
}

// handleGetDataQualityModel reports training status for a data-quality
// anomaly-detection model. Glue only ever creates these models via its own
// automated monitoring jobs (there is no public Create/Start API for them), so
// this emulator has no backing registry to check against; it validates the
// required ProfileId and reports the model as trained, matching the terminal
// state a real, already-monitored profile settles into.
func (h *Handler) handleGetDataQualityModel(
	_ context.Context,
	in *getDataQualityModelInput,
) (*getDataQualityModelOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", ErrValidation)
	}

	now := float64(time.Now().Unix())

	return &getDataQualityModelOutput{Status: stateSucceeded, StartedOn: now, CompletedOn: now}, nil
}

// getDataQualityModelResultInput holds input for GetDataQualityModelResult.
type getDataQualityModelResultInput struct {
	ProfileID   string `json:"ProfileId"`
	StatisticID string `json:"StatisticId"`
}

// getDataQualityModelResultOutput holds the result for GetDataQualityModelResult.
type getDataQualityModelResultOutput struct {
	Model       []any   `json:"Model"`
	CompletedOn float64 `json:"CompletedOn,omitempty"`
}

// handleGetDataQualityModelResult returns a statistic's model predictions for a
// profile. As with GetDataQualityModel, there is no public API to create these
// models, so no profile ever has computed predictions in this emulator; an
// empty Model list is the correct response for that case.
func (h *Handler) handleGetDataQualityModelResult(
	_ context.Context,
	in *getDataQualityModelResultInput,
) (*getDataQualityModelResultOutput, error) {
	if in.ProfileID == "" || in.StatisticID == "" {
		return nil, fmt.Errorf("%w: ProfileId and StatisticId are required", ErrValidation)
	}

	return &getDataQualityModelResultOutput{Model: []any{}}, nil
}

// getDataQualityResultInput holds input for GetDataQualityResult.
type getDataQualityResultInput struct {
	ResultID string `json:"ResultId"`
}

// getDataQualityResultOutput holds the result for GetDataQualityResult.
type getDataQualityResultOutput struct {
	ResultID string  `json:"ResultId"`
	Score    float64 `json:"Score"`
}

func (h *Handler) handleGetDataQualityResult(
	_ context.Context,
	in *getDataQualityResultInput,
) (*getDataQualityResultOutput, error) {
	found, errs := h.Backend.BatchGetDataQualityResult([]string{in.ResultID})
	if len(errs) > 0 {
		return nil, awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	}

	return &getDataQualityResultOutput{ResultID: found[0].ResultID, Score: found[0].Score}, nil
}

// defaultListDataQualityResultsLimit is used when
// ListDataQualityResultsInput.MaxResults is unset.
const defaultListDataQualityResultsLimit = 100

// listDataQualityResultsInput holds input for ListDataQualityResults.
//
// Filter is not modeled: DataQualityResult (models.go) stores only ResultID
// and Score -- none of DataSource, JobName, JobRunId, StartedAfter or
// StartedBefore have a real field on the stored entity to compare against, so
// the whole filter is accepted on the wire and left inert rather than
// fabricating a match. Only MaxResults/NextToken are wired.
type listDataQualityResultsInput struct {
	Filter     any    `json:"Filter,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// listDataQualityResultsOutput holds the result for ListDataQualityResults.
type listDataQualityResultsOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Results   []any  `json:"Results"`
}

func (h *Handler) handleListDataQualityResults(
	_ context.Context,
	in *listDataQualityResultsInput,
) (*listDataQualityResultsOutput, error) {
	all := h.Backend.ListDataQualityResults()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListDataQualityResultsLimit
	}

	page, next := paginateSlice(all, in.NextToken, limit)

	list := make([]any, 0, len(page))

	for _, r := range page {
		list = append(list, r)
	}

	return &listDataQualityResultsOutput{Results: list, NextToken: next}, nil
}

// listDataQualityStatisticAnnotationsInput holds input for ListDataQualityStatisticAnnotations.
type listDataQualityStatisticAnnotationsInput struct {
	ProfileID   string `json:"ProfileId,omitempty"`
	StatisticID string `json:"StatisticId,omitempty"`
}

// timestampedInclusionAnnotation is the annotation value with its last-modified time.
type timestampedInclusionAnnotation struct {
	Value          string  `json:"Value,omitempty"`
	LastModifiedOn float64 `json:"LastModifiedOn,omitempty"`
}

// statisticAnnotationOut is a single annotated statistic entry.
type statisticAnnotationOut struct {
	InclusionAnnotation *timestampedInclusionAnnotation `json:"InclusionAnnotation,omitempty"`
	ProfileID           string                          `json:"ProfileId,omitempty"`
	StatisticID         string                          `json:"StatisticId,omitempty"`
	StatisticRecordedOn float64                         `json:"StatisticRecordedOn,omitempty"`
}

// listDataQualityStatisticAnnotationsOutput holds the result for ListDataQualityStatisticAnnotations.
type listDataQualityStatisticAnnotationsOutput struct {
	NextToken   string                   `json:"NextToken,omitempty"`
	Annotations []statisticAnnotationOut `json:"Annotations"`
}

func (h *Handler) handleListDataQualityStatisticAnnotations(
	_ context.Context,
	in *listDataQualityStatisticAnnotationsInput,
) (*listDataQualityStatisticAnnotationsOutput, error) {
	entries := h.Backend.ListDataQualityStatisticAnnotations(in.ProfileID, in.StatisticID)
	out := make([]statisticAnnotationOut, 0, len(entries))

	for _, e := range entries {
		out = append(out, statisticAnnotationOut{
			ProfileID:           e.ProfileID,
			StatisticID:         e.StatisticID,
			StatisticRecordedOn: e.RecordedOn,
			InclusionAnnotation: &timestampedInclusionAnnotation{
				Value:          e.Inclusion,
				LastModifiedOn: e.LastModifiedOn,
			},
		})
	}

	return &listDataQualityStatisticAnnotationsOutput{Annotations: out}, nil
}

// listDataQualityStatisticsInput holds input for ListDataQualityStatistics.
type listDataQualityStatisticsInput struct {
	ProfileID   string `json:"ProfileId,omitempty"`
	StatisticID string `json:"StatisticId,omitempty"`
}

// listDataQualityStatisticsOutput holds the result for ListDataQualityStatistics.
type listDataQualityStatisticsOutput struct {
	NextToken  string `json:"NextToken,omitempty"`
	Statistics []any  `json:"Statistics"`
}

// handleListDataQualityStatistics always returns an empty list: Glue only ever
// populates statistics via its own automated data-quality monitoring jobs, which
// this emulator does not run, so no profile ever has computed statistics. This
// mirrors real AWS behavior for a profile/account that never enabled monitoring.
func (h *Handler) handleListDataQualityStatistics(
	_ context.Context,
	_ *listDataQualityStatisticsInput,
) (*listDataQualityStatisticsOutput, error) {
	return &listDataQualityStatisticsOutput{Statistics: []any{}}, nil
}

// putDataQualityProfileAnnotationInput holds input for PutDataQualityProfileAnnotation.
type putDataQualityProfileAnnotationInput struct {
	InclusionAnnotation string `json:"InclusionAnnotation,omitempty"`
	ProfileID           string `json:"ProfileId"`
}

func (h *Handler) handlePutDataQualityProfileAnnotation(
	_ context.Context,
	in *putDataQualityProfileAnnotationInput,
) (*emptyOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", ErrValidation)
	}

	if in.InclusionAnnotation == "" {
		return nil, fmt.Errorf("%w: InclusionAnnotation is required", ErrValidation)
	}

	// A profile-wide annotation is stored against the profile with no statistic ID,
	// distinguishing it from per-statistic annotations set via
	// BatchPutDataQualityStatisticAnnotation.
	h.Backend.PutDataQualityStatisticAnnotation(in.ProfileID, "", in.InclusionAnnotation)

	return &emptyOutput{}, nil
}
