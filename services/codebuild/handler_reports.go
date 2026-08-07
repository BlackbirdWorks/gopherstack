package codebuild

import (
	"context"
	"fmt"
)

type createReportGroupInput struct {
	Tags         map[string]string  `json:"tags"`
	ExportConfig ReportExportConfig `json:"exportConfig"`
	Name         string             `json:"name"`
	Type         string             `json:"type"`
}

type createReportGroupOutput struct {
	ReportGroup *ReportGroup `json:"reportGroup"`
}

func (h *Handler) handleCreateReportGroup(
	_ context.Context,
	in *createReportGroupInput,
) (*createReportGroupOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if in.Type == "" {
		return nil, fmt.Errorf("%w: type is required", errInvalidRequest)
	}

	rg, err := h.Backend.CreateReportGroup(in.Name, in.Type, in.ExportConfig, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createReportGroupOutput{ReportGroup: rg}, nil
}

type batchGetReportGroupsInput struct {
	ReportGroupArns []string `json:"reportGroupArns"`
}

type batchGetReportGroupsOutput struct {
	ReportGroups         []*ReportGroup `json:"reportGroups"`
	ReportGroupsNotFound []string       `json:"reportGroupsNotFound"`
}

func (h *Handler) handleBatchGetReportGroups(
	_ context.Context,
	in *batchGetReportGroupsInput,
) (*batchGetReportGroupsOutput, error) {
	found, notFound := h.Backend.BatchGetReportGroups(in.ReportGroupArns)

	return &batchGetReportGroupsOutput{
		ReportGroups:         found,
		ReportGroupsNotFound: notFound,
	}, nil
}

type batchGetReportsInput struct {
	ReportArns []string `json:"reportArns"`
}

type batchGetReportsOutput struct {
	Reports         []*Report `json:"reports"`
	ReportsNotFound []string  `json:"reportsNotFound"`
}

func (h *Handler) handleBatchGetReports(
	_ context.Context,
	in *batchGetReportsInput,
) (*batchGetReportsOutput, error) {
	found, notFound := h.Backend.BatchGetReports(in.ReportArns)

	return &batchGetReportsOutput{
		Reports:         found,
		ReportsNotFound: notFound,
	}, nil
}

type deleteReportInput struct {
	Arn string `json:"arn"`
}

type deleteReportOutput struct{}

func (h *Handler) handleDeleteReport(_ context.Context, in *deleteReportInput) (*deleteReportOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteReport(in.Arn); err != nil {
		return nil, err
	}

	return &deleteReportOutput{}, nil
}

type deleteReportGroupInput struct {
	Arn           string `json:"arn"`
	DeleteReports bool   `json:"deleteReports"`
}

type deleteReportGroupOutput struct{}

func (h *Handler) handleDeleteReportGroup(
	_ context.Context,
	in *deleteReportGroupInput,
) (*deleteReportGroupOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteReportGroup(in.Arn); err != nil {
		return nil, err
	}

	return &deleteReportGroupOutput{}, nil
}

type describeCodeCoveragesInput struct {
	ReportArn string `json:"reportArn"`
}

type describeCodeCoveragesOutput struct {
	CodeCoverages []map[string]any `json:"codeCoverages"`
}

func (h *Handler) handleDescribeCodeCoverages(
	_ context.Context,
	in *describeCodeCoveragesInput,
) (*describeCodeCoveragesOutput, error) {
	coverages, err := h.Backend.DescribeCodeCoverages(in.ReportArn)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(coverages))
	for _, c := range coverages {
		result = append(result, map[string]any{
			"filePath":       c.FilePath,
			"branchCoverage": c.BranchCoverage,
			"lineCoverage":   c.LineCoverage,
		})
	}

	return &describeCodeCoveragesOutput{CodeCoverages: result}, nil
}

type describeTestCasesInput struct {
	ReportArn string `json:"reportArn"`
}

type describeTestCasesOutput struct {
	TestCases []map[string]any `json:"testCases"`
}

func (h *Handler) handleDescribeTestCases(
	_ context.Context,
	in *describeTestCasesInput,
) (*describeTestCasesOutput, error) {
	cases, err := h.Backend.DescribeTestCases(in.ReportArn)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(cases))
	for _, tc := range cases {
		result = append(result, map[string]any{
			keyName:    tc.Name,
			"status":   tc.Status,
			"duration": tc.Duration,
		})
	}

	return &describeTestCasesOutput{TestCases: result}, nil
}

type getReportGroupTrendInput struct {
	ReportGroupArn string `json:"reportGroupArn"`
	TrendField     string `json:"trendField"`
}

type getReportGroupTrendOutput struct {
	Stats map[string]any `json:"stats"`
}

func (h *Handler) handleGetReportGroupTrend(
	_ context.Context,
	in *getReportGroupTrendInput,
) (*getReportGroupTrendOutput, error) {
	stats, err := h.Backend.GetReportGroupTrend(in.ReportGroupArn)
	if err != nil {
		return nil, err
	}

	return &getReportGroupTrendOutput{Stats: stats}, nil
}

type listReportGroupsInput struct {
	NextToken  string `json:"nextToken"`
	SortBy     string `json:"sortBy"`
	SortOrder  string `json:"sortOrder"`
	MaxResults int32  `json:"maxResults"`
}

type listReportGroupsOutput struct {
	NextToken    string   `json:"nextToken,omitempty"`
	ReportGroups []string `json:"reportGroups"`
}

func (h *Handler) handleListReportGroups(
	_ context.Context,
	in *listReportGroupsInput,
) (*listReportGroupsOutput, error) {
	arns := h.Backend.ListReportGroupsSortedBy(in.SortBy)

	pg, err := paginateIDs(arns, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listReportGroupsOutput{ReportGroups: pg.Data, NextToken: pg.Next}, nil
}

// reportFilter mirrors the wire shape of the real SDK's ReportFilter (a
// single optional status field).
type reportFilter struct {
	Status string `json:"status"`
}

type listReportsInput struct {
	Filter     *reportFilter `json:"filter"`
	NextToken  string        `json:"nextToken"`
	SortOrder  string        `json:"sortOrder"`
	MaxResults int32         `json:"maxResults"`
}

type listReportsOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	Reports   []string `json:"reports"`
}

func (h *Handler) handleListReports(_ context.Context, in *listReportsInput) (*listReportsOutput, error) {
	var status string
	if in.Filter != nil {
		status = in.Filter.Status
	}

	arns := h.Backend.ListReports(status)

	pg, err := paginateIDs(arns, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listReportsOutput{Reports: pg.Data, NextToken: pg.Next}, nil
}

type listReportsForReportGroupInput struct {
	Filter         *reportFilter `json:"filter"`
	ReportGroupArn string        `json:"reportGroupArn"`
	NextToken      string        `json:"nextToken"`
	SortOrder      string        `json:"sortOrder"`
	MaxResults     int32         `json:"maxResults"`
}

type listReportsForReportGroupOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	Reports   []string `json:"reports"`
}

func (h *Handler) handleListReportsForReportGroup(
	_ context.Context,
	in *listReportsForReportGroupInput,
) (*listReportsForReportGroupOutput, error) {
	var status string
	if in.Filter != nil {
		status = in.Filter.Status
	}

	arns := h.Backend.ListReportsForReportGroup(in.ReportGroupArn, status)

	pg, err := paginateIDs(arns, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listReportsForReportGroupOutput{Reports: pg.Data, NextToken: pg.Next}, nil
}

type listSharedReportGroupsInput struct{}

type listSharedReportGroupsOutput struct {
	ReportGroups []string `json:"reportGroups"`
}

func (h *Handler) handleListSharedReportGroups(
	_ context.Context,
	_ *listSharedReportGroupsInput,
) (*listSharedReportGroupsOutput, error) {
	return &listSharedReportGroupsOutput{ReportGroups: h.Backend.ListSharedReportGroups()}, nil
}

type updateReportGroupInput struct {
	ExportConfig *ReportExportConfig `json:"exportConfig,omitempty"`
	Arn          string              `json:"arn"`
}

type updateReportGroupOutput struct {
	ReportGroup *ReportGroup `json:"reportGroup"`
}

func (h *Handler) handleUpdateReportGroup(
	_ context.Context,
	in *updateReportGroupInput,
) (*updateReportGroupOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", errInvalidRequest)
	}

	rg, err := h.Backend.UpdateReportGroup(in.Arn, in.ExportConfig)
	if err != nil {
		return nil, err
	}

	return &updateReportGroupOutput{ReportGroup: rg}, nil
}
