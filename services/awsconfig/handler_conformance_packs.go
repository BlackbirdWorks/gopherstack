package awsconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for conformance pack ops.
const (
	opDeleteConformancePack                         = "DeleteConformancePack"
	opDescribeAggregateComplianceByConformancePacks = "DescribeAggregateComplianceByConformancePacks"
	opDescribeConformancePackCompliance             = "DescribeConformancePackCompliance"
	opDescribeConformancePackStatus                 = "DescribeConformancePackStatus"
	opDescribeConformancePacks                      = "DescribeConformancePacks"
	opGetAggregateConformancePackComplianceSummary  = "GetAggregateConformancePackComplianceSummary"
	opGetConformancePackComplianceDetails           = "GetConformancePackComplianceDetails"
	opGetConformancePackComplianceSummary           = "GetConformancePackComplianceSummary"
	opListConformancePackComplianceScores           = "ListConformancePackComplianceScores"
	opPutConformancePack                            = "PutConformancePack"
)

// conformancePackSupportedOps returns the operation names this family handles.
func conformancePackSupportedOps() []string {
	return []string{
		opDescribeConformancePacks,
		opDeleteConformancePack,
		opPutConformancePack,
		opDescribeConformancePackStatus,
		opDescribeConformancePackCompliance,
		opGetConformancePackComplianceDetails,
		opGetConformancePackComplianceSummary,
		opGetAggregateConformancePackComplianceSummary,
		opListConformancePackComplianceScores,
		opDescribeAggregateComplianceByConformancePacks,
	}
}

// DescribeConformancePacks request/response types and handler.
type describeConformancePacksOutput struct {
	ConformancePackDetails []ConformancePack `json:"ConformancePackDetails"`
}

func (h *Handler) handleDescribeConformancePacks(
	_ context.Context, _ *emptyInput,
) (*describeConformancePacksOutput, error) {
	return &describeConformancePacksOutput{
		ConformancePackDetails: h.Backend.DescribeConformancePacks(),
	}, nil
}

// DeleteConformancePack request/response types and handler.
type deleteConformancePackInput struct {
	ConformancePackName string `json:"ConformancePackName"`
}

type deleteConformancePackOutput struct{}

func (h *Handler) handleDeleteConformancePack(
	_ context.Context,
	in *deleteConformancePackInput,
) (*deleteConformancePackOutput, error) {
	if err := h.Backend.DeleteConformancePack(in.ConformancePackName); err != nil {
		return nil, err
	}

	return &deleteConformancePackOutput{}, nil
}

// PutConformancePack request/response types and handler.
type putConformancePackInput struct {
	ConformancePackName string `json:"ConformancePackName"`
	DeliveryS3Bucket    string `json:"DeliveryS3Bucket,omitempty"`
	DeliveryS3KeyPrefix string `json:"DeliveryS3KeyPrefix,omitempty"`
}

func (h *Handler) handlePutConformancePack(
	_ context.Context, in *putConformancePackInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutConformancePack(
		in.ConformancePackName,
		in.DeliveryS3Bucket,
		in.DeliveryS3KeyPrefix,
	)
}

// DescribeConformancePackStatus request/response types and handler.
type describeConformancePackStatusInput struct {
	ConformancePackNames []string `json:"ConformancePackNames"`
}
type describeConformancePackStatusOutput struct {
	ConformancePackStatusDetails []ConformancePackStatus `json:"ConformancePackStatusDetails"`
}

func (h *Handler) handleDescribeConformancePackStatus(
	_ context.Context, in *describeConformancePackStatusInput,
) (*describeConformancePackStatusOutput, error) {
	return &describeConformancePackStatusOutput{
		ConformancePackStatusDetails: h.Backend.DescribeConformancePackStatus(in.ConformancePackNames),
	}, nil
}

// DescribeConformancePackCompliance request/response types and handler.
type describeConformancePackComplianceInput struct {
	ConformancePackName string `json:"ConformancePackName"`
}
type describeConformancePackComplianceOutput struct {
	ConformancePackRuleComplianceList []ConformancePackComplianceItem `json:"ConformancePackRuleComplianceList"`
}

func (h *Handler) handleDescribeConformancePackCompliance(
	_ context.Context, in *describeConformancePackComplianceInput,
) (*describeConformancePackComplianceOutput, error) {
	return &describeConformancePackComplianceOutput{
		ConformancePackRuleComplianceList: h.Backend.DescribeConformancePackCompliance(in.ConformancePackName),
	}, nil
}

// GetConformancePackComplianceDetails request/response types and handler.
type getConformancePackComplianceDetailsOutput struct {
	ConformancePackRuleEvaluationResults []any `json:"ConformancePackRuleEvaluationResults"`
}

func (h *Handler) handleGetConformancePackComplianceDetails(
	_ context.Context, _ *emptyInput,
) (*getConformancePackComplianceDetailsOutput, error) {
	return &getConformancePackComplianceDetailsOutput{
		ConformancePackRuleEvaluationResults: h.Backend.GetConformancePackComplianceDetails(),
	}, nil
}

// GetConformancePackComplianceSummary request/response types and handler.
type getConformancePackComplianceSummaryOutput struct {
	ConformancePackComplianceSummaryList []any `json:"ConformancePackComplianceSummaryList"`
}

func (h *Handler) handleGetConformancePackComplianceSummary(
	_ context.Context, _ *emptyInput,
) (*getConformancePackComplianceSummaryOutput, error) {
	return &getConformancePackComplianceSummaryOutput{
		ConformancePackComplianceSummaryList: h.Backend.GetConformancePackComplianceSummary(),
	}, nil
}

// GetAggregateConformancePackComplianceSummary request/response types and handler.
type getAggregateConformancePackComplianceSummaryOutput struct {
	AggregateConformancePackComplianceSummaries []any `json:"AggregateConformancePackComplianceSummaries"`
}

func (h *Handler) handleGetAggregateConformancePackComplianceSummary(
	_ context.Context, _ *emptyInput,
) (*getAggregateConformancePackComplianceSummaryOutput, error) {
	return &getAggregateConformancePackComplianceSummaryOutput{
		AggregateConformancePackComplianceSummaries: h.Backend.GetAggregateConformancePackComplianceSummary(),
	}, nil
}

// ListConformancePackComplianceScores request/response types and handler.
type listConformancePackComplianceScoresOutput struct {
	ConformancePackComplianceScores []any `json:"ConformancePackComplianceScores"`
}

func (h *Handler) handleListConformancePackComplianceScores(
	_ context.Context, _ *emptyInput,
) (*listConformancePackComplianceScoresOutput, error) {
	return &listConformancePackComplianceScoresOutput{
		ConformancePackComplianceScores: h.Backend.ListConformancePackComplianceScores(),
	}, nil
}

// DescribeAggregateComplianceByConformancePacks request/response types and handler.
type describeAggregateComplianceByConformancePacksOutput struct {
	AggregateComplianceByConformancePacks []any `json:"AggregateComplianceByConformancePacks"`
}

func (h *Handler) handleDescribeAggregateComplianceByConformancePacks(
	_ context.Context, _ *emptyInput,
) (*describeAggregateComplianceByConformancePacksOutput, error) {
	return &describeAggregateComplianceByConformancePacksOutput{
		AggregateComplianceByConformancePacks: h.Backend.DescribeAggregateComplianceByConformancePacks(),
	}, nil
}

// buildConformancePackDispatch returns dispatch entries for conformance pack ops.
func (h *Handler) buildConformancePackDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDescribeConformancePacks:          service.WrapOp(h.handleDescribeConformancePacks),
		opDeleteConformancePack:             service.WrapOp(h.handleDeleteConformancePack),
		opPutConformancePack:                service.WrapOp(h.handlePutConformancePack),
		opDescribeConformancePackStatus:     service.WrapOp(h.handleDescribeConformancePackStatus),
		opDescribeConformancePackCompliance: service.WrapOp(h.handleDescribeConformancePackCompliance),
		opGetConformancePackComplianceDetails: service.WrapOp(
			h.handleGetConformancePackComplianceDetails,
		),
		opGetConformancePackComplianceSummary: service.WrapOp(
			h.handleGetConformancePackComplianceSummary,
		),
		opGetAggregateConformancePackComplianceSummary: service.WrapOp(
			h.handleGetAggregateConformancePackComplianceSummary,
		),
		opListConformancePackComplianceScores: service.WrapOp(
			h.handleListConformancePackComplianceScores,
		),
		opDescribeAggregateComplianceByConformancePacks: service.WrapOp(
			h.handleDescribeAggregateComplianceByConformancePacks,
		),
	}
}
