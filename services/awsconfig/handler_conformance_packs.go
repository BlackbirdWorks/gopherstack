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

// putConformancePackTemplateSSMDocumentDetails mirrors
// types.TemplateSSMDocumentDetails: the name/ARN of the SSM document (plus
// optional version) PutConformancePack uses as a template source.
type putConformancePackTemplateSSMDocumentDetails struct {
	DocumentName    string `json:"DocumentName"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
}

// PutConformancePack request/response types and handler.
type putConformancePackInput struct {
	TemplateSSMDocumentDetails *putConformancePackTemplateSSMDocumentDetails `json:"TemplateSSMDocumentDetails,omitempty"`
	ConformancePackName        string                                        `json:"ConformancePackName"`
	DeliveryS3Bucket           string                                        `json:"DeliveryS3Bucket,omitempty"`
	DeliveryS3KeyPrefix        string                                        `json:"DeliveryS3KeyPrefix,omitempty"`
	TemplateBody               string                                        `json:"TemplateBody,omitempty"`
	TemplateS3Uri              string                                        `json:"TemplateS3Uri,omitempty"`
	Tags                       []Tag                                         `json:"Tags,omitempty"`
}

func (h *Handler) handlePutConformancePack(
	_ context.Context, in *putConformancePackInput,
) (*emptyOutput, error) {
	ssmDocName := ""
	if in.TemplateSSMDocumentDetails != nil {
		ssmDocName = in.TemplateSSMDocumentDetails.DocumentName
	}

	return &emptyOutput{}, h.Backend.PutConformancePack(
		in.ConformancePackName,
		in.DeliveryS3Bucket,
		in.DeliveryS3KeyPrefix,
		in.TemplateBody,
		in.TemplateS3Uri,
		ssmDocName,
		in.Tags,
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
type describeConformancePackComplianceFiltersBody struct {
	ComplianceType  string   `json:"ComplianceType,omitempty"`
	ConfigRuleNames []string `json:"ConfigRuleNames,omitempty"`
}
type describeConformancePackComplianceInput struct {
	Filters             *describeConformancePackComplianceFiltersBody `json:"Filters,omitempty"`
	ConformancePackName string                                        `json:"ConformancePackName"`
}
type describeConformancePackComplianceOutput struct {
	ConformancePackName               string                          `json:"ConformancePackName"`
	ConformancePackRuleComplianceList []ConformancePackComplianceItem `json:"ConformancePackRuleComplianceList"`
}

func (h *Handler) handleDescribeConformancePackCompliance(
	_ context.Context, in *describeConformancePackComplianceInput,
) (*describeConformancePackComplianceOutput, error) {
	var ruleNames []string
	var complianceType string

	if in.Filters != nil {
		ruleNames = in.Filters.ConfigRuleNames
		complianceType = in.Filters.ComplianceType
	}

	items, err := h.Backend.DescribeConformancePackCompliance(in.ConformancePackName, ruleNames, complianceType)
	if err != nil {
		return nil, err
	}

	return &describeConformancePackComplianceOutput{
		ConformancePackName:               in.ConformancePackName,
		ConformancePackRuleComplianceList: items,
	}, nil
}

// GetConformancePackComplianceDetails request/response types and handler.
type getConformancePackComplianceDetailsFiltersBody struct {
	ComplianceType  string   `json:"ComplianceType,omitempty"`
	ResourceType    string   `json:"ResourceType,omitempty"`
	ConfigRuleNames []string `json:"ConfigRuleNames,omitempty"`
	ResourceIDs     []string `json:"ResourceIds,omitempty"`
}
type getConformancePackComplianceDetailsInput struct {
	Filters             *getConformancePackComplianceDetailsFiltersBody `json:"Filters,omitempty"`
	ConformancePackName string                                          `json:"ConformancePackName"`
}
type getConformancePackComplianceDetailsOutput struct {
	ConformancePackName                  string                     `json:"ConformancePackName"`
	ConformancePackRuleEvaluationResults []DetailedEvaluationResult `json:"ConformancePackRuleEvaluationResults"`
}

func (h *Handler) handleGetConformancePackComplianceDetails(
	_ context.Context, in *getConformancePackComplianceDetailsInput,
) (*getConformancePackComplianceDetailsOutput, error) {
	var ruleNames, resourceIDs []string
	var complianceType, resourceType string

	if in.Filters != nil {
		ruleNames = in.Filters.ConfigRuleNames
		complianceType = in.Filters.ComplianceType
		resourceType = in.Filters.ResourceType
		resourceIDs = in.Filters.ResourceIDs
	}

	results, err := h.Backend.GetConformancePackComplianceDetails(
		in.ConformancePackName, ruleNames, resourceType, resourceIDs, complianceType,
	)
	if err != nil {
		return nil, err
	}

	return &getConformancePackComplianceDetailsOutput{
		ConformancePackName:                  in.ConformancePackName,
		ConformancePackRuleEvaluationResults: results,
	}, nil
}

// GetConformancePackComplianceSummary request/response types and handler.
type getConformancePackComplianceSummaryInput struct {
	ConformancePackNames []string `json:"ConformancePackNames"`
}
type getConformancePackComplianceSummaryOutput struct {
	Summaries []ConformancePackComplianceSummaryEntry `json:"ConformancePackComplianceSummaryList"`
}

func (h *Handler) handleGetConformancePackComplianceSummary(
	_ context.Context, in *getConformancePackComplianceSummaryInput,
) (*getConformancePackComplianceSummaryOutput, error) {
	summaries, err := h.Backend.GetConformancePackComplianceSummary(in.ConformancePackNames)
	if err != nil {
		return nil, err
	}

	return &getConformancePackComplianceSummaryOutput{Summaries: summaries}, nil
}

// GetAggregateConformancePackComplianceSummary request/response types and
// handler. Real GetAggregateConformancePackComplianceSummaryOutput echoes
// the request's GroupByKey (api_op_GetAggregateConformancePackComplianceSummary.go)
// -- this was never emitted at all.
type getAggregateConformancePackComplianceSummaryInput struct {
	ConfigurationAggregatorName string `json:"ConfigurationAggregatorName"`
	GroupByKey                  string `json:"GroupByKey,omitempty"`
}
type getAggregateConformancePackComplianceSummaryOutput struct {
	GroupByKey string                                      `json:"GroupByKey,omitempty"`
	Summaries  []AggregateConformancePackComplianceSummary `json:"AggregateConformancePackComplianceSummaries"`
}

func (h *Handler) handleGetAggregateConformancePackComplianceSummary(
	_ context.Context, in *getAggregateConformancePackComplianceSummaryInput,
) (*getAggregateConformancePackComplianceSummaryOutput, error) {
	summaries, err := h.Backend.GetAggregateConformancePackComplianceSummary(
		in.ConfigurationAggregatorName, in.GroupByKey,
	)
	if err != nil {
		return nil, err
	}

	return &getAggregateConformancePackComplianceSummaryOutput{
		GroupByKey: in.GroupByKey,
		Summaries:  summaries,
	}, nil
}

// ListConformancePackComplianceScores request/response types and handler.
type listConformancePackComplianceScoresFiltersBody struct {
	ConformancePackNames []string `json:"ConformancePackNames"`
}
type listConformancePackComplianceScoresInput struct {
	Filters *listConformancePackComplianceScoresFiltersBody `json:"Filters,omitempty"`
}
type listConformancePackComplianceScoresOutput struct {
	ConformancePackComplianceScores []ConformancePackComplianceScoreEntry `json:"ConformancePackComplianceScores"`
}

func (h *Handler) handleListConformancePackComplianceScores(
	_ context.Context, in *listConformancePackComplianceScoresInput,
) (*listConformancePackComplianceScoresOutput, error) {
	var names []string
	if in.Filters != nil {
		names = in.Filters.ConformancePackNames
	}

	return &listConformancePackComplianceScoresOutput{
		ConformancePackComplianceScores: h.Backend.ListConformancePackComplianceScores(names),
	}, nil
}

// DescribeAggregateComplianceByConformancePacks request/response types and handler.
type aggComplianceByConformancePacksFilters struct {
	AccountID string `json:"AccountId,omitempty"`
	AwsRegion string `json:"AwsRegion,omitempty"`
}
type describeAggregateComplianceByConformancePacksInput struct {
	Filters                     *aggComplianceByConformancePacksFilters `json:"Filters,omitempty"`
	ConfigurationAggregatorName string                                  `json:"ConfigurationAggregatorName"`
}
type describeAggregateComplianceByConformancePacksOutput struct {
	Results []AggregateComplianceByConformancePack `json:"AggregateComplianceByConformancePacks"`
}

func (h *Handler) handleDescribeAggregateComplianceByConformancePacks(
	_ context.Context, in *describeAggregateComplianceByConformancePacksInput,
) (*describeAggregateComplianceByConformancePacksOutput, error) {
	var accountID, awsRegion string
	if in.Filters != nil {
		accountID = in.Filters.AccountID
		awsRegion = in.Filters.AwsRegion
	}

	results, err := h.Backend.DescribeAggregateComplianceByConformancePacks(
		in.ConfigurationAggregatorName, accountID, awsRegion,
	)
	if err != nil {
		return nil, err
	}

	return &describeAggregateComplianceByConformancePacksOutput{Results: results}, nil
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
