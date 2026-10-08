package awsconfig

import (
	"context"
	"fmt"
	"slices"
	"strings"

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
type describeConformancePacksInput struct {
	NextToken            string   `json:"NextToken,omitempty"`
	ConformancePackNames []string `json:"ConformancePackNames,omitempty"`
	Limit                int32    `json:"Limit,omitempty"`
}
type describeConformancePacksOutput struct {
	NextToken              string            `json:"NextToken,omitempty"`
	ConformancePackDetails []ConformancePack `json:"ConformancePackDetails"`
}

func (h *Handler) handleDescribeConformancePacks(
	_ context.Context, in *describeConformancePacksInput,
) (*describeConformancePacksOutput, error) {
	packs := h.Backend.DescribeConformancePacks()
	if len(in.ConformancePackNames) > 0 {
		for _, n := range in.ConformancePackNames {
			if !slices.ContainsFunc(packs, func(p ConformancePack) bool { return p.ConformancePackName == n }) {
				return nil, fmt.Errorf("%w: %s", ErrNoSuchConformancePack, n)
			}
		}

		packs = slices.DeleteFunc(packs, func(p ConformancePack) bool {
			return !slices.Contains(in.ConformancePackNames, p.ConformancePackName)
		})
	}

	slices.SortFunc(
		packs,
		func(a, b ConformancePack) int { return strings.Compare(a.ConformancePackName, b.ConformancePackName) },
	)

	p, err := paginate(packs, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeConformancePacksOutput{ConformancePackDetails: p.Data, NextToken: p.Next}, nil
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

	Parameters []ConformancePackInputParameter `json:"ConformancePackInputParameters,omitempty"`

	TemplateBody  string `json:"TemplateBody,omitempty"`
	TemplateS3Uri string `json:"TemplateS3Uri,omitempty"`
	Tags          []Tag  `json:"Tags,omitempty"`
}

type putConformancePackOutput struct {
	ConformancePackArn string `json:"ConformancePackArn"`
}

func (h *Handler) handlePutConformancePack(
	ctx context.Context, in *putConformancePackInput,
) (*putConformancePackOutput, error) {
	ssmDocName, ssmDocVersion := "", ""
	if in.TemplateSSMDocumentDetails != nil {
		ssmDocName = in.TemplateSSMDocumentDetails.DocumentName
		ssmDocVersion = in.TemplateSSMDocumentDetails.DocumentVersion
	}

	if err := validateSingleTemplateSource(in.TemplateBody, in.TemplateS3Uri, ssmDocName); err != nil {
		return nil, err
	}

	if in.TemplateBody == "" {
		body, err := h.Backend.ResolveConformancePackTemplate(ctx, in.TemplateS3Uri, ssmDocName, ssmDocVersion)
		if err != nil {
			return nil, err
		}

		if body != "" {
			in.TemplateBody, in.TemplateS3Uri, ssmDocName = body, "", ""
		}
	}

	arn, err := h.Backend.PutConformancePackWithParams(
		in.ConformancePackName,
		in.DeliveryS3Bucket,
		in.DeliveryS3KeyPrefix,
		in.TemplateBody,
		in.TemplateS3Uri,
		ssmDocName,
		in.Tags,
		in.Parameters,
	)
	if err != nil {
		return nil, err
	}

	return &putConformancePackOutput{ConformancePackArn: arn}, nil
}

// DescribeConformancePackStatus request/response types and handler.
type describeConformancePackStatusInput struct {
	NextToken            string   `json:"NextToken,omitempty"`
	ConformancePackNames []string `json:"ConformancePackNames"`
	Limit                int32    `json:"Limit,omitempty"`
}
type describeConformancePackStatusOutput struct {
	NextToken                    string                  `json:"NextToken,omitempty"`
	ConformancePackStatusDetails []ConformancePackStatus `json:"ConformancePackStatusDetails"`
}

func (h *Handler) handleDescribeConformancePackStatus(
	_ context.Context, in *describeConformancePackStatusInput,
) (*describeConformancePackStatusOutput, error) {
	statuses := h.Backend.DescribeConformancePackStatus(in.ConformancePackNames)
	slices.SortFunc(statuses, func(a, b ConformancePackStatus) int {
		return strings.Compare(a.ConformancePackName, b.ConformancePackName)
	})

	p, err := paginate(statuses, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeConformancePackStatusOutput{ConformancePackStatusDetails: p.Data, NextToken: p.Next}, nil
}

// DescribeConformancePackCompliance request/response types and handler.
type describeConformancePackComplianceFiltersBody struct {
	ComplianceType  string   `json:"ComplianceType,omitempty"`
	ConfigRuleNames []string `json:"ConfigRuleNames,omitempty"`
}
type describeConformancePackComplianceInput struct {
	Filters             *describeConformancePackComplianceFiltersBody `json:"Filters,omitempty"`
	ConformancePackName string                                        `json:"ConformancePackName"`
	NextToken           string                                        `json:"NextToken,omitempty"`
	Limit               int32                                         `json:"Limit,omitempty"`
}
type describeConformancePackComplianceOutput struct {
	NextToken                         string                          `json:"NextToken,omitempty"`
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

	p, err := paginate(items, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeConformancePackComplianceOutput{
		ConformancePackName:               in.ConformancePackName,
		ConformancePackRuleComplianceList: p.Data,
		NextToken:                         p.Next,
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
	NextToken           string                                          `json:"NextToken,omitempty"`
	Limit               int32                                           `json:"Limit,omitempty"`
}
type getConformancePackComplianceDetailsOutput struct {
	ConformancePackName                  string                     `json:"ConformancePackName"`
	NextToken                            string                     `json:"NextToken,omitempty"`
	ConformancePackRuleEvaluationResults []DetailedEvaluationResult `json:"ConformancePackRuleEvaluationResults"`
}

// getConformancePackComplianceDetailsPageDefault is the documented default
// page size (api_op_GetConformancePackComplianceDetails.go: "If you do no
// specify a number, Config uses the default. The default is 100.").
const getConformancePackComplianceDetailsPageDefault = 100

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

	p, err := paginate(results, in.NextToken, in.Limit, getConformancePackComplianceDetailsPageDefault)
	if err != nil {
		return nil, err
	}

	return &getConformancePackComplianceDetailsOutput{
		ConformancePackName:                  in.ConformancePackName,
		ConformancePackRuleEvaluationResults: p.Data,
		NextToken:                            p.Next,
	}, nil
}

// GetConformancePackComplianceSummary request/response types and handler.
type getConformancePackComplianceSummaryInput struct {
	NextToken            string   `json:"NextToken,omitempty"`
	ConformancePackNames []string `json:"ConformancePackNames"`
	Limit                int32    `json:"Limit,omitempty"`
}
type getConformancePackComplianceSummaryOutput struct {
	NextToken string                                  `json:"NextToken,omitempty"`
	Summaries []ConformancePackComplianceSummaryEntry `json:"ConformancePackComplianceSummaryList"`
}

func (h *Handler) handleGetConformancePackComplianceSummary(
	_ context.Context, in *getConformancePackComplianceSummaryInput,
) (*getConformancePackComplianceSummaryOutput, error) {
	summaries, err := h.Backend.GetConformancePackComplianceSummary(in.ConformancePackNames)
	if err != nil {
		return nil, err
	}

	p, err := paginate(summaries, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &getConformancePackComplianceSummaryOutput{Summaries: p.Data, NextToken: p.Next}, nil
}

// GetAggregateConformancePackComplianceSummary request/response types and
// handler. Real GetAggregateConformancePackComplianceSummaryOutput echoes
// the request's GroupByKey (api_op_GetAggregateConformancePackComplianceSummary.go)
// -- this was never emitted at all.
type getAggregateConformancePackComplianceSummaryInput struct {
	Filters                     *aggregateScopeFilters `json:"Filters,omitempty"`
	ConfigurationAggregatorName string                 `json:"ConfigurationAggregatorName"`
	GroupByKey                  string                 `json:"GroupByKey,omitempty"`
	NextToken                   string                 `json:"NextToken,omitempty"`
	Limit                       int32                  `json:"Limit,omitempty"`
}
type getAggregateConformancePackComplianceSummaryOutput struct {
	GroupByKey string                                      `json:"GroupByKey,omitempty"`
	NextToken  string                                      `json:"NextToken,omitempty"`
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

	if !h.Backend.aggregateScopeMatches(in.Filters) {
		summaries = []AggregateConformancePackComplianceSummary{}
	}

	p, err := paginate(summaries, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &getAggregateConformancePackComplianceSummaryOutput{
		GroupByKey: in.GroupByKey,
		Summaries:  p.Data,
		NextToken:  p.Next,
	}, nil
}

// ListConformancePackComplianceScores request/response types and handler.
type listConformancePackComplianceScoresFiltersBody struct {
	ConformancePackNames []string `json:"ConformancePackNames"`
}
type listConformancePackComplianceScoresInput struct {
	Filters   *listConformancePackComplianceScoresFiltersBody `json:"Filters,omitempty"`
	SortBy    string                                          `json:"SortBy,omitempty"`
	SortOrder string                                          `json:"SortOrder,omitempty"`
	NextToken string                                          `json:"NextToken,omitempty"`
	Limit     int32                                           `json:"Limit,omitempty"`
}
type listConformancePackComplianceScoresOutput struct {
	NextToken                       string                                `json:"NextToken,omitempty"`
	ConformancePackComplianceScores []ConformancePackComplianceScoreEntry `json:"ConformancePackComplianceScores"`
}

func (h *Handler) handleListConformancePackComplianceScores(
	_ context.Context, in *listConformancePackComplianceScoresInput,
) (*listConformancePackComplianceScoresOutput, error) {
	var names []string
	if in.Filters != nil {
		names = in.Filters.ConformancePackNames
	}

	scores := h.Backend.ListConformancePackComplianceScores(names, in.SortBy, in.SortOrder)

	p, err := paginate(scores, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &listConformancePackComplianceScoresOutput{
		ConformancePackComplianceScores: p.Data,
		NextToken:                       p.Next,
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
	NextToken                   string                                  `json:"NextToken,omitempty"`
	Limit                       int32                                   `json:"Limit,omitempty"`
}
type describeAggregateComplianceByConformancePacksOutput struct {
	NextToken string                                 `json:"NextToken,omitempty"`
	Results   []AggregateComplianceByConformancePack `json:"AggregateComplianceByConformancePacks"`
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

	p, err := paginate(results, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeAggregateComplianceByConformancePacksOutput{Results: p.Data, NextToken: p.Next}, nil
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
