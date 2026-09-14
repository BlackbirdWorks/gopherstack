package awsconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// organizationFamilyPageDefault is the documented default page size shared by
// this family's Describe/Get ops (api_op_DescribeOrganizationConfigRules.go
// and siblings: "If you do no specify a number, Config uses the default. The
// default is 100.").
const organizationFamilyPageDefault = 100

// Operation name constants for organization config rule/pack ops.
const (
	opDeleteOrganizationConfigRule                 = "DeleteOrganizationConfigRule"
	opDeleteOrganizationConformancePack            = "DeleteOrganizationConformancePack"
	opDescribeOrganizationConfigRuleStatuses       = "DescribeOrganizationConfigRuleStatuses"
	opDescribeOrganizationConfigRules              = "DescribeOrganizationConfigRules"
	opDescribeOrganizationConformancePackStatuses  = "DescribeOrganizationConformancePackStatuses"
	opDescribeOrganizationConformancePacks         = "DescribeOrganizationConformancePacks"
	opGetOrganizationConfigRuleDetailedStatus      = "GetOrganizationConfigRuleDetailedStatus"
	opGetOrganizationConformancePackDetailedStatus = "GetOrganizationConformancePackDetailedStatus"
	opGetOrganizationCustomRulePolicy              = "GetOrganizationCustomRulePolicy"
	opPutOrganizationConfigRule                    = "PutOrganizationConfigRule"
	opPutOrganizationConformancePack               = "PutOrganizationConformancePack"
)

// organizationSupportedOps returns the operation names this family handles.
func organizationSupportedOps() []string {
	return []string{
		opDeleteOrganizationConfigRule,
		opDeleteOrganizationConformancePack,
		opPutOrganizationConfigRule,
		opPutOrganizationConformancePack,
		opDescribeOrganizationConfigRules,
		opDescribeOrganizationConformancePacks,
		opDescribeOrganizationConfigRuleStatuses,
		opDescribeOrganizationConformancePackStatuses,
		opGetOrganizationConfigRuleDetailedStatus,
		opGetOrganizationConformancePackDetailedStatus,
		opGetOrganizationCustomRulePolicy,
	}
}

// DeleteOrganizationConfigRule request/response types and handler.
type deleteOrganizationConfigRuleInput struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
}

type deleteOrganizationConfigRuleOutput struct{}

func (h *Handler) handleDeleteOrganizationConfigRule(
	_ context.Context,
	in *deleteOrganizationConfigRuleInput,
) (*deleteOrganizationConfigRuleOutput, error) {
	if err := h.Backend.DeleteOrganizationConfigRule(in.OrganizationConfigRuleName); err != nil {
		return nil, err
	}

	return &deleteOrganizationConfigRuleOutput{}, nil
}

// DeleteOrganizationConformancePack request/response types and handler.
type deleteOrganizationConformancePackInput struct {
	OrganizationConformancePackName string `json:"OrganizationConformancePackName"`
}

type deleteOrganizationConformancePackOutput struct{}

func (h *Handler) handleDeleteOrganizationConformancePack(
	_ context.Context,
	in *deleteOrganizationConformancePackInput,
) (*deleteOrganizationConformancePackOutput, error) {
	if err := h.Backend.DeleteOrganizationConformancePack(in.OrganizationConformancePackName); err != nil {
		return nil, err
	}

	return &deleteOrganizationConformancePackOutput{}, nil
}

// PutOrganizationConfigRule request/response types and handler.
type putOrganizationConfigRuleInput struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
}

type putOrganizationConfigRuleOutput struct {
	OrganizationConfigRuleArn string `json:"OrganizationConfigRuleArn"`
}

func (h *Handler) handlePutOrganizationConfigRule(
	_ context.Context, in *putOrganizationConfigRuleInput,
) (*putOrganizationConfigRuleOutput, error) {
	arnStr, err := h.Backend.PutOrganizationConfigRule(in.OrganizationConfigRuleName)
	if err != nil {
		return nil, err
	}

	return &putOrganizationConfigRuleOutput{OrganizationConfigRuleArn: arnStr}, nil
}

// PutOrganizationConformancePack request/response types and handler.
type putOrganizationConformancePackInput struct {
	OrganizationConformancePackName string `json:"OrganizationConformancePackName"`
}

func (h *Handler) handlePutOrganizationConformancePack(
	_ context.Context, in *putOrganizationConformancePackInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutOrganizationConformancePack(
		in.OrganizationConformancePackName,
	)
}

// DescribeOrganizationConfigRules request/response types and handler.
type describeOrganizationConfigRulesInput struct {
	NextToken string `json:"NextToken,omitempty"`
	Limit     int32  `json:"Limit,omitempty"`
}
type describeOrganizationConfigRulesOutput struct {
	NextToken               string                   `json:"NextToken,omitempty"`
	OrganizationConfigRules []OrganizationConfigRule `json:"OrganizationConfigRules"`
}

func (h *Handler) handleDescribeOrganizationConfigRules(
	_ context.Context, in *describeOrganizationConfigRulesInput,
) (*describeOrganizationConfigRulesOutput, error) {
	p, err := paginate(
		h.Backend.DescribeOrganizationConfigRules(), in.NextToken, in.Limit, organizationFamilyPageDefault,
	)
	if err != nil {
		return nil, err
	}

	return &describeOrganizationConfigRulesOutput{OrganizationConfigRules: p.Data, NextToken: p.Next}, nil
}

// DescribeOrganizationConformancePacks request/response types and handler.
type describeOrganizationConformancePacksInput struct {
	NextToken string `json:"NextToken,omitempty"`
	Limit     int32  `json:"Limit,omitempty"`
}
type describeOrganizationConformancePacksOutput struct {
	NextToken                    string                        `json:"NextToken,omitempty"`
	OrganizationConformancePacks []OrganizationConformancePack `json:"OrganizationConformancePacks"`
}

func (h *Handler) handleDescribeOrganizationConformancePacks(
	_ context.Context, in *describeOrganizationConformancePacksInput,
) (*describeOrganizationConformancePacksOutput, error) {
	p, err := paginate(
		h.Backend.DescribeOrganizationConformancePacks(), in.NextToken, in.Limit, organizationFamilyPageDefault,
	)
	if err != nil {
		return nil, err
	}

	return &describeOrganizationConformancePacksOutput{
		OrganizationConformancePacks: p.Data,
		NextToken:                    p.Next,
	}, nil
}

// DescribeOrganizationConfigRuleStatuses request/response types and handler.
type describeOrganizationConfigRuleStatusesInput struct {
	NextToken                   string   `json:"NextToken,omitempty"`
	OrganizationConfigRuleNames []string `json:"OrganizationConfigRuleNames"`
	Limit                       int32    `json:"Limit,omitempty"`
}
type describeOrganizationConfigRuleStatusesOutput struct {
	NextToken                      string                         `json:"NextToken,omitempty"`
	OrganizationConfigRuleStatuses []OrganizationConfigRuleStatus `json:"OrganizationConfigRuleStatuses"`
}

func (h *Handler) handleDescribeOrganizationConfigRuleStatuses(
	_ context.Context, in *describeOrganizationConfigRuleStatusesInput,
) (*describeOrganizationConfigRuleStatusesOutput, error) {
	all := h.Backend.DescribeOrganizationConfigRuleStatuses(in.OrganizationConfigRuleNames)

	p, err := paginate(all, in.NextToken, in.Limit, organizationFamilyPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeOrganizationConfigRuleStatusesOutput{
		OrganizationConfigRuleStatuses: p.Data,
		NextToken:                      p.Next,
	}, nil
}

// DescribeOrganizationConformancePackStatuses request/response types and handler.
type describeOrganizationConformancePackStatusesInput struct {
	NextToken                        string   `json:"NextToken,omitempty"`
	OrganizationConformancePackNames []string `json:"OrganizationConformancePackNames"`
	Limit                            int32    `json:"Limit,omitempty"`
}
type describeOrganizationConformancePackStatusesOutput struct {
	NextToken                           string                              `json:"NextToken,omitempty"`
	OrganizationConformancePackStatuses []OrganizationConformancePackStatus `json:"OrganizationConformancePackStatuses"`
}

func (h *Handler) handleDescribeOrganizationConformancePackStatuses(
	_ context.Context, in *describeOrganizationConformancePackStatusesInput,
) (*describeOrganizationConformancePackStatusesOutput, error) {
	all := h.Backend.DescribeOrganizationConformancePackStatuses(in.OrganizationConformancePackNames)

	p, err := paginate(all, in.NextToken, in.Limit, organizationFamilyPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeOrganizationConformancePackStatusesOutput{
		OrganizationConformancePackStatuses: p.Data,
		NextToken:                           p.Next,
	}, nil
}

// GetOrganizationConfigRuleDetailedStatus request/response types and handler.
type getOrganizationConfigRuleDetailedStatusFiltersBody struct {
	AccountID string `json:"AccountId,omitempty"`
}
type getOrganizationConfigRuleDetailedStatusInput struct {
	Filters                    *getOrganizationConfigRuleDetailedStatusFiltersBody `json:"Filters,omitempty"`
	OrganizationConfigRuleName string                                              `json:"OrganizationConfigRuleName"`
	NextToken                  string                                              `json:"NextToken,omitempty"`
	Limit                      int32                                               `json:"Limit,omitempty"`
}
type getOrganizationConfigRuleDetailedStatusOutput struct {
	NextToken                            string                `json:"NextToken,omitempty"`
	OrganizationConfigRuleDetailedStatus []MemberAccountStatus `json:"OrganizationConfigRuleDetailedStatus"`
}

func (h *Handler) handleGetOrganizationConfigRuleDetailedStatus(
	_ context.Context, in *getOrganizationConfigRuleDetailedStatusInput,
) (*getOrganizationConfigRuleDetailedStatusOutput, error) {
	var accountID string
	if in.Filters != nil {
		accountID = in.Filters.AccountID
	}

	statuses, err := h.Backend.GetOrganizationConfigRuleDetailedStatus(in.OrganizationConfigRuleName, accountID)
	if err != nil {
		return nil, err
	}

	p, err := paginate(statuses, in.NextToken, in.Limit, organizationFamilyPageDefault)
	if err != nil {
		return nil, err
	}

	return &getOrganizationConfigRuleDetailedStatusOutput{
		OrganizationConfigRuleDetailedStatus: p.Data,
		NextToken:                            p.Next,
	}, nil
}

// GetOrganizationConformancePackDetailedStatus request/response types and handler.
type orgConformancePackDetailedStatusFilters struct {
	AccountID string `json:"AccountId,omitempty"`
}
type getOrganizationConformancePackDetailedStatusInput struct {
	Filters                         *orgConformancePackDetailedStatusFilters `json:"Filters,omitempty"`
	OrganizationConformancePackName string                                   `json:"OrganizationConformancePackName"`
	NextToken                       string                                   `json:"NextToken,omitempty"`
	Limit                           int32                                    `json:"Limit,omitempty"`
}
type getOrganizationConformancePackDetailedStatusOutput struct {
	NextToken string                                      `json:"NextToken,omitempty"`
	Statuses  []OrganizationConformancePackDetailedStatus `json:"OrganizationConformancePackDetailedStatuses"`
}

func (h *Handler) handleGetOrganizationConformancePackDetailedStatus(
	_ context.Context, in *getOrganizationConformancePackDetailedStatusInput,
) (*getOrganizationConformancePackDetailedStatusOutput, error) {
	var accountID string
	if in.Filters != nil {
		accountID = in.Filters.AccountID
	}

	statuses, err := h.Backend.GetOrganizationConformancePackDetailedStatus(
		in.OrganizationConformancePackName, accountID,
	)
	if err != nil {
		return nil, err
	}

	p, err := paginate(statuses, in.NextToken, in.Limit, organizationFamilyPageDefault)
	if err != nil {
		return nil, err
	}

	return &getOrganizationConformancePackDetailedStatusOutput{Statuses: p.Data, NextToken: p.Next}, nil
}

// GetOrganizationCustomRulePolicy request/response types and handler.
type getOrganizationCustomRulePolicyInput struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
}
type getOrganizationCustomRulePolicyOutput struct {
	PolicyText string `json:"PolicyText"`
}

func (h *Handler) handleGetOrganizationCustomRulePolicy(
	_ context.Context, in *getOrganizationCustomRulePolicyInput,
) (*getOrganizationCustomRulePolicyOutput, error) {
	return &getOrganizationCustomRulePolicyOutput{
		PolicyText: h.Backend.GetOrganizationCustomRulePolicy(in.OrganizationConfigRuleName),
	}, nil
}

// buildOrganizationDispatch returns dispatch entries for organization ops.
func (h *Handler) buildOrganizationDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDeleteOrganizationConfigRule:      service.WrapOp(h.handleDeleteOrganizationConfigRule),
		opDeleteOrganizationConformancePack: service.WrapOp(h.handleDeleteOrganizationConformancePack),
		opPutOrganizationConfigRule:         service.WrapOp(h.handlePutOrganizationConfigRule),
		opPutOrganizationConformancePack:    service.WrapOp(h.handlePutOrganizationConformancePack),
		opDescribeOrganizationConfigRules:   service.WrapOp(h.handleDescribeOrganizationConfigRules),
		opDescribeOrganizationConformancePacks: service.WrapOp(
			h.handleDescribeOrganizationConformancePacks,
		),
		opDescribeOrganizationConfigRuleStatuses: service.WrapOp(
			h.handleDescribeOrganizationConfigRuleStatuses,
		),
		opDescribeOrganizationConformancePackStatuses: service.WrapOp(
			h.handleDescribeOrganizationConformancePackStatuses,
		),
		opGetOrganizationConfigRuleDetailedStatus: service.WrapOp(
			h.handleGetOrganizationConfigRuleDetailedStatus,
		),
		opGetOrganizationConformancePackDetailedStatus: service.WrapOp(
			h.handleGetOrganizationConformancePackDetailedStatus,
		),
		opGetOrganizationCustomRulePolicy: service.WrapOp(h.handleGetOrganizationCustomRulePolicy),
	}
}
