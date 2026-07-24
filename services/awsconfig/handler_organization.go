package awsconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

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

func (h *Handler) handlePutOrganizationConfigRule(
	_ context.Context, in *putOrganizationConfigRuleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutOrganizationConfigRule(in.OrganizationConfigRuleName)
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
type describeOrganizationConfigRulesOutput struct {
	OrganizationConfigRules []OrganizationConfigRule `json:"OrganizationConfigRules"`
}

func (h *Handler) handleDescribeOrganizationConfigRules(
	_ context.Context, _ *emptyInput,
) (*describeOrganizationConfigRulesOutput, error) {
	return &describeOrganizationConfigRulesOutput{
		OrganizationConfigRules: h.Backend.DescribeOrganizationConfigRules(),
	}, nil
}

// DescribeOrganizationConformancePacks request/response types and handler.
type describeOrganizationConformancePacksOutput struct {
	OrganizationConformancePacks []OrganizationConformancePack `json:"OrganizationConformancePacks"`
}

func (h *Handler) handleDescribeOrganizationConformancePacks(
	_ context.Context, _ *emptyInput,
) (*describeOrganizationConformancePacksOutput, error) {
	return &describeOrganizationConformancePacksOutput{
		OrganizationConformancePacks: h.Backend.DescribeOrganizationConformancePacks(),
	}, nil
}

// DescribeOrganizationConfigRuleStatuses request/response types and handler.
type describeOrganizationConfigRuleStatusesInput struct {
	OrganizationConfigRuleNames []string `json:"OrganizationConfigRuleNames"`
}
type describeOrganizationConfigRuleStatusesOutput struct {
	OrganizationConfigRuleStatuses []OrganizationConfigRuleStatus `json:"OrganizationConfigRuleStatuses"`
}

func (h *Handler) handleDescribeOrganizationConfigRuleStatuses(
	_ context.Context, in *describeOrganizationConfigRuleStatusesInput,
) (*describeOrganizationConfigRuleStatusesOutput, error) {
	return &describeOrganizationConfigRuleStatusesOutput{
		OrganizationConfigRuleStatuses: h.Backend.DescribeOrganizationConfigRuleStatuses(
			in.OrganizationConfigRuleNames,
		),
	}, nil
}

// DescribeOrganizationConformancePackStatuses request/response types and handler.
type describeOrganizationConformancePackStatusesInput struct {
	OrganizationConformancePackNames []string `json:"OrganizationConformancePackNames"`
}
type describeOrganizationConformancePackStatusesOutput struct {
	OrganizationConformancePackStatuses []OrganizationConformancePackStatus `json:"OrganizationConformancePackStatuses"`
}

func (h *Handler) handleDescribeOrganizationConformancePackStatuses(
	_ context.Context, in *describeOrganizationConformancePackStatusesInput,
) (*describeOrganizationConformancePackStatusesOutput, error) {
	return &describeOrganizationConformancePackStatusesOutput{
		OrganizationConformancePackStatuses: h.Backend.DescribeOrganizationConformancePackStatuses(
			in.OrganizationConformancePackNames,
		),
	}, nil
}

// GetOrganizationConfigRuleDetailedStatus request/response types and handler.
type getOrganizationConfigRuleDetailedStatusFiltersBody struct {
	AccountID string `json:"AccountId,omitempty"`
}
type getOrganizationConfigRuleDetailedStatusInput struct {
	Filters                    *getOrganizationConfigRuleDetailedStatusFiltersBody `json:"Filters,omitempty"`
	OrganizationConfigRuleName string                                              `json:"OrganizationConfigRuleName"`
}
type getOrganizationConfigRuleDetailedStatusOutput struct {
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

	return &getOrganizationConfigRuleDetailedStatusOutput{OrganizationConfigRuleDetailedStatus: statuses}, nil
}

// GetOrganizationConformancePackDetailedStatus request/response types and handler.
type orgConformancePackDetailedStatusFilters struct {
	AccountID string `json:"AccountId,omitempty"`
}
type getOrganizationConformancePackDetailedStatusInput struct {
	Filters                         *orgConformancePackDetailedStatusFilters `json:"Filters,omitempty"`
	OrganizationConformancePackName string                                   `json:"OrganizationConformancePackName"`
}
type getOrganizationConformancePackDetailedStatusOutput struct {
	Statuses []OrganizationConformancePackDetailedStatus `json:"OrganizationConformancePackDetailedStatuses"`
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

	return &getOrganizationConformancePackDetailedStatusOutput{Statuses: statuses}, nil
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
