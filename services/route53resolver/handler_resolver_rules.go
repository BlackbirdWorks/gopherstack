package route53resolver

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	filterFieldDomainName         = "DomainName"
	filterFieldResolverEndpointID = "ResolverEndpointId"
	filterFieldType               = "Type"
)

// resolverRuleFilterAliases canonicalizes Filter.Name for ListResolverRules
// -- see resolverEndpointFilterAliases's doc comment for the two-forms rule.
//
//nolint:gochecknoglobals // immutable lookup table, same pattern as other services' dispatch/alias tables
var resolverRuleFilterAliases = map[string]string{
	filterFieldCreatorRequestID:   filterFieldCreatorRequestID,
	legacyFilterCreatorRequestID:  filterFieldCreatorRequestID,
	filterFieldDomainName:         filterFieldDomainName,
	"DOMAIN_NAME":                 filterFieldDomainName,
	filterFieldName:               filterFieldName,
	legacyFilterName:              filterFieldName,
	filterFieldResolverEndpointID: filterFieldResolverEndpointID,
	"RESOLVER_ENDPOINT_ID":        filterFieldResolverEndpointID,
	filterFieldStatus:             filterFieldStatus,
	legacyFilterStatus:            filterFieldStatus,
	filterFieldType:               filterFieldType,
	"TYPE":                        filterFieldType,
}

func matchResolverRuleFilter(r *ResolverRule, name string, values []string) bool {
	switch name {
	case filterFieldCreatorRequestID:
		return slices.Contains(values, r.CreatorRequestID)
	case filterFieldDomainName:
		return slices.Contains(values, r.DomainName)
	case filterFieldName:
		return slices.Contains(values, r.Name)
	case filterFieldResolverEndpointID:
		return slices.Contains(values, r.ResolverEndpointID)
	case filterFieldStatus:
		return slices.Contains(values, r.Status)
	case filterFieldType:
		return slices.Contains(values, r.RuleType)
	default:
		return false
	}
}

type resolverRuleIDInput struct {
	ResolverRuleID string `json:"ResolverRuleId"`
}

type targetIP struct {
	IP                   string `json:"Ip"`
	Ipv6                 string `json:"Ipv6,omitempty"`
	Protocol             string `json:"Protocol,omitempty"`
	ServerNameIndication string `json:"ServerNameIndication,omitempty"`
	Port                 int32  `json:"Port"`
}

type resolverRuleOutput struct {
	ID                 string     `json:"Id"`
	Arn                string     `json:"Arn"`
	Name               string     `json:"Name"`
	DomainName         string     `json:"DomainName"`
	RuleType           string     `json:"RuleType"`
	Status             string     `json:"Status"`
	StatusMessage      string     `json:"StatusMessage,omitempty"`
	ShareStatus        string     `json:"ShareStatus"`
	ResolverEndpointID string     `json:"ResolverEndpointId"`
	CreatorRequestID   string     `json:"CreatorRequestId,omitempty"`
	OwnerID            string     `json:"OwnerId,omitempty"`
	CreationTime       string     `json:"CreationTime,omitempty"`
	ModificationTime   string     `json:"ModificationTime,omitempty"`
	DelegationRecord   string     `json:"DelegationRecord,omitempty"`
	TargetIps          []targetIP `json:"TargetIps,omitempty"`
}

type createResolverRuleOutput struct {
	ResolverRule resolverRuleOutput `json:"ResolverRule"`
}

type getResolverRuleOutput struct {
	ResolverRule resolverRuleOutput `json:"ResolverRule"`
}

type deleteResolverRuleOutput struct{}

type listResolverRulesInput struct {
	NextToken  string       `json:"NextToken"`
	Filters    []wireFilter `json:"Filters"`
	MaxResults int32        `json:"MaxResults"`
}

type listResolverRulesOutput struct {
	NextToken     *string              `json:"NextToken,omitempty"`
	ResolverRules []resolverRuleOutput `json:"ResolverRules"`
}

func ruleToOutput(r *ResolverRule) resolverRuleOutput {
	tips := make([]targetIP, 0, len(r.TargetIps))
	for _, t := range r.TargetIps {
		tips = append(
			tips,
			targetIP(t),
		)
	}
	if len(tips) == 0 {
		tips = nil
	}

	return resolverRuleOutput{
		ID:                 r.ID,
		Arn:                r.ARN,
		Name:               r.Name,
		DomainName:         r.DomainName,
		RuleType:           r.RuleType,
		Status:             r.Status,
		StatusMessage:      r.StatusMessage,
		ShareStatus:        r.ShareStatus,
		ResolverEndpointID: r.ResolverEndpointID,
		TargetIps:          tips,
		CreatorRequestID:   r.CreatorRequestID,
		OwnerID:            r.OwnerID,
		CreationTime:       r.CreationTime,
		ModificationTime:   r.ModificationTime,
		DelegationRecord:   r.DelegationRecord,
	}
}

type handleCreateResolverRuleInput struct {
	Name               string       `json:"Name"`
	DomainName         string       `json:"DomainName"`
	RuleType           string       `json:"RuleType"`
	ResolverEndpointID string       `json:"ResolverEndpointId"`
	CreatorRequestID   string       `json:"CreatorRequestId"`
	DelegationRecord   string       `json:"DelegationRecord"`
	TargetIps          []targetIP   `json:"TargetIps"`
	Tags               []svcTags.KV `json:"Tags"`
}

func (h *Handler) handleCreateResolverRule(
	ctx context.Context,
	in *handleCreateResolverRuleInput,
) (*createResolverRuleOutput, error) {
	tips := make([]TargetIP, 0, len(in.TargetIps))
	for _, t := range in.TargetIps {
		tips = append(
			tips,
			TargetIP(t),
		)
	}

	r, err := h.Backend.CreateResolverRule(
		ctx,
		in.Name,
		in.DomainName,
		in.RuleType,
		in.ResolverEndpointID,
		in.CreatorRequestID,
		in.DelegationRecord,
		tips,
	)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, r.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

func (h *Handler) handleGetResolverRule(
	ctx context.Context,
	in *resolverRuleIDInput,
) (*getResolverRuleOutput, error) {
	r, err := h.Backend.GetResolverRule(ctx, in.ResolverRuleID)
	if err != nil {
		return nil, err
	}

	return &getResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

func (h *Handler) handleDeleteResolverRule(
	ctx context.Context,
	in *resolverRuleIDInput,
) (*deleteResolverRuleOutput, error) {
	if err := h.Backend.DeleteResolverRule(ctx, in.ResolverRuleID); err != nil {
		return nil, err
	}

	return &deleteResolverRuleOutput{}, nil
}

func (h *Handler) handleListResolverRules(
	ctx context.Context,
	in *listResolverRulesInput,
) (*listResolverRulesOutput, error) {
	rules := h.Backend.ListResolverRules(ctx)
	rules, err := applyFilters(rules, in.Filters, resolverRuleFilterAliases, matchResolverRuleFilter)
	if err != nil {
		return nil, err
	}
	items := make([]resolverRuleOutput, 0, len(rules))
	for _, r := range rules {
		items = append(items, ruleToOutput(r))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeSmall)

	return &listResolverRulesOutput{ResolverRules: data, NextToken: next}, nil
}

type getResolverRulePolicyInput struct {
	Arn string `json:"Arn"`
}

type getResolverRulePolicyOutput struct {
	ResolverRulePolicy string `json:"ResolverRulePolicy"`
}

func (h *Handler) handleGetResolverRulePolicy(
	ctx context.Context,
	in *getResolverRulePolicyInput,
) (*getResolverRulePolicyOutput, error) {
	// GetResolverRulePolicy declares InvalidParameterException, not
	// InvalidRequestException/ValidationException (AccessDeniedException,
	// InternalServiceErrorException, InvalidParameterException,
	// UnknownResourceException) -- the backend policy lookup is a blind map
	// read with no natural not-found path to defer to, so ErrInvalidParameter
	// ("One or more parameters in this request are not valid") is used
	// directly rather than inventing a new sentinel.
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrInvalidParameter)
	}
	policy := h.Backend.GetResolverRulePolicy(ctx, in.Arn)

	return &getResolverRulePolicyOutput{ResolverRulePolicy: policy}, nil
}

// --- PutResolverRulePolicy ---

type putResolverRulePolicyInput struct {
	Arn                string `json:"Arn"`
	ResolverRulePolicy string `json:"ResolverRulePolicy"`
}

type putResolverRulePolicyOutput struct {
	ReturnValue bool `json:"ReturnValue"`
}

func (h *Handler) handlePutResolverRulePolicy(
	ctx context.Context,
	in *putResolverRulePolicyInput,
) (*putResolverRulePolicyOutput, error) {
	// Same rationale as GetResolverRulePolicy above: PutResolverRulePolicy
	// declares InvalidParameterException, not InvalidRequestException/
	// ValidationException, and the backend policy store is a blind write.
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrInvalidParameter)
	}
	if err := h.Backend.PutResolverRulePolicy(ctx, in.Arn, in.ResolverRulePolicy); err != nil {
		return nil, err
	}

	return &putResolverRulePolicyOutput{ReturnValue: true}, nil
}

// --- UpdateResolverEndpoint ---

type updateResolverRuleInput struct {
	ResolverRuleID string             `json:"ResolverRuleId"`
	Config         resolverRuleConfig `json:"Config"`
}

type resolverRuleConfig struct {
	Name               string     `json:"Name"`
	ResolverEndpointID string     `json:"ResolverEndpointId"`
	TargetIps          []targetIP `json:"TargetIps"`
}

type updateResolverRuleOutput struct {
	ResolverRule resolverRuleOutput `json:"ResolverRule"`
}

func (h *Handler) handleUpdateResolverRule(
	ctx context.Context,
	in *updateResolverRuleInput,
) (*updateResolverRuleOutput, error) {
	if in.ResolverRuleID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleId is required", ErrValidation)
	}

	tips := make([]TargetIP, 0, len(in.Config.TargetIps))
	for _, t := range in.Config.TargetIps {
		tips = append(
			tips,
			TargetIP(t),
		)
	}
	if len(tips) == 0 {
		tips = nil
	}

	r, err := h.Backend.UpdateResolverRule(
		ctx,
		in.ResolverRuleID,
		in.Config.Name,
		in.Config.ResolverEndpointID,
		tips,
	)
	if err != nil {
		return nil, err
	}

	return &updateResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

// --- GetResolverConfig ---

func (h *Handler) opsResolverRules() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateResolverRule":    service.WrapOp(h.handleCreateResolverRule),
		"DeleteResolverRule":    service.WrapOp(h.handleDeleteResolverRule),
		"GetResolverRule":       service.WrapOp(h.handleGetResolverRule),
		"GetResolverRulePolicy": service.WrapOp(h.handleGetResolverRulePolicy),
		"ListResolverRules":     service.WrapOp(h.handleListResolverRules),
		"PutResolverRulePolicy": service.WrapOp(h.handlePutResolverRulePolicy),
		"UpdateResolverRule":    service.WrapOp(h.handleUpdateResolverRule),
	}
}
