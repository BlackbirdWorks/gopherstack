package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// firewallRuleOutput is the JSON representation of a FirewallRule.
type firewallRuleOutput struct {
	ID                   string `json:"Id"`
	Arn                  string `json:"Arn"`
	Name                 string `json:"Name"`
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	Action               string `json:"Action"`
	BlockResponse        string `json:"BlockResponse,omitempty"`
	BlockOverrideDomain  string `json:"BlockOverrideDomain,omitempty"`
	BlockOverrideDNSType string `json:"BlockOverrideDNSType,omitempty"`
	Qtype                string `json:"Qtype,omitempty"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold,omitempty"`
	CreatorRequestID     string `json:"CreatorRequestId,omitempty"`
	CreationTime         string `json:"CreationTime,omitempty"`
	ModificationTime     string `json:"ModificationTime,omitempty"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTTL,omitempty"`
	Priority             int32  `json:"Priority"`
}

type createFirewallRuleInput struct {
	Action               string `json:"Action"`
	CreatorRequestID     string `json:"CreatorRequestId"`
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	Name                 string `json:"Name"`
	BlockResponse        string `json:"BlockResponse"`
	BlockOverrideDomain  string `json:"BlockOverrideDomain"`
	BlockOverrideDNSType string `json:"BlockOverrideDNSType"`
	Qtype                string `json:"Qtype"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTTL"`
	Priority             int32  `json:"Priority"`
}

type createFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func firewallRuleToOutput(r *FirewallRule) firewallRuleOutput {
	return firewallRuleOutput{
		ID:                   r.ID,
		Arn:                  r.ARN,
		Name:                 r.Name,
		FirewallRuleGroupID:  r.FirewallRuleGroupID,
		FirewallDomainListID: r.FirewallDomainListID,
		Action:               r.Action,
		Priority:             r.Priority,
		BlockResponse:        r.BlockResponse,
		BlockOverrideDomain:  r.BlockOverrideDomain,
		BlockOverrideDNSType: r.BlockOverrideDNSType,
		BlockOverrideTTL:     r.BlockOverrideTTL,
		Qtype:                r.Qtype,
		ConfidenceThreshold:  r.ConfidenceThreshold,
		CreatorRequestID:     r.CreatorRequestID,
		CreationTime:         r.CreationTime,
		ModificationTime:     r.ModificationTime,
	}
}

func (h *Handler) handleCreateFirewallRule(
	ctx context.Context,
	in *createFirewallRuleInput,
) (*createFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}

	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	switch in.Action {
	case firewallActionAllow, firewallActionBlock, firewallActionAlert:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: Action must be %s, %s, or %s",
			ErrValidation,
			firewallActionAllow,
			firewallActionBlock,
			firewallActionAlert,
		)
	}

	rule, err := h.Backend.CreateFirewallRule(ctx, CreateFirewallRuleParams{
		FirewallRuleGroupID:  in.FirewallRuleGroupID,
		Name:                 in.Name,
		Action:               in.Action,
		BlockResponse:        in.BlockResponse,
		BlockOverrideDomain:  in.BlockOverrideDomain,
		BlockOverrideDNSType: in.BlockOverrideDNSType,
		BlockOverrideTTL:     in.BlockOverrideTTL,
		Qtype:                in.Qtype,
		ConfidenceThreshold:  in.ConfidenceThreshold,
		CreatorRequestID:     in.CreatorRequestID,
		FirewallDomainListID: in.FirewallDomainListID,
		Priority:             in.Priority,
	})
	if err != nil {
		return nil, err
	}

	return &createFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- CreateOutpostResolver ---

type deleteFirewallRuleInput struct {
	FirewallRuleID string `json:"FirewallRuleId"`
}

type deleteFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleDeleteFirewallRule(
	ctx context.Context,
	in *deleteFirewallRuleInput,
) (*deleteFirewallRuleOutput, error) {
	if in.FirewallRuleID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleId is required", ErrValidation)
	}
	rule, err := h.Backend.DeleteFirewallRule(ctx, in.FirewallRuleID)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- UpdateFirewallRule ---

type updateFirewallRuleInput struct {
	FirewallRuleID       string `json:"FirewallRuleId"`
	Name                 string `json:"Name"`
	Action               string `json:"Action"`
	BlockResponse        string `json:"BlockResponse"`
	BlockOverrideDomain  string `json:"BlockOverrideDomain"`
	BlockOverrideDNSType string `json:"BlockOverrideDNSType"`
	Qtype                string `json:"Qtype"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTTL"`
	Priority             int32  `json:"Priority"`
}

type updateFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleUpdateFirewallRule(
	ctx context.Context,
	in *updateFirewallRuleInput,
) (*updateFirewallRuleOutput, error) {
	if in.FirewallRuleID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleId is required", ErrValidation)
	}
	rule, err := h.Backend.UpdateFirewallRule(ctx, UpdateFirewallRuleParams{
		ID:                   in.FirewallRuleID,
		Name:                 in.Name,
		Action:               in.Action,
		BlockResponse:        in.BlockResponse,
		BlockOverrideDomain:  in.BlockOverrideDomain,
		BlockOverrideDNSType: in.BlockOverrideDNSType,
		BlockOverrideTTL:     in.BlockOverrideTTL,
		Qtype:                in.Qtype,
		ConfidenceThreshold:  in.ConfidenceThreshold,
		FirewallDomainListID: in.FirewallDomainListID,
		Priority:             in.Priority,
	})
	if err != nil {
		return nil, err
	}

	return &updateFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- ListFirewallRules ---

type listFirewallRulesInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	NextToken           string `json:"NextToken"`
	MaxResults          int32  `json:"MaxResults"`
}

type listFirewallRulesOutput struct {
	NextToken     *string              `json:"NextToken,omitempty"`
	FirewallRules []firewallRuleOutput `json:"FirewallRules"`
}

func (h *Handler) handleListFirewallRules(
	ctx context.Context,
	in *listFirewallRulesInput,
) (*listFirewallRulesOutput, error) {
	rules := h.Backend.ListFirewallRules(ctx, in.FirewallRuleGroupID)
	items := make([]firewallRuleOutput, 0, len(rules))
	for _, r := range rules {
		items = append(items, firewallRuleToOutput(r))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallRulesOutput{FirewallRules: data, NextToken: next}, nil
}

// --- DeleteFirewallRuleGroup ---

func (h *Handler) opsFirewallRules() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateFirewallRule": service.WrapOp(h.handleCreateFirewallRule),
		"DeleteFirewallRule": service.WrapOp(h.handleDeleteFirewallRule),
		"ListFirewallRules":  service.WrapOp(h.handleListFirewallRules),
		"UpdateFirewallRule": service.WrapOp(h.handleUpdateFirewallRule),
	}
}
