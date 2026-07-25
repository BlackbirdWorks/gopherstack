package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// firewallRuleOutput is the wire shape of types.FirewallRule. Note: the real
// SDK type has no Id or Arn member -- a firewall rule has no independent
// identity on the wire, it is addressed by the (FirewallRuleGroupId,
// FirewallDomainListId) pair (verified against types.FirewallRule and
// api_op_{Update,Delete,List}FirewallRule.go, none of which have an
// Id/Arn/FirewallRuleId member). An earlier revision of this handler invented
// Id/Arn fields here; they have been removed (see firewall_rules_test.go and
// PARITY.md for the fix note).
//
// BlockOverrideDnsType/BlockOverrideTtl json tags: verified against the real
// SDK's hand-rolled awsjson1.1 deserializer (deserializers.go,
// awsAwsjson11_deserializeDocumentFirewallRule), which does exact-case
// map-key matching, not case-insensitive struct-tag matching (same bug class
// as the OwnerID/OwnerId note elsewhere in this package). The wire keys are
// "BlockOverrideDnsType" and "BlockOverrideTtl" -- NOT "BlockOverrideDNSType"
// / "BlockOverrideTTL". A prior revision used the wrong casing here; real SDK
// clients would have silently never seen these two fields populated.
type firewallRuleOutput struct {
	Name                 string `json:"Name"`
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	Action               string `json:"Action"`
	BlockResponse        string `json:"BlockResponse,omitempty"`
	BlockOverrideDomain  string `json:"BlockOverrideDomain,omitempty"`
	BlockOverrideDNSType string `json:"BlockOverrideDnsType,omitempty"`
	Qtype                string `json:"Qtype,omitempty"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold,omitempty"`
	CreatorRequestID     string `json:"CreatorRequestId,omitempty"`
	CreationTime         string `json:"CreationTime,omitempty"`
	ModificationTime     string `json:"ModificationTime,omitempty"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTtl,omitempty"`
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
	BlockOverrideDNSType string `json:"BlockOverrideDnsType"`
	Qtype                string `json:"Qtype"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTtl"`
	Priority             int32  `json:"Priority"`
}

type createFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func firewallRuleToOutput(r *FirewallRule) firewallRuleOutput {
	return firewallRuleOutput{
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

// deleteFirewallRuleInput matches the real DeleteFirewallRuleInput shape:
// there is no FirewallRuleId member. A rule is addressed by the
// (FirewallRuleGroupId, FirewallDomainListId) pair it was created with
// (verified against api_op_DeleteFirewallRule.go; FirewallThreatProtectionId
// is the DNS Firewall Advanced counterpart to FirewallDomainListId and is not
// modeled here, matching this pass's declared scope).
type deleteFirewallRuleInput struct {
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
}

type deleteFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleDeleteFirewallRule(
	ctx context.Context,
	in *deleteFirewallRuleInput,
) (*deleteFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	rule, err := h.Backend.DeleteFirewallRule(ctx, in.FirewallRuleGroupID, in.FirewallDomainListID)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- UpdateFirewallRule ---

// updateFirewallRuleInput matches the real UpdateFirewallRuleInput shape:
// there is no FirewallRuleId member. FirewallRuleGroupId+FirewallDomainListId
// identify which rule to update -- the domain list a rule targets is part of
// its identity, not a mutable property (verified against
// api_op_UpdateFirewallRule.go).
type updateFirewallRuleInput struct {
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	Name                 string `json:"Name"`
	Action               string `json:"Action"`
	BlockResponse        string `json:"BlockResponse"`
	BlockOverrideDomain  string `json:"BlockOverrideDomain"`
	BlockOverrideDNSType string `json:"BlockOverrideDnsType"`
	Qtype                string `json:"Qtype"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTtl"`
	Priority             int32  `json:"Priority"`
}

type updateFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleUpdateFirewallRule(
	ctx context.Context,
	in *updateFirewallRuleInput,
) (*updateFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	rule, err := h.Backend.UpdateFirewallRule(ctx, UpdateFirewallRuleParams{
		FirewallRuleGroupID:  in.FirewallRuleGroupID,
		FirewallDomainListID: in.FirewallDomainListID,
		Name:                 in.Name,
		Action:               in.Action,
		BlockResponse:        in.BlockResponse,
		BlockOverrideDomain:  in.BlockOverrideDomain,
		BlockOverrideDNSType: in.BlockOverrideDNSType,
		BlockOverrideTTL:     in.BlockOverrideTTL,
		Qtype:                in.Qtype,
		ConfidenceThreshold:  in.ConfidenceThreshold,
		Priority:             in.Priority,
	})
	if err != nil {
		return nil, err
	}

	return &updateFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- ListFirewallRules ---

type listFirewallRulesInput struct {
	Priority            *int32 `json:"Priority,omitempty"`
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	NextToken           string `json:"NextToken"`
	Action              string `json:"Action"`
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
		if in.Action != "" && r.Action != in.Action {
			continue
		}
		if in.Priority != nil && r.Priority != *in.Priority {
			continue
		}
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
