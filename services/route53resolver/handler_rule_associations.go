package route53resolver

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// resolverRuleAssociationOutput is the JSON representation of a ResolverRuleAssociation.
type resolverRuleAssociationOutput struct {
	ID             string `json:"Id"`
	Name           string `json:"Name"`
	ResolverRuleID string `json:"ResolverRuleId"`
	VPCId          string `json:"VPCId"`
	Status         string `json:"Status"`
}

// --- CreateFirewallRuleGroup ---

type associateResolverRuleInput struct {
	ResolverRuleID string `json:"ResolverRuleId"`
	VPCId          string `json:"VPCId"`
	Name           string `json:"Name"`
}

type associateResolverRuleOutput struct {
	ResolverRuleAssociation resolverRuleAssociationOutput `json:"ResolverRuleAssociation"`
}

func ruleAssociationToOutput(a *ResolverRuleAssociation) resolverRuleAssociationOutput {
	return resolverRuleAssociationOutput{
		ID:             a.ID,
		Name:           a.Name,
		ResolverRuleID: a.ResolverRuleID,
		VPCId:          a.VPCID,
		Status:         a.Status,
	}
}

func (h *Handler) handleAssociateResolverRule(
	ctx context.Context,
	in *associateResolverRuleInput,
) (*associateResolverRuleOutput, error) {
	if in.ResolverRuleID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleId is required", ErrValidation)
	}

	if in.VPCId == "" {
		return nil, fmt.Errorf("%w: VPCId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateResolverRule(ctx, in.ResolverRuleID, in.VPCId, in.Name)
	if err != nil {
		return nil, err
	}

	return &associateResolverRuleOutput{
		ResolverRuleAssociation: ruleAssociationToOutput(assoc),
	}, nil
}

// --- CreateFirewallDomainList ---

type getResolverRuleAssociationInput struct {
	ResolverRuleAssociationID string `json:"ResolverRuleAssociationId"`
}

type getResolverRuleAssociationOutput struct {
	ResolverRuleAssociation resolverRuleAssociationOutput `json:"ResolverRuleAssociation"`
}

func (h *Handler) handleGetResolverRuleAssociation(
	ctx context.Context,
	in *getResolverRuleAssociationInput,
) (*getResolverRuleAssociationOutput, error) {
	if in.ResolverRuleAssociationID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.GetResolverRuleAssociation(ctx, in.ResolverRuleAssociationID)
	if err != nil {
		return nil, err
	}

	return &getResolverRuleAssociationOutput{
		ResolverRuleAssociation: ruleAssociationToOutput(assoc),
	}, nil
}

// --- DisassociateResolverRule ---

// disassociateResolverRuleInput matches the real DisassociateResolverRule
// wire shape: ResolverRuleId + VPCId, NOT an opaque association ID (that
// field only appears in Get/List responses, never in this request).
type disassociateResolverRuleInput struct {
	ResolverRuleID string `json:"ResolverRuleId"`
	VPCId          string `json:"VPCId"`
}

type disassociateResolverRuleOutput struct {
	ResolverRuleAssociation resolverRuleAssociationOutput `json:"ResolverRuleAssociation"`
}

func (h *Handler) handleDisassociateResolverRule(
	ctx context.Context,
	in *disassociateResolverRuleInput,
) (*disassociateResolverRuleOutput, error) {
	if in.ResolverRuleID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleId is required", ErrValidation)
	}
	if in.VPCId == "" {
		return nil, fmt.Errorf("%w: VPCId is required", ErrValidation)
	}
	assoc, err := h.Backend.DisassociateResolverRule(ctx, in.ResolverRuleID, in.VPCId)
	if err != nil {
		return nil, err
	}

	return &disassociateResolverRuleOutput{
		ResolverRuleAssociation: ruleAssociationToOutput(assoc),
	}, nil
}

// --- ListResolverRuleAssociations ---

type listResolverRuleAssociationsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listResolverRuleAssociationsOutput struct {
	NextToken                *string                         `json:"NextToken,omitempty"`
	ResolverRuleAssociations []resolverRuleAssociationOutput `json:"ResolverRuleAssociations"`
}

func (h *Handler) handleListResolverRuleAssociations(
	ctx context.Context,
	in *listResolverRuleAssociationsInput,
) (*listResolverRuleAssociationsOutput, error) {
	assocs := h.Backend.ListResolverRuleAssociations(ctx)
	items := make([]resolverRuleAssociationOutput, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, ruleAssociationToOutput(a))
	}
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listResolverRuleAssociationsOutput{ResolverRuleAssociations: data, NextToken: next}, nil
}

// --- GetResolverRulePolicy ---

func (h *Handler) opsRuleAssociations() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AssociateResolverRule":    service.WrapOp(h.handleAssociateResolverRule),
		"DisassociateResolverRule": service.WrapOp(h.handleDisassociateResolverRule),
		"GetResolverRuleAssociation": service.WrapOp(
			h.handleGetResolverRuleAssociation,
		),
		"ListResolverRuleAssociations": service.WrapOp(
			h.handleListResolverRuleAssociations,
		),
	}
}
