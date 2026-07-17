package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// firewallRuleGroupOutput is the JSON representation of a FirewallRuleGroup.
type firewallRuleGroupOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Status           string `json:"Status"`
	StatusMessage    string `json:"StatusMessage,omitempty"`
	OwnerID          string `json:"OwnerId"`
	ShareStatus      string `json:"ShareStatus"`
	CreationTime     string `json:"CreationTime,omitempty"`
	ModificationTime string `json:"ModificationTime,omitempty"`
	RuleCount        int32  `json:"RuleCount"`
}

// firewallRuleGroupAssociationOutput is the JSON representation of a FirewallRuleGroupAssociation.
type firewallRuleGroupAssociationOutput struct {
	ID                  string `json:"Id"`
	Arn                 string `json:"Arn"`
	Name                string `json:"Name"`
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	VpcID               string `json:"VpcId"`
	Status              string `json:"Status"`
	StatusMessage       string `json:"StatusMessage,omitempty"`
	MutationProtection  string `json:"MutationProtection"`
	ManagedOwnerName    string `json:"ManagedOwnerName,omitempty"`
	CreatorRequestID    string `json:"CreatorRequestId,omitempty"`
	CreationTime        string `json:"CreationTime,omitempty"`
	ModificationTime    string `json:"ModificationTime,omitempty"`
	Priority            int32  `json:"Priority"`
}

type createFirewallRuleGroupInput struct {
	CreatorRequestID string       `json:"CreatorRequestId"`
	Name             string       `json:"Name"`
	Tags             []svcTags.KV `json:"Tags"`
}

type createFirewallRuleGroupOutput struct {
	FirewallRuleGroup firewallRuleGroupOutput `json:"FirewallRuleGroup"`
}

func firewallRuleGroupToOutput(g *FirewallRuleGroup) firewallRuleGroupOutput {
	return firewallRuleGroupOutput{
		ID:               g.ID,
		Arn:              g.ARN,
		Name:             g.Name,
		CreatorRequestID: g.CreatorRequestID,
		Status:           g.Status,
		StatusMessage:    g.StatusMessage,
		OwnerID:          g.OwnerID,
		ShareStatus:      g.ShareStatus,
		RuleCount:        g.RuleCount,
		CreationTime:     g.CreationTime,
		ModificationTime: g.ModificationTime,
	}
}

func (h *Handler) handleCreateFirewallRuleGroup(
	ctx context.Context,
	in *createFirewallRuleGroupInput,
) (*createFirewallRuleGroupOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	g, err := h.Backend.CreateFirewallRuleGroup(ctx, in.Name, in.CreatorRequestID)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, g.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createFirewallRuleGroupOutput{FirewallRuleGroup: firewallRuleGroupToOutput(g)}, nil
}

// --- AssociateFirewallRuleGroup ---

type associateFirewallRuleGroupInput struct {
	FirewallRuleGroupID string       `json:"FirewallRuleGroupId"`
	Name                string       `json:"Name"`
	VpcID               string       `json:"VpcId"`
	CreatorRequestID    string       `json:"CreatorRequestId"`
	MutationProtection  string       `json:"MutationProtection"`
	Tags                []svcTags.KV `json:"Tags"`
	Priority            int32        `json:"Priority"`
}

type associateFirewallRuleGroupOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func firewallRuleGroupAssociationToOutput(
	a *FirewallRuleGroupAssociation,
) firewallRuleGroupAssociationOutput {
	return firewallRuleGroupAssociationOutput{
		ID:                  a.ID,
		Arn:                 a.ARN,
		Name:                a.Name,
		FirewallRuleGroupID: a.FirewallRuleGroupID,
		VpcID:               a.VpcID,
		Priority:            a.Priority,
		Status:              a.Status,
		StatusMessage:       a.StatusMessage,
		MutationProtection:  a.MutationProtection,
		ManagedOwnerName:    a.ManagedOwnerName,
		CreatorRequestID:    a.CreatorRequestID,
		CreationTime:        a.CreationTime,
		ModificationTime:    a.ModificationTime,
	}
}

func (h *Handler) handleAssociateFirewallRuleGroup(
	ctx context.Context,
	in *associateFirewallRuleGroupInput,
) (*associateFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}

	if in.VpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateFirewallRuleGroup(
		ctx,
		in.FirewallRuleGroupID,
		in.VpcID,
		in.Name,
		in.CreatorRequestID,
		in.MutationProtection,
		in.Priority,
	)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, assoc.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &associateFirewallRuleGroupOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- AssociateResolverEndpointIpAddress ---

type deleteFirewallRuleGroupInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
}

type deleteFirewallRuleGroupOutput struct {
	FirewallRuleGroup firewallRuleGroupOutput `json:"FirewallRuleGroup"`
}

func (h *Handler) handleDeleteFirewallRuleGroup(
	ctx context.Context,
	in *deleteFirewallRuleGroupInput,
) (*deleteFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}
	g, err := h.Backend.DeleteFirewallRuleGroup(ctx, in.FirewallRuleGroupID)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallRuleGroupOutput{FirewallRuleGroup: firewallRuleGroupToOutput(g)}, nil
}

// --- GetFirewallRuleGroup ---

type getFirewallRuleGroupInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
}

type getFirewallRuleGroupOutput struct {
	FirewallRuleGroup firewallRuleGroupOutput `json:"FirewallRuleGroup"`
}

func (h *Handler) handleGetFirewallRuleGroup(
	ctx context.Context,
	in *getFirewallRuleGroupInput,
) (*getFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}
	g, err := h.Backend.GetFirewallRuleGroup(ctx, in.FirewallRuleGroupID)
	if err != nil {
		return nil, err
	}

	return &getFirewallRuleGroupOutput{FirewallRuleGroup: firewallRuleGroupToOutput(g)}, nil
}

// --- ListFirewallRuleGroups ---

type listFirewallRuleGroupsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listFirewallRuleGroupsOutput struct {
	NextToken          *string                   `json:"NextToken,omitempty"`
	FirewallRuleGroups []firewallRuleGroupOutput `json:"FirewallRuleGroups"`
}

func (h *Handler) handleListFirewallRuleGroups(
	ctx context.Context,
	in *listFirewallRuleGroupsInput,
) (*listFirewallRuleGroupsOutput, error) {
	groups := h.Backend.ListFirewallRuleGroups(ctx)
	items := make([]firewallRuleGroupOutput, 0, len(groups))
	for _, g := range groups {
		items = append(items, firewallRuleGroupToOutput(g))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallRuleGroupsOutput{FirewallRuleGroups: data, NextToken: next}, nil
}

// --- GetFirewallRuleGroupPolicy ---

type getFirewallRuleGroupPolicyInput struct {
	Arn string `json:"Arn"`
}

type getFirewallRuleGroupPolicyOutput struct {
	FirewallRuleGroupPolicy string `json:"FirewallRuleGroupPolicy"`
}

func (h *Handler) handleGetFirewallRuleGroupPolicy(
	ctx context.Context,
	in *getFirewallRuleGroupPolicyInput,
) (*getFirewallRuleGroupPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	policy := h.Backend.GetFirewallRuleGroupPolicy(ctx, in.Arn)

	return &getFirewallRuleGroupPolicyOutput{FirewallRuleGroupPolicy: policy}, nil
}

// --- PutFirewallRuleGroupPolicy ---

type putFirewallRuleGroupPolicyInput struct {
	Arn                     string `json:"Arn"`
	FirewallRuleGroupPolicy string `json:"FirewallRuleGroupPolicy"`
}

type putFirewallRuleGroupPolicyOutput struct {
	ReturnValue bool `json:"ReturnValue"`
}

func (h *Handler) handlePutFirewallRuleGroupPolicy(
	ctx context.Context,
	in *putFirewallRuleGroupPolicyInput,
) (*putFirewallRuleGroupPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	if err := h.Backend.PutFirewallRuleGroupPolicy(ctx, in.Arn, in.FirewallRuleGroupPolicy); err != nil {
		return nil, err
	}

	return &putFirewallRuleGroupPolicyOutput{ReturnValue: true}, nil
}

// --- GetFirewallRuleGroupAssociation ---

type getFirewallRuleGroupAssociationInput struct {
	FirewallRuleGroupAssociationID string `json:"FirewallRuleGroupAssociationId"`
}

type getFirewallRuleGroupAssociationOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func (h *Handler) handleGetFirewallRuleGroupAssociation(
	ctx context.Context,
	in *getFirewallRuleGroupAssociationInput,
) (*getFirewallRuleGroupAssociationOutput, error) {
	if in.FirewallRuleGroupAssociationID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.GetFirewallRuleGroupAssociation(ctx, in.FirewallRuleGroupAssociationID)
	if err != nil {
		return nil, err
	}

	return &getFirewallRuleGroupAssociationOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- ListFirewallRuleGroupAssociations ---

type listFirewallRuleGroupAssociationsInput struct {
	NextToken           string `json:"NextToken"`
	VpcID               string `json:"VpcId"`
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	MaxResults          int32  `json:"MaxResults"`
}

type listFirewallRuleGroupAssociationsOutput struct {
	NextToken                     *string                              `json:"NextToken,omitempty"`
	FirewallRuleGroupAssociations []firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociations"`
}

func (h *Handler) handleListFirewallRuleGroupAssociations(
	ctx context.Context,
	in *listFirewallRuleGroupAssociationsInput,
) (*listFirewallRuleGroupAssociationsOutput, error) {
	assocs := h.Backend.ListFirewallRuleGroupAssociations(ctx, in.VpcID, in.FirewallRuleGroupID)
	items := make([]firewallRuleGroupAssociationOutput, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, firewallRuleGroupAssociationToOutput(a))
	}
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallRuleGroupAssociationsOutput{
		FirewallRuleGroupAssociations: data,
		NextToken:                     next,
	}, nil
}

// --- DisassociateFirewallRuleGroup ---

type disassociateFirewallRuleGroupInput struct {
	FirewallRuleGroupAssociationID string `json:"FirewallRuleGroupAssociationId"`
}

type disassociateFirewallRuleGroupOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func (h *Handler) handleDisassociateFirewallRuleGroup(
	ctx context.Context,
	in *disassociateFirewallRuleGroupInput,
) (*disassociateFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupAssociationID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.DisassociateFirewallRuleGroup(ctx, in.FirewallRuleGroupAssociationID)
	if err != nil {
		return nil, err
	}

	return &disassociateFirewallRuleGroupOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- UpdateFirewallRuleGroupAssociation ---

type updateFirewallRuleGroupAssociationInput struct {
	FirewallRuleGroupAssociationID string `json:"FirewallRuleGroupAssociationId"`
	Name                           string `json:"Name"`
	MutationProtection             string `json:"MutationProtection"`
	Priority                       int32  `json:"Priority"`
}

type updateFirewallRuleGroupAssociationOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func (h *Handler) handleUpdateFirewallRuleGroupAssociation(
	ctx context.Context,
	in *updateFirewallRuleGroupAssociationInput,
) (*updateFirewallRuleGroupAssociationOutput, error) {
	if in.FirewallRuleGroupAssociationID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.UpdateFirewallRuleGroupAssociation(
		ctx, in.FirewallRuleGroupAssociationID, in.Name, in.MutationProtection, in.Priority,
	)
	if err != nil {
		return nil, err
	}

	return &updateFirewallRuleGroupAssociationOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- GetFirewallDomainList ---

func (h *Handler) opsFirewallRuleGroups() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AssociateFirewallRuleGroup": service.WrapOp(
			h.handleAssociateFirewallRuleGroup,
		),
		"CreateFirewallRuleGroup": service.WrapOp(h.handleCreateFirewallRuleGroup),
		"DeleteFirewallRuleGroup": service.WrapOp(h.handleDeleteFirewallRuleGroup),
		"DisassociateFirewallRuleGroup": service.WrapOp(
			h.handleDisassociateFirewallRuleGroup,
		),
		"GetFirewallRuleGroup": service.WrapOp(h.handleGetFirewallRuleGroup),
		"GetFirewallRuleGroupAssociation": service.WrapOp(
			h.handleGetFirewallRuleGroupAssociation,
		),
		"GetFirewallRuleGroupPolicy": service.WrapOp(
			h.handleGetFirewallRuleGroupPolicy,
		),
		"ListFirewallRuleGroupAssociations": service.WrapOp(
			h.handleListFirewallRuleGroupAssociations,
		),
		"ListFirewallRuleGroups": service.WrapOp(h.handleListFirewallRuleGroups),
		"PutFirewallRuleGroupPolicy": service.WrapOp(
			h.handlePutFirewallRuleGroupPolicy,
		),
		"UpdateFirewallRuleGroupAssociation": service.WrapOp(
			h.handleUpdateFirewallRuleGroupAssociation,
		),
	}
}
