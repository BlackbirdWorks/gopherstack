package route53resolver

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// This file covers the three "bare resourceID" config families --
// FirewallConfig, ResolverConfig, ResolverDnssecConfig -- each a thin
// Get/Update/List wrapper around a single VPC-scoped setting. They share
// the generic getSimpleConfig/updateSimpleConfig/mapSlice/paginate helpers
// in handler.go; keeping all three together (with ResolverConfig's
// differently shaped output sitting between the other two) mirrors how they
// were laid out before this refactor split the file by family.

// firewallConfigOutput is the JSON representation of a FirewallConfig.
// AWS does not return an ARN for FirewallConfig.
type firewallConfigOutput struct {
	ID               string `json:"Id"`
	OwnerID          string `json:"OwnerId"`
	ResourceID       string `json:"ResourceId"`
	FirewallFailOpen string `json:"FirewallFailOpen"`
}

func firewallConfigToOutput(c *FirewallConfig) firewallConfigOutput {
	return firewallConfigOutput{
		ID:               c.ID,
		OwnerID:          c.OwnerID,
		ResourceID:       c.ResourceID,
		FirewallFailOpen: c.FirewallFailOpen,
	}
}

type getFirewallConfigInput struct {
	ResourceID string `json:"ResourceId"`
}

type getFirewallConfigOutput struct {
	FirewallConfig firewallConfigOutput `json:"FirewallConfig"`
}

func (h *Handler) handleGetFirewallConfig(
	ctx context.Context,
	in *getFirewallConfigInput,
) (*getFirewallConfigOutput, error) {
	return getSimpleConfig(
		in.ResourceID,
		func() *FirewallConfig { return h.Backend.GetFirewallConfig(ctx, in.ResourceID) },
		func(c *FirewallConfig) *getFirewallConfigOutput {
			return &getFirewallConfigOutput{FirewallConfig: firewallConfigToOutput(c)}
		},
	)
}

type updateFirewallConfigInput struct {
	ResourceID       string `json:"ResourceId"`
	FirewallFailOpen string `json:"FirewallFailOpen"`
}

type updateFirewallConfigOutput struct {
	FirewallConfig firewallConfigOutput `json:"FirewallConfig"`
}

func (h *Handler) handleUpdateFirewallConfig(
	ctx context.Context,
	in *updateFirewallConfigInput,
) (*updateFirewallConfigOutput, error) {
	return updateSimpleConfig(
		in.ResourceID,
		func() (*FirewallConfig, error) {
			return h.Backend.UpdateFirewallConfig(ctx, in.ResourceID, in.FirewallFailOpen)
		},
		func(c *FirewallConfig) *updateFirewallConfigOutput {
			return &updateFirewallConfigOutput{FirewallConfig: firewallConfigToOutput(c)}
		},
	)
}

type listFirewallConfigsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listFirewallConfigsOutput struct {
	NextToken       *string                `json:"NextToken,omitempty"`
	FirewallConfigs []firewallConfigOutput `json:"FirewallConfigs"`
}

func (h *Handler) handleListFirewallConfigs(
	ctx context.Context,
	in *listFirewallConfigsInput,
) (*listFirewallConfigsOutput, error) {
	configs := h.Backend.ListFirewallConfigs(ctx)
	items := mapSlice(configs, firewallConfigToOutput)
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallConfigsOutput{FirewallConfigs: data, NextToken: next}, nil
}

func (h *Handler) opsFirewallConfigs() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetFirewallConfig":    service.WrapOp(h.handleGetFirewallConfig),
		"ListFirewallConfigs":  service.WrapOp(h.handleListFirewallConfigs),
		"UpdateFirewallConfig": service.WrapOp(h.handleUpdateFirewallConfig),
	}
}

// --- ResolverConfig ---

// resolverConfigOutput is the JSON representation of a ResolverConfig.
type resolverConfigOutput struct {
	ID                 string `json:"Id"`
	Arn                string `json:"Arn"`
	OwnerID            string `json:"OwnerId"`
	ResourceID         string `json:"ResourceId"`
	AutodefinedReverse string `json:"AutodefinedReverse"`
}

func resolverConfigToOutput(c *ResolverConfig) resolverConfigOutput {
	return resolverConfigOutput{
		ID:                 c.ID,
		Arn:                c.ARN,
		OwnerID:            c.OwnerID,
		ResourceID:         c.ResourceID,
		AutodefinedReverse: c.AutodefinedReverse,
	}
}

type getResolverConfigInput struct {
	ResourceID string `json:"ResourceId"`
}

type getResolverConfigOutput struct {
	ResolverConfig resolverConfigOutput `json:"ResolverConfig"`
}

func (h *Handler) handleGetResolverConfig(
	ctx context.Context,
	in *getResolverConfigInput,
) (*getResolverConfigOutput, error) {
	return getSimpleConfig(
		in.ResourceID,
		func() *ResolverConfig { return h.Backend.GetResolverConfig(ctx, in.ResourceID) },
		func(c *ResolverConfig) *getResolverConfigOutput {
			return &getResolverConfigOutput{ResolverConfig: resolverConfigToOutput(c)}
		},
	)
}

type updateResolverConfigInput struct {
	ResourceID         string `json:"ResourceId"`
	AutodefinedReverse string `json:"AutodefinedReverse"`
}

type updateResolverConfigOutput struct {
	ResolverConfig resolverConfigOutput `json:"ResolverConfig"`
}

func (h *Handler) handleUpdateResolverConfig(
	ctx context.Context,
	in *updateResolverConfigInput,
) (*updateResolverConfigOutput, error) {
	return updateSimpleConfig(
		in.ResourceID,
		func() (*ResolverConfig, error) {
			return h.Backend.UpdateResolverConfig(ctx, in.ResourceID, in.AutodefinedReverse)
		},
		func(c *ResolverConfig) *updateResolverConfigOutput {
			return &updateResolverConfigOutput{ResolverConfig: resolverConfigToOutput(c)}
		},
	)
}

type listResolverConfigsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listResolverConfigsOutput struct {
	NextToken       *string                `json:"NextToken,omitempty"`
	ResolverConfigs []resolverConfigOutput `json:"ResolverConfigs"`
}

func (h *Handler) handleListResolverConfigs(
	ctx context.Context,
	in *listResolverConfigsInput,
) (*listResolverConfigsOutput, error) {
	configs := h.Backend.ListResolverConfigs(ctx)
	items := mapSlice(configs, resolverConfigToOutput)
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listResolverConfigsOutput{ResolverConfigs: data, NextToken: next}, nil
}

func (h *Handler) opsResolverConfigs() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetResolverConfig":    service.WrapOp(h.handleGetResolverConfig),
		"ListResolverConfigs":  service.WrapOp(h.handleListResolverConfigs),
		"UpdateResolverConfig": service.WrapOp(h.handleUpdateResolverConfig),
	}
}

// --- ResolverDnssecConfig ---

// resolverDnssecConfigOutput is the JSON representation of a ResolverDnssecConfig.
type resolverDnssecConfigOutput struct {
	ID         string `json:"Id"`
	OwnerID    string `json:"OwnerId"`
	ResourceID string `json:"ResourceId"`
	Validation string `json:"Validation"`
}

func resolverDnssecConfigToOutput(c *ResolverDnssecConfig) resolverDnssecConfigOutput {
	return resolverDnssecConfigOutput{
		ID:         c.ID,
		OwnerID:    c.OwnerID,
		ResourceID: c.ResourceID,
		Validation: c.ValidationStatus,
	}
}

type getResolverDnssecConfigInput struct {
	ResourceID string `json:"ResourceId"`
}

type getResolverDnssecConfigOutput struct {
	ResolverDNSSECConfig resolverDnssecConfigOutput `json:"ResolverDNSSECConfig"`
}

func (h *Handler) handleGetResolverDnssecConfig(
	ctx context.Context,
	in *getResolverDnssecConfigInput,
) (*getResolverDnssecConfigOutput, error) {
	return getSimpleConfig(
		in.ResourceID,
		func() *ResolverDnssecConfig { return h.Backend.GetResolverDnssecConfig(ctx, in.ResourceID) },
		func(c *ResolverDnssecConfig) *getResolverDnssecConfigOutput {
			return &getResolverDnssecConfigOutput{ResolverDNSSECConfig: resolverDnssecConfigToOutput(c)}
		},
	)
}

type updateResolverDnssecConfigInput struct {
	ResourceID string `json:"ResourceId"`
	Validation string `json:"Validation"`
}

type updateResolverDnssecConfigOutput struct {
	ResolverDNSSECConfig resolverDnssecConfigOutput `json:"ResolverDNSSECConfig"`
}

func (h *Handler) handleUpdateResolverDnssecConfig(
	ctx context.Context,
	in *updateResolverDnssecConfigInput,
) (*updateResolverDnssecConfigOutput, error) {
	return updateSimpleConfig(
		in.ResourceID,
		func() (*ResolverDnssecConfig, error) {
			return h.Backend.UpdateResolverDnssecConfig(ctx, in.ResourceID, in.Validation)
		},
		func(c *ResolverDnssecConfig) *updateResolverDnssecConfigOutput {
			return &updateResolverDnssecConfigOutput{ResolverDNSSECConfig: resolverDnssecConfigToOutput(c)}
		},
	)
}

type listResolverDnssecConfigsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listResolverDnssecConfigsOutput struct {
	NextToken             *string                      `json:"NextToken,omitempty"`
	ResolverDnssecConfigs []resolverDnssecConfigOutput `json:"ResolverDnssecConfigs"`
}

func (h *Handler) handleListResolverDnssecConfigs(
	ctx context.Context,
	in *listResolverDnssecConfigsInput,
) (*listResolverDnssecConfigsOutput, error) {
	configs := h.Backend.ListResolverDnssecConfigs(ctx)
	items := mapSlice(configs, resolverDnssecConfigToOutput)
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listResolverDnssecConfigsOutput{ResolverDnssecConfigs: data, NextToken: next}, nil
}

func (h *Handler) opsDnssecConfigs() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetResolverDnssecConfig":   service.WrapOp(h.handleGetResolverDnssecConfig),
		"ListResolverDnssecConfigs": service.WrapOp(h.handleListResolverDnssecConfigs),
		"UpdateResolverDnssecConfig": service.WrapOp(
			h.handleUpdateResolverDnssecConfig,
		),
	}
}
