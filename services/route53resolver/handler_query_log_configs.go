package route53resolver

import (
	"context"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	filterFieldARN              = "Arn"
	filterFieldAssociationCount = "AssociationCount"
	filterFieldCreationTime     = "CreationTime"
	filterFieldDestination      = "Destination"
	filterFieldDestinationARN   = "DestinationArn"
	filterFieldID               = "Id"
	filterFieldOwnerID          = "OwnerId"
	filterFieldShareStatus      = "ShareStatus"
)

// queryLogConfigFilterAliases canonicalizes Filter.Name for
// ListResolverQueryLogConfigs -- see resolverEndpointFilterAliases's doc
// comment for the two-forms rule.
//
//nolint:gochecknoglobals // immutable lookup table, same pattern as other services' dispatch/alias tables
var queryLogConfigFilterAliases = map[string]string{
	filterFieldARN:               filterFieldARN,
	"ARN":                        filterFieldARN,
	filterFieldAssociationCount:  filterFieldAssociationCount,
	"ASSOCIATION_COUNT":          filterFieldAssociationCount,
	filterFieldCreationTime:      filterFieldCreationTime,
	"CREATION_TIME":              filterFieldCreationTime,
	filterFieldCreatorRequestID:  filterFieldCreatorRequestID,
	legacyFilterCreatorRequestID: filterFieldCreatorRequestID,
	filterFieldDestination:       filterFieldDestination,
	"DESTINATION":                filterFieldDestination,
	filterFieldDestinationARN:    filterFieldDestinationARN,
	"DESTINATION_ARN":            filterFieldDestinationARN,
	filterFieldID:                filterFieldID,
	"ID":                         filterFieldID,
	filterFieldName:              filterFieldName,
	legacyFilterName:             filterFieldName,
	filterFieldOwnerID:           filterFieldOwnerID,
	"OWNER_ID":                   filterFieldOwnerID,
	filterFieldShareStatus:       filterFieldShareStatus,
	"SHARE_STATUS":               filterFieldShareStatus,
	filterFieldStatus:            filterFieldStatus,
	legacyFilterStatus:           filterFieldStatus,
}

func matchQueryLogConfigFilter(c *ResolverQueryLogConfig, name string, values []string) bool {
	switch name {
	case filterFieldARN:
		return slices.Contains(values, c.ARN)
	case filterFieldAssociationCount:
		return slices.Contains(values, int32ToString(c.AssociationCount))
	case filterFieldCreationTime:
		return slices.Contains(values, c.CreationTime)
	case filterFieldCreatorRequestID:
		return slices.Contains(values, c.CreatorRequestID)
	case filterFieldDestination:
		return slices.Contains(values, queryLogDestinationKind(c.DestinationARN))
	case filterFieldDestinationARN:
		return slices.Contains(values, c.DestinationARN)
	case filterFieldID:
		return slices.Contains(values, c.ID)
	case filterFieldName:
		return slices.Contains(values, c.Name)
	case filterFieldOwnerID:
		return slices.Contains(values, c.OwnerID)
	case filterFieldShareStatus:
		return slices.Contains(values, c.ShareStatus)
	case filterFieldStatus:
		return slices.Contains(values, c.Status)
	default:
		return false
	}
}

// resolverQueryLogConfigOutput is the JSON representation of a ResolverQueryLogConfig.
type resolverQueryLogConfigOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	DestinationArn   string `json:"DestinationArn"`
	Status           string `json:"Status"`
	OwnerID          string `json:"OwnerId"`
	ShareStatus      string `json:"ShareStatus"`
	CreationTime     string `json:"CreationTime,omitempty"`
	AssociationCount int32  `json:"AssociationCount"`
}

type createResolverQueryLogConfigInput struct {
	CreatorRequestID string       `json:"CreatorRequestId"`
	DestinationArn   string       `json:"DestinationArn"`
	Name             string       `json:"Name"`
	Tags             []svcTags.KV `json:"Tags"`
}

type createResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfig resolverQueryLogConfigOutput `json:"ResolverQueryLogConfig"`
}

func queryLogConfigToOutput(c *ResolverQueryLogConfig) resolverQueryLogConfigOutput {
	return resolverQueryLogConfigOutput{
		ID:               c.ID,
		Arn:              c.ARN,
		Name:             c.Name,
		CreatorRequestID: c.CreatorRequestID,
		DestinationArn:   c.DestinationARN,
		Status:           c.Status,
		OwnerID:          c.OwnerID,
		AssociationCount: c.AssociationCount,
		ShareStatus:      c.ShareStatus,
		CreationTime:     c.CreationTime,
	}
}

func (h *Handler) handleCreateResolverQueryLogConfig(
	ctx context.Context,
	in *createResolverQueryLogConfigInput,
) (*createResolverQueryLogConfigOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if in.DestinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", ErrValidation)
	}

	cfg, err := h.Backend.CreateResolverQueryLogConfig(
		ctx,
		in.Name,
		in.CreatorRequestID,
		in.DestinationArn,
	)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, cfg.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createResolverQueryLogConfigOutput{
		ResolverQueryLogConfig: queryLogConfigToOutput(cfg),
	}, nil
}

// --- AssociateResolverQueryLogConfig ---

type deleteResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
}

type deleteResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfig resolverQueryLogConfigOutput `json:"ResolverQueryLogConfig"`
}

func (h *Handler) handleDeleteResolverQueryLogConfig(
	ctx context.Context,
	in *deleteResolverQueryLogConfigInput,
) (*deleteResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}
	cfg, err := h.Backend.DeleteResolverQueryLogConfig(ctx, in.ResolverQueryLogConfigID)
	if err != nil {
		return nil, err
	}

	return &deleteResolverQueryLogConfigOutput{
		ResolverQueryLogConfig: queryLogConfigToOutput(cfg),
	}, nil
}

// --- GetResolverQueryLogConfig ---

type getResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
}

type getResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfig resolverQueryLogConfigOutput `json:"ResolverQueryLogConfig"`
}

func (h *Handler) handleGetResolverQueryLogConfig(
	ctx context.Context,
	in *getResolverQueryLogConfigInput,
) (*getResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}
	cfg, err := h.Backend.GetResolverQueryLogConfig(ctx, in.ResolverQueryLogConfigID)
	if err != nil {
		return nil, err
	}

	return &getResolverQueryLogConfigOutput{
		ResolverQueryLogConfig: queryLogConfigToOutput(cfg),
	}, nil
}

// --- ListResolverQueryLogConfigs ---

type listResolverQueryLogConfigsInput struct {
	NextToken  string       `json:"NextToken"`
	Filters    []wireFilter `json:"Filters"`
	MaxResults int32        `json:"MaxResults"`
}

type listResolverQueryLogConfigsOutput struct {
	NextToken               *string                        `json:"NextToken,omitempty"`
	ResolverQueryLogConfigs []resolverQueryLogConfigOutput `json:"ResolverQueryLogConfigs"`
}

func (h *Handler) handleListResolverQueryLogConfigs(
	ctx context.Context,
	in *listResolverQueryLogConfigsInput,
) (*listResolverQueryLogConfigsOutput, error) {
	configs := h.Backend.ListResolverQueryLogConfigs(ctx)
	configs, err := applyFilters(configs, in.Filters, queryLogConfigFilterAliases, matchQueryLogConfigFilter)
	if err != nil {
		return nil, err
	}
	items := make([]resolverQueryLogConfigOutput, 0, len(configs))
	for _, c := range configs {
		items = append(items, queryLogConfigToOutput(c))
	}
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listResolverQueryLogConfigsOutput{ResolverQueryLogConfigs: data, NextToken: next}, nil
}

// --- GetResolverQueryLogConfigAssociation ---

type getResolverQueryLogConfigPolicyInput struct {
	Arn string `json:"Arn"`
}

type getResolverQueryLogConfigPolicyOutput struct {
	ResolverQueryLogConfigPolicy string `json:"ResolverQueryLogConfigPolicy"`
}

func (h *Handler) handleGetResolverQueryLogConfigPolicy(
	ctx context.Context,
	in *getResolverQueryLogConfigPolicyInput,
) (*getResolverQueryLogConfigPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	policy := h.Backend.GetResolverQueryLogConfigPolicy(ctx, in.Arn)

	return &getResolverQueryLogConfigPolicyOutput{ResolverQueryLogConfigPolicy: policy}, nil
}

// --- PutResolverQueryLogConfigPolicy ---

type putResolverQueryLogConfigPolicyInput struct {
	Arn                          string `json:"Arn"`
	ResolverQueryLogConfigPolicy string `json:"ResolverQueryLogConfigPolicy"`
}

type putResolverQueryLogConfigPolicyOutput struct {
	ReturnValue bool `json:"ReturnValue"`
}

func (h *Handler) handlePutResolverQueryLogConfigPolicy(
	ctx context.Context,
	in *putResolverQueryLogConfigPolicyInput,
) (*putResolverQueryLogConfigPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	if err := h.Backend.PutResolverQueryLogConfigPolicy(ctx, in.Arn, in.ResolverQueryLogConfigPolicy); err != nil {
		return nil, err
	}

	return &putResolverQueryLogConfigPolicyOutput{ReturnValue: true}, nil
}

// --- GetResolverRuleAssociation ---

func (h *Handler) opsQueryLogConfigs() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateResolverQueryLogConfig": service.WrapOp(
			h.handleCreateResolverQueryLogConfig,
		),
		"DeleteResolverQueryLogConfig": service.WrapOp(
			h.handleDeleteResolverQueryLogConfig,
		),
		"GetResolverQueryLogConfig": service.WrapOp(h.handleGetResolverQueryLogConfig),
		"GetResolverQueryLogConfigPolicy": service.WrapOp(
			h.handleGetResolverQueryLogConfigPolicy,
		),
		"ListResolverQueryLogConfigs": service.WrapOp(
			h.handleListResolverQueryLogConfigs,
		),
		"PutResolverQueryLogConfigPolicy": service.WrapOp(
			h.handlePutResolverQueryLogConfigPolicy,
		),
	}
}
