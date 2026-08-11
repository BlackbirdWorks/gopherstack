package route53resolver

import (
	"context"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	filterFieldError                    = "Error"
	filterFieldResolverQueryLogConfigID = "ResolverQueryLogConfigId"
	filterFieldResourceID               = "ResourceId"
)

// queryLogConfigAssociationFilterAliases canonicalizes Filter.Name for
// ListResolverQueryLogConfigAssociations (types.Filter doc, aws-sdk-go-v2/
// service/route53resolver@v1.48.4 types/types.go): CreationTime, Error, Id,
// ResolverQueryLogConfigId, ResourceId, Status -- see
// resolverEndpointFilterAliases's doc comment for the two-forms rule.
//
//nolint:gochecknoglobals // immutable lookup table, same pattern as other services' dispatch/alias tables
var queryLogConfigAssociationFilterAliases = map[string]string{
	filterFieldCreationTime:             filterFieldCreationTime,
	"CREATION_TIME":                     filterFieldCreationTime,
	filterFieldError:                    filterFieldError,
	"ERROR":                             filterFieldError,
	filterFieldID:                       filterFieldID,
	"ID":                                filterFieldID,
	filterFieldResolverQueryLogConfigID: filterFieldResolverQueryLogConfigID,
	"RESOLVER_QUERY_LOG_CONFIG_ID":      filterFieldResolverQueryLogConfigID,
	filterFieldResourceID:               filterFieldResourceID,
	"RESOURCE_ID":                       filterFieldResourceID,
	filterFieldStatus:                   filterFieldStatus,
	legacyFilterStatus:                  filterFieldStatus,
}

func matchQueryLogConfigAssociationFilter(a *ResolverQueryLogConfigAssociation, name string, values []string) bool {
	switch name {
	case filterFieldCreationTime:
		return slices.Contains(values, a.CreationTime)
	case filterFieldError:
		return slices.Contains(values, a.Error)
	case filterFieldID:
		return slices.Contains(values, a.ID)
	case filterFieldResolverQueryLogConfigID:
		return slices.Contains(values, a.ResolverQueryLogConfigID)
	case filterFieldResourceID:
		return slices.Contains(values, a.ResourceID)
	case filterFieldStatus:
		return slices.Contains(values, a.Status)
	default:
		return false
	}
}

// resolverQueryLogConfigAssociationOutput is the JSON representation of a ResolverQueryLogConfigAssociation.
type resolverQueryLogConfigAssociationOutput struct {
	ID                       string `json:"Id"`
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
	ResourceID               string `json:"ResourceId"`
	Status                   string `json:"Status"`
	Error                    string `json:"Error,omitempty"`
	ErrorMessage             string `json:"ErrorMessage,omitempty"`
	CreationTime             string `json:"CreationTime,omitempty"`
}

type associateResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
	ResourceID               string `json:"ResourceId"`
}

type associateResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfigAssociation resolverQueryLogConfigAssociationOutput `json:"ResolverQueryLogConfigAssociation"`
}

func queryLogConfigAssociationToOutput(
	a *ResolverQueryLogConfigAssociation,
) resolverQueryLogConfigAssociationOutput {
	return resolverQueryLogConfigAssociationOutput{
		ID:                       a.ID,
		ResolverQueryLogConfigID: a.ResolverQueryLogConfigID,
		ResourceID:               a.ResourceID,
		Status:                   a.Status,
		Error:                    a.Error,
		ErrorMessage:             a.ErrorMessage,
		CreationTime:             a.CreationTime,
	}
}

func (h *Handler) handleAssociateResolverQueryLogConfig(
	ctx context.Context,
	in *associateResolverQueryLogConfigInput,
) (*associateResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}

	if err := requireResourceID(in.ResourceID); err != nil {
		return nil, err
	}

	assoc, err := h.Backend.AssociateResolverQueryLogConfig(
		ctx,
		in.ResolverQueryLogConfigID,
		in.ResourceID,
	)
	if err != nil {
		return nil, err
	}

	return &associateResolverQueryLogConfigOutput{
		ResolverQueryLogConfigAssociation: queryLogConfigAssociationToOutput(assoc),
	}, nil
}

// --- AssociateResolverRule ---

type getResolverQueryLogConfigAssociationInput struct {
	ResolverQueryLogConfigAssociationID string `json:"ResolverQueryLogConfigAssociationId"`
}

type getResolverQueryLogConfigAssociationOutput struct {
	ResolverQueryLogConfigAssociation resolverQueryLogConfigAssociationOutput `json:"ResolverQueryLogConfigAssociation"`
}

func (h *Handler) handleGetResolverQueryLogConfigAssociation(
	ctx context.Context,
	in *getResolverQueryLogConfigAssociationInput,
) (*getResolverQueryLogConfigAssociationOutput, error) {
	if in.ResolverQueryLogConfigAssociationID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.GetResolverQueryLogConfigAssociation(
		ctx,
		in.ResolverQueryLogConfigAssociationID,
	)
	if err != nil {
		return nil, err
	}

	return &getResolverQueryLogConfigAssociationOutput{
		ResolverQueryLogConfigAssociation: queryLogConfigAssociationToOutput(assoc),
	}, nil
}

// --- DisassociateResolverQueryLogConfig ---

// disassociateResolverQueryLogConfigInput matches the real
// DisassociateResolverQueryLogConfig wire shape: ResolverQueryLogConfigId +
// ResourceId, NOT an opaque association ID (that field only appears in
// Get/List responses, never in this request).
type disassociateResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
	ResourceID               string `json:"ResourceId"`
}

type disassociateResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfigAssociation resolverQueryLogConfigAssociationOutput `json:"ResolverQueryLogConfigAssociation"`
}

func (h *Handler) handleDisassociateResolverQueryLogConfig(
	ctx context.Context,
	in *disassociateResolverQueryLogConfigInput,
) (*disassociateResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}
	if err := requireResourceID(in.ResourceID); err != nil {
		return nil, err
	}
	assoc, err := h.Backend.DisassociateResolverQueryLogConfig(
		ctx,
		in.ResolverQueryLogConfigID,
		in.ResourceID,
	)
	if err != nil {
		return nil, err
	}

	return &disassociateResolverQueryLogConfigOutput{
		ResolverQueryLogConfigAssociation: queryLogConfigAssociationToOutput(assoc),
	}, nil
}

// --- ListResolverQueryLogConfigAssociations ---

type listResolverQueryLogConfigAssociationsInput struct {
	NextToken  string       `json:"NextToken"`
	Filters    []wireFilter `json:"Filters"`
	MaxResults int32        `json:"MaxResults"`
}

type queryLogAssocOutputSlice = []resolverQueryLogConfigAssociationOutput

type listResolverQueryLogConfigAssociationsOutput struct {
	NextToken                          *string                  `json:"NextToken,omitempty"`
	ResolverQueryLogConfigAssociations queryLogAssocOutputSlice `json:"ResolverQueryLogConfigAssociations"`
}

func (h *Handler) handleListResolverQueryLogConfigAssociations(
	ctx context.Context,
	in *listResolverQueryLogConfigAssociationsInput,
) (*listResolverQueryLogConfigAssociationsOutput, error) {
	assocs := h.Backend.ListResolverQueryLogConfigAssociations(ctx)
	assocs, err := applyFilters(
		assocs,
		in.Filters,
		queryLogConfigAssociationFilterAliases,
		matchQueryLogConfigAssociationFilter,
	)
	if err != nil {
		return nil, err
	}
	items := make([]resolverQueryLogConfigAssociationOutput, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, queryLogConfigAssociationToOutput(a))
	}
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listResolverQueryLogConfigAssociationsOutput{
		ResolverQueryLogConfigAssociations: data,
		NextToken:                          next,
	}, nil
}

// --- GetResolverQueryLogConfigPolicy ---

func (h *Handler) opsQueryLogAssociations() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AssociateResolverQueryLogConfig": service.WrapOp(
			h.handleAssociateResolverQueryLogConfig,
		),
		"DisassociateResolverQueryLogConfig": service.WrapOp(
			h.handleDisassociateResolverQueryLogConfig,
		),
		"GetResolverQueryLogConfigAssociation": service.WrapOp(
			h.handleGetResolverQueryLogConfigAssociation,
		),
		"ListResolverQueryLogConfigAssociations": service.WrapOp(
			h.handleListResolverQueryLogConfigAssociations,
		),
	}
}
