package route53resolver

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// outpostResolverOutput is the JSON representation of an OutpostResolver.
type outpostResolverOutput struct {
	ID                    string `json:"Id"`
	Arn                   string `json:"Arn"`
	Name                  string `json:"Name"`
	CreatorRequestID      string `json:"CreatorRequestId"`
	OutpostArn            string `json:"OutpostArn"`
	PreferredInstanceType string `json:"PreferredInstanceType"`
	Status                string `json:"Status"`
	InstanceCount         int32  `json:"InstanceCount"`
}

type createOutpostResolverInput struct {
	CreatorRequestID      string       `json:"CreatorRequestId"`
	Name                  string       `json:"Name"`
	OutpostArn            string       `json:"OutpostArn"`
	PreferredInstanceType string       `json:"PreferredInstanceType"`
	Tags                  []svcTags.KV `json:"Tags"`
	InstanceCount         int32        `json:"InstanceCount"`
}

type createOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func outpostResolverToOutput(r *OutpostResolver) outpostResolverOutput {
	return outpostResolverOutput{
		ID:                    r.ID,
		Arn:                   r.ARN,
		Name:                  r.Name,
		CreatorRequestID:      r.CreatorRequestID,
		OutpostArn:            r.OutpostARN,
		PreferredInstanceType: r.PreferredInstanceType,
		InstanceCount:         r.InstanceCount,
		Status:                r.Status,
	}
}

func (h *Handler) handleCreateOutpostResolver(
	ctx context.Context,
	in *createOutpostResolverInput,
) (*createOutpostResolverOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if in.OutpostArn == "" {
		return nil, fmt.Errorf("%w: OutpostArn is required", ErrValidation)
	}

	if in.PreferredInstanceType == "" {
		return nil, fmt.Errorf("%w: PreferredInstanceType is required", ErrValidation)
	}

	r, err := h.Backend.CreateOutpostResolver(
		ctx, in.Name, in.CreatorRequestID, in.OutpostArn, in.PreferredInstanceType, in.InstanceCount,
	)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, r.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- New handler types and functions for 46 missing operations ---

type getOutpostResolverInput struct {
	ID string `json:"Id"`
}

type getOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func (h *Handler) handleGetOutpostResolver(
	ctx context.Context,
	in *getOutpostResolverInput,
) (*getOutpostResolverOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}
	r, err := h.Backend.GetOutpostResolver(ctx, in.ID)
	if err != nil {
		return nil, err
	}

	return &getOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- DeleteOutpostResolver ---

type deleteOutpostResolverInput struct {
	ID string `json:"Id"`
}

type deleteOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func (h *Handler) handleDeleteOutpostResolver(
	ctx context.Context,
	in *deleteOutpostResolverInput,
) (*deleteOutpostResolverOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}
	r, err := h.Backend.DeleteOutpostResolver(ctx, in.ID)
	if err != nil {
		return nil, err
	}

	return &deleteOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- ListOutpostResolvers ---

type listOutpostResolversInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listOutpostResolversOutput struct {
	NextToken        *string                 `json:"NextToken,omitempty"`
	OutpostResolvers []outpostResolverOutput `json:"OutpostResolvers"`
}

func (h *Handler) handleListOutpostResolvers(
	ctx context.Context,
	in *listOutpostResolversInput,
) (*listOutpostResolversOutput, error) {
	resolvers := h.Backend.ListOutpostResolvers(ctx)
	items := make([]outpostResolverOutput, 0, len(resolvers))
	for _, r := range resolvers {
		items = append(items, outpostResolverToOutput(r))
	}
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listOutpostResolversOutput{OutpostResolvers: data, NextToken: next}, nil
}

// --- UpdateOutpostResolver ---

type updateOutpostResolverInput struct {
	ID                    string `json:"Id"`
	Name                  string `json:"Name"`
	PreferredInstanceType string `json:"PreferredInstanceType"`
	InstanceCount         int32  `json:"InstanceCount"`
}

type updateOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func (h *Handler) handleUpdateOutpostResolver(
	ctx context.Context,
	in *updateOutpostResolverInput,
) (*updateOutpostResolverOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}
	r, err := h.Backend.UpdateOutpostResolver(
		ctx,
		in.ID,
		in.Name,
		in.PreferredInstanceType,
		in.InstanceCount,
	)
	if err != nil {
		return nil, err
	}

	return &updateOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- DeleteResolverQueryLogConfig ---

func (h *Handler) opsOutpostResolvers() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateOutpostResolver": service.WrapOp(h.handleCreateOutpostResolver),
		"DeleteOutpostResolver": service.WrapOp(h.handleDeleteOutpostResolver),
		"GetOutpostResolver":    service.WrapOp(h.handleGetOutpostResolver),
		"ListOutpostResolvers":  service.WrapOp(h.handleListOutpostResolvers),
		"UpdateOutpostResolver": service.WrapOp(h.handleUpdateOutpostResolver),
	}
}
