package fsx

import "context"

// --- CreateAndAttachS3AccessPoint ---

type createAndAttachS3AccessPointOutput struct {
	S3AccessPointAttachment *S3AccessPointAttachment `json:"S3AccessPointAttachment"`
}

func (h *Handler) handleCreateAndAttachS3AccessPoint(
	_ context.Context,
	in *createAndAttachS3AccessPointInput,
) (*createAndAttachS3AccessPointOutput, error) {
	ap, err := h.Backend.CreateAndAttachS3AccessPoint(in)
	if err != nil {
		return nil, err
	}

	return &createAndAttachS3AccessPointOutput{S3AccessPointAttachment: ap}, nil
}

// --- DetachAndDeleteS3AccessPoint ---

type detachAndDeleteS3AccessPointInput struct {
	Name string `json:"Name"`
}

type detachAndDeleteS3AccessPointOutput struct {
	Lifecycle string `json:"Lifecycle"`
	Name      string `json:"Name"`
}

func (h *Handler) handleDetachAndDeleteS3AccessPoint(
	_ context.Context,
	in *detachAndDeleteS3AccessPointInput,
) (*detachAndDeleteS3AccessPointOutput, error) {
	if err := h.Backend.DetachAndDeleteS3AccessPoint(in.Name); err != nil {
		return nil, err
	}

	return &detachAndDeleteS3AccessPointOutput{Name: in.Name, Lifecycle: lifecycleDeleting}, nil
}

// --- DescribeS3AccessPointAttachments ---

type describeS3AccessPointAttachmentsInput struct {
	NextToken  string   `json:"NextToken,omitempty"`
	Names      []string `json:"Names,omitempty"`
	MaxResults int32    `json:"MaxResults,omitempty"`
}

type describeS3AccessPointAttachmentsOutput struct {
	NextToken                string                     `json:"NextToken,omitempty"`
	S3AccessPointAttachments []*S3AccessPointAttachment `json:"S3AccessPointAttachments"`
}

func (h *Handler) handleDescribeS3AccessPointAttachments(
	_ context.Context,
	in *describeS3AccessPointAttachmentsInput,
) (*describeS3AccessPointAttachmentsOutput, error) {
	aps, next, err := h.Backend.DescribeS3AccessPointAttachments(in.Names, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeS3AccessPointAttachmentsOutput{S3AccessPointAttachments: aps, NextToken: next}, nil
}
