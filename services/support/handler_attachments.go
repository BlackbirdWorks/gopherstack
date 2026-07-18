package support

import (
	"context"
	"fmt"
)

type addAttachmentsToSetInput struct {
	AttachmentSetID string       `json:"attachmentSetId,omitempty"`
	Attachments     []Attachment `json:"attachments"`
}

type addAttachmentsToSetOutput struct {
	AttachmentSetID string `json:"attachmentSetId"`
	ExpiryTime      string `json:"expiryTime"`
}

func (h *Handler) handleAddAttachmentsToSet(
	_ context.Context,
	in *addAttachmentsToSetInput,
) (*addAttachmentsToSetOutput, error) {
	setID, expiry, err := h.Backend.AddAttachmentsToSetWithAttachments(in.AttachmentSetID, in.Attachments)
	if err != nil {
		return nil, err
	}

	return &addAttachmentsToSetOutput{
		AttachmentSetID: setID,
		ExpiryTime:      expiry.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

type describeAttachmentInput struct {
	AttachmentID string `json:"attachmentId"`
}

type attachmentView struct {
	FileName string `json:"fileName"`
	Data     []byte `json:"data"`
}

type describeAttachmentOutput struct {
	Attachment attachmentView `json:"attachment"`
}

func (h *Handler) handleDescribeAttachment(
	_ context.Context,
	in *describeAttachmentInput,
) (*describeAttachmentOutput, error) {
	if in.AttachmentID == "" {
		return nil, fmt.Errorf("%w: attachmentId is required", ErrValidation)
	}

	a, err := h.Backend.DescribeAttachment(in.AttachmentID)
	if err != nil {
		return nil, err
	}

	return &describeAttachmentOutput{
		Attachment: attachmentView{
			FileName: a.FileName,
			Data:     a.Data,
		},
	}, nil
}
