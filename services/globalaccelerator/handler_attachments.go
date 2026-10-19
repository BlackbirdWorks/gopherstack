package globalaccelerator

import (
	"context"
)

func registerAttachmentOps(ops map[string]opFunc) {
	ops["CreateCrossAccountAttachment"] = bind((*Handler).createAttachment)
	ops["DescribeCrossAccountAttachment"] = bind((*Handler).describeAttachment)
	ops["ListCrossAccountAttachments"] = bind((*Handler).listAttachments)
	ops["UpdateCrossAccountAttachment"] = bind((*Handler).updateAttachment)
	ops["DeleteCrossAccountAttachment"] = bind((*Handler).deleteAttachment)
	ops["ListCrossAccountResourceAccounts"] = bind((*Handler).listResourceAccounts)
	ops["ListCrossAccountResources"] = bind((*Handler).listCrossAccountResources)
}

func (h *Handler) createAttachment(ctx context.Context, in *createAttachmentReq) (*attachmentResp, error) {
	tagMap, err := validateTags(in.Tags)
	if err != nil {
		return nil, err
	}

	owner := callerAccount(ctx, h.Backend.AccountID())

	a, err := createOnce(h, "CreateCrossAccountAttachment", in.IdempotencyToken, in,
		func(a *Attachment) string { return a.ARN },
		h.Backend.GetCrossAccountAttachment,
		func() (*Attachment, error) {
			return h.Backend.CreateCrossAccountAttachment(AttachmentInput{
				Name: in.Name, Principals: in.Principals, Resources: in.Resources, Tags: tagMap, Owner: owner,
			})
		})
	if err != nil {
		return nil, err
	}

	return &attachmentResp{CrossAccountAttachment: attachmentToWire(a)}, nil
}

func (h *Handler) describeAttachment(_ context.Context, in *attachmentArnReq) (*attachmentResp, error) {
	a, err := h.Backend.GetCrossAccountAttachment(in.AttachmentArn)
	if err != nil {
		return nil, err
	}

	return &attachmentResp{CrossAccountAttachment: attachmentToWire(a)}, nil
}

func (h *Handler) listAttachments(ctx context.Context, in *listPageReq) (*listAttachmentsResp, error) {
	pg, err := h.Backend.ListCrossAccountAttachments(
		callerAccount(ctx, h.Backend.AccountID()),
		in.NextToken,
		in.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &listAttachmentsResp{NextToken: pg.Next, Attachments: mapPage(pg, attachmentToWire)}, nil
}

func (h *Handler) updateAttachment(_ context.Context, in *updateAttachmentReq) (*attachmentResp, error) {
	a, err := h.Backend.UpdateCrossAccountAttachment(in.AttachmentArn, AttachmentPatch{
		Name: in.Name, AddPrincipals: in.AddPrincipals, RemovePrincipals: in.RemovePrincipals,
		AddResources: in.AddResources, RemoveResources: in.RemoveResources,
	})
	if err != nil {
		return nil, err
	}

	return &attachmentResp{CrossAccountAttachment: attachmentToWire(a)}, nil
}

func (h *Handler) deleteAttachment(_ context.Context, in *attachmentArnReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteCrossAccountAttachment(in.AttachmentArn)
}

func (h *Handler) listResourceAccounts(ctx context.Context, _ *emptyResp) (*resourceAccountsResp, error) {
	return &resourceAccountsResp{
		OwnerAccountIDs: h.Backend.ListCrossAccountResourceAccounts(callerAccount(ctx, h.Backend.AccountID())),
	}, nil
}

func (h *Handler) listCrossAccountResources(
	ctx context.Context, in *listCrossAccountResourcesReq,
) (*listCrossAccountResourcesResp, error) {
	pg, err := h.Backend.ListCrossAccountResources(
		callerAccount(ctx, h.Backend.AccountID()), in.OwnerAccountID, in.AcceleratorArn, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listCrossAccountResourcesResp{NextToken: pg.Next, Resources: mapPage(pg, crossAccountResourceToWire)}, nil
}

func crossAccountResourceToWire(r CrossAccountResource) crossAccountResourceWire {
	return crossAccountResourceWire{AttachmentArn: r.AttachmentARN, Cidr: r.Cidr, EndpointID: r.EndpointID}
}
