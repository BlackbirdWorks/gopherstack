package connect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

type queueEmailsOut struct {
	Stamp
	NextToken                string                `json:"NextToken,omitempty"`
	EmailAddressMetadataList []EmailAddressSummary `json:"EmailAddressMetadataList"`
}

func (h *Handler) emailOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateEmailAddress"] = replay(h, "CreateEmailAddress",
		func(in *CreateEmailAddressInput) string { return in.ClientToken }, b.CreateEmailAddress)
	ops["DescribeEmailAddress"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		return passthrough(b.DescribeEmailAddress(instOf(req), req.Params["EmailAddressId"]))
	}
	ops["UpdateEmailAddressMetadata"] = typed(b.UpdateEmailAddressMetadata)
	ops["DeleteEmailAddress"] = deleteOp("EmailAddressId", b.DeleteEmailAddress)
	ops["AssociateEmailAddressAlias"] = typed(b.AssociateEmailAddressAlias)
	ops["DisassociateEmailAddressAlias"] = typed(b.DisassociateEmailAddressAlias)
	ops["AssociateQueueEmailAddresses"] = typed(b.AssociateQueueEmailAddresses)
	ops["DisassociateQueueEmailAddresses"] = typed(b.DisassociateQueueEmailAddresses)
	ops["ListQueueEmailAddresses"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		token, m, err := pageArgs(req)
		if err != nil {
			return nil, 0, err
		}

		items, stamp, next, err := b.ListQueueEmailAddresses(instOf(req), req.Params["QueueId"], token, m)
		if err != nil {
			return nil, 0, err
		}

		return &queueEmailsOut{Stamp: stamp, NextToken: next, EmailAddressMetadataList: orEmpty(items)}, 0, nil
	}
}
