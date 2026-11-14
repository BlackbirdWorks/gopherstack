package connect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) contactControlOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateContact"] = replay(h, "CreateContact",
		func(in *CreateContactInput) string { return in.ClientToken }, b.CreateContact)
	ops["BatchPutContact"] = replay(h, "BatchPutContact",
		func(in *BatchPutContactInput) string { return in.ClientToken }, b.BatchPutContact)
	ops["AssociateContactWithUser"] = typed(b.AssociateContactWithUser)
	ops["CreateParticipant"] = replay(h, "CreateParticipant",
		func(in *CreateParticipantInput) string { return in.ClientToken },
		func(_ context.Context, in *CreateParticipantInput) (*CreateParticipantOut, error) {
			return b.CreateParticipant(in)
		})
	ops["CreatePersistentContactAssociation"] = typed(b.CreatePersistentContactAssociation)
	ops["StartEmailContact"] = replay(h, "StartEmailContact",
		func(in *StartEmailContactInput) string { return in.ClientToken }, b.StartEmailContact)
	ops["StartOutboundEmailContact"] = replay(h, "StartOutboundEmailContact",
		func(in *StartOutboundEmailContactInput) string { return in.ClientToken }, b.StartOutboundEmailContact)
	ops["SendOutboundEmail"] = replay(h, "SendOutboundEmail",
		func(in *SendOutboundEmailInput) string { return in.ClientToken }, b.SendOutboundEmail)
	ops["PauseContact"] = typed(b.PauseContact)
	ops["UpdateParticipantRoleConfig"] = typed(b.UpdateParticipantRoleConfig)
	ops["UpdateContactRoutingData"] = typed(b.UpdateContactRoutingData)
	ops["DismissUserContact"] = typed(b.DismissUserContact)
	ops["ResumeContact"] = typed(b.ResumeContact)
	ops["StartContactRecording"] = typed(b.StartContactRecording)
	ops["StopContactRecording"] = typed(b.StopContactRecording)
	ops["SuspendContactRecording"] = typed(b.SuspendContactRecording)
	ops["ResumeContactRecording"] = typed(b.ResumeContactRecording)
	ops["MonitorContact"] = replay(h, "MonitorContact",
		func(in *MonitorContactInput) string { return in.ClientToken }, b.MonitorContact)
	ops["StartContactStreaming"] = replay(h, "StartContactStreaming",
		func(in *StartContactStreamingInput) string { return in.ClientToken },
		func(_ context.Context, in *StartContactStreamingInput) (*StartStreamingOut, error) {
			return b.StartContactStreaming(in)
		})
	ops["StopContactStreaming"] = typed(b.StopContactStreaming)
	ops["StartContactMediaProcessing"] = typedCtx(b.StartContactMediaProcessing)
	ops["StopContactMediaProcessing"] = typed(b.StopContactMediaProcessing)
}
