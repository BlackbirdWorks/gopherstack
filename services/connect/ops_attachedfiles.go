package connect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) attachedFileOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["StartAttachedFileUpload"] = replay(h, "StartAttachedFileUpload",
		func(in *StartAttachedFileUploadInput) string { return in.ClientToken }, b.StartAttachedFileUpload)
	ops["CreateAttachedFile"] = replay(h, "CreateAttachedFile",
		func(in *CreateAttachedFileInput) string { return in.ClientToken }, b.CreateAttachedFile)
	ops["CompleteAttachedFileUpload"] = typed(b.CompleteAttachedFileUpload)
	ops["GetAttachedFile"] = typedCtx(b.GetAttachedFile)
	ops["BatchGetAttachedFileMetadata"] = typed(b.BatchGetAttachedFileMetadata)
	ops["DeleteAttachedFile"] = typed(b.DeleteAttachedFile)
	ops["DescribeAttachedFilesConfiguration"] = typed(b.DescribeAttachedFilesConfiguration)
	ops["UpdateAttachedFilesConfiguration"] = typedCtx(b.UpdateAttachedFilesConfiguration)
	ops["ListAttachedFilesConfigurations"] = listOp(
		"AttachedFilesConfigurations",
		func(r *restroute.Request, t string, m int32) ([]AttachedFilesConfiguration, string, error) {
			return b.ListAttachedFilesConfigurations(instOf(r), t, m)
		},
	)
}
