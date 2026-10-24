package connect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) workspaceOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateWorkspace"] = typedCtx(b.CreateWorkspace)
	ops["DescribeWorkspace"] = describeOp("WorkspaceId", "Workspace", b.DescribeWorkspace)
	ops["ListWorkspaces"] = listOp(
		"WorkspaceSummaryList",
		func(r *restroute.Request, t string, m int32) ([]WorkspaceSummary, string, error) {
			return b.ListWorkspaces(instOf(r), t, m)
		},
	)
	ops["DeleteWorkspace"] = deleteOp("WorkspaceId", b.DeleteWorkspace)
	ops["UpdateWorkspaceMetadata"] = typedCtx(b.UpdateWorkspaceMetadata)
	ops["UpdateWorkspaceTheme"] = typedCtx(b.UpdateWorkspaceTheme)
	ops["UpdateWorkspaceVisibility"] = typedCtx(b.UpdateWorkspaceVisibility)
	ops["AssociateWorkspace"] = typedCtx(b.AssociateWorkspace)
	ops["DisassociateWorkspace"] = typedCtx(b.DisassociateWorkspace)
	ops["CreateWorkspacePage"] = typedCtx(b.CreateWorkspacePage)
	ops["UpdateWorkspacePage"] = typedCtx(b.UpdateWorkspacePage)
	ops["DeleteWorkspacePage"] = typedCtx(b.DeleteWorkspacePage)
	ops["ListWorkspacePages"] = typed(func(in *ListWorkspacePagesInput) (*map[string]any, error) {
		items, next, err := b.ListWorkspacePages(in)

		return listBody("WorkspacePageList", orEmpty(items), next), err
	})
	ops["ImportWorkspaceMedia"] = typedCtx(b.ImportWorkspaceMedia)
	ops["DeleteWorkspaceMedia"] = typedCtx(b.DeleteWorkspaceMedia)
	ops["ListWorkspaceMedia"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		items, err := b.ListWorkspaceMedia(instOf(req), req.Params["WorkspaceId"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"Media": orEmpty(items)}, 0, nil
	}
}
