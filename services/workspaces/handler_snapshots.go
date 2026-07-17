package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildSnapshotsOps returns the map of workspace snapshot operations.
func (h *Handler) buildSnapshotsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DescribeWorkspaceSnapshots": service.WrapOp(h.handleDescribeWorkspaceSnapshots),
	}
}

type describeWorkspaceSnapshotsInput struct {
	WorkspaceId string `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
}

type describeWorkspaceSnapshotsOutput struct {
	RebuildSnapshots []any `json:"RebuildSnapshots"`
	RestoreSnapshots []any `json:"RestoreSnapshots"`
}

func (h *Handler) handleDescribeWorkspaceSnapshots(
	_ context.Context, _ *describeWorkspaceSnapshotsInput,
) (*describeWorkspaceSnapshotsOutput, error) {
	return &describeWorkspaceSnapshotsOutput{
		RebuildSnapshots: []any{},
		RestoreSnapshots: []any{},
	}, nil
}
