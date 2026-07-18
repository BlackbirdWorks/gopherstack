package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildPoolsOps returns the map of WorkSpaces pool operations.
func (h *Handler) buildPoolsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateWorkspacesPool":           service.WrapOp(h.handleCreateWorkspacesPool),
		"DescribeWorkspacesPools":        service.WrapOp(h.handleDescribeWorkspacesPools),
		"StartWorkspacesPool":            service.WrapOp(h.handleStartWorkspacesPool),
		"StopWorkspacesPool":             service.WrapOp(h.handleStopWorkspacesPool),
		"TerminateWorkspacesPool":        service.WrapOp(h.handleTerminateWorkspacesPool),
		"UpdateWorkspacesPool":           service.WrapOp(h.handleUpdateWorkspacesPool),
		"DescribeWorkspacesPoolSessions": service.WrapOp(h.handleDescribeWorkspacesPoolSessions),
		"TerminateWorkspacesPoolSession": service.WrapOp(h.handleTerminateWorkspacesPoolSession),
	}
}

type createWorkspacesPoolInput struct {
	PoolName    string    `json:"PoolName"`
	BundleId    string    `json:"BundleId"`    //nolint:revive,staticcheck // existing issue.
	DirectoryId string    `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	Description string    `json:"Description"`
	Tags        []tagItem `json:"Tags"`
	Capacity    struct {
		DesiredUserSessions int32 `json:"DesiredUserSessions"`
	} `json:"Capacity"`
}

type workspacesPoolResp struct {
	PoolId      string `json:"PoolId"` //nolint:revive,staticcheck // existing issue.
	PoolArn     string `json:"PoolArn"`
	PoolName    string `json:"PoolName"`
	BundleId    string `json:"BundleId"`    //nolint:revive,staticcheck // existing issue.
	DirectoryId string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	Description string `json:"Description"`
	State       string `json:"State"`
	CreatedAt   string `json:"CreatedAt,omitempty"`
}

type createWorkspacesPoolOutput struct {
	WorkspacesPool workspacesPoolResp `json:"WorkspacesPool"`
}

func (h *Handler) handleCreateWorkspacesPool(
	_ context.Context, req *createWorkspacesPoolInput,
) (*createWorkspacesPoolOutput, error) {
	pool, err := h.Backend.CreateWorkspacesPool(
		req.PoolName, req.BundleId, req.DirectoryId, req.Description, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createWorkspacesPoolOutput{WorkspacesPool: toPoolResp(pool)}, nil
}

func toPoolResp(p *storedPool) workspacesPoolResp {
	return workspacesPoolResp{
		PoolId:      p.PoolID,
		PoolArn:     p.PoolArn,
		PoolName:    p.PoolName,
		BundleId:    p.BundleID,
		DirectoryId: p.DirectoryID,
		Description: p.Description,
		State:       p.State,
		CreatedAt:   p.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

type describeWorkspacesPoolsInput struct {
	NextToken string   `json:"NextToken"`
	PoolIds   []string `json:"PoolIds"` //nolint:revive // existing issue.
	Limit     int32    `json:"Limit"`
}

type describeWorkspacesPoolsOutput struct {
	NextToken       string               `json:"NextToken,omitempty"`
	WorkspacesPools []workspacesPoolResp `json:"WorkspacesPools"`
}

func (h *Handler) handleDescribeWorkspacesPools(
	_ context.Context, req *describeWorkspacesPoolsInput,
) (*describeWorkspacesPoolsOutput, error) {
	pools, nextToken, err := h.Backend.DescribeWorkspacesPools(
		req.PoolIds,
		req.Limit,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspacesPoolResp, 0, len(pools))
	for _, p := range pools {
		items = append(items, toPoolResp(p))
	}

	return &describeWorkspacesPoolsOutput{WorkspacesPools: items, NextToken: nextToken}, nil
}

type startWorkspacesPoolInput struct {
	PoolId string `json:"PoolId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleStartWorkspacesPool(
	_ context.Context,
	req *startWorkspacesPoolInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StartWorkspacesPool(req.PoolId)
}

type stopWorkspacesPoolInput struct {
	PoolId string `json:"PoolId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleStopWorkspacesPool(
	_ context.Context,
	req *stopWorkspacesPoolInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StopWorkspacesPool(req.PoolId)
}

type terminateWorkspacesPoolInput struct {
	PoolId string `json:"PoolId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleTerminateWorkspacesPool(
	_ context.Context, req *terminateWorkspacesPoolInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.TerminateWorkspacesPool(req.PoolId)
}

type updateWorkspacesPoolInput struct {
	PoolId      string `json:"PoolId"` //nolint:revive,staticcheck // existing issue.
	Description string `json:"Description"`
	BundleId    string `json:"BundleId"`    //nolint:revive,staticcheck // existing issue.
	DirectoryId string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
}

type updateWorkspacesPoolOutput struct {
	WorkspacesPool workspacesPoolResp `json:"WorkspacesPool"`
}

func (h *Handler) handleUpdateWorkspacesPool(
	_ context.Context, req *updateWorkspacesPoolInput,
) (*updateWorkspacesPoolOutput, error) {
	pool, err := h.Backend.UpdateWorkspacesPool(
		req.PoolId,
		req.Description,
		req.BundleId,
		req.DirectoryId,
	)
	if err != nil {
		return nil, err
	}

	return &updateWorkspacesPoolOutput{WorkspacesPool: toPoolResp(pool)}, nil
}

type describeWorkspacesPoolSessionsInput struct {
	PoolId    string `json:"PoolId"` //nolint:revive,staticcheck // existing issue.
	UserId    string `json:"UserId"` //nolint:revive,staticcheck // existing issue.
	NextToken string `json:"NextToken"`
	Limit     int32  `json:"Limit"`
}

type poolSessionResp struct {
	SessionId string `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
	PoolId    string `json:"PoolId"`    //nolint:revive,staticcheck // existing issue.
	UserId    string `json:"UserId"`    //nolint:revive,staticcheck // existing issue.
}

type describeWorkspacesPoolSessionsOutput struct {
	NextToken string            `json:"NextToken,omitempty"`
	Sessions  []poolSessionResp `json:"Sessions"`
}

func (h *Handler) handleDescribeWorkspacesPoolSessions(
	_ context.Context, req *describeWorkspacesPoolSessionsInput,
) (*describeWorkspacesPoolSessionsOutput, error) {
	sessions, nextToken, err := h.Backend.DescribeWorkspacesPoolSessions(
		req.PoolId, req.UserId, req.Limit, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]poolSessionResp, 0, len(sessions))
	for _, s := range sessions {
		items = append(
			items,
			poolSessionResp{SessionId: s.SessionID, PoolId: s.PoolID, UserId: s.UserID},
		)
	}

	return &describeWorkspacesPoolSessionsOutput{Sessions: items, NextToken: nextToken}, nil
}

type terminateWorkspacesPoolSessionInput struct {
	SessionId string `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleTerminateWorkspacesPoolSession(
	_ context.Context, req *terminateWorkspacesPoolSessionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.TerminateWorkspacesPoolSession(req.SessionId)
}
