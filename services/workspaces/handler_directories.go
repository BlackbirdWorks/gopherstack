package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildDirectoriesOps returns the map of workspace directory operations.
func (h *Handler) buildDirectoriesOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DescribeWorkspaceDirectories": service.WrapOp(h.handleDescribeWorkspaceDirectories),
		"RegisterWorkspaceDirectory":   service.WrapOp(h.handleRegisterWorkspaceDirectory),
		"DeregisterWorkspaceDirectory": service.WrapOp(h.handleDeregisterWorkspaceDirectory),
		"ModifyWorkspaceCreationProperties": service.WrapOp(
			h.handleModifyWorkspaceCreationProperties,
		),
	}
}

// --- DescribeWorkspaceDirectories ---

type describeDirectoriesInput struct {
	NextToken    string   `json:"NextToken"`
	DirectoryIDs []string `json:"DirectoryIds"`
}

type describeDirectoriesOutput struct {
	NextToken   string    `json:"NextToken,omitempty"`
	Directories []dirResp `json:"Directories"`
}

type dirResp struct {
	DirectoryID   string   `json:"DirectoryId"`
	DirectoryName string   `json:"DirectoryName,omitempty"`
	DirectoryType string   `json:"DirectoryType,omitempty"`
	Alias         string   `json:"Alias,omitempty"`
	State         string   `json:"State"`
	SubnetIds     []string `json:"SubnetIds,omitempty"` //nolint:revive // AWS API uses SubnetIds capitalization
}

func (h *Handler) handleDescribeWorkspaceDirectories(
	ctx context.Context, req *describeDirectoriesInput,
) (*describeDirectoriesOutput, error) {
	dirs, nextToken, err := h.Backend.DescribeWorkspaceDirectories(
		ctx,
		req.DirectoryIDs,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]dirResp, 0, len(dirs))
	for _, d := range dirs {
		items = append(items, dirResp{
			DirectoryID:   d.DirectoryID,
			DirectoryName: d.DirectoryName,
			DirectoryType: d.DirectoryType,
			Alias:         d.Alias,
			State:         d.State,
			SubnetIds:     d.SubnetIDs,
		})
	}

	return &describeDirectoriesOutput{Directories: items, NextToken: nextToken}, nil
}

type registerWorkspaceDirectoryInput struct {
	DirectoryId       string    `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	SubnetIds         []string  `json:"SubnetIds"`   //nolint:revive // existing issue.
	Tags              []tagItem `json:"Tags"`
	EnableSelfService bool      `json:"EnableSelfService"`
}

type registerWorkspaceDirectoryOutput struct {
	DirectoryId string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	State       string `json:"State"`
}

func (h *Handler) handleRegisterWorkspaceDirectory(
	_ context.Context, req *registerWorkspaceDirectoryInput,
) (*registerWorkspaceDirectoryOutput, error) {
	if err := h.Backend.RegisterWorkspaceDirectory(req.DirectoryId, req.SubnetIds); err != nil {
		return nil, err
	}

	return &registerWorkspaceDirectoryOutput{
		DirectoryId: req.DirectoryId,
		State:       stateRegistered,
	}, nil
}

type deregisterWorkspaceDirectoryInput struct {
	DirectoryId string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeregisterWorkspaceDirectory(
	_ context.Context, req *deregisterWorkspaceDirectoryInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeregisterWorkspaceDirectory(req.DirectoryId)
}

type modifyWorkspaceCreationPropertiesInput struct {
	ResourceId                  string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	WorkspaceCreationProperties struct {
		DefaultOu                       string `json:"DefaultOu"`
		CustomSecurityGroupId           string `json:"CustomSecurityGroupId"` //nolint:revive,staticcheck // existing issue.
		EnableWorkDocs                  bool   `json:"EnableWorkDocs"`
		EnableInternetAccess            bool   `json:"EnableInternetAccess"`
		UserEnabledAsLocalAdministrator bool   `json:"UserEnabledAsLocalAdministrator"`
		EnableMaintenanceMode           bool   `json:"EnableMaintenanceMode"`
	} `json:"WorkspaceCreationProperties"`
}

func (h *Handler) handleModifyWorkspaceCreationProperties(
	_ context.Context, req *modifyWorkspaceCreationPropertiesInput,
) (*emptyOutput, error) {
	props := map[string]string{
		"DefaultOu":             req.WorkspaceCreationProperties.DefaultOu,
		"CustomSecurityGroupId": req.WorkspaceCreationProperties.CustomSecurityGroupId,
	}

	return &emptyOutput{}, h.Backend.ModifyWorkspaceCreationProperties(req.ResourceId, props)
}
