package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildAppendixAOps returns the map of appendix-A operations.
func (h *Handler) buildAppendixAOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		// IP Groups
		"CreateIpGroup":        service.WrapOp(h.handleCreateIpGroup),
		"DescribeIpGroups":     service.WrapOp(h.handleDescribeIpGroups),
		"DeleteIpGroup":        service.WrapOp(h.handleDeleteIpGroup),
		"AuthorizeIpRules":     service.WrapOp(h.handleAuthorizeIpRules),
		"RevokeIpRules":        service.WrapOp(h.handleRevokeIpRules),
		"UpdateRulesOfIpGroup": service.WrapOp(h.handleUpdateRulesOfIpGroup),
		"AssociateIpGroups":    service.WrapOp(h.handleAssociateIpGroups),
		"DisassociateIpGroups": service.WrapOp(h.handleDisassociateIpGroups),
		// Connection Aliases
		"CreateConnectionAlias":       service.WrapOp(h.handleCreateConnectionAlias),
		"DescribeConnectionAliases":   service.WrapOp(h.handleDescribeConnectionAliases),
		"DeleteConnectionAlias":       service.WrapOp(h.handleDeleteConnectionAlias),
		"AssociateConnectionAlias":    service.WrapOp(h.handleAssociateConnectionAlias),
		"DisassociateConnectionAlias": service.WrapOp(h.handleDisassociateConnectionAlias),
		"DescribeConnectionAliasPermissions": service.WrapOp(
			h.handleDescribeConnectionAliasPermissions,
		),
		"UpdateConnectionAliasPermission": service.WrapOp(
			h.handleUpdateConnectionAliasPermission,
		),
		// Bundles
		"CreateWorkspaceBundle": service.WrapOp(h.handleCreateWorkspaceBundle),
		"DeleteWorkspaceBundle": service.WrapOp(h.handleDeleteWorkspaceBundle),
		"UpdateWorkspaceBundle": service.WrapOp(h.handleUpdateWorkspaceBundle),
		// Images
		"CopyWorkspaceImage":          service.WrapOp(h.handleCopyWorkspaceImage),
		"CreateWorkspaceImage":        service.WrapOp(h.handleCreateWorkspaceImage),
		"DeleteWorkspaceImage":        service.WrapOp(h.handleDeleteWorkspaceImage),
		"ImportWorkspaceImage":        service.WrapOp(h.handleImportWorkspaceImage),
		"ImportCustomWorkspaceImage":  service.WrapOp(h.handleImportCustomWorkspaceImage),
		"CreateUpdatedWorkspaceImage": service.WrapOp(h.handleCreateUpdatedWorkspaceImage),
		"DescribeWorkspaceImages":     service.WrapOp(h.handleDescribeWorkspaceImages),
		"DescribeWorkspaceImagePermissions": service.WrapOp(
			h.handleDescribeWorkspaceImagePermissions,
		),
		"UpdateWorkspaceImagePermission": service.WrapOp(
			h.handleUpdateWorkspaceImagePermission,
		),
		"DescribeCustomWorkspaceImageImport": service.WrapOp(
			h.handleDescribeCustomWorkspaceImageImport,
		),
		// Pools
		"CreateWorkspacesPool":           service.WrapOp(h.handleCreateWorkspacesPool),
		"DescribeWorkspacesPools":        service.WrapOp(h.handleDescribeWorkspacesPools),
		"StartWorkspacesPool":            service.WrapOp(h.handleStartWorkspacesPool),
		"StopWorkspacesPool":             service.WrapOp(h.handleStopWorkspacesPool),
		"TerminateWorkspacesPool":        service.WrapOp(h.handleTerminateWorkspacesPool),
		"UpdateWorkspacesPool":           service.WrapOp(h.handleUpdateWorkspacesPool),
		"DescribeWorkspacesPoolSessions": service.WrapOp(h.handleDescribeWorkspacesPoolSessions),
		"TerminateWorkspacesPoolSession": service.WrapOp(h.handleTerminateWorkspacesPoolSession),
		// Directories
		"RegisterWorkspaceDirectory":   service.WrapOp(h.handleRegisterWorkspaceDirectory),
		"DeregisterWorkspaceDirectory": service.WrapOp(h.handleDeregisterWorkspaceDirectory),
		// Account
		"DescribeAccount":              service.WrapOp(h.handleDescribeAccount),
		"DescribeAccountModifications": service.WrapOp(h.handleDescribeAccountModifications),
		"ModifyAccount":                service.WrapOp(h.handleModifyAccount),
		"ModifyEndpointEncryptionMode": service.WrapOp(h.handleModifyEndpointEncryptionMode),
		// Connect Client Add-Ins
		"CreateConnectClientAddIn":    service.WrapOp(h.handleCreateConnectClientAddIn),
		"DeleteConnectClientAddIn":    service.WrapOp(h.handleDeleteConnectClientAddIn),
		"DescribeConnectClientAddIns": service.WrapOp(h.handleDescribeConnectClientAddIns),
		"UpdateConnectClientAddIn":    service.WrapOp(h.handleUpdateConnectClientAddIn),
		// Client Branding
		"ImportClientBranding":   service.WrapOp(h.handleImportClientBranding),
		"DescribeClientBranding": service.WrapOp(h.handleDescribeClientBranding),
		"DeleteClientBranding":   service.WrapOp(h.handleDeleteClientBranding),
		// Client Properties
		"DescribeClientProperties": service.WrapOp(h.handleDescribeClientProperties),
		"ModifyClientProperties":   service.WrapOp(h.handleModifyClientProperties),
		// Directory modify ops
		"ModifyCertificateBasedAuthProperties": service.WrapOp(
			h.handleModifyCertificateBasedAuthProperties,
		),
		"ModifySamlProperties": service.WrapOp(h.handleModifySamlProperties),
		"ModifySelfservicePermissions": service.WrapOp(
			h.handleModifySelfservicePermissions,
		),
		"ModifyStreamingProperties": service.WrapOp(h.handleModifyStreamingProperties),
		"ModifyWorkspaceAccessProperties": service.WrapOp(
			h.handleModifyWorkspaceAccessProperties,
		),
		"ModifyWorkspaceCreationProperties": service.WrapOp(
			h.handleModifyWorkspaceCreationProperties,
		),
		// Account Links
		"CreateAccountLinkInvitation": service.WrapOp(h.handleCreateAccountLinkInvitation),
		"AcceptAccountLinkInvitation": service.WrapOp(h.handleAcceptAccountLinkInvitation),
		"RejectAccountLinkInvitation": service.WrapOp(h.handleRejectAccountLinkInvitation),
		"DeleteAccountLinkInvitation": service.WrapOp(h.handleDeleteAccountLinkInvitation),
		"GetAccountLink":              service.WrapOp(h.handleGetAccountLink),
		"ListAccountLinks":            service.WrapOp(h.handleListAccountLinks),
		// Applications
		"AssociateWorkspaceApplication": service.WrapOp(h.handleAssociateWorkspaceApplication),
		"DisassociateWorkspaceApplication": service.WrapOp(
			h.handleDisassociateWorkspaceApplication,
		),
		"DeployWorkspaceApplications":     service.WrapOp(h.handleDeployWorkspaceApplications),
		"DescribeWorkspaceAssociations":   service.WrapOp(h.handleDescribeWorkspaceAssociations),
		"DescribeApplicationAssociations": service.WrapOp(h.handleDescribeApplicationAssociations),
		"DescribeApplications":            service.WrapOp(h.handleDescribeApplications),
		"DescribeImageAssociations":       service.WrapOp(h.handleDescribeImageAssociations),
		"DescribeBundleAssociations":      service.WrapOp(h.handleDescribeBundleAssociations),
		// Workspace-level ops
		"MigrateWorkspace":           service.WrapOp(h.handleMigrateWorkspace),
		"RestoreWorkspace":           service.WrapOp(h.handleRestoreWorkspace),
		"DescribeWorkspaceSnapshots": service.WrapOp(h.handleDescribeWorkspaceSnapshots),
		"CreateStandbyWorkspaces":    service.WrapOp(h.handleCreateStandbyWorkspaces),
		// Other
		"ListAvailableManagementCidrRanges": service.WrapOp(
			h.handleListAvailableManagementCidrRanges,
		),
	}
}

// =============================================================================
// IP Groups
// =============================================================================

type createIpGroupInput struct { //nolint:revive,staticcheck // existing issue.
	GroupName string       `json:"GroupName"`
	GroupDesc string       `json:"GroupDesc"`
	Tags      []tagItem    `json:"Tags"`
	UserRules []ipRuleItem `json:"UserRules"`
}

type createIpGroupOutput struct { //nolint:revive,staticcheck // existing issue.
	GroupId string `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateIpGroup( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *createIpGroupInput,
) (*createIpGroupOutput, error) {
	tags := tagsToMap(req.Tags)

	id, err := h.Backend.CreateIpGroup(req.GroupName, req.GroupDesc, req.UserRules, tags)
	if err != nil {
		return nil, err
	}

	return &createIpGroupOutput{GroupId: id}, nil
}

type describeIpGroupsInput struct { //nolint:revive,staticcheck // existing issue.
	NextToken  string   `json:"NextToken"`
	GroupIds   []string `json:"GroupIds"` //nolint:revive // existing issue.
	MaxResults int32    `json:"MaxResults"`
}

type workspacesIpGroupResp struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string       `json:"groupId"` //nolint:revive,staticcheck // existing issue.
	GroupName string       `json:"groupName"`
	GroupDesc string       `json:"groupDesc"`
	UserRules []ipRuleItem `json:"userRules"`
}

type describeIpGroupsOutput struct { //nolint:revive,staticcheck // existing issue.
	NextToken string                  `json:"NextToken,omitempty"`
	Result    []workspacesIpGroupResp `json:"Result"`
}

func (h *Handler) handleDescribeIpGroups( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *describeIpGroupsInput,
) (*describeIpGroupsOutput, error) {
	groups, nextToken, err := h.Backend.DescribeIpGroups(
		req.GroupIds,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspacesIpGroupResp, 0, len(groups))
	for _, g := range groups {
		items = append(items, workspacesIpGroupResp{
			GroupId:   g.GroupID,
			GroupName: g.GroupName,
			GroupDesc: g.GroupDesc,
			UserRules: g.UserRules,
		})
	}

	return &describeIpGroupsOutput{Result: items, NextToken: nextToken}, nil
}

type deleteIpGroupInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId string `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteIpGroup( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *deleteIpGroupInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteIpGroup(req.GroupId)
}

type authorizeIpRulesInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string       `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
	UserRules []ipRuleItem `json:"UserRules"`
}

func (h *Handler) handleAuthorizeIpRules( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *authorizeIpRulesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.AuthorizeIpRules(req.GroupId, req.UserRules)
}

type revokeIpRulesInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string   `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
	UserRules []string `json:"UserRules"`
}

func (h *Handler) handleRevokeIpRules( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *revokeIpRulesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.RevokeIpRules(req.GroupId, req.UserRules)
}

type updateRulesOfIpGroupInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string       `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
	UserRules []ipRuleItem `json:"UserRules"`
}

func (h *Handler) handleUpdateRulesOfIpGroup( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *updateRulesOfIpGroupInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateRulesOfIpGroup(req.GroupId, req.UserRules)
}

type associateIpGroupsInput struct { //nolint:revive,staticcheck // existing issue.
	DirectoryId string   `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	GroupIds    []string `json:"GroupIds"`    //nolint:revive // existing issue.
}

func (h *Handler) handleAssociateIpGroups( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *associateIpGroupsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.AssociateIpGroups(req.DirectoryId, req.GroupIds)
}

type disassociateIpGroupsInput struct { //nolint:revive,staticcheck // existing issue.
	DirectoryId string   `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	GroupIds    []string `json:"GroupIds"`    //nolint:revive // existing issue.
}

func (h *Handler) handleDisassociateIpGroups( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *disassociateIpGroupsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DisassociateIpGroups(req.DirectoryId, req.GroupIds)
}

// =============================================================================
// Connection Aliases
// =============================================================================

type createConnectionAliasInput struct {
	ConnectionString string    `json:"ConnectionString"`
	Tags             []tagItem `json:"Tags"`
}

type createConnectionAliasOutput struct {
	AliasId string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateConnectionAlias(
	_ context.Context, req *createConnectionAliasInput,
) (*createConnectionAliasOutput, error) {
	id, err := h.Backend.CreateConnectionAlias(req.ConnectionString, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return &createConnectionAliasOutput{AliasId: id}, nil
}

type describeConnectionAliasesInput struct {
	ResourceId string   `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string   `json:"NextToken"`
	AliasIds   []string `json:"AliasIds"` //nolint:revive // existing issue.
	Limit      int32    `json:"Limit"`
}

type connAliasResp struct {
	AliasId              string               `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	ConnectionString     string               `json:"ConnectionString"`
	State                string               `json:"State"`
	OwnerAccountId       string               `json:"OwnerAccountId"` //nolint:revive,staticcheck // existing issue.
	ConnectionIdentifier string               `json:"ConnectionIdentifier,omitempty"`
	Associations         []connAliasAssocResp `json:"Associations,omitempty"`
}

type connAliasAssocResp struct {
	AssociationStatus    string `json:"AssociationStatus"`
	AssociatedAccountId  string `json:"AssociatedAccountId,omitempty"` //nolint:revive,staticcheck // existing issue.
	ResourceId           string `json:"ResourceId,omitempty"`          //nolint:revive,staticcheck // existing issue.
	ConnectionIdentifier string `json:"ConnectionIdentifier,omitempty"`
}

type describeConnectionAliasesOutput struct {
	NextToken         string          `json:"NextToken,omitempty"`
	ConnectionAliases []connAliasResp `json:"ConnectionAliases"`
}

func (h *Handler) handleDescribeConnectionAliases(
	_ context.Context, req *describeConnectionAliasesInput,
) (*describeConnectionAliasesOutput, error) {
	aliases, nextToken, err := h.Backend.DescribeConnectionAliases(
		req.AliasIds, req.ResourceId, req.Limit, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]connAliasResp, 0, len(aliases))
	for _, a := range aliases {
		item := connAliasResp{
			AliasId:              a.AliasID,
			ConnectionString:     a.ConnectionString,
			State:                a.State,
			OwnerAccountId:       a.OwnerAccountID,
			ConnectionIdentifier: a.ConnectionIdentifier,
		}
		if a.AssociatedResource != "" {
			item.Associations = []connAliasAssocResp{{
				AssociationStatus:    "ASSOCIATED_WITH_OWNER_ACCOUNT",
				ResourceId:           a.AssociatedResource,
				ConnectionIdentifier: a.ConnectionIdentifier,
			}}
		}
		items = append(items, item)
	}

	return &describeConnectionAliasesOutput{ConnectionAliases: items, NextToken: nextToken}, nil
}

type deleteConnectionAliasInput struct {
	AliasId string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteConnectionAlias(
	_ context.Context, req *deleteConnectionAliasInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteConnectionAlias(req.AliasId)
}

type associateConnectionAliasInput struct {
	AliasId    string `json:"AliasId"`    //nolint:revive,staticcheck // existing issue.
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
}

type associateConnectionAliasOutput struct {
	ConnectionIdentifier string `json:"ConnectionIdentifier"`
}

func (h *Handler) handleAssociateConnectionAlias(
	_ context.Context, req *associateConnectionAliasInput,
) (*associateConnectionAliasOutput, error) {
	ci, err := h.Backend.AssociateConnectionAlias(req.AliasId, req.ResourceId)
	if err != nil {
		return nil, err
	}

	return &associateConnectionAliasOutput{ConnectionIdentifier: ci}, nil
}

type disassociateConnectionAliasInput struct {
	AliasId string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDisassociateConnectionAlias(
	_ context.Context, req *disassociateConnectionAliasInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DisassociateConnectionAlias(req.AliasId)
}

type describeConnectionAliasPermissionsInput struct {
	AliasId    string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type connAliasPermResp struct {
	SharedAccountId  string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
	AllowAssociation bool   `json:"AllowAssociation"`
}

type describeConnectionAliasPermissionsOutput struct {
	AliasId                    string              `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	NextToken                  string              `json:"NextToken,omitempty"`
	ConnectionAliasPermissions []connAliasPermResp `json:"ConnectionAliasPermissions"`
}

func (h *Handler) handleDescribeConnectionAliasPermissions(
	_ context.Context, req *describeConnectionAliasPermissionsInput,
) (*describeConnectionAliasPermissionsOutput, error) {
	aliasID, perms, nextToken, err := h.Backend.DescribeConnectionAliasPermissions(
		req.AliasId, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]connAliasPermResp, 0, len(perms))
	for _, p := range perms {
		items = append(items, connAliasPermResp{
			SharedAccountId:  p.AccountID,
			AllowAssociation: p.AllowAssociation,
		})
	}

	return &describeConnectionAliasPermissionsOutput{
		AliasId:                    aliasID,
		ConnectionAliasPermissions: items,
		NextToken:                  nextToken,
	}, nil
}

type updateConnectionAliasPermissionInput struct {
	AliasId                   string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	ConnectionAliasPermission struct {
		SharedAccountId  string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
		AllowAssociation bool   `json:"AllowAssociation"`
	} `json:"ConnectionAliasPermission"`
}

func (h *Handler) handleUpdateConnectionAliasPermission(
	_ context.Context, req *updateConnectionAliasPermissionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateConnectionAliasPermission(
		req.AliasId,
		req.ConnectionAliasPermission.SharedAccountId,
		req.ConnectionAliasPermission.AllowAssociation,
	)
}

// =============================================================================
// Bundles
// =============================================================================

type createWorkspaceBundleInput struct {
	BundleName        string `json:"BundleName"`
	BundleDescription string `json:"BundleDescription"`
	ImageId           string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	ComputeType       struct {
		Name string `json:"Name"`
	} `json:"ComputeType"`
	Tags        []tagItem `json:"Tags"`
	UserStorage struct {
		Capacity int32 `json:"Capacity"`
	} `json:"UserStorage"`
	RootStorage struct {
		Capacity int32 `json:"Capacity"`
	} `json:"RootStorage"`
}

type workspaceBundleResp struct {
	BundleId    string `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
	Name        string `json:"Name"`
	Owner       string `json:"Owner"`
	Description string `json:"Description"`
	ImageId     string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	ComputeType struct {
		Name string `json:"Name"`
	} `json:"ComputeType"`
	UserStorage struct {
		Capacity int32 `json:"Capacity"`
	} `json:"UserStorage"`
	RootStorage struct {
		Capacity int32 `json:"Capacity"`
	} `json:"RootStorage"`
}

type createWorkspaceBundleOutput struct {
	WorkspaceBundle workspaceBundleResp `json:"WorkspaceBundle"`
}

func (h *Handler) handleCreateWorkspaceBundle(
	_ context.Context, req *createWorkspaceBundleInput,
) (*createWorkspaceBundleOutput, error) {
	bun, err := h.Backend.CreateWorkspaceBundle(
		req.BundleName,
		req.BundleDescription,
		req.ImageId,
		req.ComputeType.Name,
		tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	resp := workspaceBundleResp{
		BundleId:    bun.BundleID,
		Name:        bun.Name,
		Description: bun.Description,
		ImageId:     bun.ImageID,
	}
	resp.ComputeType.Name = bun.ComputeType

	return &createWorkspaceBundleOutput{WorkspaceBundle: resp}, nil
}

type deleteWorkspaceBundleInput struct {
	BundleId string `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteWorkspaceBundle(
	_ context.Context, req *deleteWorkspaceBundleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteWorkspaceBundle(req.BundleId)
}

type updateWorkspaceBundleInput struct {
	BundleId string `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
	ImageId  string `json:"ImageId"`  //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleUpdateWorkspaceBundle(
	_ context.Context, req *updateWorkspaceBundleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateWorkspaceBundle(req.BundleId, req.ImageId)
}

// =============================================================================
// Images
// =============================================================================

type copyWorkspaceImageInput struct {
	Name          string    `json:"Name"`
	SourceImageId string    `json:"SourceImageId"` //nolint:revive,staticcheck // existing issue.
	SourceRegion  string    `json:"SourceRegion"`
	Description   string    `json:"Description"`
	Tags          []tagItem `json:"Tags"`
}

type copyWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCopyWorkspaceImage(
	_ context.Context, req *copyWorkspaceImageInput,
) (*copyWorkspaceImageOutput, error) {
	id, err := h.Backend.CopyWorkspaceImage(
		req.Name, req.SourceImageId, req.SourceRegion, req.Description, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &copyWorkspaceImageOutput{ImageId: id}, nil
}

type createWorkspaceImageInput struct {
	Name        string    `json:"Name"`
	Description string    `json:"Description"`
	WorkspaceId string    `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
	Tags        []tagItem `json:"Tags"`
}

type workspaceImageResp struct {
	ImageId     string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	Name        string `json:"Name"`
	Description string `json:"Description"`
	State       string `json:"State"`
	Created     string `json:"Created,omitempty"`
}

type createWorkspaceImageOutput struct {
	ImageId     string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	Name        string `json:"Name"`
	Description string `json:"Description"`
	State       string `json:"State"`
	Created     string `json:"Created,omitempty"`
}

func (h *Handler) handleCreateWorkspaceImage(
	_ context.Context, req *createWorkspaceImageInput,
) (*createWorkspaceImageOutput, error) {
	img, err := h.Backend.CreateWorkspaceImage(
		req.Name,
		req.Description,
		req.WorkspaceId,
		tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createWorkspaceImageOutput{
		ImageId:     img.ImageID,
		Name:        img.Name,
		Description: img.Description,
		State:       img.State,
		Created:     img.Created.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type deleteWorkspaceImageInput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteWorkspaceImage(
	_ context.Context, req *deleteWorkspaceImageInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteWorkspaceImage(req.ImageId)
}

type importWorkspaceImageInput struct {
	Ec2ImageId       string    `json:"Ec2ImageId"` //nolint:revive,staticcheck // existing issue.
	ImageName        string    `json:"ImageName"`
	ImageDescription string    `json:"ImageDescription"`
	Tags             []tagItem `json:"Tags"`
}

type importWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleImportWorkspaceImage(
	_ context.Context, req *importWorkspaceImageInput,
) (*importWorkspaceImageOutput, error) {
	id, err := h.Backend.ImportWorkspaceImage(
		req.Ec2ImageId, req.ImageName, req.ImageDescription, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &importWorkspaceImageOutput{ImageId: id}, nil
}

type importCustomWorkspaceImageInput struct {
	ImageName        string `json:"ImageName"`
	ImageDescription string `json:"ImageDescription"`
}

type importCustomWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	State   string `json:"State"`
}

func (h *Handler) handleImportCustomWorkspaceImage(
	_ context.Context, req *importCustomWorkspaceImageInput,
) (*importCustomWorkspaceImageOutput, error) {
	img, err := h.Backend.ImportCustomWorkspaceImage(req.ImageName, req.ImageDescription)
	if err != nil {
		return nil, err
	}

	return &importCustomWorkspaceImageOutput{ImageId: img.ImageID, State: img.State}, nil
}

type createUpdatedWorkspaceImageInput struct {
	SourceImageId string    `json:"SourceImageId"` //nolint:revive,staticcheck // existing issue.
	Name          string    `json:"Name"`
	Description   string    `json:"Description"`
	Tags          []tagItem `json:"Tags"`
}

type createUpdatedWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateUpdatedWorkspaceImage(
	_ context.Context, req *createUpdatedWorkspaceImageInput,
) (*createUpdatedWorkspaceImageOutput, error) {
	id, err := h.Backend.CreateUpdatedWorkspaceImage(
		req.SourceImageId, req.Name, req.Description, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createUpdatedWorkspaceImageOutput{ImageId: id}, nil
}

type describeWorkspaceImagesInput struct {
	ImageType  string   `json:"ImageType"`
	NextToken  string   `json:"NextToken"`
	ImageIds   []string `json:"ImageIds"` //nolint:revive // existing issue.
	MaxResults int32    `json:"MaxResults"`
}

type describeWorkspaceImagesOutput struct {
	NextToken string               `json:"NextToken,omitempty"`
	Images    []workspaceImageResp `json:"Images"`
}

func (h *Handler) handleDescribeWorkspaceImages(
	_ context.Context, req *describeWorkspaceImagesInput,
) (*describeWorkspaceImagesOutput, error) {
	images, nextToken, err := h.Backend.DescribeWorkspaceImages(
		req.ImageIds, req.ImageType, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceImageResp, 0, len(images))
	for _, img := range images {
		items = append(items, workspaceImageResp{
			ImageId:     img.ImageID,
			Name:        img.Name,
			Description: img.Description,
			State:       img.State,
			Created:     img.Created.Format("2006-01-02T15:04:05Z"),
		})
	}

	return &describeWorkspaceImagesOutput{Images: items, NextToken: nextToken}, nil
}

type describeWorkspaceImagePermissionsInput struct {
	ImageId    string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type imgPermResp struct {
	SharedAccountId string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
	ImagePermission struct {
		AllowCopyImage bool `json:"AllowCopyImage"`
	} `json:"ImagePermission"`
}

type describeWorkspaceImagePermissionsOutput struct {
	ImageId          string        `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	NextToken        string        `json:"NextToken,omitempty"`
	ImagePermissions []imgPermResp `json:"ImagePermissions"`
}

func (h *Handler) handleDescribeWorkspaceImagePermissions(
	_ context.Context, req *describeWorkspaceImagePermissionsInput,
) (*describeWorkspaceImagePermissionsOutput, error) {
	imageID, perms, err := h.Backend.DescribeWorkspaceImagePermissions(req.ImageId)
	if err != nil {
		return nil, err
	}

	items := make([]imgPermResp, 0, len(perms))
	for accountID, allowCopy := range perms {
		r := imgPermResp{SharedAccountId: accountID}
		r.ImagePermission.AllowCopyImage = allowCopy
		items = append(items, r)
	}

	return &describeWorkspaceImagePermissionsOutput{
		ImageId:          imageID,
		ImagePermissions: items,
	}, nil
}

type updateWorkspaceImagePermissionInput struct {
	ImageId         string `json:"ImageId"`         //nolint:revive,staticcheck // existing issue.
	SharedAccountId string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
	AllowCopyImage  bool   `json:"AllowCopyImage"`
}

func (h *Handler) handleUpdateWorkspaceImagePermission(
	_ context.Context, req *updateWorkspaceImagePermissionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateWorkspaceImagePermission(
		req.ImageId, req.SharedAccountId, req.AllowCopyImage,
	)
}

type describeCustomWorkspaceImageImportInput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

type describeCustomWorkspaceImageImportOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	State   string `json:"State"`
	Created string `json:"Created,omitempty"`
}

func (h *Handler) handleDescribeCustomWorkspaceImageImport(
	_ context.Context, req *describeCustomWorkspaceImageImportInput,
) (*describeCustomWorkspaceImageImportOutput, error) {
	img, err := h.Backend.DescribeCustomWorkspaceImageImport(req.ImageId)
	if err != nil {
		return nil, err
	}

	return &describeCustomWorkspaceImageImportOutput{
		ImageId: img.ImageID,
		State:   img.State,
		Created: img.Created.Format("2006-01-02T15:04:05Z"),
	}, nil
}

// =============================================================================
// Pools
// =============================================================================

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

// =============================================================================
// Directories
// =============================================================================

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

// =============================================================================
// Account
// =============================================================================

type describeAccountOutput struct {
	DedicatedTenancySupport             string `json:"DedicatedTenancySupport"`
	DedicatedTenancyManagementCidrRange string `json:"DedicatedTenancyManagementCidrRange,omitempty"`
}

func (h *Handler) handleDescribeAccount(
	_ context.Context,
	_ *emptyOutput,
) (*describeAccountOutput, error) {
	cfg := h.Backend.DescribeAccount()

	return &describeAccountOutput{
		DedicatedTenancySupport:             cfg.DedicatedTenancySupport,
		DedicatedTenancyManagementCidrRange: cfg.ManagementCidrRange,
	}, nil
}

type describeAccountModificationsOutput struct {
	NextToken            string `json:"NextToken,omitempty"`
	AccountModifications []any  `json:"AccountModifications"`
}

func (h *Handler) handleDescribeAccountModifications(
	_ context.Context, _ *emptyOutput,
) (*describeAccountModificationsOutput, error) {
	return &describeAccountModificationsOutput{AccountModifications: []any{}}, nil
}

type modifyAccountInput struct {
	DedicatedTenancyManagementCidrRange string `json:"DedicatedTenancyManagementCidrRange"`
	DedicatedTenancySupport             string `json:"DedicatedTenancySupport"`
}

func (h *Handler) handleModifyAccount(
	_ context.Context,
	req *modifyAccountInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.ModifyAccount(
		req.DedicatedTenancyManagementCidrRange, req.DedicatedTenancySupport,
	)
}

type modifyEndpointEncryptionModeInput struct {
	DirectoryId            string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	EndpointEncryptionMode string `json:"EndpointEncryptionMode"`
}

func (h *Handler) handleModifyEndpointEncryptionMode(
	_ context.Context, req *modifyEndpointEncryptionModeInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.ModifyEndpointEncryptionMode(
		req.DirectoryId, req.EndpointEncryptionMode,
	)
}

// =============================================================================
// Connect Client Add-Ins
// =============================================================================

type createConnectClientAddInInput struct {
	Name       string `json:"Name"`
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	URL        string `json:"URL"`
}

type createConnectClientAddInOutput struct {
	AddInId string `json:"AddInId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateConnectClientAddIn(
	_ context.Context, req *createConnectClientAddInInput,
) (*createConnectClientAddInOutput, error) {
	id, err := h.Backend.CreateConnectClientAddIn(req.Name, req.ResourceId, req.URL)
	if err != nil {
		return nil, err
	}

	return &createConnectClientAddInOutput{AddInId: id}, nil
}

type deleteConnectClientAddInInput struct {
	AddInId    string `json:"AddInId"`    //nolint:revive,staticcheck // existing issue.
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteConnectClientAddIn(
	_ context.Context, req *deleteConnectClientAddInInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteConnectClientAddIn(req.AddInId, req.ResourceId)
}

type describeConnectClientAddInsInput struct {
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type connectAddInResp struct {
	AddInId string `json:"AddInId"` //nolint:revive,staticcheck // existing issue.
	Name    string `json:"Name"`
	URL     string `json:"URL"`
}

type describeConnectClientAddInsOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	AddIns    []connectAddInResp `json:"AddIns"`
}

func (h *Handler) handleDescribeConnectClientAddIns(
	_ context.Context, req *describeConnectClientAddInsInput,
) (*describeConnectClientAddInsOutput, error) {
	addIns, nextToken, err := h.Backend.DescribeConnectClientAddIns(
		req.ResourceId, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]connectAddInResp, 0, len(addIns))
	for _, a := range addIns {
		items = append(items, connectAddInResp{AddInId: a.AddInID, Name: a.Name, URL: a.URL})
	}

	return &describeConnectClientAddInsOutput{AddIns: items, NextToken: nextToken}, nil
}

type updateConnectClientAddInInput struct {
	AddInId    string `json:"AddInId"`    //nolint:revive,staticcheck // existing issue.
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	Name       string `json:"Name"`
	URL        string `json:"URL"`
}

func (h *Handler) handleUpdateConnectClientAddIn(
	_ context.Context, req *updateConnectClientAddInInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateConnectClientAddIn(
		req.AddInId, req.ResourceId, req.Name, req.URL,
	)
}

// =============================================================================
// Client Branding
// =============================================================================

type importClientBrandingInput struct {
	DeviceTypeAndroid map[string]any `json:"DeviceTypeAndroid"`
	DeviceTypeIos     map[string]any `json:"DeviceTypeIos"`
	DeviceTypeLinux   map[string]any `json:"DeviceTypeLinux"`
	DeviceTypeOsx     map[string]any `json:"DeviceTypeOsx"`
	DeviceTypeWeb     map[string]any `json:"DeviceTypeWeb"`
	DeviceTypeWindows map[string]any `json:"DeviceTypeWindows"`
	ResourceId        string         `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
}

type importClientBrandingOutput struct {
	DeviceTypeAndroid map[string]any `json:"DeviceTypeAndroid,omitempty"`
	DeviceTypeIos     map[string]any `json:"DeviceTypeIos,omitempty"`
	DeviceTypeLinux   map[string]any `json:"DeviceTypeLinux,omitempty"`
	DeviceTypeOsx     map[string]any `json:"DeviceTypeOsx,omitempty"`
	DeviceTypeWeb     map[string]any `json:"DeviceTypeWeb,omitempty"`
	DeviceTypeWindows map[string]any `json:"DeviceTypeWindows,omitempty"`
}

func (h *Handler) handleImportClientBranding(
	_ context.Context, req *importClientBrandingInput,
) (*importClientBrandingOutput, error) {
	platforms := map[string]map[string]any{}

	if req.DeviceTypeAndroid != nil {
		platforms["DeviceTypeAndroid"] = req.DeviceTypeAndroid
	}
	if req.DeviceTypeIos != nil {
		platforms["DeviceTypeIos"] = req.DeviceTypeIos
	}
	if req.DeviceTypeLinux != nil {
		platforms["DeviceTypeLinux"] = req.DeviceTypeLinux
	}
	if req.DeviceTypeOsx != nil {
		platforms["DeviceTypeOsx"] = req.DeviceTypeOsx
	}
	if req.DeviceTypeWeb != nil {
		platforms["DeviceTypeWeb"] = req.DeviceTypeWeb
	}
	if req.DeviceTypeWindows != nil {
		platforms["DeviceTypeWindows"] = req.DeviceTypeWindows
	}

	if err := h.Backend.ImportClientBranding(req.ResourceId, platforms); err != nil {
		return nil, err
	}

	return &importClientBrandingOutput{
		DeviceTypeAndroid: req.DeviceTypeAndroid,
		DeviceTypeIos:     req.DeviceTypeIos,
		DeviceTypeLinux:   req.DeviceTypeLinux,
		DeviceTypeOsx:     req.DeviceTypeOsx,
		DeviceTypeWeb:     req.DeviceTypeWeb,
		DeviceTypeWindows: req.DeviceTypeWindows,
	}, nil
}

type describeClientBrandingInput struct {
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
}

type describeClientBrandingOutput struct {
	DeviceTypeAndroid map[string]any `json:"DeviceTypeAndroid,omitempty"`
	DeviceTypeIos     map[string]any `json:"DeviceTypeIos,omitempty"`
	DeviceTypeLinux   map[string]any `json:"DeviceTypeLinux,omitempty"`
	DeviceTypeOsx     map[string]any `json:"DeviceTypeOsx,omitempty"`
	DeviceTypeWeb     map[string]any `json:"DeviceTypeWeb,omitempty"`
	DeviceTypeWindows map[string]any `json:"DeviceTypeWindows,omitempty"`
}

func (h *Handler) handleDescribeClientBranding(
	_ context.Context, req *describeClientBrandingInput,
) (*describeClientBrandingOutput, error) {
	platforms, err := h.Backend.DescribeClientBranding(req.ResourceId)
	if err != nil {
		return nil, err
	}

	return &describeClientBrandingOutput{
		DeviceTypeAndroid: platforms["DeviceTypeAndroid"],
		DeviceTypeIos:     platforms["DeviceTypeIos"],
		DeviceTypeLinux:   platforms["DeviceTypeLinux"],
		DeviceTypeOsx:     platforms["DeviceTypeOsx"],
		DeviceTypeWeb:     platforms["DeviceTypeWeb"],
		DeviceTypeWindows: platforms["DeviceTypeWindows"],
	}, nil
}

type deleteClientBrandingInput struct {
	ResourceId string   `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	Platforms  []string `json:"Platforms"`
}

func (h *Handler) handleDeleteClientBranding(
	_ context.Context, req *deleteClientBrandingInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteClientBranding(req.ResourceId, req.Platforms)
}

// =============================================================================
// Client Properties
// =============================================================================

type describeClientPropertiesInput struct {
	ResourceIds []string `json:"ResourceIds"` //nolint:revive // existing issue.
}

type clientPropsResult struct {
	ResourceId       string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	ClientProperties struct {
		ReconnectEnabled string `json:"ReconnectEnabled,omitempty"`
	} `json:"ClientProperties"`
}

type describeClientPropertiesOutput struct {
	ClientPropertiesList []clientPropsResult `json:"ClientPropertiesList"`
}

func (h *Handler) handleDescribeClientProperties(
	_ context.Context, req *describeClientPropertiesInput,
) (*describeClientPropertiesOutput, error) {
	propsMap, err := h.Backend.DescribeClientProperties(req.ResourceIds)
	if err != nil {
		return nil, err
	}

	items := make([]clientPropsResult, 0, len(req.ResourceIds))
	for _, id := range req.ResourceIds {
		r := clientPropsResult{ResourceId: id}
		r.ClientProperties.ReconnectEnabled = propsMap[id].ReconnectEnabled
		items = append(items, r)
	}

	return &describeClientPropertiesOutput{ClientPropertiesList: items}, nil
}

type modifyClientPropertiesInput struct {
	ResourceId       string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	ClientProperties struct {
		ReconnectEnabled string `json:"ReconnectEnabled"`
	} `json:"ClientProperties"`
}

func (h *Handler) handleModifyClientProperties(
	_ context.Context, req *modifyClientPropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.ModifyClientProperties(
		req.ResourceId, req.ClientProperties.ReconnectEnabled,
	)
}

// =============================================================================
// Directory modify ops
// =============================================================================

type modifyCertificateBasedAuthPropertiesInput struct {
	DirectoryId                    string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	CertificateBasedAuthProperties struct {
		Status                  string `json:"Status"`
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	} `json:"CertificateBasedAuthProperties"`
}

func (h *Handler) handleModifyCertificateBasedAuthProperties(
	_ context.Context, req *modifyCertificateBasedAuthPropertiesInput,
) (*emptyOutput, error) {
	props := map[string]string{
		"Status":                  req.CertificateBasedAuthProperties.Status,
		"CertificateAuthorityArn": req.CertificateBasedAuthProperties.CertificateAuthorityArn,
	}

	return &emptyOutput{}, h.Backend.ModifyCertificateBasedAuthProperties(req.DirectoryId, props)
}

type modifySamlPropertiesInput struct {
	DirectoryId    string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	SamlProperties struct {
		Status                  string `json:"Status"`
		UserAccessUrl           string `json:"UserAccessUrl"` //nolint:revive,staticcheck // existing issue.
		RelayStateParameterName string `json:"RelayStateParameterName"`
	} `json:"SamlProperties"`
}

func (h *Handler) handleModifySamlProperties(
	_ context.Context, req *modifySamlPropertiesInput,
) (*emptyOutput, error) {
	props := map[string]string{
		"Status":                  req.SamlProperties.Status,
		"UserAccessUrl":           req.SamlProperties.UserAccessUrl,
		"RelayStateParameterName": req.SamlProperties.RelayStateParameterName,
	}

	return &emptyOutput{}, h.Backend.ModifySamlProperties(req.DirectoryId, props)
}

type modifySelfservicePermissionsInput struct {
	DirectoryId            string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	SelfservicePermissions struct {
		RestartWorkspace   string `json:"RestartWorkspace"`
		IncreaseVolumeSize string `json:"IncreaseVolumeSize"`
		ChangeComputeType  string `json:"ChangeComputeType"`
		SwitchRunningMode  string `json:"SwitchRunningMode"`
		RebuildWorkspace   string `json:"RebuildWorkspace"`
	} `json:"SelfservicePermissions"`
}

func (h *Handler) handleModifySelfservicePermissions(
	_ context.Context, req *modifySelfservicePermissionsInput,
) (*emptyOutput, error) {
	props := map[string]string{
		"RestartWorkspace":   req.SelfservicePermissions.RestartWorkspace,
		"IncreaseVolumeSize": req.SelfservicePermissions.IncreaseVolumeSize,
		"ChangeComputeType":  req.SelfservicePermissions.ChangeComputeType,
		"SwitchRunningMode":  req.SelfservicePermissions.SwitchRunningMode,
		"RebuildWorkspace":   req.SelfservicePermissions.RebuildWorkspace,
	}

	return &emptyOutput{}, h.Backend.ModifySelfservicePermissions(req.DirectoryId, props)
}

type modifyStreamingPropertiesInput struct {
	DirectoryId         string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	StreamingProperties struct {
		StreamingExperiencePreferredProtocol string `json:"StreamingExperiencePreferredProtocol"`
		UserSettings                         []struct {
			Action     string `json:"Action"`
			Permission string `json:"Permission"`
		} `json:"UserSettings"`
	} `json:"StreamingProperties"`
}

func (h *Handler) handleModifyStreamingProperties(
	_ context.Context, req *modifyStreamingPropertiesInput,
) (*emptyOutput, error) {
	props := map[string]string{
		"StreamingExperiencePreferredProtocol": req.StreamingProperties.StreamingExperiencePreferredProtocol,
	}

	return &emptyOutput{}, h.Backend.ModifyStreamingProperties(req.DirectoryId, props)
}

type modifyWorkspaceAccessPropertiesInput struct {
	DirectoryId               string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	WorkspaceAccessProperties struct {
		DeviceTypeWindows    string `json:"DeviceTypeWindows"`
		DeviceTypeOsx        string `json:"DeviceTypeOsx"`
		DeviceTypeWeb        string `json:"DeviceTypeWeb"`
		DeviceTypeIos        string `json:"DeviceTypeIos"`
		DeviceTypeAndroid    string `json:"DeviceTypeAndroid"`
		DeviceTypeChromeos   string `json:"DeviceTypeChromeOs"`
		DeviceTypeZeroclient string `json:"DeviceTypeZeroClient"`
		DeviceTypeLinux      string `json:"DeviceTypeLinux"`
	} `json:"WorkspaceAccessProperties"`
}

func (h *Handler) handleModifyWorkspaceAccessProperties(
	_ context.Context, req *modifyWorkspaceAccessPropertiesInput,
) (*emptyOutput, error) {
	props := map[string]string{
		"DeviceTypeWindows":    req.WorkspaceAccessProperties.DeviceTypeWindows,
		"DeviceTypeOsx":        req.WorkspaceAccessProperties.DeviceTypeOsx,
		"DeviceTypeWeb":        req.WorkspaceAccessProperties.DeviceTypeWeb,
		"DeviceTypeIos":        req.WorkspaceAccessProperties.DeviceTypeIos,
		"DeviceTypeAndroid":    req.WorkspaceAccessProperties.DeviceTypeAndroid,
		"DeviceTypeChromeOs":   req.WorkspaceAccessProperties.DeviceTypeChromeos,
		"DeviceTypeZeroClient": req.WorkspaceAccessProperties.DeviceTypeZeroclient,
		"DeviceTypeLinux":      req.WorkspaceAccessProperties.DeviceTypeLinux,
	}

	return &emptyOutput{}, h.Backend.ModifyWorkspaceAccessProperties(req.DirectoryId, props)
}

type modifyWorkspaceCreationPropertiesInput struct {
	DirectoryId                 string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
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

	return &emptyOutput{}, h.Backend.ModifyWorkspaceCreationProperties(req.DirectoryId, props)
}

// =============================================================================
// Account Links
// =============================================================================

type createAccountLinkInvitationInput struct {
	TargetAccountId string `json:"TargetAccountId"` //nolint:revive,staticcheck // existing issue.
	ClientToken     string `json:"ClientToken"`
}

type accountLinkResp struct {
	LinkId          string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
	Status          string `json:"Status"`
	SourceAccountId string `json:"SourceAccountId"` //nolint:revive,staticcheck // existing issue.
	TargetAccountId string `json:"TargetAccountId"` //nolint:revive,staticcheck // existing issue.
}

type createAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleCreateAccountLinkInvitation(
	_ context.Context, req *createAccountLinkInvitationInput,
) (*createAccountLinkInvitationOutput, error) {
	link, err := h.Backend.CreateAccountLinkInvitation(req.TargetAccountId)
	if err != nil {
		return nil, err
	}

	return &createAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

func toAccountLinkResp(l *storedAccountLink) accountLinkResp {
	return accountLinkResp{
		LinkId:          l.LinkID,
		Status:          l.Status,
		SourceAccountId: l.SourceAccountID,
		TargetAccountId: l.TargetAccountID,
	}
}

type acceptAccountLinkInvitationInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type acceptAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleAcceptAccountLinkInvitation(
	_ context.Context, req *acceptAccountLinkInvitationInput,
) (*acceptAccountLinkInvitationOutput, error) {
	link, err := h.Backend.AcceptAccountLinkInvitation(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &acceptAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type rejectAccountLinkInvitationInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type rejectAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleRejectAccountLinkInvitation(
	_ context.Context, req *rejectAccountLinkInvitationInput,
) (*rejectAccountLinkInvitationOutput, error) {
	link, err := h.Backend.RejectAccountLinkInvitation(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &rejectAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type deleteAccountLinkInvitationInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type deleteAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleDeleteAccountLinkInvitation(
	_ context.Context, req *deleteAccountLinkInvitationInput,
) (*deleteAccountLinkInvitationOutput, error) {
	link, err := h.Backend.DeleteAccountLinkInvitation(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &deleteAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type getAccountLinkInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type getAccountLinkOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleGetAccountLink(
	_ context.Context, req *getAccountLinkInput,
) (*getAccountLinkOutput, error) {
	link, err := h.Backend.GetAccountLink(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &getAccountLinkOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type listAccountLinksInput struct {
	LinkStatusFilter string `json:"LinkStatusFilter"`
	NextToken        string `json:"NextToken"`
	MaxResults       int32  `json:"MaxResults"`
}

type listAccountLinksOutput struct {
	NextToken    string            `json:"NextToken,omitempty"`
	AccountLinks []accountLinkResp `json:"AccountLinks"`
}

func (h *Handler) handleListAccountLinks(
	_ context.Context, req *listAccountLinksInput,
) (*listAccountLinksOutput, error) {
	links, nextToken, err := h.Backend.ListAccountLinks(
		req.LinkStatusFilter,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]accountLinkResp, 0, len(links))
	for _, l := range links {
		items = append(items, toAccountLinkResp(l))
	}

	return &listAccountLinksOutput{AccountLinks: items, NextToken: nextToken}, nil
}

// =============================================================================
// Applications
// =============================================================================

type associateWorkspaceApplicationInput struct {
	WorkspaceId   string `json:"WorkspaceId"`   //nolint:revive,staticcheck // existing issue.
	ApplicationId string `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
}

type workspaceAssocResp struct {
	WorkspaceId          string `json:"WorkspaceId"`          //nolint:revive,staticcheck // existing issue.
	AssociatedResourceId string `json:"AssociatedResourceId"` //nolint:revive,staticcheck // existing issue.
	AssociationStatus    string `json:"AssociationStatus"`
}

type associateWorkspaceApplicationOutput struct {
	Association workspaceAssocResp `json:"Association"`
}

func (h *Handler) handleAssociateWorkspaceApplication(
	_ context.Context, req *associateWorkspaceApplicationInput,
) (*associateWorkspaceApplicationOutput, error) {
	if err := h.Backend.AssociateWorkspaceApplication(req.WorkspaceId, req.ApplicationId); err != nil {
		return nil, err
	}

	return &associateWorkspaceApplicationOutput{
		Association: workspaceAssocResp{
			WorkspaceId:          req.WorkspaceId,
			AssociatedResourceId: req.ApplicationId,
			AssociationStatus:    "INSTALLED", //nolint:goconst // existing issue.
		},
	}, nil
}

type disassociateWorkspaceApplicationInput struct {
	WorkspaceId   string `json:"WorkspaceId"`   //nolint:revive,staticcheck // existing issue.
	ApplicationId string `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
}

type disassociateWorkspaceApplicationOutput struct {
	Association workspaceAssocResp `json:"Association"`
}

func (h *Handler) handleDisassociateWorkspaceApplication(
	_ context.Context, req *disassociateWorkspaceApplicationInput,
) (*disassociateWorkspaceApplicationOutput, error) {
	if err := h.Backend.DisassociateWorkspaceApplication(req.WorkspaceId, req.ApplicationId); err != nil {
		return nil, err
	}

	return &disassociateWorkspaceApplicationOutput{
		Association: workspaceAssocResp{
			WorkspaceId:          req.WorkspaceId,
			AssociatedResourceId: req.ApplicationId,
			AssociationStatus:    "UNINSTALLED",
		},
	}, nil
}

type deployWorkspaceApplicationsInput struct {
	WorkspaceId string `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
	Force       bool   `json:"Force"`
}

type deployWorkspaceApplicationsOutput struct {
	Deployment struct {
		Associations []workspaceAssocResp `json:"Associations"`
	} `json:"Deployment"`
}

func (h *Handler) handleDeployWorkspaceApplications(
	_ context.Context, req *deployWorkspaceApplicationsInput,
) (*deployWorkspaceApplicationsOutput, error) {
	associations, err := h.Backend.DeployWorkspaceApplications(req.WorkspaceId, req.Force)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceAssocResp, 0, len(associations))
	for _, a := range associations {
		items = append(items, workspaceAssocResp{
			WorkspaceId:          a["WorkspaceId"],
			AssociatedResourceId: a["AssociatedResourceId"],
			AssociationStatus:    a["AssociationStatus"],
		})
	}

	out := &deployWorkspaceApplicationsOutput{}
	out.Deployment.Associations = items

	return out, nil
}

type describeWorkspaceAssociationsInput struct {
	WorkspaceId             string   `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
}

type describeWorkspaceAssociationsOutput struct {
	Associations []workspaceAssocResp `json:"Associations"`
}

func (h *Handler) handleDescribeWorkspaceAssociations(
	_ context.Context, req *describeWorkspaceAssociationsInput,
) (*describeWorkspaceAssociationsOutput, error) {
	assocs, err := h.Backend.DescribeWorkspaceAssociations(
		req.WorkspaceId,
		req.AssociatedResourceTypes,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceAssocResp, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, workspaceAssocResp{
			WorkspaceId:          a["WorkspaceId"],
			AssociatedResourceId: a["AssociatedResourceId"],
			AssociationStatus:    a["AssociationStatus"],
		})
	}

	return &describeWorkspaceAssociationsOutput{Associations: items}, nil
}

type describeApplicationAssociationsInput struct {
	ApplicationId           string   `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
	NextToken               string   `json:"NextToken"`
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
	MaxResults              int32    `json:"MaxResults"`
}

type describeApplicationAssociationsOutput struct {
	NextToken    string               `json:"NextToken,omitempty"`
	Associations []workspaceAssocResp `json:"Associations"`
}

func (h *Handler) handleDescribeApplicationAssociations(
	_ context.Context, req *describeApplicationAssociationsInput,
) (*describeApplicationAssociationsOutput, error) {
	assocs, nextToken, err := h.Backend.DescribeApplicationAssociations(
		req.ApplicationId, req.AssociatedResourceTypes, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceAssocResp, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, workspaceAssocResp{
			WorkspaceId:          a["WorkspaceId"],
			AssociatedResourceId: a["AssociatedResourceId"],
			AssociationStatus:    a["AssociationStatus"],
		})
	}

	return &describeApplicationAssociationsOutput{Associations: items, NextToken: nextToken}, nil
}

type describeApplicationsInput struct {
	NextToken      string   `json:"NextToken"`
	ApplicationIds []string `json:"ApplicationIds"` //nolint:revive // existing issue.
	MaxResults     int32    `json:"MaxResults"`
}

type applicationResp struct {
	ApplicationId string `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
	Name          string `json:"Name"`
	Owner         string `json:"Owner"`
	State         string `json:"State"`
}

type describeApplicationsOutput struct {
	NextToken    string            `json:"NextToken,omitempty"`
	Applications []applicationResp `json:"Applications"`
}

func (h *Handler) handleDescribeApplications(
	_ context.Context, req *describeApplicationsInput,
) (*describeApplicationsOutput, error) {
	apps, nextToken, err := h.Backend.DescribeApplications(
		req.ApplicationIds,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]applicationResp, 0, len(apps))
	for _, a := range apps {
		items = append(items, applicationResp{
			ApplicationId: a.AppID,
			Name:          a.Name,
			Owner:         a.Owner,
			State:         a.State,
		})
	}

	return &describeApplicationsOutput{Applications: items, NextToken: nextToken}, nil
}

type describeImageAssociationsInput struct {
	ImageId                 string   `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
}

type describeImageAssociationsOutput struct {
	Associations []any `json:"Associations"`
}

func (h *Handler) handleDescribeImageAssociations(
	_ context.Context, _ *describeImageAssociationsInput,
) (*describeImageAssociationsOutput, error) {
	return &describeImageAssociationsOutput{Associations: []any{}}, nil
}

type describeBundleAssociationsInput struct {
	BundleId                string   `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
}

type describeBundleAssociationsOutput struct {
	Associations []any `json:"Associations"`
}

func (h *Handler) handleDescribeBundleAssociations(
	_ context.Context, _ *describeBundleAssociationsInput,
) (*describeBundleAssociationsOutput, error) {
	return &describeBundleAssociationsOutput{Associations: []any{}}, nil
}

// =============================================================================
// Workspace-level ops
// =============================================================================

type migrateWorkspaceInput struct {
	SourceWorkspaceId string `json:"SourceWorkspaceId"` //nolint:revive,staticcheck // existing issue.
	BundleId          string `json:"BundleId"`          //nolint:revive,staticcheck // existing issue.
}

type migrateWorkspaceOutput struct {
	SourceWorkspaceId string `json:"SourceWorkspaceId"` //nolint:revive,staticcheck // existing issue.
	TargetWorkspaceId string `json:"TargetWorkspaceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleMigrateWorkspace(
	_ context.Context, req *migrateWorkspaceInput,
) (*migrateWorkspaceOutput, error) {
	srcID, tgtID, err := h.Backend.MigrateWorkspace(req.SourceWorkspaceId, req.BundleId)
	if err != nil {
		return nil, err
	}

	return &migrateWorkspaceOutput{SourceWorkspaceId: srcID, TargetWorkspaceId: tgtID}, nil
}

type restoreWorkspaceInput struct {
	WorkspaceId string `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleRestoreWorkspace(
	_ context.Context,
	req *restoreWorkspaceInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.RestoreWorkspace(req.WorkspaceId)
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

type standbyWorkspaceSpec struct {
	PrimaryWorkspaceId  string `json:"PrimaryWorkspaceId"` //nolint:revive,staticcheck // existing issue.
	DirectoryId         string `json:"DirectoryId"`        //nolint:revive,staticcheck // existing issue.
	VolumeEncryptionKey string `json:"VolumeEncryptionKey"`
}

type createStandbyWorkspacesInput struct {
	PrimaryRegion     string                 `json:"PrimaryRegion"`
	StandbyWorkspaces []standbyWorkspaceSpec `json:"StandbyWorkspaces"`
}

type createStandbyWorkspacesOutput struct {
	FailedStandbyRequests  []any               `json:"FailedStandbyRequests"`
	PendingStandbyRequests []map[string]string `json:"PendingStandbyRequests"`
}

func (h *Handler) handleCreateStandbyWorkspaces(
	_ context.Context, req *createStandbyWorkspacesInput,
) (*createStandbyWorkspacesOutput, error) {
	specs := make([]map[string]string, 0, len(req.StandbyWorkspaces))
	for _, s := range req.StandbyWorkspaces {
		specs = append(specs, map[string]string{
			"DirectoryId": s.DirectoryId,
			"UserName":    "",
			"BundleId":    "",
		})
	}

	failed, pending, err := h.Backend.CreateStandbyWorkspaces(req.PrimaryRegion, specs)
	if err != nil {
		return nil, err
	}

	failedOut := make([]any, 0, len(failed))
	for _, f := range failed {
		failedOut = append(failedOut, f)
	}

	return &createStandbyWorkspacesOutput{
		FailedStandbyRequests:  failedOut,
		PendingStandbyRequests: pending,
	}, nil
}

// =============================================================================
// Other
// =============================================================================

type listAvailableManagementCidrRangesInput struct {
	ManagementCidrRangeConstraint string `json:"ManagementCidrRangeConstraint"`
	NextToken                     string `json:"NextToken"`
	MaxResults                    int32  `json:"MaxResults"`
}

type listAvailableManagementCidrRangesOutput struct {
	NextToken            string   `json:"NextToken,omitempty"`
	ManagementCidrRanges []string `json:"ManagementCidrRanges"`
}

func (h *Handler) handleListAvailableManagementCidrRanges(
	_ context.Context, _ *listAvailableManagementCidrRangesInput,
) (*listAvailableManagementCidrRangesOutput, error) {
	return &listAvailableManagementCidrRangesOutput{
		ManagementCidrRanges: []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
	}, nil
}

// =============================================================================
// Helpers
// =============================================================================

func tagsToMap(tags []tagItem) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}
