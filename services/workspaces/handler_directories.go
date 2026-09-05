package workspaces

import (
	"context"
	"strconv"

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
	CertificateBasedAuthProperties *certBasedAuthPropsResp `json:"CertificateBasedAuthProperties,omitempty"`
	SamlProperties                 *samlPropsResp          `json:"SamlProperties,omitempty"`
	SelfservicePermissions         *selfSvcPermsResp       `json:"SelfservicePermissions,omitempty"`
	WorkspaceAccessProperties      *accessPropsResp        `json:"WorkspaceAccessProperties,omitempty"`
	WorkspaceCreationProperties    *creationPropsResp      `json:"WorkspaceCreationProperties,omitempty"`
	DirectoryID                    string                  `json:"DirectoryId"`
	DirectoryName                  string                  `json:"DirectoryName,omitempty"`
	DirectoryType                  string                  `json:"DirectoryType,omitempty"`
	Alias                          string                  `json:"Alias,omitempty"`
	State                          string                  `json:"State"`
	EndpointEncryptionMode         string                  `json:"EndpointEncryptionMode,omitempty"`
	//nolint:revive // AWS API uses SubnetIds capitalization
	SubnetIds  []string `json:"SubnetIds,omitempty"`
	IPGroupIDs []string `json:"ipGroupIds,omitempty"`
}

// certBasedAuthPropsResp mirrors types.CertificateBasedAuthProperties.
type certBasedAuthPropsResp struct {
	Status                  string `json:"Status,omitempty"`
	CertificateAuthorityArn string `json:"CertificateAuthorityArn,omitempty"`
}

// samlPropsResp mirrors types.SamlProperties.
type samlPropsResp struct {
	Status                  string `json:"Status,omitempty"`
	UserAccessUrl           string `json:"UserAccessUrl,omitempty"` //nolint:revive,staticcheck // matches wire key
	RelayStateParameterName string `json:"RelayStateParameterName,omitempty"`
}

// selfSvcPermsResp mirrors types.SelfservicePermissions.
type selfSvcPermsResp struct {
	RestartWorkspace   string `json:"RestartWorkspace,omitempty"`
	IncreaseVolumeSize string `json:"IncreaseVolumeSize,omitempty"`
	ChangeComputeType  string `json:"ChangeComputeType,omitempty"`
	SwitchRunningMode  string `json:"SwitchRunningMode,omitempty"`
	RebuildWorkspace   string `json:"RebuildWorkspace,omitempty"`
}

// accessPropsResp mirrors types.WorkspaceAccessProperties's device-type members.
type accessPropsResp struct {
	DeviceTypeWindows    string `json:"DeviceTypeWindows,omitempty"`
	DeviceTypeOsx        string `json:"DeviceTypeOsx,omitempty"`
	DeviceTypeWeb        string `json:"DeviceTypeWeb,omitempty"`
	DeviceTypeIos        string `json:"DeviceTypeIos,omitempty"`
	DeviceTypeAndroid    string `json:"DeviceTypeAndroid,omitempty"`
	DeviceTypeChromeOs   string `json:"DeviceTypeChromeOs,omitempty"`
	DeviceTypeZeroClient string `json:"DeviceTypeZeroClient,omitempty"`
	DeviceTypeLinux      string `json:"DeviceTypeLinux,omitempty"`
}

// creationPropsResp mirrors types.DefaultWorkspaceCreationProperties's real
// members this backend threads through -- see WorkspaceCreationProperties's
// doc comment in interfaces.go.
type creationPropsResp struct {
	EnableInternetAccess            *bool  `json:"EnableInternetAccess,omitempty"`
	EnableMaintenanceMode           *bool  `json:"EnableMaintenanceMode,omitempty"`
	UserEnabledAsLocalAdministrator *bool  `json:"UserEnabledAsLocalAdministrator,omitempty"`
	DefaultOu                       string `json:"DefaultOu,omitempty"`
	//nolint:revive,staticcheck // matches wire key
	CustomSecurityGroupId string `json:"CustomSecurityGroupId,omitempty"`
}

func toCertBasedAuthPropsResp(p *CertificateBasedAuthProperties) *certBasedAuthPropsResp {
	if p == nil {
		return nil
	}

	return &certBasedAuthPropsResp{Status: p.Status, CertificateAuthorityArn: p.CertificateAuthorityArn}
}

func toSamlPropsResp(p *SamlProperties) *samlPropsResp {
	if p == nil {
		return nil
	}

	return &samlPropsResp{
		Status:                  p.Status,
		UserAccessUrl:           p.UserAccessUrl,
		RelayStateParameterName: p.RelayStateParameterName,
	}
}

func toSelfSvcPermsResp(p *SelfservicePermissions) *selfSvcPermsResp {
	if p == nil {
		return nil
	}

	return &selfSvcPermsResp{
		RestartWorkspace:   p.RestartWorkspace,
		IncreaseVolumeSize: p.IncreaseVolumeSize,
		ChangeComputeType:  p.ChangeComputeType,
		SwitchRunningMode:  p.SwitchRunningMode,
		RebuildWorkspace:   p.RebuildWorkspace,
	}
}

func toAccessPropsResp(p *WorkspaceAccessProperties) *accessPropsResp {
	if p == nil {
		return nil
	}

	return &accessPropsResp{
		DeviceTypeWindows:    p.DeviceTypeWindows,
		DeviceTypeOsx:        p.DeviceTypeOsx,
		DeviceTypeWeb:        p.DeviceTypeWeb,
		DeviceTypeIos:        p.DeviceTypeIos,
		DeviceTypeAndroid:    p.DeviceTypeAndroid,
		DeviceTypeChromeOs:   p.DeviceTypeChromeOs,
		DeviceTypeZeroClient: p.DeviceTypeZeroClient,
		DeviceTypeLinux:      p.DeviceTypeLinux,
	}
}

func toCreationPropsResp(p *WorkspaceCreationProperties) *creationPropsResp {
	if p == nil {
		return nil
	}

	return &creationPropsResp{
		DefaultOu:                       p.DefaultOu,
		CustomSecurityGroupId:           p.CustomSecurityGroupId,
		EnableInternetAccess:            p.EnableInternetAccess,
		EnableMaintenanceMode:           p.EnableMaintenanceMode,
		UserEnabledAsLocalAdministrator: p.UserEnabledAsLocalAdministrator,
	}
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
			DirectoryID:                    d.DirectoryID,
			DirectoryName:                  d.DirectoryName,
			DirectoryType:                  d.DirectoryType,
			Alias:                          d.Alias,
			State:                          d.State,
			SubnetIds:                      d.SubnetIDs,
			IPGroupIDs:                     d.IPGroupIDs,
			EndpointEncryptionMode:         d.EndpointEncryptionMode,
			CertificateBasedAuthProperties: toCertBasedAuthPropsResp(d.CertificateBasedAuthProperties),
			SamlProperties:                 toSamlPropsResp(d.SamlProperties),
			SelfservicePermissions:         toSelfSvcPermsResp(d.SelfservicePermissions),
			WorkspaceAccessProperties:      toAccessPropsResp(d.WorkspaceAccessProperties),
			WorkspaceCreationProperties:    toCreationPropsResp(d.WorkspaceCreationProperties),
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
	if err := h.Backend.RegisterWorkspaceDirectory(req.DirectoryId, req.SubnetIds, tagsToMap(req.Tags)); err != nil {
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
		EnableInternetAccess            bool   `json:"EnableInternetAccess"`
		UserEnabledAsLocalAdministrator bool   `json:"UserEnabledAsLocalAdministrator"`
		EnableMaintenanceMode           bool   `json:"EnableMaintenanceMode"`
	} `json:"WorkspaceCreationProperties"`
}

func (h *Handler) handleModifyWorkspaceCreationProperties(
	_ context.Context, req *modifyWorkspaceCreationPropertiesInput,
) (*emptyOutput, error) {
	cp := req.WorkspaceCreationProperties
	props := map[string]string{
		"DefaultOu":                       cp.DefaultOu,
		"CustomSecurityGroupId":           cp.CustomSecurityGroupId,
		"EnableInternetAccess":            strconv.FormatBool(cp.EnableInternetAccess),
		"EnableMaintenanceMode":           strconv.FormatBool(cp.EnableMaintenanceMode),
		"UserEnabledAsLocalAdministrator": strconv.FormatBool(cp.UserEnabledAsLocalAdministrator),
	}

	return &emptyOutput{}, h.Backend.ModifyWorkspaceCreationProperties(req.ResourceId, props)
}
