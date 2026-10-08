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
	NextToken               string   `json:"NextToken"`
	DirectoryIDs            []string `json:"DirectoryIds"`
	WorkspaceDirectoryNames []string `json:"WorkspaceDirectoryNames"`
	Filters                 []struct {
		Name   string   `json:"Name"`
		Values []string `json:"Values"`
	} `json:"Filters"`
	Limit int32 `json:"Limit"`
}

type describeDirectoriesOutput struct {
	NextToken   string    `json:"NextToken,omitempty"`
	Directories []dirResp `json:"Directories"`
}

type dirResp struct {
	CertificateBasedAuthProperties *certBasedAuthPropsResp `json:"CertificateBasedAuthProperties,omitempty"`
	StreamingProperties            *StreamingProperties    `json:"StreamingProperties,omitempty"`
	ActiveDirectoryConfig          *adConfigResp           `json:"ActiveDirectoryConfig,omitempty"`
	MicrosoftEntraConfig           *entraConfigResp        `json:"MicrosoftEntraConfig,omitempty"`
	IDCConfig                      *idcConfigResp          `json:"IDCConfig,omitempty"`
	UserIdentityType               string                  `json:"UserIdentityType,omitempty"`
	WorkspaceType                  string                  `json:"WorkspaceType,omitempty"`
	Tenancy                        string                  `json:"Tenancy,omitempty"`
	WorkspaceDirectoryName         string                  `json:"WorkspaceDirectoryName,omitempty"`
	WorkspaceDirectoryDescription  string                  `json:"WorkspaceDirectoryDescription,omitempty"`
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
	CustomerUserName               string                  `json:"CustomerUserName,omitempty"`
	//nolint:revive // AWS API uses SubnetIds capitalization
	SubnetIds []string `json:"SubnetIds,omitempty"`
	//nolint:revive,staticcheck // matches wire key
	DnsIpAddresses []string `json:"DnsIpAddresses,omitempty"`
	IPGroupIDs     []string `json:"ipGroupIds,omitempty"`
}

type adConfigResp struct {
	DomainName              string `json:"DomainName"`
	ServiceAccountSecretArn string `json:"ServiceAccountSecretArn"`
}

type entraConfigResp struct {
	ApplicationConfigSecretArn string `json:"ApplicationConfigSecretArn,omitempty"`
	TenantId                   string `json:"TenantId,omitempty"` //nolint:revive,staticcheck // wire key
}

type idcConfigResp struct {
	ApplicationArn string `json:"ApplicationArn,omitempty"`
	InstanceArn    string `json:"InstanceArn"`
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

func toADConfigResp(c *DirectoryActiveDirectoryConfig) *adConfigResp {
	if c == nil {
		return nil
	}

	return &adConfigResp{DomainName: c.DomainName, ServiceAccountSecretArn: c.ServiceAccountSecretArn}
}

func toEntraConfigResp(c *DirectoryMicrosoftEntraConfig) *entraConfigResp {
	if c == nil {
		return nil
	}

	return &entraConfigResp{ApplicationConfigSecretArn: c.ApplicationConfigSecretArn, TenantId: c.TenantID}
}

func toIDCConfigResp(c *DirectoryIDCConfig) *idcConfigResp {
	if c == nil {
		return nil
	}

	return &idcConfigResp{ApplicationArn: c.ApplicationArn, InstanceArn: c.InstanceArn}
}

func (h *Handler) handleDescribeWorkspaceDirectories(
	ctx context.Context, req *describeDirectoriesInput,
) (*describeDirectoriesOutput, error) {
	filters := make([]DirectoryFilter, 0, len(req.Filters))
	for _, f := range req.Filters {
		filters = append(filters, DirectoryFilter{Name: f.Name, Values: f.Values})
	}

	dirs, nextToken, err := h.Backend.DescribeWorkspaceDirectoriesFiltered(
		ctx,
		req.DirectoryIDs,
		req.WorkspaceDirectoryNames,
		filters,
		req.Limit,
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
			CustomerUserName:               d.CustomerUserName,
			State:                          d.State,
			SubnetIds:                      d.SubnetIDs,
			DnsIpAddresses:                 d.DNSIPAddresses,
			IPGroupIDs:                     d.IPGroupIDs,
			EndpointEncryptionMode:         d.EndpointEncryptionMode,
			CertificateBasedAuthProperties: toCertBasedAuthPropsResp(d.CertificateBasedAuthProperties),
			SamlProperties:                 toSamlPropsResp(d.SamlProperties),
			SelfservicePermissions:         toSelfSvcPermsResp(d.SelfservicePermissions),
			WorkspaceAccessProperties:      toAccessPropsResp(d.WorkspaceAccessProperties),
			WorkspaceCreationProperties:    toCreationPropsResp(d.WorkspaceCreationProperties),
			StreamingProperties:            d.StreamingProperties,
			ActiveDirectoryConfig:          toADConfigResp(d.ActiveDirectoryConfig),
			MicrosoftEntraConfig:           toEntraConfigResp(d.MicrosoftEntraConfig),
			IDCConfig:                      toIDCConfigResp(d.IDCConfig),
			UserIdentityType:               d.UserIdentityType,
			WorkspaceType:                  d.WorkspaceType,
			Tenancy:                        d.Tenancy,
			WorkspaceDirectoryName:         d.WorkspaceDirectoryName,
			WorkspaceDirectoryDescription:  d.WorkspaceDirectoryDescription,
		})
	}

	return &describeDirectoriesOutput{Directories: items, NextToken: nextToken}, nil
}

type registerWorkspaceDirectoryInput struct {
	ActiveDirectoryConfig *adConfigResp    `json:"ActiveDirectoryConfig"`
	MicrosoftEntraConfig  *entraConfigResp `json:"MicrosoftEntraConfig"`
	DirectoryId           string           `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	IdcInstanceArn        string           `json:"IdcInstanceArn"`
	Tenancy               string           `json:"Tenancy"`
	UserIdentityType      string           `json:"UserIdentityType"`
	WorkspaceType         string           `json:"WorkspaceType"`
	//nolint:revive // existing issue.
	SubnetIds                     []string  `json:"SubnetIds"`
	WorkspaceDirectoryName        string    `json:"WorkspaceDirectoryName"`
	WorkspaceDirectoryDescription string    `json:"WorkspaceDirectoryDescription"`
	Tags                          []tagItem `json:"Tags"`
	EnableSelfService             bool      `json:"EnableSelfService"`
}

type registerWorkspaceDirectoryOutput struct {
	DirectoryId string `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	State       string `json:"State"`
}

func (h *Handler) handleRegisterWorkspaceDirectory(
	_ context.Context, req *registerWorkspaceDirectoryInput,
) (*registerWorkspaceDirectoryOutput, error) {
	reg := DirectoryRegistration{
		DirectoryID:                   req.DirectoryId,
		SubnetIDs:                     req.SubnetIds,
		Tags:                          tagsToMap(req.Tags),
		IdcInstanceArn:                req.IdcInstanceArn,
		Tenancy:                       req.Tenancy,
		UserIdentityType:              req.UserIdentityType,
		WorkspaceType:                 req.WorkspaceType,
		WorkspaceDirectoryName:        req.WorkspaceDirectoryName,
		WorkspaceDirectoryDescription: req.WorkspaceDirectoryDescription,
	}

	if c := req.ActiveDirectoryConfig; c != nil {
		reg.ActiveDirectoryConfig = &DirectoryActiveDirectoryConfig{
			DomainName: c.DomainName, ServiceAccountSecretArn: c.ServiceAccountSecretArn,
		}
	}

	if c := req.MicrosoftEntraConfig; c != nil {
		reg.MicrosoftEntraConfig = &DirectoryMicrosoftEntraConfig{
			ApplicationConfigSecretArn: c.ApplicationConfigSecretArn, TenantID: c.TenantId,
		}
	}

	id, err := h.Backend.RegisterWorkspaceDirectoryWithConfig(reg)
	if err != nil {
		return nil, err
	}

	return &registerWorkspaceDirectoryOutput{DirectoryId: id, State: stateRegistered}, nil
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
	WorkspaceCreationProperties struct {
		EnableInternetAccess            *bool  `json:"EnableInternetAccess"`
		UserEnabledAsLocalAdministrator *bool  `json:"UserEnabledAsLocalAdministrator"`
		EnableMaintenanceMode           *bool  `json:"EnableMaintenanceMode"`
		DefaultOu                       string `json:"DefaultOu"`
		CustomSecurityGroupId           string `json:"CustomSecurityGroupId"` //nolint:revive,staticcheck // existing issue.
	} `json:"WorkspaceCreationProperties"`
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleModifyWorkspaceCreationProperties(
	_ context.Context, req *modifyWorkspaceCreationPropertiesInput,
) (*emptyOutput, error) {
	cp := req.WorkspaceCreationProperties
	props := nonEmptyProps(map[string]string{
		"DefaultOu":             cp.DefaultOu,
		"CustomSecurityGroupId": cp.CustomSecurityGroupId,
	})

	for key, v := range map[string]*bool{
		"EnableInternetAccess":            cp.EnableInternetAccess,
		"EnableMaintenanceMode":           cp.EnableMaintenanceMode,
		"UserEnabledAsLocalAdministrator": cp.UserEnabledAsLocalAdministrator,
	} {
		if v != nil {
			props[key] = strconv.FormatBool(*v)
		}
	}

	return &emptyOutput{}, h.Backend.ModifyWorkspaceCreationProperties(req.ResourceId, props)
}
