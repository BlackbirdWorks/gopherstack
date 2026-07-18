package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildWorkspacePropertiesOps returns the map of directory-scoped property operations.
func (h *Handler) buildWorkspacePropertiesOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ModifyEndpointEncryptionMode": service.WrapOp(h.handleModifyEndpointEncryptionMode),
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
	}
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
