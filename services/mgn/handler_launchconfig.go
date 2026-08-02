package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleGetLaunchConfiguration(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req getLaunchConfigurationRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	lc, err := h.Backend.GetLaunchConfiguration(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toLaunchConfigurationWire(lc))
}

func (h *Handler) handleUpdateLaunchConfiguration(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req updateLaunchConfigurationRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := UpdateLaunchConfigurationInput{
		BootMode:                            req.BootMode,
		CopyPrivateIP:                       req.CopyPrivateIP,
		CopyTags:                            req.CopyTags,
		EnableMapAutoTagging:                req.EnableMapAutoTagging,
		LaunchDisposition:                   req.LaunchDisposition,
		Licensing:                           fromLicensingWire(req.Licensing),
		MapAutoTaggingMpeID:                 req.MapAutoTaggingMpeID,
		Name:                                req.Name,
		PostLaunchActions:                   fromPostLaunchActionsWire(req.PostLaunchActions),
		TargetInstanceTypeRightSizingMethod: req.TargetInstanceTypeRightSizingMethod,
	}

	lc, err := h.Backend.UpdateLaunchConfiguration(req.SourceServerID, in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toLaunchConfigurationWire(lc))
}

func (h *Handler) handleCreateLaunchConfigurationTemplate(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req createLaunchConfigurationTemplateRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := CreateLaunchConfigurationTemplateInput{
		Licensing:                           fromLicensingWire(req.Licensing),
		PostLaunchActions:                   fromPostLaunchActionsWire(req.PostLaunchActions),
		LargeVolumeConf:                     fromLaunchTemplateDiskConfWire(req.LargeVolumeConf),
		SmallVolumeConf:                     fromLaunchTemplateDiskConfWire(req.SmallVolumeConf),
		Tags:                                req.Tags,
		BootMode:                            req.BootMode,
		Ec2LaunchTemplateID:                 req.Ec2LaunchTemplateID,
		LaunchDisposition:                   req.LaunchDisposition,
		MapAutoTaggingMpeID:                 req.MapAutoTaggingMpeID,
		ParametersEncryptionKey:             req.ParametersEncryptionKey,
		TargetInstanceTypeRightSizingMethod: req.TargetInstanceTypeRightSizingMethod,
		SmallVolumeMaxSize:                  req.SmallVolumeMaxSize,
		AssociatePublicIPAddress:            req.AssociatePublicIPAddress,
		CopyPrivateIP:                       req.CopyPrivateIP,
		CopyTags:                            req.CopyTags,
		EnableMapAutoTagging:                req.EnableMapAutoTagging,
		EnableParametersEncryption:          req.EnableParametersEncryption,
	}

	tmpl, err := h.Backend.CreateLaunchConfigurationTemplate(in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toLaunchConfigurationTemplateWire(tmpl))
}

func (h *Handler) handleDeleteLaunchConfigurationTemplate(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req launchConfigurationTemplateIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteLaunchConfigurationTemplate(req.LaunchConfigurationTemplateID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleDescribeLaunchConfigurationTemplates(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req describeLaunchConfigurationTemplatesRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.DescribeLaunchConfigurationTemplates(
		req.LaunchConfigurationTemplateIDs, req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	items := make([]launchConfigurationTemplateWire, len(pg.Data))
	for i, t := range pg.Data {
		items[i] = toLaunchConfigurationTemplateWire(t)
	}

	return marshalResponse(describeLaunchConfigurationTemplatesResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleUpdateLaunchConfigurationTemplate(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req updateLaunchConfigurationTemplateRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := UpdateLaunchConfigurationTemplateInput{
		Licensing:                           fromLicensingWire(req.Licensing),
		PostLaunchActions:                   fromPostLaunchActionsWire(req.PostLaunchActions),
		LargeVolumeConf:                     fromLaunchTemplateDiskConfWire(req.LargeVolumeConf),
		SmallVolumeConf:                     fromLaunchTemplateDiskConfWire(req.SmallVolumeConf),
		BootMode:                            req.BootMode,
		Ec2LaunchTemplateID:                 req.Ec2LaunchTemplateID,
		LaunchDisposition:                   req.LaunchDisposition,
		MapAutoTaggingMpeID:                 req.MapAutoTaggingMpeID,
		TargetInstanceTypeRightSizingMethod: req.TargetInstanceTypeRightSizingMethod,
		SmallVolumeMaxSize:                  req.SmallVolumeMaxSize,
		AssociatePublicIPAddress:            req.AssociatePublicIPAddress,
		CopyPrivateIP:                       req.CopyPrivateIP,
		CopyTags:                            req.CopyTags,
		EnableMapAutoTagging:                req.EnableMapAutoTagging,
	}

	tmpl, err := h.Backend.UpdateLaunchConfigurationTemplate(req.LaunchConfigurationTemplateID, in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toLaunchConfigurationTemplateWire(tmpl))
}
