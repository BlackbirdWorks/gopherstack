package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildClientBrandingOps returns the map of client branding and client properties operations.
func (h *Handler) buildClientBrandingOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ImportClientBranding":     service.WrapOp(h.handleImportClientBranding),
		"DescribeClientBranding":   service.WrapOp(h.handleDescribeClientBranding),
		"DeleteClientBranding":     service.WrapOp(h.handleDeleteClientBranding),
		"DescribeClientProperties": service.WrapOp(h.handleDescribeClientProperties),
		"ModifyClientProperties":   service.WrapOp(h.handleModifyClientProperties),
	}
}

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
