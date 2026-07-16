package sagemaker

import (
	"context"
	"encoding/json"
)

// ---------------------------------------------------------------------------
// DeviceFleet handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags         map[string]string `json:"Tags"`
		OutputConfig *struct {
			S3OutputLocation string `json:"S3OutputLocation"`
			KmsKeyID         string `json:"KmsKeyId"`
		} `json:"OutputConfig"`
		DeviceFleetName string `json:"DeviceFleetName"`
		Description     string `json:"Description"`
		RoleArn         string `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	var outputConfig *DeviceFleetOutputConfig
	if req.OutputConfig != nil {
		outputConfig = &DeviceFleetOutputConfig{
			S3OutputLocation: req.OutputConfig.S3OutputLocation,
			KmsKeyID:         req.OutputConfig.KmsKeyID,
		}
	}

	if _, err := h.Backend.CreateDeviceFleet(ctx, CreateDeviceFleetOptions{
		DeviceFleetName: req.DeviceFleetName,
		Description:     req.Description,
		RoleArn:         req.RoleArn,
		OutputConfig:    outputConfig,
		Tags:            req.Tags,
	}); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDescribeDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	f, err := h.Backend.DescribeDeviceFleet(ctx, req.DeviceFleetName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(f)
}

func (h *Handler) handleListDeviceFleets(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	fleets, next := h.Backend.ListDeviceFleets(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(fleets))
	for _, f := range fleets {
		items = append(items, map[string]any{
			keyDeviceFleetName:  f.DeviceFleetName,
			"DeviceFleetArn":    f.DeviceFleetArn,
			keyCreationTime:     f.CreationTime,
			keyLastModifiedTime: f.LastModifiedTime,
		})
	}

	return listResp("DeviceFleetSummaries", items, next)
}

func (h *Handler) handleUpdateDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
		Description     string `json:"Description"`
		RoleArn         string `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateDeviceFleet(ctx, req.DeviceFleetName, req.Description, req.RoleArn); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDeleteDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteDeviceFleet(ctx, req.DeviceFleetName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// Device handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleRegisterDevices(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
		Devices         []struct {
			Tags         map[string]string `json:"Tags"`
			DeviceName   string            `json:"DeviceName"`
			Description  string            `json:"Description"`
			IotThingName string            `json:"IotThingName"`
		} `json:"Devices"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	inputs := make([]RegisterDeviceInput, 0, len(req.Devices))
	for _, d := range req.Devices {
		inputs = append(inputs, RegisterDeviceInput{
			DeviceName:   d.DeviceName,
			Description:  d.Description,
			IotThingName: d.IotThingName,
			Tags:         d.Tags,
		})
	}

	if err := h.Backend.RegisterDevices(ctx, req.DeviceFleetName, inputs); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDeregisterDevices(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string   `json:"DeviceFleetName"`
		DeviceNames     []string `json:"DeviceNames"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeregisterDevices(ctx, req.DeviceFleetName, req.DeviceNames); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDescribeDevice(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DeviceName      string `json:"DeviceName"`
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	d, err := h.Backend.DescribeDevice(ctx, req.DeviceFleetName, req.DeviceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(d)
}

func (h *Handler) handleListDevices(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken       string `json:"NextToken"`
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	devices, next := h.Backend.ListDevices(ctx, req.DeviceFleetName, req.NextToken)

	items := make([]map[string]any, 0, len(devices))
	for _, d := range devices {
		items = append(items, map[string]any{
			keyDeviceName:      d.DeviceName,
			keyDeviceFleetName: d.DeviceFleetName,
			"DeviceArn":        d.DeviceArn,
			"RegistrationTime": d.RegistrationTime,
		})
	}

	return listResp("DeviceSummaries", items, next)
}
