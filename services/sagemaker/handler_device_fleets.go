package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// DeviceFleet handlers
// ---------------------------------------------------------------------------

// edgeOutputConfigInput is the wire shape of types.EdgeOutputConfig
// (types/types.go:7856-7903), shared by CreateDeviceFleet's and
// UpdateDeviceFleet's OutputConfig member.
type edgeOutputConfigInput struct {
	S3OutputLocation       string `json:"S3OutputLocation"`
	KmsKeyID               string `json:"KmsKeyId"`
	PresetDeploymentConfig string `json:"PresetDeploymentConfig"`
	PresetDeploymentType   string `json:"PresetDeploymentType"`
}

func (c *edgeOutputConfigInput) toDeviceFleetOutputConfig() *DeviceFleetOutputConfig {
	if c == nil {
		return nil
	}

	return &DeviceFleetOutputConfig{
		S3OutputLocation:       c.S3OutputLocation,
		KmsKeyID:               c.KmsKeyID,
		PresetDeploymentConfig: c.PresetDeploymentConfig,
		PresetDeploymentType:   c.PresetDeploymentType,
	}
}

type createDeviceFleetInput struct {
	OutputConfig       *edgeOutputConfigInput `json:"OutputConfig"`
	DeviceFleetName    string                 `json:"DeviceFleetName"`
	Description        string                 `json:"Description"`
	RoleArn            string                 `json:"RoleArn"`
	EnableIotRoleAlias *bool                  `json:"EnableIotRoleAlias"`
	Tags               []tagObject            `json:"Tags"`
}

func (h *Handler) handleCreateDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req createDeviceFleetInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DeviceFleetName == "" {
		return nil, fmt.Errorf("%w: DeviceFleetName is required", errInvalidRequest)
	}
	// OutputConfig (and its S3OutputLocation) is a required member of
	// CreateDeviceFleetInput in the real API — reject early rather than
	// silently persisting a DeviceFleet whose later DescribeDeviceFleet
	// response would omit the (also required) OutputConfig field.
	if req.OutputConfig == nil || req.OutputConfig.S3OutputLocation == "" {
		return nil, fmt.Errorf("%w: OutputConfig.S3OutputLocation is required", errInvalidRequest)
	}

	if _, err := h.Backend.CreateDeviceFleet(ctx, CreateDeviceFleetOptions{
		DeviceFleetName:    req.DeviceFleetName,
		Description:        req.Description,
		RoleArn:            req.RoleArn,
		OutputConfig:       req.OutputConfig.toDeviceFleetOutputConfig(),
		Tags:               fromTagObjects(req.Tags),
		EnableIotRoleAlias: req.EnableIotRoleAlias != nil && *req.EnableIotRoleAlias,
	}); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

type describeDeviceFleetInput struct {
	DeviceFleetName string `json:"DeviceFleetName"`
}

func (h *Handler) handleDescribeDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req describeDeviceFleetInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	f, err := h.Backend.DescribeDeviceFleet(ctx, req.DeviceFleetName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(f)
}

// listDeviceFleetsInput is ListDeviceFleets' request shape
// (api_op_ListDeviceFleets.go:30-61).
type listDeviceFleetsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	NameContains           string   `json:"NameContains"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListDeviceFleets(ctx context.Context, body []byte) ([]byte, error) {
	var req listDeviceFleetsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	fleets, next := h.Backend.ListDeviceFleets(ctx, ListDeviceFleetsParams{
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
		NameContains:           req.NameContains,
		NextToken:              req.NextToken,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	})

	items := make([]map[string]any, 0, len(fleets))
	for _, f := range fleets {
		items = append(items, map[string]any{
			keyDeviceFleetName:  f.DeviceFleetName,
			"DeviceFleetArn":    f.DeviceFleetArn,
			keyCreationTime:     epochSeconds(f.CreationTime),
			keyLastModifiedTime: epochSeconds(f.LastModifiedTime),
		})
	}

	return listResp("DeviceFleetSummaries", items, next)
}

type updateDeviceFleetInput struct {
	OutputConfig       *edgeOutputConfigInput `json:"OutputConfig"`
	EnableIotRoleAlias *bool                  `json:"EnableIotRoleAlias"`
	DeviceFleetName    string                 `json:"DeviceFleetName"`
	Description        string                 `json:"Description"`
	RoleArn            string                 `json:"RoleArn"`
}

func (h *Handler) handleUpdateDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req updateDeviceFleetInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DeviceFleetName == "" {
		return nil, fmt.Errorf("%w: DeviceFleetName is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateDeviceFleet(
		ctx, req.DeviceFleetName, req.Description, req.RoleArn,
		req.OutputConfig.toDeviceFleetOutputConfig(), req.EnableIotRoleAlias,
	); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

type deleteDeviceFleetInput struct {
	DeviceFleetName string `json:"DeviceFleetName"`
}

func (h *Handler) handleDeleteDeviceFleet(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteDeviceFleetInput

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

// registerDeviceEntry is one entry of RegisterDevicesInput.Devices
// (types.Device, types/types.go:7222-7236) — note this wire type has no Tags
// field of its own; see registerDevicesInput.Tags.
type registerDeviceEntry struct {
	DeviceName   string `json:"DeviceName"`
	Description  string `json:"Description"`
	IotThingName string `json:"IotThingName"`
}

// registerDevicesInput is RegisterDevices' request shape
// (api_op_RegisterDevices.go:28-40). Tags is a top-level, batch-wide field —
// the previous handler instead read a nonexistent per-device Tags key that
// the real client never sends, so every registration's tags were silently
// dropped regardless of what a real client sent.
type registerDevicesInput struct {
	DeviceFleetName string                `json:"DeviceFleetName"`
	Devices         []registerDeviceEntry `json:"Devices"`
	Tags            []tagObject           `json:"Tags"`
}

func (h *Handler) handleRegisterDevices(ctx context.Context, body []byte) ([]byte, error) {
	var req registerDevicesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	inputs := make([]RegisterDeviceInput, 0, len(req.Devices))
	for _, d := range req.Devices {
		inputs = append(inputs, RegisterDeviceInput(d))
	}

	if err := h.Backend.RegisterDevices(ctx, req.DeviceFleetName, inputs, fromTagObjects(req.Tags)); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

type deregisterDevicesInput struct {
	DeviceFleetName string   `json:"DeviceFleetName"`
	DeviceNames     []string `json:"DeviceNames"`
}

func (h *Handler) handleDeregisterDevices(ctx context.Context, body []byte) ([]byte, error) {
	var req deregisterDevicesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeregisterDevices(ctx, req.DeviceFleetName, req.DeviceNames); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// describeDeviceInput is DescribeDevice's request shape
// (api_op_DescribeDevice.go:28-43). NextToken (pagination of the Models
// field within one device's description) is decoded but unused: this
// backend's Device never carries Models (see ListDevices' doc comment in
// device_fleets.go), so there is never a second page to token into.
type describeDeviceInput struct {
	DeviceName      string `json:"DeviceName"`
	DeviceFleetName string `json:"DeviceFleetName"`
	NextToken       string `json:"NextToken"`
}

func (h *Handler) handleDescribeDevice(ctx context.Context, body []byte) ([]byte, error) {
	var req describeDeviceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	d, err := h.Backend.DescribeDevice(ctx, req.DeviceFleetName, req.DeviceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(d)
}

// listDevicesInput is ListDevices' request shape (api_op_ListDevices.go:
// 30-49). LatestHeartbeatAfter/ModelName are decoded for wire-shape fidelity
// but are real no-ops — see ListDevices' doc comment in device_fleets.go.
type listDevicesInput struct {
	LatestHeartbeatAfter *float64 `json:"LatestHeartbeatAfter"`
	DeviceFleetName      string   `json:"DeviceFleetName"`
	ModelName            string   `json:"ModelName"`
	NextToken            string   `json:"NextToken"`
	MaxResults           int32    `json:"MaxResults"`
}

func (h *Handler) handleListDevices(ctx context.Context, body []byte) ([]byte, error) {
	var req listDevicesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	devices, next := h.Backend.ListDevices(ctx, req.DeviceFleetName, req.NextToken, req.MaxResults)

	items := make([]map[string]any, 0, len(devices))
	for _, d := range devices {
		items = append(items, map[string]any{
			keyDeviceName:      d.DeviceName,
			keyDeviceFleetName: d.DeviceFleetName,
			"DeviceArn":        d.DeviceArn,
			"RegistrationTime": epochSeconds(d.RegistrationTime),
		})
	}

	return listResp("DeviceSummaries", items, next)
}
