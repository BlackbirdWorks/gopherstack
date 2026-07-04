package sagemaker

import (
	"context"
	"encoding/json"
)

// accuracy4 operation name constants.
const (
	opCreateDeviceFleet                     = "CreateDeviceFleet"
	opDescribeDeviceFleet                   = "DescribeDeviceFleet"
	opListDeviceFleets                      = "ListDeviceFleets"
	opUpdateDeviceFleet                     = "UpdateDeviceFleet"
	opDeleteDeviceFleet                     = "DeleteDeviceFleet"
	opRegisterDevices                       = "RegisterDevices"
	opDeregisterDevices                     = "DeregisterDevices"
	opDescribeDevice                        = "DescribeDevice"
	opListDevices                           = "ListDevices"
	opCreateInferenceComponent              = "CreateInferenceComponent"
	opDescribeInferenceComponent            = "DescribeInferenceComponent"
	opListInferenceComponents               = "ListInferenceComponents"
	opUpdateInferenceComponent              = "UpdateInferenceComponent"
	opUpdateInferenceComponentRuntimeConfig = "UpdateInferenceComponentRuntimeConfig"
	opDeleteInferenceComponent              = "DeleteInferenceComponent"
	opCreateClusterSchedulerConfig          = "CreateClusterSchedulerConfig"
	opDescribeClusterSchedulerConfig        = "DescribeClusterSchedulerConfig"
	opListClusterSchedulerConfigs           = "ListClusterSchedulerConfigs"
	opUpdateClusterSchedulerConfig          = "UpdateClusterSchedulerConfig"
	opDeleteClusterSchedulerConfig          = "DeleteClusterSchedulerConfig"
	opCreateComputeQuota                    = "CreateComputeQuota"
	opDescribeComputeQuota                  = "DescribeComputeQuota"
	opListComputeQuotas                     = "ListComputeQuotas"
	opUpdateComputeQuota                    = "UpdateComputeQuota"
	opDeleteComputeQuota                    = "DeleteComputeQuota"
)

// accuracy4OpsSupported returns the real stateful operations implemented in accuracy4.
func accuracy4OpsSupported() []string {
	return []string{
		opCreateDeviceFleet,
		opDescribeDeviceFleet,
		opListDeviceFleets,
		opUpdateDeviceFleet,
		opDeleteDeviceFleet,
		opRegisterDevices,
		opDeregisterDevices,
		opDescribeDevice,
		opListDevices,
		opCreateInferenceComponent,
		opDescribeInferenceComponent,
		opListInferenceComponents,
		opUpdateInferenceComponent,
		opUpdateInferenceComponentRuntimeConfig,
		opDeleteInferenceComponent,
		opCreateClusterSchedulerConfig,
		opDescribeClusterSchedulerConfig,
		opListClusterSchedulerConfigs,
		opUpdateClusterSchedulerConfig,
		opDeleteClusterSchedulerConfig,
		opCreateComputeQuota,
		opDescribeComputeQuota,
		opListComputeQuotas,
		opUpdateComputeQuota,
		opDeleteComputeQuota,
	}
}

// dispatchAccuracy4Ops dispatches the accuracy4 real stateful operations.
//
//nolint:cyclop,funlen // large switch for 25 operations
func (h *Handler) dispatchAccuracy4Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// DeviceFleet
	case opCreateDeviceFleet:
		r, err := h.handleCreateDeviceFleet(ctx, body)

		return r, true, err
	case opDescribeDeviceFleet:
		r, err := h.handleDescribeDeviceFleet(ctx, body)

		return r, true, err
	case opListDeviceFleets:
		r, err := h.handleListDeviceFleets(ctx, body)

		return r, true, err
	case opUpdateDeviceFleet:
		r, err := h.handleUpdateDeviceFleet(ctx, body)

		return r, true, err
	case opDeleteDeviceFleet:
		r, err := h.handleDeleteDeviceFleet(ctx, body)

		return r, true, err

	// Device
	case opRegisterDevices:
		r, err := h.handleRegisterDevices(ctx, body)

		return r, true, err
	case opDeregisterDevices:
		r, err := h.handleDeregisterDevices(ctx, body)

		return r, true, err
	case opDescribeDevice:
		r, err := h.handleDescribeDevice(ctx, body)

		return r, true, err
	case opListDevices:
		r, err := h.handleListDevices(ctx, body)

		return r, true, err

	// InferenceComponent
	case opCreateInferenceComponent:
		r, err := h.handleCreateInferenceComponent(ctx, body)

		return r, true, err
	case opDescribeInferenceComponent:
		r, err := h.handleDescribeInferenceComponent(ctx, body)

		return r, true, err
	case opListInferenceComponents:
		r, err := h.handleListInferenceComponents(ctx, body)

		return r, true, err
	case opUpdateInferenceComponent:
		r, err := h.handleUpdateInferenceComponent(ctx, body)

		return r, true, err
	case opUpdateInferenceComponentRuntimeConfig:
		r, err := h.handleUpdateInferenceComponentRuntimeConfig(ctx, body)

		return r, true, err
	case opDeleteInferenceComponent:
		r, err := h.handleDeleteInferenceComponent(ctx, body)

		return r, true, err

	// ClusterSchedulerConfig
	case opCreateClusterSchedulerConfig:
		r, err := h.handleCreateClusterSchedulerConfig(ctx, body)

		return r, true, err
	case opDescribeClusterSchedulerConfig:
		r, err := h.handleDescribeClusterSchedulerConfig(ctx, body)

		return r, true, err
	case opListClusterSchedulerConfigs:
		r, err := h.handleListClusterSchedulerConfigs(ctx, body)

		return r, true, err
	case opUpdateClusterSchedulerConfig:
		r, err := h.handleUpdateClusterSchedulerConfig(ctx, body)

		return r, true, err
	case opDeleteClusterSchedulerConfig:
		r, err := h.handleDeleteClusterSchedulerConfig(ctx, body)

		return r, true, err

	// ComputeQuota
	case opCreateComputeQuota:
		r, err := h.handleCreateComputeQuota(ctx, body)

		return r, true, err
	case opDescribeComputeQuota:
		r, err := h.handleDescribeComputeQuota(ctx, body)

		return r, true, err
	case opListComputeQuotas:
		r, err := h.handleListComputeQuotas(ctx, body)

		return r, true, err
	case opUpdateComputeQuota:
		r, err := h.handleUpdateComputeQuota(ctx, body)

		return r, true, err
	case opDeleteComputeQuota:
		r, err := h.handleDeleteComputeQuota(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

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

// ---------------------------------------------------------------------------
// InferenceComponent handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags          map[string]string `json:"Tags"`
		RuntimeConfig *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"RuntimeConfig"`
		InferenceComponentName string `json:"InferenceComponentName"`
		EndpointName           string `json:"EndpointName"`
		VariantName            string `json:"VariantName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.RuntimeConfig != nil {
		copyCount = req.RuntimeConfig.CopyCount
	}

	c, err := h.Backend.CreateInferenceComponent(ctx, CreateInferenceComponentOptions{
		InferenceComponentName: req.InferenceComponentName,
		EndpointName:           req.EndpointName,
		VariantName:            req.VariantName,
		CopyCount:              copyCount,
		Tags:                   req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleDescribeInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

func (h *Handler) handleListInferenceComponents(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		EndpointName string `json:"EndpointNameEquals"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	components, next := h.Backend.ListInferenceComponents(ctx, req.EndpointName, req.NextToken)

	items := make([]map[string]any, 0, len(components))
	for _, c := range components {
		items = append(items, map[string]any{
			"InferenceComponentName":   c.InferenceComponentName,
			keyInferenceComponentArn:   c.InferenceComponentArn,
			"EndpointName":             c.EndpointName,
			"InferenceComponentStatus": c.InferenceComponentStatus,
			keyCreationTime:            c.CreationTime,
			keyLastModifiedTime:        c.LastModifiedTime,
		})
	}

	return listResp("InferenceComponents", items, next)
}

func (h *Handler) handleUpdateInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		RuntimeConfig *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"RuntimeConfig"`
		InferenceComponentName string `json:"InferenceComponentName"`
		VariantName            string `json:"VariantName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.RuntimeConfig != nil {
		copyCount = req.RuntimeConfig.CopyCount
	}

	if err := h.Backend.UpdateInferenceComponent(
		ctx,
		req.InferenceComponentName,
		req.VariantName,
		copyCount,
	); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleUpdateInferenceComponentRuntimeConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DesiredRuntimeConfig *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"DesiredRuntimeConfig"`
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.DesiredRuntimeConfig != nil {
		copyCount = req.DesiredRuntimeConfig.CopyCount
	}

	if err := h.Backend.UpdateInferenceComponentRuntimeConfig(ctx, req.InferenceComponentName, copyCount); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleDeleteInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteInferenceComponent(ctx, req.InferenceComponentName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                       map[string]string `json:"Tags"`
		ClusterSchedulerConfigName string            `json:"ClusterSchedulerConfigName"`
		ClusterArn                 string            `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.CreateClusterSchedulerConfig(ctx, CreateClusterSchedulerConfigOptions{
		ClusterSchedulerConfigName: req.ClusterSchedulerConfigName,
		ClusterArn:                 req.ClusterArn,
		Tags:                       req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
	})
}

func (h *Handler) handleDescribeClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

func (h *Handler) handleListClusterSchedulerConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	configs, next := h.Backend.ListClusterSchedulerConfigs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, map[string]any{
			"ClusterSchedulerConfigName": c.ClusterSchedulerConfigName,
			keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
			keyStatus:                    c.Status,
			keyCreationTime:              c.CreationTime,
			keyLastModifiedTime:          c.LastModifiedTime,
		})
	}

	return listResp("ClusterSchedulerConfigSummaries", items, next)
}

func (h *Handler) handleUpdateClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
		ClusterArn                 string `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName, req.ClusterArn); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
	})
}

func (h *Handler) handleDeleteClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// ComputeQuota handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags             map[string]string `json:"Tags"`
		ComputeQuotaName string            `json:"ComputeQuotaName"`
		ClusterArn       string            `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.CreateComputeQuota(ctx, CreateComputeQuotaOptions{
		ComputeQuotaName: req.ComputeQuotaName,
		ClusterArn:       req.ClusterArn,
		Tags:             req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyComputeQuotaArn: q.ComputeQuotaArn,
	})
}

func (h *Handler) handleDescribeComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.DescribeComputeQuota(ctx, req.ComputeQuotaName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(q)
}

func (h *Handler) handleListComputeQuotas(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	quotas, next := h.Backend.ListComputeQuotas(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(quotas))
	for _, q := range quotas {
		items = append(items, map[string]any{
			"ComputeQuotaName":  q.ComputeQuotaName,
			keyComputeQuotaArn:  q.ComputeQuotaArn,
			keyStatus:           q.Status,
			keyCreationTime:     q.CreationTime,
			keyLastModifiedTime: q.LastModifiedTime,
		})
	}

	return listResp("ComputeQuotaSummaries", items, next)
}

func (h *Handler) handleUpdateComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
		ClusterArn       string `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateComputeQuota(ctx, req.ComputeQuotaName, req.ClusterArn); err != nil {
		return nil, err
	}

	q, err := h.Backend.DescribeComputeQuota(ctx, req.ComputeQuotaName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyComputeQuotaArn: q.ComputeQuotaArn,
	})
}

func (h *Handler) handleDeleteComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteComputeQuota(ctx, req.ComputeQuotaName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
