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
//nolint:cyclop,gocyclo,funlen // large switch for 25 operations
func (h *Handler) dispatchAccuracy4Ops(
	_ context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// DeviceFleet
	case opCreateDeviceFleet:
		r, err := h.handleCreateDeviceFleet(body)
		return r, true, err
	case opDescribeDeviceFleet:
		r, err := h.handleDescribeDeviceFleet(body)
		return r, true, err
	case opListDeviceFleets:
		r, err := h.handleListDeviceFleets(body)
		return r, true, err
	case opUpdateDeviceFleet:
		r, err := h.handleUpdateDeviceFleet(body)
		return r, true, err
	case opDeleteDeviceFleet:
		r, err := h.handleDeleteDeviceFleet(body)
		return r, true, err

	// Device
	case opRegisterDevices:
		r, err := h.handleRegisterDevices(body)
		return r, true, err
	case opDeregisterDevices:
		r, err := h.handleDeregisterDevices(body)
		return r, true, err
	case opDescribeDevice:
		r, err := h.handleDescribeDevice(body)
		return r, true, err
	case opListDevices:
		r, err := h.handleListDevices(body)
		return r, true, err

	// InferenceComponent
	case opCreateInferenceComponent:
		r, err := h.handleCreateInferenceComponent(body)
		return r, true, err
	case opDescribeInferenceComponent:
		r, err := h.handleDescribeInferenceComponent(body)
		return r, true, err
	case opListInferenceComponents:
		r, err := h.handleListInferenceComponents(body)
		return r, true, err
	case opUpdateInferenceComponent:
		r, err := h.handleUpdateInferenceComponent(body)
		return r, true, err
	case opUpdateInferenceComponentRuntimeConfig:
		r, err := h.handleUpdateInferenceComponentRuntimeConfig(body)
		return r, true, err
	case opDeleteInferenceComponent:
		r, err := h.handleDeleteInferenceComponent(body)
		return r, true, err

	// ClusterSchedulerConfig
	case opCreateClusterSchedulerConfig:
		r, err := h.handleCreateClusterSchedulerConfig(body)
		return r, true, err
	case opDescribeClusterSchedulerConfig:
		r, err := h.handleDescribeClusterSchedulerConfig(body)
		return r, true, err
	case opListClusterSchedulerConfigs:
		r, err := h.handleListClusterSchedulerConfigs(body)
		return r, true, err
	case opUpdateClusterSchedulerConfig:
		r, err := h.handleUpdateClusterSchedulerConfig(body)
		return r, true, err
	case opDeleteClusterSchedulerConfig:
		r, err := h.handleDeleteClusterSchedulerConfig(body)
		return r, true, err

	// ComputeQuota
	case opCreateComputeQuota:
		r, err := h.handleCreateComputeQuota(body)
		return r, true, err
	case opDescribeComputeQuota:
		r, err := h.handleDescribeComputeQuota(body)
		return r, true, err
	case opListComputeQuotas:
		r, err := h.handleListComputeQuotas(body)
		return r, true, err
	case opUpdateComputeQuota:
		r, err := h.handleUpdateComputeQuota(body)
		return r, true, err
	case opDeleteComputeQuota:
		r, err := h.handleDeleteComputeQuota(body)
		return r, true, err
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// DeviceFleet handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDeviceFleet(body []byte) ([]byte, error) {
	var req struct {
		Tags            map[string]string `json:"Tags"`
		DeviceFleetName string            `json:"DeviceFleetName"`
		Description     string            `json:"Description"`
		RoleArn         string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if _, err := h.Backend.CreateDeviceFleet(CreateDeviceFleetOptions{
		DeviceFleetName: req.DeviceFleetName,
		Description:     req.Description,
		RoleArn:         req.RoleArn,
		Tags:            req.Tags,
	}); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDescribeDeviceFleet(body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	f, err := h.Backend.DescribeDeviceFleet(req.DeviceFleetName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(f)
}

func (h *Handler) handleListDeviceFleets(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	fleets, next := h.Backend.ListDeviceFleets(req.NextToken)

	items := make([]map[string]any, 0, len(fleets))
	for _, f := range fleets {
		items = append(items, map[string]any{
			keyDeviceFleetName:   f.DeviceFleetName,
			"DeviceFleetArn":     f.DeviceFleetArn,
			keyCreationTime:      f.CreationTime,
			keyLastModifiedTime:  f.LastModifiedTime,
		})
	}

	return listResp("DeviceFleetSummaries", items, next)
}

func (h *Handler) handleUpdateDeviceFleet(body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
		Description     string `json:"Description"`
		RoleArn         string `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateDeviceFleet(req.DeviceFleetName, req.Description, req.RoleArn); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDeleteDeviceFleet(body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteDeviceFleet(req.DeviceFleetName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// Device handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleRegisterDevices(body []byte) ([]byte, error) {
	var req struct {
		Devices []struct {
			DeviceName   string            `json:"DeviceName"`
			Description  string            `json:"Description"`
			IotThingName string            `json:"IotThingName"`
			Tags         map[string]string `json:"Tags"`
		} `json:"Devices"`
		DeviceFleetName string `json:"DeviceFleetName"`
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

	if err := h.Backend.RegisterDevices(req.DeviceFleetName, inputs); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDeregisterDevices(body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string   `json:"DeviceFleetName"`
		DeviceNames     []string `json:"DeviceNames"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeregisterDevices(req.DeviceFleetName, req.DeviceNames); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDescribeDevice(body []byte) ([]byte, error) {
	var req struct {
		DeviceName      string `json:"DeviceName"`
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	d, err := h.Backend.DescribeDevice(req.DeviceFleetName, req.DeviceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(d)
}

func (h *Handler) handleListDevices(body []byte) ([]byte, error) {
	var req struct {
		NextToken       string `json:"NextToken"`
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	devices, next := h.Backend.ListDevices(req.DeviceFleetName, req.NextToken)

	items := make([]map[string]any, 0, len(devices))
	for _, d := range devices {
		items = append(items, map[string]any{
			"DeviceName":       d.DeviceName,
			"DeviceFleetName":  d.DeviceFleetName,
			"DeviceArn":        d.DeviceArn,
			"RegistrationTime": d.RegistrationTime,
		})
	}

	return listResp("DeviceSummaries", items, next)
}

// ---------------------------------------------------------------------------
// InferenceComponent handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceComponent(body []byte) ([]byte, error) {
	var req struct {
		Tags                   map[string]string `json:"Tags"`
		InferenceComponentName string            `json:"InferenceComponentName"`
		EndpointName           string            `json:"EndpointName"`
		VariantName            string            `json:"VariantName"`
		RuntimeConfig          *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"RuntimeConfig"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.RuntimeConfig != nil {
		copyCount = req.RuntimeConfig.CopyCount
	}

	c, err := h.Backend.CreateInferenceComponent(CreateInferenceComponentOptions{
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

func (h *Handler) handleDescribeInferenceComponent(body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

func (h *Handler) handleListInferenceComponents(body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		EndpointName string `json:"EndpointNameEquals"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	components, next := h.Backend.ListInferenceComponents(req.EndpointName, req.NextToken)

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

func (h *Handler) handleUpdateInferenceComponent(body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
		VariantName            string `json:"VariantName"`
		RuntimeConfig          *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"RuntimeConfig"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.RuntimeConfig != nil {
		copyCount = req.RuntimeConfig.CopyCount
	}

	if err := h.Backend.UpdateInferenceComponent(req.InferenceComponentName, req.VariantName, copyCount); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleUpdateInferenceComponentRuntimeConfig(body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
		DesiredRuntimeConfig   *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"DesiredRuntimeConfig"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.DesiredRuntimeConfig != nil {
		copyCount = req.DesiredRuntimeConfig.CopyCount
	}

	if err := h.Backend.UpdateInferenceComponentRuntimeConfig(req.InferenceComponentName, copyCount); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleDeleteInferenceComponent(body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteInferenceComponent(req.InferenceComponentName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateClusterSchedulerConfig(body []byte) ([]byte, error) {
	var req struct {
		Tags                       map[string]string `json:"Tags"`
		ClusterSchedulerConfigName string            `json:"ClusterSchedulerConfigName"`
		ClusterArn                 string            `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.CreateClusterSchedulerConfig(CreateClusterSchedulerConfigOptions{
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

func (h *Handler) handleDescribeClusterSchedulerConfig(body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeClusterSchedulerConfig(req.ClusterSchedulerConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

func (h *Handler) handleListClusterSchedulerConfigs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	configs, next := h.Backend.ListClusterSchedulerConfigs(req.NextToken)

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

func (h *Handler) handleUpdateClusterSchedulerConfig(body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
		ClusterArn                 string `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateClusterSchedulerConfig(req.ClusterSchedulerConfigName, req.ClusterArn); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeClusterSchedulerConfig(req.ClusterSchedulerConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
	})
}

func (h *Handler) handleDeleteClusterSchedulerConfig(body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteClusterSchedulerConfig(req.ClusterSchedulerConfigName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// ComputeQuota handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateComputeQuota(body []byte) ([]byte, error) {
	var req struct {
		Tags             map[string]string `json:"Tags"`
		ComputeQuotaName string            `json:"ComputeQuotaName"`
		ClusterArn       string            `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.CreateComputeQuota(CreateComputeQuotaOptions{
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

func (h *Handler) handleDescribeComputeQuota(body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.DescribeComputeQuota(req.ComputeQuotaName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(q)
}

func (h *Handler) handleListComputeQuotas(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	quotas, next := h.Backend.ListComputeQuotas(req.NextToken)

	items := make([]map[string]any, 0, len(quotas))
	for _, q := range quotas {
		items = append(items, map[string]any{
			"ComputeQuotaName":    q.ComputeQuotaName,
			keyComputeQuotaArn:    q.ComputeQuotaArn,
			keyStatus:             q.Status,
			keyCreationTime:       q.CreationTime,
			keyLastModifiedTime:   q.LastModifiedTime,
		})
	}

	return listResp("ComputeQuotaSummaries", items, next)
}

func (h *Handler) handleUpdateComputeQuota(body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
		ClusterArn       string `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateComputeQuota(req.ComputeQuotaName, req.ClusterArn); err != nil {
		return nil, err
	}

	q, err := h.Backend.DescribeComputeQuota(req.ComputeQuotaName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyComputeQuotaArn: q.ComputeQuotaArn,
	})
}

func (h *Handler) handleDeleteComputeQuota(body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteComputeQuota(req.ComputeQuotaName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
