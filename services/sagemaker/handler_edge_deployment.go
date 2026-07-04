package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// edge deployment operation name constants.
const (
	opCreateEdgeDeploymentPlan   = "CreateEdgeDeploymentPlan"
	opDescribeEdgeDeploymentPlan = "DescribeEdgeDeploymentPlan"
	opDeleteEdgeDeploymentPlan   = "DeleteEdgeDeploymentPlan"
	opListEdgeDeploymentPlans    = "ListEdgeDeploymentPlans"
	opCreateEdgeDeploymentStage  = "CreateEdgeDeploymentStage"
	opDeleteEdgeDeploymentStage  = "DeleteEdgeDeploymentStage"
	opStartEdgeDeploymentStage   = "StartEdgeDeploymentStage"
	opStopEdgeDeploymentStage    = "StopEdgeDeploymentStage"
	opGetDeviceFleetReport       = "GetDeviceFleetReport"
	opListStageDevices           = "ListStageDevices"
	opUpdateDevices              = "UpdateDevices"
)

// edgeDeploymentOpsSupported returns the real stateful operations implemented
// for the Edge Manager deployment-plan/device-fleet-report family.
func edgeDeploymentOpsSupported() []string {
	return []string{
		opCreateEdgeDeploymentPlan,
		opDescribeEdgeDeploymentPlan,
		opDeleteEdgeDeploymentPlan,
		opListEdgeDeploymentPlans,
		opCreateEdgeDeploymentStage,
		opDeleteEdgeDeploymentStage,
		opStartEdgeDeploymentStage,
		opStopEdgeDeploymentStage,
		opGetDeviceFleetReport,
		opListStageDevices,
		opUpdateDevices,
	}
}

// dispatchAccuracy4AndEdgeOps combines the accuracy4 (DeviceFleet/Device/
// InferenceComponent/etc.) and edge deployment dispatchers into a single
// call site so dispatchNewOps only grows by one branch per grouped family
// instead of one per family.
func (h *Handler) dispatchAccuracy4AndEdgeOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchAccuracy4Ops(ctx, op, body); ok {
		return r, true, err
	}

	return h.dispatchEdgeDeploymentOps(ctx, op, body)
}

// dispatchEdgeDeploymentOps dispatches the edge deployment plan/stage and
// device fleet reporting operations.
func (h *Handler) dispatchEdgeDeploymentOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateEdgeDeploymentPlan:
		r, err := h.handleCreateEdgeDeploymentPlan(ctx, body)

		return r, true, err
	case opDescribeEdgeDeploymentPlan:
		r, err := h.handleDescribeEdgeDeploymentPlan(ctx, body)

		return r, true, err
	case opDeleteEdgeDeploymentPlan:
		return nil, true, h.handleDeleteEdgeDeploymentPlan(ctx, body)
	case opListEdgeDeploymentPlans:
		r, err := h.handleListEdgeDeploymentPlans(ctx, body)

		return r, true, err
	case opCreateEdgeDeploymentStage:
		return nil, true, h.handleCreateEdgeDeploymentStage(ctx, body)
	case opDeleteEdgeDeploymentStage:
		return nil, true, h.handleDeleteEdgeDeploymentStage(ctx, body)
	case opStartEdgeDeploymentStage:
		return nil, true, h.handleStartEdgeDeploymentStage(ctx, body)
	case opStopEdgeDeploymentStage:
		return nil, true, h.handleStopEdgeDeploymentStage(ctx, body)
	case opGetDeviceFleetReport:
		r, err := h.handleGetDeviceFleetReport(ctx, body)

		return r, true, err
	case opListStageDevices:
		r, err := h.handleListStageDevices(ctx, body)

		return r, true, err
	case opUpdateDevices:
		return nil, true, h.handleUpdateDevices(ctx, body)
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// EdgeDeploymentPlan handlers
// ---------------------------------------------------------------------------

type edgeDeploymentStageRequest struct {
	StageName             string          `json:"StageName"`
	DeviceSelectionConfig json.RawMessage `json:"DeviceSelectionConfig,omitempty"`
	DeploymentConfig      json.RawMessage `json:"DeploymentConfig,omitempty"`
}

func toEdgeDeploymentStageInputs(reqs []edgeDeploymentStageRequest) []EdgeDeploymentStageInput {
	inputs := make([]EdgeDeploymentStageInput, 0, len(reqs))
	for _, s := range reqs {
		inputs = append(inputs, EdgeDeploymentStageInput(s))
	}

	return inputs
}

func (h *Handler) handleCreateEdgeDeploymentPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EdgeDeploymentPlanName string `json:"EdgeDeploymentPlanName"`
		DeviceFleetName        string `json:"DeviceFleetName"`
		ModelConfigs           []struct {
			ModelHandle          string `json:"ModelHandle"`
			EdgePackagingJobName string `json:"EdgePackagingJobName"`
		} `json:"ModelConfigs"`
		Stages []edgeDeploymentStageRequest `json:"Stages"`
		Tags   []tagObject                  `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	modelConfigs := make([]EdgeDeploymentModelConfig, 0, len(req.ModelConfigs))
	for _, mc := range req.ModelConfigs {
		modelConfigs = append(modelConfigs, EdgeDeploymentModelConfig{
			ModelHandle:          mc.ModelHandle,
			EdgePackagingJobName: mc.EdgePackagingJobName,
		})
	}

	p, err := h.Backend.CreateEdgeDeploymentPlan(ctx, CreateEdgeDeploymentPlanOptions{
		EdgeDeploymentPlanName: req.EdgeDeploymentPlanName,
		DeviceFleetName:        req.DeviceFleetName,
		ModelConfigs:           modelConfigs,
		Stages:                 toEdgeDeploymentStageInputs(req.Stages),
		Tags:                   fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyEdgeDeploymentPlanArn: p.EdgeDeploymentPlanArn})
}

// stageStatusSummary builds the DeploymentStageStatusSummary shape for a
// single stage. Per-stage device counts are always zero: this emulator does
// not simulate per-device deployment progress.
func stageStatusSummary(s *EdgeDeploymentStage) map[string]any {
	summary := map[string]any{
		"StageName": s.StageName,
		"DeploymentStatus": map[string]any{
			"StageStatus":                  s.DeploymentStatus,
			"EdgeDeploymentSuccessInStage": 0,
			"EdgeDeploymentPendingInStage": 0,
			"EdgeDeploymentFailedInStage":  0,
		},
	}

	if s.DeviceSelectionConfig != nil {
		summary["DeviceSelectionConfig"] = s.DeviceSelectionConfig
	}

	if s.DeploymentConfig != nil {
		summary["DeploymentConfig"] = s.DeploymentConfig
	}

	return summary
}

func (h *Handler) handleDescribeEdgeDeploymentPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EdgeDeploymentPlanName string `json:"EdgeDeploymentPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	p, err := h.Backend.DescribeEdgeDeploymentPlan(ctx, req.EdgeDeploymentPlanName)
	if err != nil {
		return nil, err
	}

	modelConfigs := make([]map[string]any, 0, len(p.ModelConfigs))
	for _, mc := range p.ModelConfigs {
		modelConfigs = append(modelConfigs, map[string]any{
			"ModelHandle":          mc.ModelHandle,
			"EdgePackagingJobName": mc.EdgePackagingJobName,
		})
	}

	stages := make([]map[string]any, 0, len(p.Stages))
	for _, s := range p.Stages {
		stages = append(stages, stageStatusSummary(s))
	}

	return json.Marshal(map[string]any{
		keyDeviceFleetName:        p.DeviceFleetName,
		keyEdgeDeploymentPlanArn:  p.EdgeDeploymentPlanArn,
		keyEdgeDeploymentPlanName: p.EdgeDeploymentPlanName,
		"ModelConfigs":            modelConfigs,
		"Stages":                  stages,
		"EdgeDeploymentSuccess":   0,
		"EdgeDeploymentPending":   0,
		"EdgeDeploymentFailed":    0,
		keyCreationTime:           epochSeconds(p.CreationTime),
		keyLastModifiedTime:       epochSeconds(p.LastModifiedTime),
	})
}

func (h *Handler) handleDeleteEdgeDeploymentPlan(ctx context.Context, body []byte) error {
	var req struct {
		EdgeDeploymentPlanName string `json:"EdgeDeploymentPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return h.Backend.DeleteEdgeDeploymentPlan(ctx, req.EdgeDeploymentPlanName)
}

func (h *Handler) handleListEdgeDeploymentPlans(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	plans, nextToken := h.Backend.ListEdgeDeploymentPlans(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(plans))
	for _, p := range plans {
		items = append(items, map[string]any{
			keyDeviceFleetName:        p.DeviceFleetName,
			keyEdgeDeploymentPlanArn:  p.EdgeDeploymentPlanArn,
			keyEdgeDeploymentPlanName: p.EdgeDeploymentPlanName,
			"EdgeDeploymentSuccess":   0,
			"EdgeDeploymentPending":   0,
			"EdgeDeploymentFailed":    0,
			keyCreationTime:           epochSeconds(p.CreationTime),
			keyLastModifiedTime:       epochSeconds(p.LastModifiedTime),
		})
	}

	return listResp("EdgeDeploymentPlanSummaries", items, nextToken)
}

// ---------------------------------------------------------------------------
// EdgeDeploymentStage handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateEdgeDeploymentStage(ctx context.Context, body []byte) error {
	var req struct {
		EdgeDeploymentPlanName string                       `json:"EdgeDeploymentPlanName"`
		Stages                 []edgeDeploymentStageRequest `json:"Stages"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return h.Backend.CreateEdgeDeploymentStage(ctx, req.EdgeDeploymentPlanName, toEdgeDeploymentStageInputs(req.Stages))
}

func (h *Handler) handleDeleteEdgeDeploymentStage(ctx context.Context, body []byte) error {
	var req struct {
		EdgeDeploymentPlanName string `json:"EdgeDeploymentPlanName"`
		StageName              string `json:"StageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return h.Backend.DeleteEdgeDeploymentStage(ctx, req.EdgeDeploymentPlanName, req.StageName)
}

func (h *Handler) handleStartEdgeDeploymentStage(ctx context.Context, body []byte) error {
	var req struct {
		EdgeDeploymentPlanName string `json:"EdgeDeploymentPlanName"`
		StageName              string `json:"StageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return h.Backend.StartEdgeDeploymentStage(ctx, req.EdgeDeploymentPlanName, req.StageName)
}

func (h *Handler) handleStopEdgeDeploymentStage(ctx context.Context, body []byte) error {
	var req struct {
		EdgeDeploymentPlanName string `json:"EdgeDeploymentPlanName"`
		StageName              string `json:"StageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return h.Backend.StopEdgeDeploymentStage(ctx, req.EdgeDeploymentPlanName, req.StageName)
}

// ---------------------------------------------------------------------------
// GetDeviceFleetReport / ListStageDevices / UpdateDevices handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleGetDeviceFleetReport(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	f, registeredCount, err := h.Backend.GetDeviceFleetReport(ctx, req.DeviceFleetName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyDeviceFleetName: f.DeviceFleetName,
		"DeviceFleetArn":   f.DeviceFleetArn,
		"ReportGenerated":  epochSeconds(f.LastModifiedTime),
		"DeviceStats": map[string]any{
			"ConnectedDeviceCount":  0,
			"RegisteredDeviceCount": registeredCount,
		},
		"AgentVersions": []any{},
		"ModelStats":    []any{},
	}

	if f.Description != "" {
		resp["Description"] = f.Description
	}

	if f.OutputConfig != nil {
		outputConfig := map[string]any{"S3OutputLocation": f.OutputConfig.S3OutputLocation}
		if f.OutputConfig.KmsKeyID != "" {
			outputConfig["KmsKeyId"] = f.OutputConfig.KmsKeyID
		}

		resp["OutputConfig"] = outputConfig
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListStageDevices(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EdgeDeploymentPlanName string `json:"EdgeDeploymentPlanName"`
		StageName              string `json:"StageName"`
		NextToken              string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	plan, devices, status, nextToken, err := h.Backend.ListStageDevices(
		ctx, req.EdgeDeploymentPlanName, req.StageName, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(devices))
	for _, d := range devices {
		item := map[string]any{
			keyEdgeDeploymentPlanArn:  plan.EdgeDeploymentPlanArn,
			keyEdgeDeploymentPlanName: plan.EdgeDeploymentPlanName,
			"StageName":               req.StageName,
			keyDeviceFleetName:        d.DeviceFleetName,
			keyDeviceName:             d.DeviceName,
			"DeviceArn":               d.DeviceArn,
			"DeviceDeploymentStatus":  status,
		}

		if d.Description != "" {
			item["Description"] = d.Description
		}

		items = append(items, item)
	}

	return listResp("DeviceDeploymentSummaries", items, nextToken)
}

func (h *Handler) handleUpdateDevices(ctx context.Context, body []byte) error {
	var req struct {
		DeviceFleetName string `json:"DeviceFleetName"`
		Devices         []struct {
			DeviceName   string `json:"DeviceName"`
			Description  string `json:"Description"`
			IotThingName string `json:"IotThingName"`
		} `json:"Devices"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	inputs := make([]UpdateDeviceInput, 0, len(req.Devices))
	for _, d := range req.Devices {
		inputs = append(inputs, UpdateDeviceInput{
			DeviceName:   d.DeviceName,
			Description:  d.Description,
			IotThingName: d.IotThingName,
		})
	}

	return h.Backend.UpdateDevices(ctx, req.DeviceFleetName, inputs)
}
