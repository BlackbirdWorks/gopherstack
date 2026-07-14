package sagemaker

import (
	"context"
)

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

// dispatchAccuracy4Ops dispatches the accuracy4 real stateful operations (25
// operations across 4 resource families) by delegating to one sub-dispatcher
// per family, so no single switch needs a case for every operation.
func (h *Handler) dispatchAccuracy4Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchDeviceFleetOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchInferenceComponentOps(ctx, op, body); ok {
		return r, true, err
	}

	return h.dispatchClusterSchedulerComputeQuotaOps(ctx, op, body)
}

// dispatchDeviceFleetOps dispatches DeviceFleet and Device operations.
func (h *Handler) dispatchDeviceFleetOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
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
	}

	return nil, false, nil
}

// dispatchInferenceComponentOps dispatches InferenceComponent operations.
func (h *Handler) dispatchInferenceComponentOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
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
	}

	return nil, false, nil
}

// dispatchClusterSchedulerComputeQuotaOps dispatches ClusterSchedulerConfig
// and ComputeQuota operations.
func (h *Handler) dispatchClusterSchedulerComputeQuotaOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
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
