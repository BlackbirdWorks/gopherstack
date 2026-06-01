package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// DeviceFleet tests
// ---------------------------------------------------------------------------

func TestHandler_DeviceFleetLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
		"RoleArn":         "arn:aws:iam::000000000000:role/TestRole",
		"Description":     "test fleet",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Empty(t, createResp)

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-fleet", descResp["DeviceFleetName"])
	assert.Equal(t, "test fleet", descResp["Description"])
	assert.Contains(t, descResp["DeviceFleetArn"], "my-fleet")

	// List
	rec = doSageMakerRequest(t, h, "ListDeviceFleets", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["DeviceFleetSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Update
	rec = doSageMakerRequest(t, h, "UpdateDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
		"Description":     "updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "updated description", descResp["Description"])

	// Delete
	rec = doSageMakerRequest(t, h, "DeleteDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = doSageMakerRequest(t, h, "DescribeDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeviceFleet_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeDeviceFleet", map[string]any{
		"DeviceFleetName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeviceFleet_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"DeviceFleetName": "dup-fleet"}
	doSageMakerRequest(t, h, "CreateDeviceFleet", body)

	rec := doSageMakerRequest(t, h, "CreateDeviceFleet", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Device tests
// ---------------------------------------------------------------------------

func TestHandler_DeviceLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create fleet first
	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{"DeviceFleetName": "fleet-a"})

	// Register devices
	rec := doSageMakerRequest(t, h, "RegisterDevices", map[string]any{
		"DeviceFleetName": "fleet-a",
		"Devices": []any{
			map[string]any{"DeviceName": "dev-1", "Description": "first device"},
			map[string]any{"DeviceName": "dev-2"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe device
	rec = doSageMakerRequest(t, h, "DescribeDevice", map[string]any{
		"DeviceFleetName": "fleet-a",
		"DeviceName":      "dev-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "dev-1", descResp["DeviceName"])
	assert.Equal(t, "fleet-a", descResp["DeviceFleetName"])

	// List devices
	rec = doSageMakerRequest(t, h, "ListDevices", map[string]any{
		"DeviceFleetName": "fleet-a",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["DeviceSummaries"].([]any)
	assert.Len(t, summaries, 2)

	// Deregister one device
	rec = doSageMakerRequest(t, h, "DeregisterDevices", map[string]any{
		"DeviceFleetName": "fleet-a",
		"DeviceNames":     []any{"dev-1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify only one remains
	rec = doSageMakerRequest(t, h, "ListDevices", map[string]any{"DeviceFleetName": "fleet-a"})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries = listResp["DeviceSummaries"].([]any)
	assert.Len(t, summaries, 1)
}

func TestHandler_ListDevices_NoFleetFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{"DeviceFleetName": "fleet-a"})
	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{"DeviceFleetName": "fleet-b"})
	doSageMakerRequest(t, h, "RegisterDevices", map[string]any{
		"DeviceFleetName": "fleet-a",
		"Devices":         []any{map[string]any{"DeviceName": "dev-a"}},
	})
	doSageMakerRequest(t, h, "RegisterDevices", map[string]any{
		"DeviceFleetName": "fleet-b",
		"Devices":         []any{map[string]any{"DeviceName": "dev-b"}},
	})

	// List all devices (no fleet filter)
	rec := doSageMakerRequest(t, h, "ListDevices", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["DeviceSummaries"].([]any)
	assert.Len(t, summaries, 2)
}

func TestHandler_DescribeDevice_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{"DeviceFleetName": "fleet-a"})

	rec := doSageMakerRequest(t, h, "DescribeDevice", map[string]any{
		"DeviceFleetName": "fleet-a",
		"DeviceName":      "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// InferenceComponent tests
// ---------------------------------------------------------------------------

func TestHandler_InferenceComponentLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
		"EndpointName":           "my-endpoint",
		"VariantName":            "variant-1",
		"RuntimeConfig":          map[string]any{"CopyCount": 2},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["InferenceComponentArn"], "my-component")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-component", descResp["InferenceComponentName"])
	assert.Equal(t, "my-endpoint", descResp["EndpointName"])
	assert.Equal(t, "Creating", descResp["InferenceComponentStatus"])

	// List
	rec = doSageMakerRequest(t, h, "ListInferenceComponents", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	components := listResp["InferenceComponents"].([]any)
	assert.Len(t, components, 1)

	// Update runtime config
	rec = doSageMakerRequest(t, h, "UpdateInferenceComponentRuntimeConfig", map[string]any{
		"InferenceComponentName": "my-component",
		"DesiredRuntimeConfig":   map[string]any{"CopyCount": 4},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Contains(t, updateResp["InferenceComponentArn"], "my-component")

	// Update component
	rec = doSageMakerRequest(t, h, "UpdateInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
		"VariantName":            "variant-2",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doSageMakerRequest(t, h, "DeleteInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = doSageMakerRequest(t, h, "DescribeInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InferenceComponent_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeInferenceComponent", map[string]any{
		"InferenceComponentName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InferenceComponent_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"InferenceComponentName": "dup-component",
		"EndpointName":           "ep",
	}
	doSageMakerRequest(t, h, "CreateInferenceComponent", body)

	rec := doSageMakerRequest(t, h, "CreateInferenceComponent", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListInferenceComponents_EndpointFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceComponent", map[string]any{
		"InferenceComponentName": "comp-a",
		"EndpointName":           "ep-1",
	})
	doSageMakerRequest(t, h, "CreateInferenceComponent", map[string]any{
		"InferenceComponentName": "comp-b",
		"EndpointName":           "ep-2",
	})

	rec := doSageMakerRequest(t, h, "ListInferenceComponents", map[string]any{
		"EndpointNameEquals": "ep-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	components := resp["InferenceComponents"].([]any)
	assert.Len(t, components, 1)
}

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig tests
// ---------------------------------------------------------------------------

func TestHandler_ClusterSchedulerConfigLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateClusterSchedulerConfig", map[string]any{
		"ClusterSchedulerConfigName": "my-config",
		"ClusterArn":                 "arn:aws:sagemaker:us-east-1:000000000000:cluster/my-cluster",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["ClusterSchedulerConfigArn"], "my-config")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeClusterSchedulerConfig", map[string]any{
		"ClusterSchedulerConfigName": "my-config",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-config", descResp["ClusterSchedulerConfigName"])
	assert.Equal(t, "Creating", descResp["Status"])

	// List
	rec = doSageMakerRequest(t, h, "ListClusterSchedulerConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["ClusterSchedulerConfigSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Update
	rec = doSageMakerRequest(t, h, "UpdateClusterSchedulerConfig", map[string]any{
		"ClusterSchedulerConfigName": "my-config",
		"ClusterArn":                 "arn:aws:sagemaker:us-east-1:000000000000:cluster/new-cluster",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doSageMakerRequest(t, h, "DeleteClusterSchedulerConfig", map[string]any{
		"ClusterSchedulerConfigName": "my-config",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = doSageMakerRequest(t, h, "DescribeClusterSchedulerConfig", map[string]any{
		"ClusterSchedulerConfigName": "my-config",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ClusterSchedulerConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeClusterSchedulerConfig", map[string]any{
		"ClusterSchedulerConfigName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ClusterSchedulerConfig_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"ClusterSchedulerConfigName": "dup-config"}
	doSageMakerRequest(t, h, "CreateClusterSchedulerConfig", body)

	rec := doSageMakerRequest(t, h, "CreateClusterSchedulerConfig", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ComputeQuota tests
// ---------------------------------------------------------------------------

func TestHandler_ComputeQuotaLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateComputeQuota", map[string]any{
		"ComputeQuotaName": "my-quota",
		"ClusterArn":       "arn:aws:sagemaker:us-east-1:000000000000:cluster/my-cluster",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["ComputeQuotaArn"], "my-quota")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeComputeQuota", map[string]any{
		"ComputeQuotaName": "my-quota",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-quota", descResp["ComputeQuotaName"])
	assert.Equal(t, "Created", descResp["Status"])

	// List
	rec = doSageMakerRequest(t, h, "ListComputeQuotas", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["ComputeQuotaSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Update
	rec = doSageMakerRequest(t, h, "UpdateComputeQuota", map[string]any{
		"ComputeQuotaName": "my-quota",
		"ClusterArn":       "arn:aws:sagemaker:us-east-1:000000000000:cluster/new-cluster",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Contains(t, updateResp["ComputeQuotaArn"], "my-quota")

	// Delete
	rec = doSageMakerRequest(t, h, "DeleteComputeQuota", map[string]any{
		"ComputeQuotaName": "my-quota",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = doSageMakerRequest(t, h, "DescribeComputeQuota", map[string]any{
		"ComputeQuotaName": "my-quota",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ComputeQuota_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeComputeQuota", map[string]any{
		"ComputeQuotaName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ComputeQuota_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"ComputeQuotaName": "dup-quota"}
	doSageMakerRequest(t, h, "CreateComputeQuota", body)

	rec := doSageMakerRequest(t, h, "CreateComputeQuota", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListComputeQuotas_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		doSageMakerRequest(t, h, "CreateComputeQuota", map[string]any{
			"ComputeQuotaName": "quota-" + string(rune('a'+i)),
		})
	}

	rec := doSageMakerRequest(t, h, "ListComputeQuotas", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["ComputeQuotaSummaries"].([]any)
	assert.Len(t, summaries, 3)
}
