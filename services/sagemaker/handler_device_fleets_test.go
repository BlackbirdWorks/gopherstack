package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DeviceFleetLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
		"RoleArn":         "arn:aws:iam::000000000000:role/TestRole",
		"Description":     "test fleet",
		"OutputConfig": map[string]any{
			"S3OutputLocation": "s3://my-bucket/output",
		},
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

	// OutputConfig is a required member of DescribeDeviceFleetOutput in the
	// real API — it must always be present, never omitted.
	outCfg, ok := descResp["OutputConfig"].(map[string]any)
	require.True(t, ok, "OutputConfig must be present in DescribeDeviceFleet response")
	assert.Equal(t, "s3://my-bucket/output", outCfg["S3OutputLocation"])

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
		"OutputConfig": map[string]any{
			"S3OutputLocation": "s3://my-bucket/new-output",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeDeviceFleet", map[string]any{
		"DeviceFleetName": "my-fleet",
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "updated description", descResp["Description"])
	outCfg, ok = descResp["OutputConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3://my-bucket/new-output", outCfg["S3OutputLocation"])

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

	body := map[string]any{
		"DeviceFleetName": "dup-fleet",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	}
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
	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "fleet-a",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})

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

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "fleet-a",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})
	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "fleet-b",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})
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

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "fleet-a",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})

	rec := doSageMakerRequest(t, h, "DescribeDevice", map[string]any{
		"DeviceFleetName": "fleet-a",
		"DeviceName":      "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// InferenceComponent tests
// ---------------------------------------------------------------------------
