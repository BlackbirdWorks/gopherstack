package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
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

// TestHandler_CreateDeviceFleet_IotRoleAlias_RealClient asserts
// EnableIotRoleAlias -- entirely absent before this pass, on both
// CreateDeviceFleetInput and UpdateDeviceFleetInput -- synthesizes
// DescribeDeviceFleetOutput.IotRoleAlias as "SageMakerEdge-{DeviceFleetName}"
// (api_op_CreateDeviceFleet.go:43-48), and that UpdateDeviceFleet can toggle
// it back off.
func TestHandler_CreateDeviceFleet_IotRoleAlias_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName:    aws.String("iot-fleet"),
		OutputConfig:       &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
		EnableIotRoleAlias: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.DescribeDeviceFleet(t.Context(), &sagemakersdk.DescribeDeviceFleetInput{
		DeviceFleetName: aws.String("iot-fleet"),
	})
	require.NoError(t, err)
	assert.Equal(t, "SageMakerEdge-iot-fleet", aws.ToString(out.IotRoleAlias))

	_, err = client.UpdateDeviceFleet(t.Context(), &sagemakersdk.UpdateDeviceFleetInput{
		DeviceFleetName:    aws.String("iot-fleet"),
		OutputConfig:       &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
		EnableIotRoleAlias: aws.Bool(false),
	})
	require.NoError(t, err)

	out, err = client.DescribeDeviceFleet(t.Context(), &sagemakersdk.DescribeDeviceFleetInput{
		DeviceFleetName: aws.String("iot-fleet"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(out.IotRoleAlias))
}

// TestHandler_CreateDeviceFleet_PresetDeploymentConfig_RealClient asserts
// OutputConfig.PresetDeploymentConfig/PresetDeploymentType -- both absent
// before this pass -- round-trip through Describe.
func TestHandler_CreateDeviceFleet_PresetDeploymentConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("preset-fleet"),
		OutputConfig: &smtypes.EdgeOutputConfig{
			S3OutputLocation:       aws.String("s3://bucket/out"),
			PresetDeploymentConfig: aws.String(`{"ComponentName":"my-component"}`),
			PresetDeploymentType:   smtypes.EdgePresetDeploymentTypeGreengrassV2Component,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDeviceFleet(t.Context(), &sagemakersdk.DescribeDeviceFleetInput{
		DeviceFleetName: aws.String("preset-fleet"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.OutputConfig)
	assert.JSONEq(t, `{"ComponentName":"my-component"}`, aws.ToString(out.OutputConfig.PresetDeploymentConfig))
	assert.Equal(t, smtypes.EdgePresetDeploymentTypeGreengrassV2Component, out.OutputConfig.PresetDeploymentType)
}

// TestHandler_ListDeviceFleets_FilterSortPage_RealClient asserts
// ListDeviceFleetsInput's NameContains/SortBy/SortOrder/MaxResults/
// CreationTimeAfter -- all absent or (for the two time filters) undecodable
// before this pass -- now work. The real client sends CreationTimeAfter as
// an awsjson1.1 epoch-second number, which a *time.Time-typed request field
// cannot decode at all.
func TestHandler_ListDeviceFleets_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"alpha-fleet", "beta-fleet", "gamma-widget"}
	for _, n := range names {
		_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
			DeviceFleetName: aws.String(n),
			OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
		})
		require.NoError(t, err)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListDeviceFleets(t.Context(), &sagemakersdk.ListDeviceFleetsInput{
			NameContains: aws.String("fleet"),
		})
		require.NoError(t, err)
		assert.Len(t, out.DeviceFleetSummaries, 2)
	})

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListDeviceFleets(t.Context(), &sagemakersdk.ListDeviceFleetsInput{
			SortBy:    smtypes.ListDeviceFleetsSortByName,
			SortOrder: smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.DeviceFleetSummaries, 3)
		assert.Equal(t, "alpha-fleet", aws.ToString(out.DeviceFleetSummaries[0].DeviceFleetName))
		assert.Equal(t, "gamma-widget", aws.ToString(out.DeviceFleetSummaries[2].DeviceFleetName))
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListDeviceFleets(t.Context(), &sagemakersdk.ListDeviceFleetsInput{
			MaxResults: aws.Int32(1),
			SortBy:     smtypes.ListDeviceFleetsSortByName,
			SortOrder:  smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.DeviceFleetSummaries, 1)
		assert.Equal(t, "alpha-fleet", aws.ToString(out.DeviceFleetSummaries[0].DeviceFleetName))
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})

	t.Run("creation time filter does not error", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListDeviceFleets(t.Context(), &sagemakersdk.ListDeviceFleetsInput{
			CreationTimeAfter: aws.Time(time.Now().Add(-time.Hour)),
		})
		require.NoError(t, err)
		assert.Len(t, out.DeviceFleetSummaries, 3)
	})
}

// TestHandler_RegisterDevices_Tags_RealClient asserts RegisterDevicesInput's
// top-level Tags -- previously read from a per-device Tags key the real
// client never sends (types.Device has no Tags field at all), so every
// registration's tags were silently dropped -- now apply to every device in
// the batch and are reachable through ListTagsForResource on the device ARN.
func TestHandler_RegisterDevices_Tags_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("tag-fleet"),
		OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
	})
	require.NoError(t, err)

	_, err = client.RegisterDevices(t.Context(), &sagemakersdk.RegisterDevicesInput{
		DeviceFleetName: aws.String("tag-fleet"),
		Devices:         []smtypes.Device{{DeviceName: aws.String("tagged-device")}},
		Tags:            []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	desc, err := client.DescribeDevice(t.Context(), &sagemakersdk.DescribeDeviceInput{
		DeviceFleetName: aws.String("tag-fleet"),
		DeviceName:      aws.String("tagged-device"),
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTags(t.Context(), &sagemakersdk.ListTagsInput{
		ResourceArn: desc.DeviceArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(tagsOut.Tags[0].Value))
}

// TestHandler_ListDevices_MaxResults_RealClient asserts ListDevicesInput's
// MaxResults -- absent before this pass -- caps the page of devices returned.
func TestHandler_ListDevices_MaxResults_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("page-fleet"),
		OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
	})
	require.NoError(t, err)

	_, err = client.RegisterDevices(t.Context(), &sagemakersdk.RegisterDevicesInput{
		DeviceFleetName: aws.String("page-fleet"),
		Devices: []smtypes.Device{
			{DeviceName: aws.String("dev-a")},
			{DeviceName: aws.String("dev-b")},
		},
	})
	require.NoError(t, err)

	out, err := client.ListDevices(t.Context(), &sagemakersdk.ListDevicesInput{
		DeviceFleetName: aws.String("page-fleet"),
		MaxResults:      aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.DeviceSummaries, 1)
	assert.NotEmpty(t, aws.ToString(out.NextToken))
}

// ---------------------------------------------------------------------------
// InferenceComponent tests
// ---------------------------------------------------------------------------
