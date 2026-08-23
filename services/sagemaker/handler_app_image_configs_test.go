package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{
		"AppImageConfigName": "my-config",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AppImageConfigArn"], "my-config")
}

func TestHandler_DescribeAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-1"})
	rec := doSageMakerRequest(t, h, "DescribeAppImageConfig", map[string]any{"AppImageConfigName": "aic-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "aic-1", resp["AppImageConfigName"])
}

func TestHandler_DeleteAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	rec := doSageMakerRequest(t, h, "DeleteAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-upd"})
	rec := doSageMakerRequest(t, h, "UpdateAppImageConfig", map[string]any{"AppImageConfigName": "aic-upd"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AppImageConfigArn"], "aic-upd")
}

// ---------------------------------------------------------------------------
// InferenceExperiment
// ---------------------------------------------------------------------------

func TestHandler_ListAppImageConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAppImageConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["AppImageConfigs"])

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{
		"AppImageConfigName": "my-config",
	})

	rec = doSageMakerRequest(t, h, "ListAppImageConfigs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	configs := resp["AppImageConfigs"].([]any)
	assert.Len(t, configs, 1)
	c := configs[0].(map[string]any)
	assert.Equal(t, "my-config", c["AppImageConfigName"])
}

// TestHandler_AppImageConfig_KernelConfigsRoundTrip_RealClient asserts
// KernelGatewayImageConfig/JupyterLabAppImageConfig/CodeEditorAppImageConfig
// -- previously all three entirely absent from both Create and Update
// decode -- now round-trip through Describe and are applied by Update.
func TestHandler_AppImageConfig_KernelConfigsRoundTrip_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateAppImageConfig(t.Context(), &sagemakersdk.CreateAppImageConfigInput{
		AppImageConfigName: aws.String("aic-kernel"),
		KernelGatewayImageConfig: &smtypes.KernelGatewayImageConfig{
			KernelSpecs: []smtypes.KernelSpec{{Name: aws.String("python3")}},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeAppImageConfig(t.Context(), &sagemakersdk.DescribeAppImageConfigInput{
		AppImageConfigName: aws.String("aic-kernel"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.KernelGatewayImageConfig)
	require.Len(t, out.KernelGatewayImageConfig.KernelSpecs, 1)
	assert.Equal(t, "python3", aws.ToString(out.KernelGatewayImageConfig.KernelSpecs[0].Name))

	_, err = client.UpdateAppImageConfig(t.Context(), &sagemakersdk.UpdateAppImageConfigInput{
		AppImageConfigName: aws.String("aic-kernel"),
		JupyterLabAppImageConfig: &smtypes.JupyterLabAppImageConfig{
			ContainerConfig: &smtypes.ContainerConfig{ContainerEntrypoint: []string{"/bin/start"}},
		},
	})
	require.NoError(t, err)

	out, err = client.DescribeAppImageConfig(t.Context(), &sagemakersdk.DescribeAppImageConfigInput{
		AppImageConfigName: aws.String("aic-kernel"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.JupyterLabAppImageConfig)
	require.NotNil(t, out.JupyterLabAppImageConfig.ContainerConfig)
	assert.Equal(t, []string{"/bin/start"}, out.JupyterLabAppImageConfig.ContainerConfig.ContainerEntrypoint)
}

// TestHandler_ListAppImageConfigs_DefaultSortOrder_RealClient asserts the
// op's own doc default (api_op_ListAppImageConfigs.go:63,66: SortBy
// CreationTime, SortOrder Descending) -- previously the handler decoded
// only NextToken and dropped every filter/sort control entirely.
func TestHandler_ListAppImageConfigs_DefaultSortOrder_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"first-aic", "second-aic"} {
		_, err := client.CreateAppImageConfig(t.Context(), &sagemakersdk.CreateAppImageConfigInput{
			AppImageConfigName: aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := client.ListAppImageConfigs(t.Context(), &sagemakersdk.ListAppImageConfigsInput{})
	require.NoError(t, err)
	require.Len(t, out.AppImageConfigs, 2)
	assert.Equal(t, "second-aic", aws.ToString(out.AppImageConfigs[0].AppImageConfigName))
	assert.Equal(t, "first-aic", aws.ToString(out.AppImageConfigs[1].AppImageConfigName))

	out, err = client.ListAppImageConfigs(t.Context(), &sagemakersdk.ListAppImageConfigsInput{
		NameContains: aws.String("first"),
	})
	require.NoError(t, err)
	require.Len(t, out.AppImageConfigs, 1)
	assert.Equal(t, "first-aic", aws.ToString(out.AppImageConfigs[0].AppImageConfigName))
}

// ---------------------------------------------------------------------------
// ListTrainingJobsForHyperParameterTuningJob tests
// ---------------------------------------------------------------------------
