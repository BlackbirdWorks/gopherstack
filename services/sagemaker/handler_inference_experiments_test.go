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

func minimalInferenceExperimentBody(name string) map[string]any {
	return map[string]any{
		"Name":         name,
		"Type":         "ShadowMode",
		"EndpointName": "my-endpoint",
		"ModelVariants": []map[string]any{
			{"ModelName": "m1", "VariantName": "v1"},
		},
		"ShadowModeConfig": map[string]any{
			"SourceModelVariantName": "v1",
		},
	}
}

func TestHandler_CreateInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateInferenceExperiment", minimalInferenceExperimentBody("my-exp"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["InferenceExperimentArn"], "my-exp")
}

func TestHandler_CreateInferenceExperiment_RequiredFieldsEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range []struct {
		body map[string]any
		name string
	}{
		{name: "missing endpoint name", body: map[string]any{"Name": "x", "Type": "ShadowMode"}},
		{
			name: "missing model variants",
			body: map[string]any{"Name": "x", "Type": "ShadowMode", "EndpointName": "ep"},
		},
		{
			name: "missing shadow mode config",
			body: map[string]any{
				"Name": "x", "Type": "ShadowMode", "EndpointName": "ep",
				"ModelVariants": []map[string]any{{"ModelName": "m1", "VariantName": "v1"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "CreateInferenceExperiment", tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_DescribeInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", minimalInferenceExperimentBody("exp-1"))
	rec := doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "exp-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "exp-1", resp["Name"])
	assert.Contains(t, resp, "EndpointMetadata")
	assert.Contains(t, resp, "ModelVariants")
}

func TestHandler_StopInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", minimalInferenceExperimentBody("exp-stop"))
	rec := doSageMakerRequest(t, h, "StopInferenceExperiment", map[string]any{
		"Name":                "exp-stop",
		"ModelVariantActions": map[string]any{"v1": "Retain"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["InferenceExperimentArn"], "exp-stop")
}

func TestHandler_DeleteInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", minimalInferenceExperimentBody("exp-del"))
	rec := doSageMakerRequest(t, h, "DeleteInferenceExperiment", map[string]any{"Name": "exp-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["InferenceExperimentArn"], "exp-del")

	rec = doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "exp-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer
// ---------------------------------------------------------------------------

func TestHandler_InferenceExperiment_StartAndUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", minimalInferenceExperimentBody("inf-exp"))

	recStart := doSageMakerRequest(t, h, "StartInferenceExperiment", map[string]any{"Name": "inf-exp"})
	require.Equal(t, http.StatusOK, recStart.Code)

	recUpdate := doSageMakerRequest(t, h, "UpdateInferenceExperiment", map[string]any{
		"Name": "inf-exp", "Description": "updated",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "inf-exp"})
	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "Running", out["Status"])
	assert.Equal(t, "updated", out["Description"])
}

func TestHandler_InferenceExperiment_StartAndUpdate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"StartInferenceExperiment", "UpdateInferenceExperiment"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"Name": "no-such-experiment"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// HubContent Update/UpdateReference
// ---------------------------------------------------------------------------

func TestHandler_ListInferenceExperiments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListInferenceExperiments", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["InferenceExperiments"])

	body := minimalInferenceExperimentBody("my-exp")
	body["RoleArn"] = "arn:aws:iam::000000000000:role/TestRole"
	doSageMakerRequest(t, h, "CreateInferenceExperiment", body)

	rec = doSageMakerRequest(t, h, "ListInferenceExperiments", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	exps := resp["InferenceExperiments"].([]any)
	assert.Len(t, exps, 1)
	e := exps[0].(map[string]any)
	assert.Equal(t, "my-exp", e["Name"])
}

// TestHandler_CreateInferenceExperiment_FullFields_RealClient asserts
// EndpointName/ModelVariants/ShadowModeConfig -- all required by the real
// API, previously never read at all -- round-trip through Describe, and
// that EndpointMetadata is populated from the live Endpoint store.
func TestHandler_CreateInferenceExperiment_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
		EndpointConfigName: aws.String("exp-endpoint-config"),
		ProductionVariants: []smtypes.ProductionVariant{
			{
				VariantName:          aws.String("v1"),
				ModelName:            aws.String("m1"),
				InitialInstanceCount: aws.Int32(1),
				InstanceType:         smtypes.ProductionVariantInstanceTypeMlM5Large,
			},
		},
	})
	require.NoError(t, err)
	_, err = client.CreateEndpoint(t.Context(), &sagemakersdk.CreateEndpointInput{
		EndpointName:       aws.String("exp-endpoint"),
		EndpointConfigName: aws.String("exp-endpoint-config"),
	})
	require.NoError(t, err)

	_, err = client.CreateInferenceExperiment(t.Context(), &sagemakersdk.CreateInferenceExperimentInput{
		Name:         aws.String("full-exp"),
		Type:         smtypes.InferenceExperimentTypeShadowMode,
		EndpointName: aws.String("exp-endpoint"),
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/ExpRole"),
		ModelVariants: []smtypes.ModelVariantConfig{
			{
				ModelName:   aws.String("m1"),
				VariantName: aws.String("v1"),
				InfrastructureConfig: &smtypes.ModelInfrastructureConfig{
					InfrastructureType: smtypes.ModelInfrastructureTypeRealTimeInference,
					RealTimeInferenceConfig: &smtypes.RealTimeInferenceConfig{
						InstanceType:  smtypes.ProductionVariantInstanceTypeMlM5Large,
						InstanceCount: aws.Int32(1),
					},
				},
			},
		},
		ShadowModeConfig: &smtypes.ShadowModeConfig{
			SourceModelVariantName: aws.String("v1"),
			ShadowModelVariants:    []smtypes.ShadowModelVariantConfig{},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeInferenceExperiment(t.Context(), &sagemakersdk.DescribeInferenceExperimentInput{
		Name: aws.String("full-exp"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.EndpointMetadata)
	assert.Equal(t, "exp-endpoint", aws.ToString(out.EndpointMetadata.EndpointName))
	assert.Equal(t, "exp-endpoint-config", aws.ToString(out.EndpointMetadata.EndpointConfigName))

	require.Len(t, out.ModelVariants, 1)
	assert.Equal(t, "v1", aws.ToString(out.ModelVariants[0].VariantName))
	assert.Equal(t, smtypes.ModelVariantStatusInService, out.ModelVariants[0].Status)

	require.NotNil(t, out.ShadowModeConfig)
	assert.Equal(t, "v1", aws.ToString(out.ShadowModeConfig.SourceModelVariantName))
}

// TestHandler_StopInferenceExperiment_Arn_RealClient asserts
// StopInferenceExperimentOutput.InferenceExperimentArn -- previously
// discarded entirely, the handler returned no body at all -- is now
// populated, and that ModelVariantActions' Remove/Promote are applied to
// the stored variant list.
func TestHandler_StopInferenceExperiment_Arn_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	createInferenceExperimentRealClient(t, client, "stop-exp", "v1", "v2")

	out, err := client.StopInferenceExperiment(t.Context(), &sagemakersdk.StopInferenceExperimentInput{
		Name: aws.String("stop-exp"),
		ModelVariantActions: map[string]smtypes.ModelVariantAction{
			"v1": smtypes.ModelVariantActionPromote,
		},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.InferenceExperimentArn), "stop-exp")

	desc, err := client.DescribeInferenceExperiment(t.Context(), &sagemakersdk.DescribeInferenceExperimentInput{
		Name: aws.String("stop-exp"),
	})
	require.NoError(t, err)
	require.Len(t, desc.ModelVariants, 1)
	assert.Equal(t, "v1", aws.ToString(desc.ModelVariants[0].VariantName))
}

// TestHandler_DeleteInferenceExperiment_Arn_RealClient asserts
// DeleteInferenceExperimentOutput.InferenceExperimentArn -- previously
// discarded entirely -- is now populated.
func TestHandler_DeleteInferenceExperiment_Arn_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	createInferenceExperimentRealClient(t, client, "delete-exp", "v1")

	out, err := client.DeleteInferenceExperiment(t.Context(), &sagemakersdk.DeleteInferenceExperimentInput{
		Name: aws.String("delete-exp"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.InferenceExperimentArn), "delete-exp")
}

// TestHandler_ListInferenceExperiments_FilterSortPage_RealClient asserts
// ListInferenceExperimentsInput's NameContains/StatusEquals/Type/SortBy/
// SortOrder -- all absent before this pass -- now work.
func TestHandler_ListInferenceExperiments_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	createInferenceExperimentRealClient(t, client, "alpha-exp", "v1")
	createInferenceExperimentRealClient(t, client, "beta-exp", "v1")
	createInferenceExperimentRealClient(t, client, "gamma-widget", "v1")

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListInferenceExperiments(t.Context(), &sagemakersdk.ListInferenceExperimentsInput{
			NameContains: aws.String("exp"),
		})
		require.NoError(t, err)
		assert.Len(t, out.InferenceExperiments, 2)
	})

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListInferenceExperiments(t.Context(), &sagemakersdk.ListInferenceExperimentsInput{
			SortBy:    smtypes.SortInferenceExperimentsByName,
			SortOrder: smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.InferenceExperiments, 3)
		assert.Equal(t, "alpha-exp", aws.ToString(out.InferenceExperiments[0].Name))
	})

	t.Run("status filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListInferenceExperiments(t.Context(), &sagemakersdk.ListInferenceExperimentsInput{
			StatusEquals: smtypes.InferenceExperimentStatusRunning,
		})
		require.NoError(t, err)
		assert.Len(t, out.InferenceExperiments, 3)
	})
}

func createInferenceExperimentRealClient(
	t *testing.T,
	client *sagemakersdk.Client,
	name string,
	variantNames ...string,
) {
	t.Helper()

	variants := make([]smtypes.ModelVariantConfig, 0, len(variantNames))
	shadows := make([]smtypes.ShadowModelVariantConfig, 0, len(variantNames)-1)

	for i, v := range variantNames {
		variants = append(variants, smtypes.ModelVariantConfig{
			ModelName:   aws.String("m-" + v),
			VariantName: aws.String(v),
			InfrastructureConfig: &smtypes.ModelInfrastructureConfig{
				InfrastructureType: smtypes.ModelInfrastructureTypeRealTimeInference,
				RealTimeInferenceConfig: &smtypes.RealTimeInferenceConfig{
					InstanceType:  smtypes.ProductionVariantInstanceTypeMlM5Large,
					InstanceCount: aws.Int32(1),
				},
			},
		})

		if i > 0 {
			shadows = append(shadows, smtypes.ShadowModelVariantConfig{
				ShadowModelVariantName: aws.String(v),
				SamplingPercentage:     aws.Int32(10),
			})
		}
	}

	_, err := client.CreateInferenceExperiment(t.Context(), &sagemakersdk.CreateInferenceExperimentInput{
		Name:          aws.String(name),
		Type:          smtypes.InferenceExperimentTypeShadowMode,
		EndpointName:  aws.String(name + "-endpoint"),
		RoleArn:       aws.String("arn:aws:iam::000000000000:role/ExpRole"),
		ModelVariants: variants,
		ShadowModeConfig: &smtypes.ShadowModeConfig{
			SourceModelVariantName: aws.String(variantNames[0]),
			ShadowModelVariants:    shadows,
		},
	})
	require.NoError(t, err)
}
