package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeEndpoint_FullResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create endpoint config with a production variant
	doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
		"EndpointConfigName": "ec",
		"ProductionVariants": []any{
			map[string]any{
				"VariantName":          "main",
				"ModelName":            "my-model",
				"InitialInstanceCount": 1,
				"InstanceType":         "ml.m5.large",
				"InitialVariantWeight": 1.0,
			},
		},
	})

	// Create endpoint
	rec := doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "ep1",
		"EndpointConfigName": "ec",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeEndpoint", map[string]any{
		"EndpointName": "ep1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "ep1", descResp["EndpointName"])
	assert.NotEmpty(t, descResp["EndpointArn"])
	// Status is Creating initially (FSM transitions async)
	assert.Contains(t, []any{"Creating", "InService"}, descResp["EndpointStatus"])

	// ProductionVariants must be populated from the endpoint config, using
	// the AWS DescribeEndpoint wire shape (Desired*/Current*, not Initial*).
	variants, ok := descResp["ProductionVariants"].([]any)
	require.True(t, ok, "ProductionVariants must be present in DescribeEndpoint response")
	require.Len(t, variants, 1)
	variant, ok := variants[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "main", variant["VariantName"])
	assert.InDelta(t, 1.0, variant["DesiredWeight"], 0.0001)
	assert.InDelta(t, float64(1), variant["DesiredInstanceCount"], 0.0001)
	assert.Nil(t, variant["CurrentWeight"])
	assert.Nil(t, variant["CurrentInstanceCount"])
}

func TestHandler_CreateEndpoint_UnknownEndpointConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "ep-missing-config",
		"EndpointConfigName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationException", errResp["__type"])

	rec = doSageMakerRequest(t, h, "DescribeEndpoint", map[string]any{
		"EndpointName": "ep-missing-config",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeEndpoint_EventuallyInService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
		"EndpointConfigName": "ec2",
		"ProductionVariants": []any{
			map[string]any{
				"VariantName":          "v1",
				"ModelName":            "m1",
				"InitialInstanceCount": 1,
				"InstanceType":         "ml.m5.large",
				"InitialVariantWeight": 1.0,
			},
		},
	})

	doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "ep2",
		"EndpointConfigName": "ec2",
	})

	// Wait for lifecycle
	time.Sleep(400 * time.Millisecond)

	rec := doSageMakerRequest(t, h, "DescribeEndpoint", map[string]any{
		"EndpointName": "ep2",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "InService", descResp["EndpointStatus"])

	variants, ok := descResp["ProductionVariants"].([]any)
	require.True(t, ok)
	require.Len(t, variants, 1)
	variant, ok := variants[0].(map[string]any)
	require.True(t, ok)
	// Once InService, Current* mirrors Desired*.
	assert.InDelta(t, 1.0, variant["CurrentWeight"], 0.0001)
	assert.InDelta(t, float64(1), variant["CurrentInstanceCount"], 0.0001)
}

// ---------------------------------------------------------------------------
// Snapshot/Restore preserves TransformJob, featureRecords, featureMetadata (gap #28)
// ---------------------------------------------------------------------------

func TestHandler_EndpointLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create endpoint config first.
	doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
		"EndpointConfigName": "my-ep-config",
		"ProductionVariants": []map[string]any{
			{
				"VariantName":          "main",
				"ModelName":            "m",
				"InstanceType":         "ml.m5.large",
				"InitialInstanceCount": 1,
			},
		},
	})

	// Create endpoint.
	recCreate := doSageMakerRequest(t, h, "CreateEndpoint", map[string]any{
		"EndpointName":       "my-endpoint",
		"EndpointConfigName": "my-ep-config",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["EndpointArn"])

	// Describe endpoint.
	recDesc := doSageMakerRequest(
		t,
		h,
		"DescribeEndpoint",
		map[string]any{"EndpointName": "my-endpoint"},
	)
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List endpoints.
	recList := doSageMakerRequest(t, h, "ListEndpoints", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["Endpoints"].([]any), 1)

	// Update endpoint.
	recUpdate := doSageMakerRequest(t, h, "UpdateEndpoint", map[string]any{
		"EndpointName":       "my-endpoint",
		"EndpointConfigName": "my-ep-config",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// UpdateEndpointWeightsAndCapacities.
	recUpdateWeights := doSageMakerRequest(
		t,
		h,
		"UpdateEndpointWeightsAndCapacities",
		map[string]any{
			"EndpointName":                "my-endpoint",
			"DesiredWeightsAndCapacities": []map[string]any{},
		},
	)
	assert.Equal(t, http.StatusOK, recUpdateWeights.Code)

	// Delete endpoint.
	recDelete := doSageMakerRequest(
		t,
		h,
		"DeleteEndpoint",
		map[string]any{"EndpointName": "my-endpoint"},
	)
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_Endpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeEndpoint", "UpdateEndpoint", "DeleteEndpoint"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"EndpointName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Training Job lifecycle
// ---------------------------------------------------------------------------
