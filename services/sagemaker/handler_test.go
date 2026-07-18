package sagemaker_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func newTestHandler(t *testing.T) *sagemaker.Handler {
	t.Helper()

	return sagemaker.NewHandler(sagemaker.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doSageMakerRequest(
	t *testing.T,
	h *sagemaker.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SageMaker."+target)
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/sagemaker/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "SageMaker", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateModel")
	assert.Contains(t, ops, "DescribeModel")
	assert.Contains(t, ops, "ListModels")
	assert.Contains(t, ops, "DeleteModel")
	assert.Contains(t, ops, "CreateEndpointConfig")
	assert.Contains(t, ops, "DescribeEndpointConfig")
	assert.Contains(t, ops, "ListEndpointConfigs")
	assert.Contains(t, ops, "DeleteEndpointConfig")
	assert.Contains(t, ops, "AddTags")
	assert.Contains(t, ops, "ListTags")
	assert.Contains(t, ops, "DeleteTags")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "sagemaker", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches SageMaker.CreateModel",
			target: "SageMaker.CreateModel",
			want:   true,
		},
		{
			name:   "matches SageMaker.DescribeModel",
			target: "SageMaker.DescribeModel",
			want:   true,
		},
		{
			name:   "does not match DynamoDB.PutItem",
			target: "DynamoDB.PutItem",
			want:   false,
		},
		{
			name:   "does not match empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "sagemaker", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSageMakerRequest(t, h, "UnknownOp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "extract CreateModel operation",
			target: "SageMaker.CreateModel",
			want:   "CreateModel",
		},
		{
			name:   "extract DescribeEndpointConfig operation",
			target: "SageMaker.DescribeEndpointConfig",
			want:   "DescribeEndpointConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.want, resource)
		})
	}
}

// ---------------------------------------------------------------------------
// Persistence: the new state introduced by this round of de-stubbing must
// survive a Snapshot/Restore roundtrip.
// ---------------------------------------------------------------------------

func TestHandler_SageMakerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create various resources.
	doSageMakerRequest(t, h, "CreateModel", map[string]any{
		"ModelName":        "reset-model",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/test",
	})
	doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{"FeatureGroupName": "reset-fg"})
	doSageMakerRequest(t, h, "CreatePipeline", map[string]any{"PipelineName": "reset-pipeline"})
	doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "reset-domain"})

	// Verify they exist.
	recList := doSageMakerRequest(t, h, "ListModels", map[string]any{})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut["Models"])

	// Reset.
	h.Backend.Reset()

	// Models gone.
	recList2 := doSageMakerRequest(t, h, "ListModels", map[string]any{})
	require.Equal(t, http.StatusOK, recList2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["Models"])

	// Feature groups gone.
	recFG := doSageMakerRequest(t, h, "ListFeatureGroups", map[string]any{})
	require.Equal(t, http.StatusOK, recFG.Code)

	var fgOut map[string]any
	require.NoError(t, json.Unmarshal(recFG.Body.Bytes(), &fgOut))
	assert.Empty(t, fgOut["FeatureGroupSummaries"])
}

// ---------------------------------------------------------------------------
// Missing name / input validation
// ---------------------------------------------------------------------------

func TestHandler_SageMaker_ValidationErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		op   string
	}{
		{op: "CreateDomain", body: map[string]any{}},
		{op: "CreateUserProfile", body: map[string]any{}},
		{op: "CreateApp", body: map[string]any{}},
		{op: "CreateFeatureGroup", body: map[string]any{}},
		{op: "CreatePipeline", body: map[string]any{}},
		{op: "CreateExperiment", body: map[string]any{}},
		{op: "CreateTrial", body: map[string]any{}},
		{op: "CreateTrialComponent", body: map[string]any{}},
		{op: "CreateTrainingJob", body: map[string]any{}},
		{op: "CreateNotebookInstance", body: map[string]any{}},
		{op: "CreateHyperParameterTuningJob", body: map[string]any{}},
		{op: "CreateEndpoint", body: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "op=%s", tt.op)
		})
	}
}

// ---------------------------------------------------------------------------
// GetSupportedOperations includes stateful ops
// ---------------------------------------------------------------------------

func TestHandler_GetSupportedOperations_Stateful(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateDomain", "DescribeDomain", "ListDomains", "DeleteDomain", "UpdateDomain",
		"CreateUserProfile", "DescribeUserProfile", "ListUserProfiles", "DeleteUserProfile",
		"CreateApp", "DescribeApp", "ListApps", "DeleteApp",
		"CreateFeatureGroup", "DescribeFeatureGroup", "ListFeatureGroups", "DeleteFeatureGroup",
		"CreatePipeline", "DescribePipeline", "ListPipelines", "UpdatePipeline", "DeletePipeline",
		"StartPipelineExecution", "DescribePipelineExecution", "ListPipelineExecutions",
		"CreateExperiment", "DescribeExperiment", "ListExperiments", "DeleteExperiment",
		"CreateTrial", "DescribeTrial", "ListTrials", "DeleteTrial",
		"CreateTrialComponent", "DescribeTrialComponent", "DeleteTrialComponent",
		"CreateEndpoint", "DescribeEndpoint", "ListEndpoints", "DeleteEndpoint", "UpdateEndpoint",
		"CreateTrainingJob", "DescribeTrainingJob", "ListTrainingJobs", "StopTrainingJob", "DeleteTrainingJob",
		"CreateNotebookInstance", "DescribeNotebookInstance", "ListNotebookInstances",
		"StartNotebookInstance", "StopNotebookInstance", "DeleteNotebookInstance",
		"CreateHyperParameterTuningJob", "DescribeHyperParameterTuningJob",
		"ListHyperParameterTuningJobs", "StopHyperParameterTuningJob", "DeleteHyperParameterTuningJob",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_GetSupportedOperations_Extended(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	required := []string{
		"CreateTransformJob",
		"DescribeTransformJob",
		"ListTransformJobs",
		"StopTransformJob",
		"UpdateFeatureGroup",
		"UpdateExperiment",
		"UpdateTrial",
		"UpdateTrialComponent",
		"ListPipelineParametersForExecution",
	}

	for _, op := range required {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}
