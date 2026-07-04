package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Algorithm — DescribeAlgorithm / DeleteAlgorithm / ListAlgorithms
// ---------------------------------------------------------------------------

func TestHandler_DescribeAlgorithm(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{
		"AlgorithmName":        "my-algo",
		"AlgorithmDescription": "a description",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{"AlgorithmName": "my-algo"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-algo", resp["AlgorithmName"])
	assert.Equal(t, "a description", resp["AlgorithmDescription"])
	assert.Equal(t, "Completed", resp["AlgorithmStatus"])
	assert.Contains(t, resp["AlgorithmArn"], "algorithm/my-algo")
	assert.NotNil(t, resp["AlgorithmStatusDetails"])
	assert.NotNil(t, resp["CreationTime"])
}

func TestHandler_DescribeAlgorithm_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{"AlgorithmName": "missing"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeAlgorithm_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAlgorithm(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{"AlgorithmName": "algo-del"})

	rec := doSageMakerRequest(t, h, "DeleteAlgorithm", map[string]any{"AlgorithmName": "algo-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{"AlgorithmName": "algo-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAlgorithm_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteAlgorithm", map[string]any{"AlgorithmName": "missing"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListAlgorithms(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAlgorithms", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["AlgorithmSummaryList"])

	doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{"AlgorithmName": "algo-a"})
	doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{"AlgorithmName": "algo-b"})

	rec = doSageMakerRequest(t, h, "ListAlgorithms", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	list, ok := resp["AlgorithmSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer — CreatePresignedMlflowTrackingServerUrl
// ---------------------------------------------------------------------------

func TestHandler_CreatePresignedMlflowTrackingServerUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "ts-1",
	})

	rec := doSageMakerRequest(t, h, "CreatePresignedMlflowTrackingServerUrl", map[string]any{
		"TrackingServerName": "ts-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AuthorizedUrl"], "ts-1")
}

func TestHandler_CreatePresignedMlflowTrackingServerUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePresignedMlflowTrackingServerUrl", map[string]any{
		"TrackingServerName": "missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// MlflowApp
// ---------------------------------------------------------------------------

func TestHandler_MlflowApp_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateMlflowApp", map[string]any{
		"Name":             "app-1",
		"ArtifactStoreUri": "s3://bucket/path",
		"RoleArn":          "arn:aws:iam::000000000000:role/mlflow",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	appARN := createResp["Arn"]
	require.Contains(t, appARN, "mlflow-app/app-1")

	describeRec := doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "app-1", describeResp["Name"])
	assert.Equal(t, "Created", describeResp["Status"])
	assert.Equal(t, "s3://bucket/path", describeResp["ArtifactStoreUri"])

	updateRec := doSageMakerRequest(t, h, "UpdateMlflowApp", map[string]any{
		"Arn":              appARN,
		"ArtifactStoreUri": "s3://bucket/new-path",
	})
	assert.Equal(t, http.StatusOK, updateRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "s3://bucket/new-path", describeResp["ArtifactStoreUri"])

	presignRec := doSageMakerRequest(t, h, "CreatePresignedMlflowAppUrl", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, presignRec.Code)

	var presignResp map[string]string
	require.NoError(t, json.Unmarshal(presignRec.Body.Bytes(), &presignResp))
	assert.Contains(t, presignResp["AuthorizedUrl"], "app-1")

	listRec := doSageMakerRequest(t, h, "ListMlflowApps", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["Summaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	deleteRec := doSageMakerRequest(t, h, "DeleteMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, deleteRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusBadRequest, describeRec.Code)
}

func TestHandler_CreateMlflowApp_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMlflowApp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeMlflowApp_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeMlflowApp", map[string]any{
		"Arn": "arn:aws:sagemaker:us-east-1:0:mlflow-app/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateMlflowApp_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"Name": "dup-app"}

	rec := doSageMakerRequest(t, h, "CreateMlflowApp", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateMlflowApp", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ---------------------------------------------------------------------------
// PartnerApp — UpdatePartnerApp / ListPartnerApps / CreatePartnerAppPresignedUrl
// ---------------------------------------------------------------------------

func TestHandler_PartnerApp_ExtendedLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name":             "papp-1",
		"Type":             "comet",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/partner",
		"Tier":             "small",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	appARN := createResp["Arn"]

	describeRec := doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, describeRec.Code)

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "papp-1", describeResp["Name"])
	assert.Equal(t, "comet", describeResp["Type"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/partner", describeResp["ExecutionRoleArn"])
	assert.Equal(t, "small", describeResp["Tier"])

	updateRec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{
		"Arn":  appARN,
		"Tier": "large",
	})
	assert.Equal(t, http.StatusOK, updateRec.Code)

	describeRec = doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": appARN})
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Equal(t, "large", describeResp["Tier"])

	listRec := doSageMakerRequest(t, h, "ListPartnerApps", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, ok := listResp["Summaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	presignRec := doSageMakerRequest(t, h, "CreatePartnerAppPresignedUrl", map[string]any{"Arn": appARN})
	assert.Equal(t, http.StatusOK, presignRec.Code)

	var presignResp map[string]string
	require.NoError(t, json.Unmarshal(presignRec.Body.Bytes(), &presignResp))
	assert.Contains(t, presignResp["Url"], "papp-1")
}

func TestHandler_UpdatePartnerApp_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{
		"Arn":  "arn:aws:sagemaker:us-east-1:0:partner-app/missing",
		"Tier": "large",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdatePartnerApp_MissingArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdatePartnerApp", map[string]any{"Tier": "large"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreatePartnerAppPresignedUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePartnerAppPresignedUrl", map[string]any{
		"Arn": "arn:aws:sagemaker:us-east-1:0:partner-app/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListPartnerApps_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListPartnerApps", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Summaries"])
}

func TestHandler_DeletePartnerApp_ReturnsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{"Name": "papp-del"})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DeletePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, createResp["Arn"], resp["Arn"])
}
