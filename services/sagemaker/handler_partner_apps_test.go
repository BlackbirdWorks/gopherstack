package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreatePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name": "my-app",
		"Type": "custom",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["Arn"], "my-app")
}

func TestHandler_DescribePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{
		"Name": "app-1",
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "app-1", resp["Name"])
}

func TestHandler_DeletePartnerApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreatePartnerApp", map[string]any{"Name": "app-del"})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doSageMakerRequest(t, h, "DeletePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribePartnerApp", map[string]any{"Arn": createResp["Arn"]})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// TrainingPlan
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
