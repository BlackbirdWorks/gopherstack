package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["TrackingServerArn"], "my-server")
}

func TestHandler_DescribeMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-1"})
	rec := doSageMakerRequest(t, h, "DescribeMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ts-1", resp["TrackingServerName"])
}

func TestHandler_StartStopMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})

	rec := doSageMakerRequest(t, h, "StartMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "StopMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-ss"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	rec := doSageMakerRequest(t, h, "DeleteMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeMlflowTrackingServer", map[string]any{"TrackingServerName": "ts-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelCard
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

func TestHandler_ListMlflowTrackingServers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Initially empty
	rec := doSageMakerRequest(t, h, "ListMlflowTrackingServers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["TrackingServerSummaries"])

	// Create one
	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		"MlflowVersion":      "2.0.0",
	})

	// List shows it
	rec = doSageMakerRequest(t, h, "ListMlflowTrackingServers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["TrackingServerSummaries"].([]any)
	assert.Len(t, summaries, 1)

	summary := summaries[0].(map[string]any)
	assert.Equal(t, "my-server", summary["TrackingServerName"])
	assert.Equal(t, "2.0.0", summary["MlflowVersion"])
}

// ---------------------------------------------------------------------------
// UpdateMlflowTrackingServer tests
// ---------------------------------------------------------------------------

func TestHandler_UpdateMlflowTrackingServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		"MlflowVersion":      "2.0.0",
	})

	rec := doSageMakerRequest(t, h, "UpdateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "my-server",
		"MlflowVersion":      "2.1.0",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["TrackingServerArn"])
}

func TestHandler_UpdateMlflowTrackingServer_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateMlflowTrackingServer", map[string]any{
		"TrackingServerName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ModelCard list tests
// ---------------------------------------------------------------------------
