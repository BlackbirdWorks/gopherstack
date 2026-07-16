package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
