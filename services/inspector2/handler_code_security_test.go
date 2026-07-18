package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodeSecurityIntegrationLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/integration/create", map[string]any{
		"name": "my-integration",
		"type": "GITHUB",
		"tags": map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	integARN, _ := createResp["integrationArn"].(string)
	require.NotEmpty(t, integARN)
	assert.Equal(t, "ACTIVE", createResp["status"])

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/get", map[string]any{
		"integrationArn": integARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, integARN, getResp["integrationArn"])

	// List
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	integrations, _ := listResp["integrations"].([]any)
	assert.Len(t, integrations, 1)

	// Update
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/update", map[string]any{
		"integrationArn": integARN,
		"details":        map[string]any{"webhookSecret": "new-secret"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/delete", map[string]any{
		"integrationArn": integARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get after delete returns 404
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/get", map[string]any{
		"integrationArn": integARN,
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCodeSecurityScanConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/create", map[string]any{
		"name":          "my-scan-config",
		"scopeSettings": map[string]any{"projectSelectionScope": "ALL"},
		"tags":          map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	cfgARN, _ := createResp["scanConfigurationArn"].(string)
	require.NotEmpty(t, cfgARN)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/get", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	cfgs, _ := listResp["scanConfigurations"].([]any)
	assert.Len(t, cfgs, 1)

	// Update
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/update", map[string]any{
		"scanConfigurationArn": cfgARN,
		"scopeSettings":        map[string]any{"projectSelectionScope": "SPECIFIC"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Batch associate
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/batch/associate", map[string]any{
		"scanConfigurationArn": cfgARN,
		"associateConfigurationRequests": []any{
			map[string]any{"resource": "arn:aws:codecommit:us-east-1:123456789012:my-repo"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List associations
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/associations/list", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp))
	assocs, _ := assocResp["associations"].([]any)
	assert.Len(t, assocs, 1)

	// Batch disassociate
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/batch/disassociate", map[string]any{
		"scanConfigurationArn": cfgARN,
		"disassociateConfigurationRequests": []any{
			map[string]any{"resource": "arn:aws:codecommit:us-east-1:123456789012:my-repo"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List associations now empty
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/associations/list", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var assocResp2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp2))
	assocs2, _ := assocResp2["associations"].([]any)
	assert.Empty(t, assocs2)

	// Delete
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/delete", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestCodeSecurityScanLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Start
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan/start", map[string]any{
		"resourceId": "arn:aws:codecommit:us-east-1:123456789012:my-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	scanID, _ := startResp["scanId"].(string)
	require.NotEmpty(t, scanID)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan/get", map[string]any{
		"scanId": scanID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, scanID, getResp["scanId"])
}
