package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSbomExportLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/sbomexport/create", map[string]any{
		"sbomFormat":    "CYCLONEDX_1_4",
		"s3Destination": map[string]any{"bucketName": "my-bucket", "keyPrefix": "sbom/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	reportID, _ := createResp["reportId"].(string)
	require.NotEmpty(t, reportID)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/sbomexport/get", map[string]any{
		"reportId": reportID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["status"])

	// Cancel
	rec = auditDo(t, h, http.MethodPost, "/sbomexport/cancel", map[string]any{
		"reportId": reportID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get unknown returns 404
	rec = auditDo(t, h, http.MethodPost, "/sbomexport/get", map[string]any{
		"reportId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
