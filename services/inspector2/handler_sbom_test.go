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

	// Create. Real CreateSbomExportInput's field is "reportFormat", not
	// "sbomFormat" (a gopherstack-invented key with no counterpart on the
	// real request shape) -- and "resourceFilterCriteria" is a real member
	// too.
	rec := auditDo(t, h, http.MethodPost, "/sbomexport/create", map[string]any{
		"reportFormat":  "CYCLONEDX_1_4",
		"s3Destination": map[string]any{"bucketName": "my-bucket", "keyPrefix": "sbom/"},
		"resourceFilterCriteria": map[string]any{
			"ecrImageTags": []any{map[string]any{"comparison": "EQUALS", "value": "latest"}},
		},
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
	assert.Equal(t, "CYCLONEDX_1_4", getResp["format"])

	destination, ok := getResp["s3Destination"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-bucket", destination["bucketName"])

	filterCriteria, ok := getResp["filterCriteria"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, filterCriteria, "ecrImageTags")

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
