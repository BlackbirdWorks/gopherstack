package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloudTrailImportLifecycle covers StartImport, GetImport, ListImports, StopImport.
func TestCloudTrailImportLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create an EDS first.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "import-eds"})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// StartImport.
	rec = doCloudTrailOp(t, h, "StartImport", map[string]any{
		"Destinations": []string{edsARN},
		"ImportSource": map[string]any{
			"S3": map[string]any{
				"S3LocationUri":  "s3://my-bucket/cloudtrail-logs/",
				"S3BucketRegion": "us-east-1",
				"S3PrefixType":   "Dynamic",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	importResp := parseCloudTrailResp(t, rec)
	importID, _ := importResp["ImportId"].(string)
	require.NotEmpty(t, importID)

	// GetImport.
	rec = doCloudTrailOp(t, h, "GetImport", map[string]any{"ImportId": importID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListImports.
	rec = doCloudTrailOp(t, h, "ListImports", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// StopImport.
	rec = doCloudTrailOp(t, h, "StopImport", map[string]any{"ImportId": importID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestImportTimestamps verifies that StartImport and GetImport return
// CreatedTimestamp and UpdatedTimestamp fields.
func TestImportTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "import-ts-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartImport", map[string]any{
		"Destinations": []string{edsARN},
		"ImportSource": map[string]any{
			"S3": map[string]any{
				"S3LocationUri":  "s3://my-bucket/logs/",
				"S3BucketRegion": "us-east-1",
				"S3PrefixType":   "Dynamic",
			},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	startResp := parseCloudTrailResp(t, startRec)
	assert.NotNil(t, startResp["CreatedTimestamp"], "StartImport must return CreatedTimestamp")
	assert.NotNil(t, startResp["UpdatedTimestamp"], "StartImport must return UpdatedTimestamp")

	importID, _ := startResp["ImportId"].(string)
	require.NotEmpty(t, importID)

	getRec := doCloudTrailOp(t, h, "GetImport", map[string]any{"ImportId": importID})
	require.Equal(t, http.StatusOK, getRec.Code)

	getResp := parseCloudTrailResp(t, getRec)
	assert.NotNil(t, getResp["CreatedTimestamp"], "GetImport must return CreatedTimestamp")
	assert.NotNil(t, getResp["UpdatedTimestamp"], "GetImport must return UpdatedTimestamp")
}

// TestStopImportTimestamps verifies StopImport returns timestamps.
func TestStopImportTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "stop-import-ts-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartImport", map[string]any{
		"Destinations": []string{edsARN},
		"ImportSource": map[string]any{
			"S3": map[string]any{
				"S3LocationUri":  "s3://my-bucket/logs/",
				"S3BucketRegion": "us-east-1",
				"S3PrefixType":   "Dynamic",
			},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	importID, _ := parseCloudTrailResp(t, startRec)["ImportId"].(string)

	stopRec := doCloudTrailOp(t, h, "StopImport", map[string]any{"ImportId": importID})
	require.Equal(t, http.StatusOK, stopRec.Code)

	stopResp := parseCloudTrailResp(t, stopRec)
	assert.NotNil(t, stopResp["CreatedTimestamp"], "StopImport must return CreatedTimestamp")
	assert.NotNil(t, stopResp["UpdatedTimestamp"], "StopImport must return UpdatedTimestamp")
}
