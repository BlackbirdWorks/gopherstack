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

// TestStartImport_S3ImportSourceFieldsEchoed verifies that S3BucketRegion and
// S3BucketAccessRoleArn -- previously accepted on the wire but silently
// dropped, with only S3LocationUri modeled -- are stored and echoed back on
// StartImport/GetImport, matching the real (all-required) S3ImportSource shape.
func TestStartImport_S3ImportSourceFieldsEchoed(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "s3-src-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartImport", map[string]any{
		"Destinations": []string{edsARN},
		"ImportSource": map[string]any{
			"S3": map[string]any{
				"S3LocationUri":         "s3://my-bucket/logs/",
				"S3BucketRegion":        "eu-west-1",
				"S3BucketAccessRoleArn": "arn:aws:iam::123456789012:role/import-role",
			},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	startResp := parseCloudTrailResp(t, startRec)

	assertS3ImportSource := func(t *testing.T, resp map[string]any) {
		t.Helper()
		src, ok := resp["ImportSource"].(map[string]any)
		require.True(t, ok, "ImportSource must be present")
		s3, ok := src["S3"].(map[string]any)
		require.True(t, ok, "ImportSource.S3 must be present")
		assert.Equal(t, "s3://my-bucket/logs/", s3["S3LocationUri"])
		assert.Equal(t, "eu-west-1", s3["S3BucketRegion"])
		assert.Equal(t, "arn:aws:iam::123456789012:role/import-role", s3["S3BucketAccessRoleArn"])
	}

	assertS3ImportSource(t, startResp)

	importID, _ := startResp["ImportId"].(string)
	require.NotEmpty(t, importID)

	getRec := doCloudTrailOp(t, h, "GetImport", map[string]any{"ImportId": importID})
	require.Equal(t, http.StatusOK, getRec.Code)
	assertS3ImportSource(t, parseCloudTrailResp(t, getRec))
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

// TestListImports_NextTokenPagination verifies ListImports honors
// NextToken/MaxResults pagination (previously always returned every import
// in one page).
func TestListImports_NextTokenPagination(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "import-page-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	for range 3 {
		rec := doCloudTrailOp(t, h, "StartImport", map[string]any{
			"Destinations": []string{edsARN},
			"ImportSource": map[string]any{
				"S3": map[string]any{"S3LocationUri": "s3://bucket/logs/"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doCloudTrailOp(t, h, "ListImports", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	imports, ok := resp["Imports"].([]any)
	require.True(t, ok)
	require.Len(t, imports, 2)
	nextToken, _ := resp["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	rec = doCloudTrailOp(t, h, "ListImports", map[string]any{"NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseCloudTrailResp(t, rec)
	imports, ok = resp["Imports"].([]any)
	require.True(t, ok)
	assert.Len(t, imports, 1)
}
