package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_EDS_RetentionPeriod_Default verifies that CreateEventDataStore defaults
// RetentionPeriod to 2557 days (7 years) when not specified, matching real AWS behavior.
func TestParity_EDS_RetentionPeriod_Default(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "rp-default-eds",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	rp, _ := resp["RetentionPeriod"].(float64)
	assert.InDelta(t, float64(2557), rp, 0, "RetentionPeriod must default to 2557 days")
}

// TestParity_EDS_AdvancedEventSelectors_AlwaysPresent verifies that AdvancedEventSelectors
// is always present in EDS responses, even as empty array, matching real AWS behavior.
func TestParity_EDS_AdvancedEventSelectors_AlwaysPresent(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "aes-always-eds",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	aes, hasField := resp["AdvancedEventSelectors"]
	assert.True(t, hasField, "AdvancedEventSelectors must always be present in EDS response")

	aesList, ok := aes.([]any)
	assert.True(t, ok, "AdvancedEventSelectors must be an array")
	assert.Empty(t, aesList, "AdvancedEventSelectors must be empty array when none configured")
}

// TestParity_EDS_AdvancedEventSelectors_GetDataStore verifies AdvancedEventSelectors
// is present in GetEventDataStore response.
func TestParity_EDS_AdvancedEventSelectors_GetDataStore(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "aes-get-eds",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	edsARN, _ := parseCloudTrailResp(t, createRec)["EventDataStoreArn"].(string)

	getRec := doCloudTrailOp(t, h, "GetEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	resp := parseCloudTrailResp(t, getRec)
	_, hasField := resp["AdvancedEventSelectors"]
	assert.True(t, hasField, "AdvancedEventSelectors must be present in GetEventDataStore response")
}

// TestParity_GetEventSelectors_AdvancedOnly verifies that GetEventSelectors does NOT
// return EventSelectors when AdvancedEventSelectors are active.
func TestParity_GetEventSelectors_AdvancedOnly(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "ges-adv-trail",
		"S3BucketName": "bucket",
	})
	doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
		"TrailName": "ges-adv-trail",
		"AdvancedEventSelectors": []map[string]any{
			{
				"Name": "All S3 events",
				"FieldSelectors": []map[string]any{
					{"Field": "eventCategory", "Equals": []string{"Data"}},
				},
			},
		},
	})

	rec := doCloudTrailOp(t, h, "GetEventSelectors", map[string]any{
		"TrailName": "ges-adv-trail",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	advSels, ok := resp["AdvancedEventSelectors"].([]any)
	require.True(t, ok, "AdvancedEventSelectors must be present")
	assert.Len(t, advSels, 1)

	_, hasBasic := resp["EventSelectors"]
	assert.False(t, hasBasic, "EventSelectors must NOT be present when AdvancedEventSelectors are active")
}

// TestParity_GetTrailStatus_TimeLoggingStarted verifies that GetTrailStatus returns
// TimeLoggingStarted as a string field when logging is active.
func TestParity_GetTrailStatus_TimeLoggingStarted(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "time-start-trail",
		"S3BucketName": "bucket",
	})
	doCloudTrailOp(t, h, "StartLogging", map[string]any{
		"Name": "time-start-trail",
	})

	rec := doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
		"Name": "time-start-trail",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	assert.True(t, resp["IsLogging"].(bool))
	assert.NotNil(t, resp["StartLoggingTime"], "StartLoggingTime (float) must be present")
	assert.NotEmpty(t, resp["TimeLoggingStarted"], "TimeLoggingStarted (string) must be present")

	_, isString := resp["TimeLoggingStarted"].(string)
	assert.True(t, isString, "TimeLoggingStarted must be a string timestamp")
}

// TestParity_GetTrailStatus_TimeLoggingStopped verifies that GetTrailStatus returns
// TimeLoggingStopped as a string field after StopLogging.
func TestParity_GetTrailStatus_TimeLoggingStopped(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "time-stop-trail",
		"S3BucketName": "bucket",
	})
	doCloudTrailOp(t, h, "StartLogging", map[string]any{"Name": "time-stop-trail"})
	doCloudTrailOp(t, h, "StopLogging", map[string]any{"Name": "time-stop-trail"})

	rec := doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
		"Name": "time-stop-trail",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	assert.NotNil(t, resp["StopLoggingTime"], "StopLoggingTime (float) must be present")
	assert.NotEmpty(t, resp["TimeLoggingStopped"], "TimeLoggingStopped (string) must be present")

	_, isString := resp["TimeLoggingStopped"].(string)
	assert.True(t, isString, "TimeLoggingStopped must be a string timestamp")
}

// TestParity_Import_Timestamps verifies that StartImport and GetImport return
// CreatedTimestamp and UpdatedTimestamp fields.
func TestParity_Import_Timestamps(t *testing.T) {
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

// TestParity_StopImport_Timestamps verifies StopImport returns timestamps.
func TestParity_StopImport_Timestamps(t *testing.T) {
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

// TestParity_DescribeQuery_CreationTime verifies DescribeQuery returns CreationTime.
func TestParity_DescribeQuery_CreationTime(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "dq-ct-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartQuery", map[string]any{
		"QueryStatement": "SELECT * FROM " + edsARN + " LIMIT 1",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	queryID, _ := parseCloudTrailResp(t, startRec)["QueryId"].(string)
	require.NotEmpty(t, queryID)

	descRec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{
		"QueryId": queryID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseCloudTrailResp(t, descRec)
	assert.NotNil(t, resp["CreationTime"], "DescribeQuery must return CreationTime")
}

// TestParity_GetQueryResults_QueryStatistics verifies GetQueryResults returns QueryStatistics.
func TestParity_GetQueryResults_QueryStatistics(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	edsRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "gqr-qs-eds"})
	require.Equal(t, http.StatusOK, edsRec.Code)
	edsARN, _ := parseCloudTrailResp(t, edsRec)["EventDataStoreArn"].(string)

	startRec := doCloudTrailOp(t, h, "StartQuery", map[string]any{
		"QueryStatement": "SELECT * FROM " + edsARN + " LIMIT 1",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	queryID, _ := parseCloudTrailResp(t, startRec)["QueryId"].(string)
	require.NotEmpty(t, queryID)

	getRec := doCloudTrailOp(t, h, "GetQueryResults", map[string]any{
		"EventDataStore": edsARN,
		"QueryId":        queryID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	resp := parseCloudTrailResp(t, getRec)
	stats, hasStats := resp["QueryStatistics"]
	assert.True(t, hasStats, "GetQueryResults must return QueryStatistics")
	statsMap, ok := stats.(map[string]any)
	require.True(t, ok, "QueryStatistics must be an object")
	_, hasTRC := statsMap["TotalResultsCount"]
	assert.True(t, hasTRC, "QueryStatistics must contain TotalResultsCount")
}
