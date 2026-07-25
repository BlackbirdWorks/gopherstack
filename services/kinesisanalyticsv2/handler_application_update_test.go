package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKAV2_CreateApplication_ExtendedInlineConfiguration verifies that
// CreateApplication's ApplicationCodeConfiguration/FlinkApplicationConfiguration/
// EnvironmentProperties/ApplicationSnapshotConfiguration/
// ApplicationSystemRollbackConfiguration/ApplicationEncryptionConfiguration
// request fields -- previously accepted on the wire but never modeled at all
// (see PARITY.md) -- are seeded and echoed back in
// ApplicationConfigurationDescription.
func TestKAV2_CreateApplication_ExtendedInlineConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "extended-config-app",
		"RuntimeEnvironment": "FLINK-1_18",
		"ApplicationConfiguration": map[string]any{
			"ApplicationCodeConfiguration": map[string]any{
				"CodeContentType": "PLAINTEXT",
				"CodeContent":     map[string]any{"TextContent": "SELECT 1;"},
			},
			"FlinkApplicationConfiguration": map[string]any{
				"CheckpointConfiguration": map[string]any{"ConfigurationType": "DEFAULT"},
				"MonitoringConfiguration": map[string]any{
					"ConfigurationType": "CUSTOM",
					"LogLevel":          "INFO",
					"MetricsLevel":      "APPLICATION",
				},
				"ParallelismConfiguration": map[string]any{
					"ConfigurationType": "CUSTOM",
					"Parallelism":       2,
				},
			},
			"EnvironmentProperties": map[string]any{
				"PropertyGroups": []map[string]any{
					{"PropertyGroupId": "g1", "PropertyMap": map[string]any{"k": "v"}},
				},
			},
			"ApplicationSnapshotConfiguration":       map[string]any{"SnapshotsEnabled": true},
			"ApplicationSystemRollbackConfiguration": map[string]any{"RollbackEnabled": true},
			"ApplicationEncryptionConfiguration": map[string]any{
				"KeyType": "CUSTOMER_MANAGED_KEY",
				"KeyId":   "alias/my-key",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	detail := out["ApplicationDetail"].(map[string]any)
	appConfig := detail["ApplicationConfigurationDescription"].(map[string]any)

	codeDesc := appConfig["ApplicationCodeConfigurationDescription"].(map[string]any)
	assert.Equal(t, "PLAINTEXT", codeDesc["CodeContentType"])
	assert.Equal(t, "SELECT 1;", codeDesc["CodeContentDescription"].(map[string]any)["TextContent"])

	flinkDesc := appConfig["FlinkApplicationConfigurationDescription"].(map[string]any)
	checkpointDesc := flinkDesc["CheckpointConfigurationDescription"].(map[string]any)
	// DEFAULT must force the documented literal values.
	assert.InEpsilon(t, 60000.0, checkpointDesc["CheckpointInterval"], 1e-9)
	assert.InEpsilon(t, 5000.0, checkpointDesc["MinPauseBetweenCheckpoints"], 1e-9)
	assert.Equal(t, true, checkpointDesc["CheckpointingEnabled"])

	parallelismDesc := flinkDesc["ParallelismConfigurationDescription"].(map[string]any)
	assert.InEpsilon(t, 2.0, parallelismDesc["Parallelism"], 1e-9)

	envDesc := appConfig["EnvironmentPropertyDescriptions"].(map[string]any)
	groups := envDesc["PropertyGroupDescriptions"].([]any)
	require.Len(t, groups, 1)
	assert.Equal(t, "g1", groups[0].(map[string]any)["PropertyGroupId"])

	snapDesc := appConfig["ApplicationSnapshotConfigurationDescription"].(map[string]any)
	assert.Equal(t, true, snapDesc["SnapshotsEnabled"])

	rollbackDesc := appConfig["ApplicationSystemRollbackConfigurationDescription"].(map[string]any)
	assert.Equal(t, true, rollbackDesc["RollbackEnabled"])

	encDesc := appConfig["ApplicationEncryptionConfigurationDescription"].(map[string]any)
	assert.Equal(t, "alias/my-key", encDesc["KeyId"])

	// Inline config must not bump the version past 1.
	assert.InEpsilon(t, 1.0, detail["ApplicationVersionId"], 1e-9)
}

// TestKAV2_UpdateApplication_ConditionalTokenWireRoundTrip verifies a client
// can read ApplicationDetail.ConditionalToken from a DescribeApplication (or
// CreateApplication) response and use it on a subsequent UpdateApplication
// call, exactly as real AWS documents ("You get the application's current
// ConditionalToken using DescribeApplication").
func TestKAV2_UpdateApplication_ConditionalTokenWireRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	createRec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "token-wire-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	token, ok := createOut["ApplicationDetail"].(map[string]any)["ConditionalToken"].(string)
	require.True(t, ok, "expected ConditionalToken in CreateApplication response")
	require.NotEmpty(t, token)

	updateRec := doKAV2Request(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":          "token-wire-app",
		"ConditionalToken":         token,
		"RuntimeEnvironmentUpdate": "FLINK-1_19",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateOut))
	assert.Equal(t, "FLINK-1_19", updateOut["ApplicationDetail"].(map[string]any)["RuntimeEnvironment"])

	// A second call reusing the now-stale token must fail.
	staleRec := doKAV2Request(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":  "token-wire-app",
		"ConditionalToken": token,
	})
	require.Equal(t, http.StatusBadRequest, staleRec.Code)
	assertErrorType(t, staleRec.Body.Bytes(), "ConcurrentModificationException")
}

// TestKAV2_StartApplication_RunConfiguration verifies StartApplication's
// RunConfiguration request field (previously never parsed at all) is
// accepted and echoed back via a subsequent DescribeApplication.
func TestKAV2_StartApplication_RunConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	require.Equal(t, http.StatusOK, doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "start-runcfg-wire-app",
		"RuntimeEnvironment": "FLINK-1_18",
	}).Code)

	startRec := doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "start-runcfg-wire-app",
		"RunConfiguration": map[string]any{
			"ApplicationRestoreConfiguration": map[string]any{
				"ApplicationRestoreType": "RESTORE_FROM_LATEST_SNAPSHOT",
			},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	assert.NotEmpty(t, startOut["OperationId"])

	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{"ApplicationName": "start-runcfg-wire-app"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	appConfig := descOut["ApplicationDetail"].(map[string]any)["ApplicationConfigurationDescription"].(map[string]any)
	runConfig := appConfig["RunConfigurationDescription"].(map[string]any)
	restoreConfig := runConfig["ApplicationRestoreConfigurationDescription"].(map[string]any)
	assert.Equal(t, "RESTORE_FROM_LATEST_SNAPSHOT", restoreConfig["ApplicationRestoreType"])
}

// TestKAV2_DeleteApplication_CreateTimestampMismatch verifies DeleteApplication
// rejects a mismatched CreateTimestamp instead of silently deleting.
func TestKAV2_DeleteApplication_CreateTimestampMismatch(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	require.Equal(t, http.StatusOK, doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "delete-ts-app",
		"RuntimeEnvironment": "FLINK-1_18",
	}).Code)

	rec := doKAV2Request(t, h, "DeleteApplication", map[string]any{
		"ApplicationName": "delete-ts-app",
		"CreateTimestamp": 1,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assertErrorType(t, rec.Body.Bytes(), "InvalidArgumentException")

	// The application must still exist.
	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{"ApplicationName": "delete-ts-app"})
	require.Equal(t, http.StatusOK, descRec.Code)
}

// TestKAV2_AddDeleteVpcAndCWLOption_WireIncludesOperationID verifies the
// four Add*/Delete* config ops whose real AWS response shapes carry an
// OperationId field (AddApplicationCloudWatchLoggingOption,
// AddApplicationVpcConfiguration, DeleteApplicationCloudWatchLoggingOption,
// DeleteApplicationVpcConfiguration -- verified against aws-sdk-go-v2's
// api_op_*.go) return one on the wire.
func TestKAV2_AddDeleteVpcAndCWLOption_WireIncludesOperationID(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	require.Equal(t, http.StatusOK, doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "opid-wire-app",
		"RuntimeEnvironment": "FLINK-1_18",
	}).Code)

	addCWLRec := doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName": "opid-wire-app",
		"CloudWatchLoggingOption": map[string]any{
			"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
		},
	})
	require.Equal(t, http.StatusOK, addCWLRec.Code)
	assertHasOperationID(t, addCWLRec.Body.Bytes())

	addVpcRec := doKAV2Request(t, h, "AddApplicationVpcConfiguration", map[string]any{
		"ApplicationName": "opid-wire-app",
		"VpcConfiguration": map[string]any{
			"SubnetIds":        []string{"subnet-1"},
			"SecurityGroupIds": []string{"sg-1"},
		},
	})
	require.Equal(t, http.StatusOK, addVpcRec.Code)
	assertHasOperationID(t, addVpcRec.Body.Bytes())

	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{"ApplicationName": "opid-wire-app"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	detail := descOut["ApplicationDetail"].(map[string]any)
	cwlOptions := detail["CloudWatchLoggingOptionDescriptions"].([]any)
	cwlID := cwlOptions[0].(map[string]any)["CloudWatchLoggingOptionId"].(string)
	appConfig := detail["ApplicationConfigurationDescription"].(map[string]any)
	vpcID := appConfig["VpcConfigurationDescriptions"].([]any)[0].(map[string]any)["VpcConfigurationId"].(string)

	delCWLRec := doKAV2Request(t, h, "DeleteApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName":           "opid-wire-app",
		"CloudWatchLoggingOptionId": cwlID,
	})
	require.Equal(t, http.StatusOK, delCWLRec.Code)
	assertHasOperationID(t, delCWLRec.Body.Bytes())

	delVpcRec := doKAV2Request(t, h, "DeleteApplicationVpcConfiguration", map[string]any{
		"ApplicationName":    "opid-wire-app",
		"VpcConfigurationId": vpcID,
	})
	require.Equal(t, http.StatusOK, delVpcRec.Code)
	assertHasOperationID(t, delVpcRec.Body.Bytes())
}

func assertHasOperationID(t *testing.T, body []byte) {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	opID, ok := out["OperationId"].(string)
	require.True(t, ok, "expected OperationId in response")
	assert.NotEmpty(t, opID)
}
