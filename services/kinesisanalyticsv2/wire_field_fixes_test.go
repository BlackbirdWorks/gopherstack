package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKAV2_UpdateApplication_ApplicationDescription_NotARealField proves
// that UpdateApplication's ApplicationDescription request field -- accepted
// and actually applied to backend state by handleUpdateApplication/
// applyBasicFields -- is a gopherstack-invented member. Real AWS's
// UpdateApplicationInput (aws-sdk-go-v2/service/kinesisanalyticsv2@v1.41.4,
// api_op_UpdateApplication.go:33-78) has exactly eight members --
// ApplicationName, ApplicationConfigurationUpdate,
// CloudWatchLoggingOptionUpdates, ConditionalToken,
// CurrentApplicationVersionId, RunConfigurationUpdate,
// RuntimeEnvironmentUpdate, ServiceExecutionRoleUpdate -- with no
// ApplicationDescription; real AWS provides no way to change an
// application's description after CreateApplication. Sending it must be a
// no-op, matching a real client (whose Go SDK struct has no such field to
// even serialize) and a real server (which would ignore an unrecognized
// JSON key).
func TestKAV2_UpdateApplication_ApplicationDescription_NotARealField(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":        "desc-invented-app",
		"RuntimeEnvironment":     "FLINK-1_18",
		"ApplicationDescription": "original",
	})

	updateRec := doKAV2Request(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             "desc-invented-app",
		"ApplicationDescription":      "should not apply",
		"CurrentApplicationVersionId": 1,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateOut))
	detail, ok := updateOut["ApplicationDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "original", detail["ApplicationDescription"],
		"UpdateApplication has no ApplicationDescription member in real AWS; it must not mutate the description")

	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{"ApplicationName": "desc-invented-app"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	descDetail, ok := descOut["ApplicationDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "original", descDetail["ApplicationDescription"])
}
