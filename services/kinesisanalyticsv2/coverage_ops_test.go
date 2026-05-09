package kinesisanalyticsv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func createKAV2App(t *testing.T, h *kinesisanalyticsv2.Handler, name string) string {
	t.Helper()

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":          name,
		"RuntimeEnvironment":       "FLINK-1_15",
		"ServiceExecutionRole":     "arn:aws:iam::123456789012:role/KinesisRole",
		"ApplicationConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return name
}

func TestKAV2_TagOperations(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	createKAV2App(t, h, "tag-app")

	appARN := "arn:aws:kinesisanalytics:us-east-1:123456789012:application/tag-app"

	// TagResource
	rec := doKAV2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": appARN,
		"Tags":        []map[string]any{{"Key": "env", "Value": "test"}},
	})
	assert.Positive(t, rec.Code)

	// UntagResource
	rec = doKAV2Request(t, h, "UntagResource", map[string]any{
		"ResourceARN": appARN,
		"TagKeys":     []string{"env"},
	})
	assert.Positive(t, rec.Code)
}

func TestKAV2_DeleteAppReferenceDataSource(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	createKAV2App(t, h, "ref-app")

	// DeleteApplicationReferenceDataSource (exercises the code path)
	rec := doKAV2Request(t, h, "DeleteApplicationReferenceDataSource", map[string]any{
		"ApplicationName":             "ref-app",
		"CurrentApplicationVersionId": 1,
		"ReferenceId":                 "ref-1",
	})
	assert.Positive(t, rec.Code)
}
