package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKAV2_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "snap-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	startRec := doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "snap-app",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	// Create snapshot.
	rec := doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
		"ApplicationName": "snap-app",
		"SnapshotName":    "snap-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List snapshots.
	listRec := doKAV2Request(t, h, "ListApplicationSnapshots", map[string]any{
		"ApplicationName": "snap-app",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	snaps, ok := listOut["SnapshotSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, snaps, 1)

	// Describe snapshot.
	descRec := doKAV2Request(t, h, "DescribeApplicationSnapshot", map[string]any{
		"ApplicationName": "snap-app",
		"SnapshotName":    "snap-1",
	})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Delete snapshot.
	delRec := doKAV2Request(t, h, "DeleteApplicationSnapshot", map[string]any{
		"ApplicationName": "snap-app",
		"SnapshotName":    "snap-1",
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}
