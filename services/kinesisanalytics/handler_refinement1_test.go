package kinesisanalytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// TestRefinement1_Reset verifies that Reset clears all applications and resets the ID counter.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*kinesisanalytics.InMemoryBackend)
		name  string
	}{
		{
			name:  "empty backend",
			setup: func(_ *kinesisanalytics.InMemoryBackend) {},
		},
		{
			name: "with applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-1", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-2", "", "", nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)
			b.Reset()

			count := kinesisanalytics.ApplicationCount(b)
			assert.Zero(t, count)

			// After reset, creating an application should work (maps are reinitialized).
			_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "post-reset", "", "", nil)
			require.NoError(t, err)
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies repeated Reset calls leave the backend clean.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := newBackend()

	for range 3 {
		_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "temp", "", "", nil)
		b.Reset()
		assert.Zero(t, kinesisanalytics.ApplicationCount(b))
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "reset-app", "", "", nil)

	h.Reset()

	assert.Zero(t, kinesisanalytics.ApplicationCount(b))
}

// TestRefinement1_HandlerOpsPreBuilt verifies that the dispatch table is pre-built.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	count := kinesisanalytics.HandlerOpsLen(h)
	// 20 total operations
	assert.Equal(t, 20, count)
}

// TestRefinement1_GetSupportedOperations_AllOps verifies all 20 ops are listed.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateApplication",
		"DeleteApplication",
		"DescribeApplication",
		"ListApplications",
		"StartApplication",
		"StopApplication",
		"UpdateApplication",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
		"DeleteApplicationReferenceDataSource",
		"DiscoverInputSchema",
	}

	assert.ElementsMatch(t, expected, ops)
}

// TestRefinement1_ProviderInit_NilCtx verifies ErrNilAppContext is returned for nil input.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &kinesisanalytics.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, kinesisanalytics.ErrNilAppContext)
}

// TestRefinement1_ErrNilAppContextValue verifies the ErrNilAppContext is non-nil and descriptive.
func TestRefinement1_ErrNilAppContextValue(t *testing.T) {
	t.Parallel()

	require.Error(t, kinesisanalytics.ErrNilAppContext)
	assert.Contains(t, kinesisanalytics.ErrNilAppContext.Error(), "nil")
}

// TestRefinement1_ProviderInit_NonNilCtx verifies Provider.Init succeeds with valid AppContext.
func TestRefinement1_ProviderInit_NonNilCtx(t *testing.T) {
	t.Parallel()

	p := &kinesisanalytics.Provider{}
	h, err := p.Init(&service.AppContext{})

	require.NoError(t, err)
	require.NotNil(t, h)
}

// TestRefinement1_SeedHelper_AddApplicationInternal verifies AddApplicationInternal plants an app.
func TestRefinement1_SeedHelper_AddApplicationInternal(t *testing.T) {
	t.Parallel()

	b := newBackend()
	now := time.Now().UTC()
	b.AddApplicationInternal(&kinesisanalytics.Application{
		ApplicationName:          "seeded-app",
		ApplicationARN:           "arn:aws:kinesisanalytics:us-east-1:000000000000:application/seeded-app",
		ApplicationStatus:        "READY",
		ApplicationVersionID:     1,
		CreateTimestamp:          &now,
		LastUpdateTimestamp:      &now,
		CloudWatchLoggingOptions: []kinesisanalytics.CloudWatchLoggingOptionDesc{},
		Inputs:                   []kinesisanalytics.InputDescription{},
		Outputs:                  []kinesisanalytics.OutputDescription{},
		ReferenceDataSources:     []kinesisanalytics.ReferenceDataSourceDescription{},
	})

	assert.Equal(t, 1, kinesisanalytics.ApplicationCount(b))

	app, err := b.DescribeApplication(context.Background(), "seeded-app")
	require.NoError(t, err)
	assert.Equal(t, "seeded-app", app.ApplicationName)
}

// TestRefinement1_ExportCountHelpers verifies the ApplicationCount helper.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := newBackend()
	assert.Zero(t, kinesisanalytics.ApplicationCount(b))

	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cnt-1", "", "", nil)
	assert.Equal(t, 1, kinesisanalytics.ApplicationCount(b))

	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cnt-2", "", "", nil)
	assert.Equal(t, 2, kinesisanalytics.ApplicationCount(b))
}

// TestRefinement1_NonNilSlices verifies Application always has non-nil sub-resource slices.
func TestRefinement1_NonNilSlices(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "slice-app", "", "", nil)
	require.NoError(t, err)

	assert.NotNil(t, app.CloudWatchLoggingOptions)
	assert.NotNil(t, app.Inputs)
	assert.NotNil(t, app.Outputs)
	assert.NotNil(t, app.ReferenceDataSources)
}

// TestRefinement1_DescribeApplication_ReturnsCopy verifies DescribeApplication returns a deep copy.
func TestRefinement1_DescribeApplication_ReturnsCopy(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "copy-app", "", "", map[string]string{"k": "v"})

	app1, err := b.DescribeApplication(context.Background(), "copy-app")
	require.NoError(t, err)

	// Mutating the returned copy must not affect the stored application.
	app1.ApplicationName = "mutated"
	app1.Tags["k"] = "mutated"

	app2, err := b.DescribeApplication(context.Background(), "copy-app")
	require.NoError(t, err)

	assert.Equal(t, "copy-app", app2.ApplicationName)
	assert.Equal(t, "v", app2.Tags["k"])
}

// TestRefinement1_SortedListTagsForResource verifies tags are returned in alphabetical key order.
func TestRefinement1_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "sorted-tags", "", "", map[string]string{
		"z": "last",
		"a": "first",
		"m": "mid",
	})

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": app.ApplicationARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	require.Len(t, resp.Tags, 3)
	assert.Equal(t, "a", resp.Tags[0].Key)
	assert.Equal(t, "m", resp.Tags[1].Key)
	assert.Equal(t, "z", resp.Tags[2].Key)
}

// TestRefinement1_VersionNotBumpedOnDeleteCWLNotFound verifies no phantom version bump when CWL ID not found.
func TestRefinement1_VersionNotBumpedOnDeleteCWLNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "bump-test", "", "", nil)

	// Try to delete a non-existent CWL option — version 1 passed.
	err := b.DeleteApplicationCloudWatchLoggingOption(context.Background(), "bump-test", 1, "nonexistent-cwl-id")
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	// Version should still be 1 (not bumped) so the next version-1 call succeeds.
	app, err := b.DescribeApplication(context.Background(), "bump-test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

// TestRefinement1_VersionNotBumpedOnDeleteOutputNotFound verifies no phantom bump for output.
func TestRefinement1_VersionNotBumpedOnDeleteOutputNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "out-bump", "", "", nil)

	err := b.DeleteApplicationOutput(context.Background(), "out-bump", 1, "nonexistent-output-id")
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	app, _ := b.DescribeApplication(context.Background(), "out-bump")
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

// TestRefinement1_VersionNotBumpedOnDeleteRefNotFound verifies no phantom bump for reference data source.
func TestRefinement1_VersionNotBumpedOnDeleteRefNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-bump", "", "", nil)

	err := b.DeleteApplicationReferenceDataSource(context.Background(), "ref-bump", 1, "nonexistent-ref-id")
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	app, _ := b.DescribeApplication(context.Background(), "ref-bump")
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

// TestRefinement1_VersionNotBumpedOnAddInputProcConfigNotFound verifies no bump when inputID missing.
func TestRefinement1_VersionNotBumpedOnAddInputProcConfigNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ipc-bump", "", "", nil)

	err := b.AddApplicationInputProcessingConfiguration(
		context.Background(),
		"ipc-bump",
		1,
		"nonexistent-input-id",
		nil,
	)
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	app, _ := b.DescribeApplication(context.Background(), "ipc-bump")
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves full state including nextID.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kinesisanalytics.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "empty",
			setup:   func(_ *kinesisanalytics.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "with_applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "persist-1", "desc", "SELECT 1",
					map[string]string{"env": "test"})
				// Add a sub-resource to exercise nextID persistence.
				_ = b.AddApplicationCloudWatchLoggingOption(context.Background(), "persist-1", app.ApplicationVersionID,
					kinesisanalytics.CloudWatchLoggingOptionDesc{
						LogStreamARN: "arn:aws:logs:us-east-1:000:log-group:x:log-stream:y",
						RoleARN:      "arn:aws:iam::000000000000:role/r",
					})
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "persist-2", "", "", nil)
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantLen, kinesisanalytics.ApplicationCount(b2))

			if tt.wantLen > 0 {
				app, err := b2.DescribeApplication(context.Background(), "persist-1")
				require.NoError(t, err)
				assert.Equal(t, "test", app.Tags["env"])
				assert.Len(t, app.CloudWatchLoggingOptions, 1)

				// nextID is preserved: next allocation should not collide with existing IDs.
				_ = b2.AddApplicationCloudWatchLoggingOption(
					context.Background(),
					"persist-1",
					app.ApplicationVersionID,
					kinesisanalytics.CloudWatchLoggingOptionDesc{
						LogStreamARN: "arn:aws:logs:us-east-1:000000000000:log-group:g2:log-stream:s2",
						RoleARN:      "arn:aws:iam::000000000000:role/r",
					},
				)
				app2, _ := b2.DescribeApplication(context.Background(), "persist-1")
				// The new option must have a different ID from the restored one.
				assert.NotEqual(t, app.CloudWatchLoggingOptions[0].CloudWatchLoggingOptionID,
					app2.CloudWatchLoggingOptions[1].CloudWatchLoggingOptionID)
			}
		})
	}
}

// TestRefinement1_PersistenceEmpty verifies Snapshot on empty backend produces valid JSON.
func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend()
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Zero(t, kinesisanalytics.ApplicationCount(b2))
}

// TestRefinement1_HandlerSnapshotRestore verifies Handler.Snapshot/Restore delegates correctly.
func TestRefinement1_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "hs-app", "", "", nil)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2, b2 := newTestHandlerWithBackend(t)
	require.NoError(t, h2.Restore(t.Context(), snap))
	assert.Equal(t, 1, kinesisanalytics.ApplicationCount(b2))
}

// TestRefinement1_ValidationErrors_DeleteSubResources verifies empty ID returns 400.
func TestRefinement1_ValidationErrors_DeleteSubResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "DeleteCWL_missing_app_name",
			action: "DeleteApplicationCloudWatchLoggingOption",
			body:   map[string]any{"CloudWatchLoggingOptionId": "cwl-1"},
		},
		{
			name:   "DeleteCWL_missing_option_id",
			action: "DeleteApplicationCloudWatchLoggingOption",
			body:   map[string]any{"ApplicationName": "my-app"},
		},
		{
			name:   "DeleteOutput_missing_output_id",
			action: "DeleteApplicationOutput",
			body:   map[string]any{"ApplicationName": "my-app"},
		},
		{
			name:   "DeleteRef_missing_reference_id",
			action: "DeleteApplicationReferenceDataSource",
			body:   map[string]any{"ApplicationName": "my-app"},
		},
		{
			name:   "DeleteInputProcConf_missing_input_id",
			action: "DeleteApplicationInputProcessingConfiguration",
			body:   map[string]any{"ApplicationName": "my-app"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_AddCWLRoundTrip verifies CWL option appears in DescribeApplication.
func TestRefinement1_AddCWLRoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cwl-rt-app", "", "", nil)

	rec := doRequest(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": app.ApplicationVersionID,
		"CloudWatchLoggingOption": map[string]any{
			"LogStreamARN": "arn:aws:logs:us-east-1:000:log-group:g:log-stream:s",
			"RoleARN":      "arn:aws:iam::000:role/r",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify it appears in describe.
	rec2 := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationName": app.ApplicationName})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	detail := resp["ApplicationDetail"].(map[string]any)
	cwlList := detail["CloudWatchLoggingOptionDescriptions"].([]any)
	assert.Len(t, cwlList, 1)
}

// TestRefinement1_AddOutputRoundTrip verifies output appears in DescribeApplication.
func TestRefinement1_AddOutputRoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "out-rt-app", "", "", nil)

	rec := doRequest(t, h, "AddApplicationOutput", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": app.ApplicationVersionID,
		"Output": map[string]any{
			"Name": "DESTINATION_SQL_STREAM",
			"KinesisStreamsOutput": map[string]any{
				"ResourceARN": "arn:aws:kinesis:us-east-1:000:stream/s",
				"RoleARN":     "arn:aws:iam::000:role/r",
			},
			"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationName": app.ApplicationName})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	detail := resp["ApplicationDetail"].(map[string]any)
	outList := detail["OutputDescriptions"].([]any)
	assert.Len(t, outList, 1)
}

// TestRefinement1_AddRefDataSourceRoundTrip verifies ref data source appears in describe response.
func TestRefinement1_AddRefDataSourceRoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-rt-app", "", "", nil)

	rec := doRequest(t, h, "AddApplicationReferenceDataSource", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": app.ApplicationVersionID,
		"ReferenceDataSource": map[string]any{
			"TableName": "MyTable",
			"S3ReferenceDataSource": map[string]any{
				"BucketARN":        "arn:aws:s3:::my-bucket",
				"FileKey":          "ref.csv",
				"ReferenceRoleARN": "arn:aws:iam::000000000000:role/r",
			},
			"ReferenceSchema": map[string]any{
				"RecordFormat":  map[string]any{"RecordFormatType": "CSV"},
				"RecordColumns": []map[string]any{{"Name": "COL1", "SqlType": "VARCHAR(4)"}},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationName": app.ApplicationName})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	detail := resp["ApplicationDetail"].(map[string]any)
	refList := detail["ReferenceDataSourceDescriptions"].([]any)
	assert.Len(t, refList, 1)
}

// TestRefinement1_DeleteCWLRoundTrip verifies CWL option is removed after delete.
func TestRefinement1_DeleteCWLRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-cwl-app", "", "", nil)

	// Add a CWL option.
	_ = b.AddApplicationCloudWatchLoggingOption(context.Background(), app.ApplicationName, app.ApplicationVersionID,
		kinesisanalytics.CloudWatchLoggingOptionDesc{
			LogStreamARN: "arn:aws:logs:us-east-1:000:log-group:g:log-stream:s",
			RoleARN:      "arn:aws:iam::000000000000:role/r",
		})

	app2, _ := b.DescribeApplication(context.Background(), app.ApplicationName)
	require.Len(t, app2.CloudWatchLoggingOptions, 1)
	cwlID := app2.CloudWatchLoggingOptions[0].CloudWatchLoggingOptionID

	// Delete it.
	err := b.DeleteApplicationCloudWatchLoggingOption(
		context.Background(),
		app.ApplicationName,
		app2.ApplicationVersionID,
		cwlID,
	)
	require.NoError(t, err)

	app3, _ := b.DescribeApplication(context.Background(), app.ApplicationName)
	assert.Empty(t, app3.CloudWatchLoggingOptions)
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation maps to 400 InvalidArgumentException.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	// Trigger validation via missing ApplicationName.
	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateApplication", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidArgumentException", errResp["__type"])
}

// TestRefinement1_UnknownAction returns InvalidArgumentException.
func TestRefinement1_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "NonExistentAction", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_TagResource_NotFound returns 404 for unknown ARN.
func TestRefinement1_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/nope",
		"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_UntagResource_NotFound returns 404 for unknown ARN.
func TestRefinement1_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/nope",
		"TagKeys":     []string{"k"},
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_ListApplications_Pagination verifies pagination with ExclusiveStartApplicationName.
func TestRefinement1_ListApplications_Pagination(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "page-a", "", "", nil)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "page-b", "", "", nil)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "page-c", "", "", nil)

	// Request page 2 starting after "page-a".
	apps, hasMore, _ := b.ListApplications(context.Background(), "page-a", 1)
	assert.Len(t, apps, 1)
	assert.Equal(t, "page-b", apps[0].ApplicationName)
	assert.True(t, hasMore)
}

// TestRefinement1_CreateApplication_SetsTimestamps verifies CreateTimestamp and LastUpdateTimestamp are set.
func TestRefinement1_CreateApplication_SetsTimestamps(t *testing.T) {
	t.Parallel()

	b := newBackend()
	before := time.Now()
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ts-app", "", "", nil)
	after := time.Now()

	require.NoError(t, err)
	require.NotNil(t, app.CreateTimestamp)
	require.NotNil(t, app.LastUpdateTimestamp)
	assert.False(t, app.CreateTimestamp.Before(before))
	assert.False(t, app.CreateTimestamp.After(after))
}

// TestRefinement1_UpdateApplication_BumpsVersion verifies UpdateApplication increments the version.
func TestRefinement1_UpdateApplication_BumpsVersion(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ver-bump", "", "SELECT 1", nil)
	assert.Equal(t, int64(1), app.ApplicationVersionID)

	_, err := kinesisanalytics.UpdateAppCode(b, "ver-bump", 1, "SELECT 2")
	require.NoError(t, err)

	app2, _ := b.DescribeApplication(context.Background(), "ver-bump")
	assert.Equal(t, int64(2), app2.ApplicationVersionID)
	assert.Equal(t, "SELECT 2", app2.ApplicationCode)
}

// TestRefinement1_ConcurrentModificationException verifies version mismatch returns 400.
func TestRefinement1_ConcurrentModificationException(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "conc-app", "", "", nil)

	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": int64(999), // wrong version
		"ApplicationUpdate":           map[string]any{},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ConcurrentModificationException", errResp["__type"])
}

// TestRefinement1_ResourceAlreadyExists_Returns400 verifies duplicate app returns 400 ResourceInUseException.
func TestRefinement1_ResourceAlreadyExists_Returns400(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-dup", "", "", nil)

	rec := doRequest(t, h, "CreateApplication", map[string]any{"ApplicationName": "dup-dup"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceInUseException", errResp["__type"])
}

// TestRefinement1_DeepCopyInput verifies appCopy handles Inputs with pointer fields.
func TestRefinement1_DeepCopyInput(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dc-input-app", "", "", nil)

	// Add input.
	_ = b.AddApplicationInput(
		context.Background(),
		"dc-input-app",
		app.ApplicationVersionID,
		kinesisanalytics.InputDescription{
			NamePrefix: "src_",
			KinesisStreamsInputDescription: &kinesisanalytics.KinesisStreamsInputDesc{
				ResourceARN: "arn:aws:kinesis:us-east-1:000:stream/st",
			},
		},
	)

	// Add processing config to the input.
	app2, _ := b.DescribeApplication(context.Background(), "dc-input-app")
	inputID := app2.Inputs[0].InputID

	_ = b.AddApplicationInputProcessingConfiguration(
		context.Background(),
		"dc-input-app",
		app2.ApplicationVersionID,
		inputID,
		&kinesisanalytics.InputProcessingConfigurationDesc{
			InputLambdaProcessor: &kinesisanalytics.LambdaProcessorDesc{
				ResourceARN: "arn:aws:lambda:us-east-1:000:function:fn",
			},
		},
	)

	app3, _ := b.DescribeApplication(context.Background(), "dc-input-app")
	require.Len(t, app3.Inputs, 1)
	require.NotNil(t, app3.Inputs[0].InputProcessingConfigurationDescription)

	// Mutate the copy — original must be unaffected.
	app3.Inputs[0].InputProcessingConfigurationDescription.InputLambdaProcessor.ResourceARN = "mutated"

	app4, _ := b.DescribeApplication(context.Background(), "dc-input-app")
	assert.Equal(
		t,
		"arn:aws:lambda:us-east-1:000:function:fn",
		app4.Inputs[0].InputProcessingConfigurationDescription.InputLambdaProcessor.ResourceARN,
	)
}
