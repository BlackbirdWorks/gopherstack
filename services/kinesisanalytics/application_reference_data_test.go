package kinesisanalytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func TestHandler_AddApplicationReferenceDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds reference data source",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "ref-app",
				"CurrentApplicationVersionId": 1,
				"ReferenceDataSource": map[string]any{
					"TableName": "MY_REF_TABLE",
					"S3ReferenceDataSource": map[string]any{
						"BucketARN":        "arn:aws:s3:::my-bucket",
						"FileKey":          "data.csv",
						"ReferenceRoleARN": "arn:aws:iam::000000000000:role/role",
					},
					"ReferenceSchema": map[string]any{
						"RecordFormat":  map[string]any{"RecordFormatType": "CSV"},
						"RecordColumns": []map[string]any{{"Name": "COL1", "SqlType": "VARCHAR(4)"}},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app not found",
			input: map[string]any{
				"ApplicationName":             "ghost",
				"CurrentApplicationVersionId": 1,
				"ReferenceDataSource": map[string]any{
					"TableName": "MY_REF",
					"S3ReferenceDataSource": map[string]any{
						"BucketARN":        "arn:aws:s3:::my-bucket",
						"FileKey":          "data.csv",
						"ReferenceRoleARN": "arn:aws:iam::000000000000:role/r",
					},
					"ReferenceSchema": map[string]any{
						"RecordFormat":  map[string]any{"RecordFormatType": "CSV"},
						"RecordColumns": []map[string]any{{"Name": "COL1", "SqlType": "VARCHAR(4)"}},
					},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "ref-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "AddApplicationReferenceDataSource", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "ref-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.ReferenceDataSources)
			}
		})
	}
}

// TestAddApplicationReferenceDataSource_LimitErrorCode verifies that exceeding the
// per-application reference-data-source cap surfaces as InvalidArgumentException,
// matching the modeled error set for AddApplicationReferenceDataSource (no
// LimitExceededException in its AWS API definition).
func TestAddApplicationReferenceDataSource_LimitErrorCode(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-cap-app", "", "", nil)
	require.NoError(t, err)

	ref := kinesisanalytics.ReferenceDataSourceDescription{
		TableName: "T1",
		S3ReferenceDataSourceDescription: &kinesisanalytics.S3ReferenceDataSourceDesc{
			BucketARN: "arn:aws:s3:::b", FileKey: "k", ReferenceRoleARN: "arn:aws:iam::000000000000:role/r",
		},
	}
	require.NoError(t, b.AddApplicationReferenceDataSource(
		context.Background(), app.ApplicationName, app.ApplicationVersionID, ref,
	))

	err = b.AddApplicationReferenceDataSource(context.Background(), app.ApplicationName, 2, ref)
	require.ErrorIs(t, err, awserr.ErrInvalidParameter)
	assert.NotErrorIs(t, err, awserr.ErrConflict)
}

func TestHandler_DeleteApplicationReferenceDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(refID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing reference data source",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-ref-app", "", "", nil)
				_ = b.AddApplicationReferenceDataSource(
					context.Background(),
					"del-ref-app",
					1,
					kinesisanalytics.ReferenceDataSourceDescription{TableName: "REF_TBL"},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-ref-app")

				return app.ReferenceDataSources[0].ReferenceID
			},
			input: func(refID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-ref-app",
					"CurrentApplicationVersionId": 2,
					"ReferenceId":                 refID,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "reference id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-ref-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(refID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-ref-notfound",
					"CurrentApplicationVersionId": 1,
					"ReferenceId":                 refID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			refID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationReferenceDataSource", tt.input(refID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "del-ref-app")
				require.NoError(t, err)
				assert.Empty(t, app.ReferenceDataSources)
			}
		})
	}
}

// TestDeleteApplicationReferenceDataSource_Validation verifies missing reference ID returns 400.
func TestDeleteApplicationReferenceDataSource_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteApplicationReferenceDataSource", map[string]any{"ApplicationName": "my-app"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestVersionNotBumpedOnDeleteRefNotFound verifies no phantom bump for reference data source.
func TestVersionNotBumpedOnDeleteRefNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-bump", "", "", nil)

	err := b.DeleteApplicationReferenceDataSource(context.Background(), "ref-bump", 1, "nonexistent-ref-id")
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	app, _ := b.DescribeApplication(context.Background(), "ref-bump")
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

// TestAddRefDataSourceRoundTrip verifies ref data source appears in describe response.
func TestAddRefDataSourceRoundTrip(t *testing.T) {
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
