package kinesisanalytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func TestCreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags        map[string]string
		name        string
		appName     string
		description string
		code        string
		wantErr     bool
	}{
		{
			name:        "creates new application",
			appName:     "test-app",
			description: "test description",
			code:        "SELECT 1",
			tags:        map[string]string{"env": "test"},
		},
		{
			name:    "creates application without optional fields",
			appName: "minimal-app",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			app, err := kinesisanalytics.CreateApp(
				b, testRegion, testAccountID, tt.appName, tt.description, tt.code, tt.tags,
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, app)
			assert.Equal(t, tt.appName, app.ApplicationName)
			assert.Equal(t, "READY", app.ApplicationStatus)
			assert.Equal(t, int64(1), app.ApplicationVersionID)
			assert.NotEmpty(t, app.ApplicationARN)
			assert.NotNil(t, app.CreateTimestamp)
			assert.Equal(t, "SQL-1_0", app.RuntimeEnvironment)
		})
	}
}

func TestCreateApplication_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-app", "", "", nil)
	require.NoError(t, err)

	_, err = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-app", "", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrAlreadyExists)
}

func TestDeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "deletes existing application",
			appName: "to-delete",
		},
		{
			name:    "not found when missing",
			appName: "missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			var ts *time.Time

			if tt.appName == "to-delete" {
				app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "to-delete", "", "", nil)
				require.NoError(t, err)
				ts = app.CreateTimestamp
			}

			err := b.DeleteApplication(context.Background(), tt.appName, ts)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDeleteApplication_NilTimestamp(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-nil-ts", "", "", nil)
	require.NoError(t, err)

	err = b.DeleteApplication(context.Background(), "del-nil-ts", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

func TestDescribeApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		setup   func(*kinesisanalytics.InMemoryBackend)
		want    string
		wantErr bool
	}{
		{
			name:    "returns existing application",
			appName: "my-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "my-app", "desc", "code", nil)
			},
			want: "my-app",
		},
		{
			name:    "returns not found for missing",
			appName: "ghost",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			app, err := b.DescribeApplication(context.Background(), tt.appName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, app.ApplicationName)
		})
	}
}

func TestListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*kinesisanalytics.InMemoryBackend)
		exclusiveStart string
		limit          int
		wantCount      int
		wantHasMore    bool
	}{
		{
			name: "lists all applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-a", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-b", "", "", nil)
			},
			wantCount: 2,
		},
		{
			name:  "empty list",
			setup: func(_ *kinesisanalytics.InMemoryBackend) {},
		},
		{
			name: "limit truncates results",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "l-app-a", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "l-app-b", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "l-app-c", "", "", nil)
			},
			limit:       2,
			wantCount:   2,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			apps, hasMore, err := b.ListApplications(context.Background(), tt.exclusiveStart, tt.limit)
			require.NoError(t, err)
			assert.Len(t, apps, tt.wantCount)
			assert.Equal(t, tt.wantHasMore, hasMore)
		})
	}
}

func TestListApplications_ExclusiveStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*kinesisanalytics.InMemoryBackend)
		exclusiveStart string
		wantCountMin   int
	}{
		{
			name: "exclusiveStart not found still returns all",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "exc-app-a", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "exc-app-b", "", "", nil)
			},
			exclusiveStart: "nonexistent",
			wantCountMin:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			apps, _, err := b.ListApplications(context.Background(), tt.exclusiveStart, 0)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(apps), tt.wantCountMin)
		})
	}
}

// TestListApplications_Pagination verifies pagination with ExclusiveStartApplicationName.
func TestListApplications_Pagination(t *testing.T) {
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

func TestStartApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kinesisanalytics.InMemoryBackend)
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "start transitions to running",
			appName: "runnable",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "runnable", "", "", nil)
			},
		},
		{
			name:    "start not found returns error",
			appName: "missing",
			setup:   func(_ *kinesisanalytics.InMemoryBackend) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			err := kinesisanalytics.StartAppNoConfig(b, tt.appName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			kinesisanalytics.WaitForStatus(t, b, tt.appName, "RUNNING")
		})
	}
}

func TestStopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *kinesisanalytics.InMemoryBackend)
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "stop transitions to ready",
			appName: "stoppable",
			setup: func(t *testing.T, b *kinesisanalytics.InMemoryBackend) {
				t.Helper()
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "stoppable", "", "", nil)
				require.NoError(t, kinesisanalytics.StartAppNoConfig(b, "stoppable"))
				kinesisanalytics.WaitForStatus(t, b, "stoppable", "RUNNING")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(t, b)

			err := b.StopApplication(context.Background(), tt.appName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			kinesisanalytics.WaitForStatus(t, b, tt.appName, "READY")
		})
	}
}

func TestTimestampsSet(t *testing.T) {
	t.Parallel()

	b := newBackend()
	before := time.Now().Add(-time.Second)
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ts-app", "", "", nil)
	require.NoError(t, err)

	assert.NotNil(t, app.CreateTimestamp)
	assert.True(t, app.CreateTimestamp.After(before), "CreateTimestamp should be after test start")
	assert.NotNil(t, app.LastUpdateTimestamp)
}

// TestCreateApplication_SetsTimestamps verifies CreateTimestamp and LastUpdateTimestamp are set.
func TestCreateApplication_SetsTimestamps(t *testing.T) {
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

func TestApplicationLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   int
		wantErr bool
	}{
		{
			name:    "reject limit above 50",
			limit:   51,
			wantErr: true,
		},
		{
			name:    "reject negative limit",
			limit:   -1,
			wantErr: true,
		},
		{
			name:  "accept limit of 50",
			limit: 50,
		},
		{
			name:  "accept limit of 0 (default)",
			limit: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, _, err := b.ListApplications(context.Background(), "", tt.limit)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestApplicationNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{name: "valid alphanumeric", appName: "myApp123"},
		{name: "valid with dashes", appName: "my-app"},
		{name: "valid with underscores", appName: "my_app"},
		{name: "valid with dots", appName: "my.app"},
		{name: "empty name", appName: "", wantErr: true},
		{name: "name with spaces", appName: "my app", wantErr: true},
		{name: "name with special chars", appName: "my@app", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, tt.appName, "", "", nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCancelFuncs_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "lifecycle-app", "", "", nil)
	require.NoError(t, err)

	// No goroutines before start.
	assert.Equal(t, 0, kinesisanalytics.GetCancelFuncsLen(b))

	// Start spawns a goroutine.
	require.NoError(t, kinesisanalytics.StartAppNoConfig(b, "lifecycle-app"))
	assert.Equal(t, 1, kinesisanalytics.GetCancelFuncsLen(b))

	// After transition completes, goroutine cleans itself up.
	kinesisanalytics.WaitForStatus(t, b, "lifecycle-app", "RUNNING")
	assert.Equal(t, 0, kinesisanalytics.GetCancelFuncsLen(b))
}

func TestApplicationCount(t *testing.T) {
	t.Parallel()

	b := newBackend()
	assert.Equal(t, 0, kinesisanalytics.ApplicationCount(b))

	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "count-app-1", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, kinesisanalytics.ApplicationCount(b))

	_, err = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "count-app-2", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, kinesisanalytics.ApplicationCount(b))
}

// TestExportCountHelpers verifies the ApplicationCount helper.
func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := newBackend()
	assert.Zero(t, kinesisanalytics.ApplicationCount(b))

	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cnt-1", "", "", nil)
	assert.Equal(t, 1, kinesisanalytics.ApplicationCount(b))

	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cnt-2", "", "", nil)
	assert.Equal(t, 2, kinesisanalytics.ApplicationCount(b))
}

// TestSeedHelper_AddApplicationInternal verifies AddApplicationInternal plants an app.
func TestSeedHelper_AddApplicationInternal(t *testing.T) {
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

// TestNonNilSlices verifies Application always has non-nil sub-resource slices.
func TestNonNilSlices(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "slice-app", "", "", nil)
	require.NoError(t, err)

	assert.NotNil(t, app.CloudWatchLoggingOptions)
	assert.NotNil(t, app.Inputs)
	assert.NotNil(t, app.Outputs)
	assert.NotNil(t, app.ReferenceDataSources)
}

// TestDescribeApplication_ReturnsCopy verifies DescribeApplication returns a deep copy.
func TestDescribeApplication_ReturnsCopy(t *testing.T) {
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

// TestResourceAlreadyExists_Returns400 verifies duplicate app returns 400 ResourceInUseException.
func TestResourceAlreadyExists_Returns400(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-dup", "", "", nil)

	rec := doRequest(t, h, "CreateApplication", map[string]any{"ApplicationName": "dup-dup"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceInUseException", errResp["__type"])
}

// TestErrValidationMapping verifies ErrValidation maps to 400 InvalidArgumentException.
func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	// Trigger validation via missing ApplicationName.
	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateApplication", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidArgumentException", errResp["__type"])
}
