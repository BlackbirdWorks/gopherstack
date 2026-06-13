package kinesisanalyticsv2_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func newTestBackend(t *testing.T) *kinesisanalyticsv2.InMemoryBackend {
	t.Helper()

	return kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestBackend_CreateApplication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name        string
		appName     string
		runtime     string
		serviceRole string
		wantStatus  string
		wantErr     bool
	}{
		{
			name:        "success",
			appName:     "my-app",
			runtime:     "FLINK-1_18",
			serviceRole: "arn:aws:iam::000000000000:role/service-role",
			wantStatus:  "READY",
		},
		{
			name:        "no service role",
			appName:     "app-no-role",
			runtime:     "SQL-1_0",
			serviceRole: "",
			wantStatus:  "READY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			app, err := b.CreateApplication(ctx, tt.appName, tt.runtime, tt.serviceRole, "", "", nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.appName, app.ApplicationName)
			assert.Equal(t, tt.runtime, app.RuntimeEnvironment)
			assert.Equal(t, tt.wantStatus, app.ApplicationStatus)
			assert.NotEmpty(t, app.ApplicationARN)
			assert.Equal(t, int64(1), app.ApplicationVersionID)
		})
	}
}

func TestBackend_CreateApplication_AlreadyExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "my-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateApplication(ctx, "my-app", "FLINK-1_18", "", "", "", nil)
	require.Error(t, err)
}

func TestBackend_DescribeApplication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name    string
		appName string
		create  bool
		wantErr bool
	}{
		{
			name:    "found",
			appName: "test-app",
			create:  true,
		},
		{
			name:    "not found",
			appName: "missing-app",
			create:  false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.create {
				_, err := b.CreateApplication(ctx, tt.appName, "FLINK-1_18", "", "", "", nil)
				require.NoError(t, err)
			}

			app, err := b.DescribeApplication(ctx, tt.appName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.appName, app.ApplicationName)
		})
	}
}

func TestBackend_ListApplications(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		appNames []string
		wantLen  int
	}{
		{
			name:     "empty",
			appNames: nil,
			wantLen:  0,
		},
		{
			name:     "single",
			appNames: []string{"app1"},
			wantLen:  1,
		},
		{
			name:     "multiple",
			appNames: []string{"app1", "app2", "app3"},
			wantLen:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			for _, name := range tt.appNames {
				_, err := b.CreateApplication(ctx, name, "FLINK-1_18", "", "", "", nil)
				require.NoError(t, err)
			}

			apps, _ := b.ListApplications(ctx, "")
			assert.Len(t, apps, tt.wantLen)
		})
	}
}

func TestBackend_UpdateApplication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name              string
		appName           string
		updateServiceRole string
		updateDescription string
		wantVersionID     int64
		createFirst       bool
		wantErr           bool
	}{
		{
			name:              "success",
			appName:           "update-app",
			createFirst:       true,
			updateServiceRole: "arn:aws:iam::000000000000:role/new-role",
			updateDescription: "updated description",
			wantVersionID:     2,
		},
		{
			name:        "not found",
			appName:     "missing-app",
			createFirst: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.createFirst {
				_, err := b.CreateApplication(ctx, tt.appName, "FLINK-1_18", "", "", "", nil)
				require.NoError(t, err)
			}

			app, err := b.UpdateApplication(ctx, tt.appName, tt.updateServiceRole, tt.updateDescription)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantVersionID, app.ApplicationVersionID)
			assert.Equal(t, tt.updateServiceRole, app.ServiceExecutionRole)
			assert.Equal(t, tt.updateDescription, app.ApplicationDescription)
		})
	}
}

func TestBackend_DeleteApplication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name        string
		appName     string
		createFirst bool
		wantErr     bool
	}{
		{
			name:        "success",
			appName:     "delete-app",
			createFirst: true,
		},
		{
			name:        "not found",
			appName:     "missing-app",
			createFirst: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.createFirst {
				_, err := b.CreateApplication(ctx, tt.appName, "FLINK-1_18", "", "", "", nil)
				require.NoError(t, err)
			}

			err := b.DeleteApplication(ctx, tt.appName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = b.DescribeApplication(ctx, tt.appName)
			require.Error(t, err)
		})
	}
}

func TestBackend_StartStopApplication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name       string
		op         string
		wantStatus string
		wantErr    bool
	}{
		{
			name:       "start",
			op:         "start",
			wantStatus: "RUNNING",
		},
		{
			name:       "stop",
			op:         "stop",
			wantStatus: "READY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			_, err := b.CreateApplication(ctx, "app-lifecycle", "FLINK-1_18", "", "", "", nil)
			require.NoError(t, err)

			if tt.op == "start" {
				err = b.StartApplication(ctx, "app-lifecycle")
			} else {
				err = b.StopApplication(ctx, "app-lifecycle")
			}

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			app, descErr := b.DescribeApplication(ctx, "app-lifecycle")
			require.NoError(t, descErr)
			assert.Equal(t, tt.wantStatus, app.ApplicationStatus)
		})
	}
}

func TestBackend_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	_, err := b.CreateApplication(ctx, "snap-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	// Create snapshot.
	snap, err := b.CreateApplicationSnapshot(ctx, "snap-app", "snap-1")
	require.NoError(t, err)
	assert.Equal(t, "snap-1", snap.SnapshotName)
	assert.Equal(t, "READY", snap.SnapshotStatus)

	// List snapshots.
	snaps, _, err := b.ListApplicationSnapshots(ctx, "snap-app", "")
	require.NoError(t, err)
	assert.Len(t, snaps, 1)

	// Duplicate snapshot name.
	_, err = b.CreateApplicationSnapshot(ctx, "snap-app", "snap-1")
	require.Error(t, err)

	// Delete snapshot.
	err = b.DeleteApplicationSnapshot(ctx, "snap-app", "snap-1")
	require.NoError(t, err)

	snaps, _, err = b.ListApplicationSnapshots(ctx, "snap-app", "")
	require.NoError(t, err)
	assert.Empty(t, snaps)
}

func TestBackend_Tags(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	app, err := b.CreateApplication(ctx, "tagged-app", "FLINK-1_18", "", "", "", []kinesisanalyticsv2.Tag{
		{Key: "env", Value: "test"},
	})
	require.NoError(t, err)

	appARN := app.ApplicationARN

	// ListTagsForResource.
	tags, err := b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].Key)
	assert.Equal(t, "test", tags[0].Value)

	// TagResource - add new tag.
	err = b.TagResource(ctx, appARN, []kinesisanalyticsv2.Tag{{Key: "team", Value: "platform"}})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	assert.Len(t, tags, 2)

	// TagResource - update existing tag.
	err = b.TagResource(ctx, appARN, []kinesisanalyticsv2.Tag{{Key: "env", Value: "prod"}})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	tagMap := kinesisanalyticsv2.TagsToMapForTest(tags)
	assert.Equal(t, "prod", tagMap["env"])

	// UntagResource.
	err = b.UntagResource(ctx, appARN, []string{"team"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err)
	assert.Len(t, tags, 1)
}

func TestBackend_Tags_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.ListTagsForResource(ctx, "arn:aws:kinesisanalytics:us-east-1:000000000000:application/missing")
	require.Error(t, err)

	err = b.TagResource(ctx, "arn:aws:kinesisanalytics:us-east-1:000000000000:application/missing", nil)
	require.Error(t, err)

	err = b.UntagResource(ctx, "arn:aws:kinesisanalytics:us-east-1:000000000000:application/missing", nil)
	require.Error(t, err)
}

func TestBackend_ListApplicationsPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name          string
		count         int
		wantNextToken bool
	}{
		{
			name:          "single_page",
			count:         5,
			wantNextToken: false,
		},
		{
			name:          "multi_page",
			count:         55, // exceeds kav2DefaultPageSize=50
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			for i := range tt.count {
				_, err := b.CreateApplication(
					ctx,
					fmt.Sprintf("paged-app-%04d", i),
					"FLINK-1_18", "", "", "", nil,
				)
				require.NoError(t, err)
			}

			apps, outToken := b.ListApplications(ctx, "")
			if tt.wantNextToken {
				assert.Len(t, apps, 50)
				assert.NotEmpty(t, outToken)

				// Second page.
				apps2, outToken2 := b.ListApplications(ctx, outToken)
				assert.Len(t, apps2, tt.count-50)
				assert.Empty(t, outToken2)
			} else {
				assert.Len(t, apps, tt.count)
				assert.Empty(t, outToken)
			}
		})
	}
}

func TestBackend_ListApplicationSnapshotsPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name          string
		count         int
		wantNextToken bool
	}{
		{
			name:          "single_page",
			count:         5,
			wantNextToken: false,
		},
		{
			name:          "multi_page",
			count:         55, // exceeds kav2DefaultPageSize=50
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			_, err := b.CreateApplication(ctx, "paged-snap-app", "FLINK-1_18", "", "", "", nil)
			require.NoError(t, err)

			for i := range tt.count {
				_, err = b.CreateApplicationSnapshot(ctx, "paged-snap-app", fmt.Sprintf("snap-%04d", i))
				require.NoError(t, err)
			}

			snaps, outToken, err := b.ListApplicationSnapshots(ctx, "paged-snap-app", "")
			require.NoError(t, err)

			if tt.wantNextToken {
				assert.Len(t, snaps, 50)
				assert.NotEmpty(t, outToken)

				// Second page.
				var snaps2 []*kinesisanalyticsv2.Snapshot
				var outToken2 string
				snaps2, outToken2, err = b.ListApplicationSnapshots(ctx, "paged-snap-app", outToken)
				require.NoError(t, err)
				assert.Len(t, snaps2, tt.count-50)
				assert.Empty(t, outToken2)
			} else {
				assert.Len(t, snaps, tt.count)
				assert.Empty(t, outToken)
			}
		})
	}
}
