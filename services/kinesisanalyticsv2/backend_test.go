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
		currentVersionID  int64
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
		{
			name:              "version matches",
			appName:           "update-app-versioned",
			createFirst:       true,
			currentVersionID:  1,
			updateServiceRole: "arn:aws:iam::000000000000:role/new-role",
			wantVersionID:     2,
		},
		{
			name:             "version mismatch",
			appName:          "update-app-mismatch",
			createFirst:      true,
			currentVersionID: 99,
			wantErr:          true,
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

			app, opID, err := b.UpdateApplication(
				ctx, tt.appName, tt.currentVersionID, tt.updateServiceRole, tt.updateDescription,
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantVersionID, app.ApplicationVersionID)
			assert.Equal(t, tt.updateServiceRole, app.ServiceExecutionRole)
			assert.Equal(t, tt.updateDescription, app.ApplicationDescription)
			assert.NotEmpty(t, opID)
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

			var opID string

			if tt.op == "start" {
				opID, err = b.StartApplication(ctx, "app-lifecycle")
			} else {
				_, err = b.StartApplication(ctx, "app-lifecycle")
				require.NoError(t, err)
				opID, err = b.StopApplication(ctx, "app-lifecycle")
			}

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, opID)

			app, descErr := b.DescribeApplication(ctx, "app-lifecycle")
			require.NoError(t, descErr)
			assert.Equal(t, tt.wantStatus, app.ApplicationStatus)

			// The operation must be discoverable via DescribeApplicationOperation
			// and ListApplicationOperations -- these were previously always
			// empty because nothing ever populated the operations map.
			op, opErr := b.DescribeApplicationOperation(ctx, "app-lifecycle", opID)
			require.NoError(t, opErr)
			assert.Equal(t, kinesisanalyticsv2.OperationStatusSuccessful, op.OperationStatus)

			ops, _, listErr := b.ListApplicationOperations(ctx, "app-lifecycle", "")
			require.NoError(t, listErr)
			assert.NotEmpty(t, ops)
		})
	}
}

func TestBackend_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	_, err := b.CreateApplication(ctx, "snap-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.StartApplication(ctx, "snap-app")
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

			_, err = b.StartApplication(ctx, "paged-snap-app")
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

// TestBackend_DeleteApplication_CleansOperations verifies that DeleteApplication
// removes the application's entry from the operations map, preventing unbounded growth.
func TestBackend_DeleteApplication_CleansOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "cleanup-ops-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	// Populate a real operations-map entry (StartApplication records one via
	// recordOperation) so this test actually exercises cleanup of non-empty
	// state, not just a no-op delete against an already-empty map.
	_, err = b.StartApplication(ctx, "cleanup-ops-app")
	require.NoError(t, err)
	require.Equal(t, 1, kinesisanalyticsv2.OperationsMapKeyCount(b, "us-east-1"))

	err = b.DeleteApplication(ctx, "cleanup-ops-app")
	require.NoError(t, err)

	// The operations map entry for the deleted app must be gone.
	count := kinesisanalyticsv2.OperationsMapKeyCount(b, "us-east-1")
	assert.Equal(t, 0, count, "operations map must be cleaned up on application delete")
}

// TestBackend_RollbackApplication verifies that RollbackApplication reverts
// an application to its previous configuration version. Before
// checkAndBumpVersion started recording version history (see backend.go),
// b.versions only ever held the version-1 snapshot from CreateApplication, so
// RollbackApplication's "at least 2 versions" guard could never be satisfied
// by real traffic and this operation always failed.
func TestBackend_RollbackApplication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		app, err := b.CreateApplication(ctx, "rollback-app", "FLINK-1_18", "", "first description", "", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), app.ApplicationVersionID)

		updated, opID, err := b.UpdateApplication(ctx, "rollback-app", 1, "", "second description")
		require.NoError(t, err)
		require.Equal(t, int64(2), updated.ApplicationVersionID)
		require.NotEmpty(t, opID)

		rolledBack, rollbackOpID, err := b.RollbackApplication(ctx, "rollback-app", 2)
		require.NoError(t, err)
		assert.NotEmpty(t, rollbackOpID)
		assert.Equal(t, int64(3), rolledBack.ApplicationVersionID)
		assert.Equal(t, "first description", rolledBack.ApplicationDescription)

		op, err := b.DescribeApplicationOperation(ctx, "rollback-app", rollbackOpID)
		require.NoError(t, err)
		assert.Equal(t, "RollbackApplication", op.Operation)
		assert.Equal(t, kinesisanalyticsv2.OperationStatusSuccessful, op.OperationStatus)
	})

	t.Run("not enough version history", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.CreateApplication(ctx, "rollback-fresh-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		_, _, err = b.RollbackApplication(ctx, "rollback-fresh-app", 0)
		require.Error(t, err)
	})

	t.Run("version mismatch", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.CreateApplication(ctx, "rollback-mismatch-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		_, _, err = b.UpdateApplication(ctx, "rollback-mismatch-app", 1, "", "second description")
		require.NoError(t, err)

		_, _, err = b.RollbackApplication(ctx, "rollback-mismatch-app", 99)
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrConcurrentModification)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, _, err := b.RollbackApplication(ctx, "missing-rollback-app", 0)
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)
	})
}

// TestBackend_ApplicationVersionHistory verifies that ListApplicationVersions
// and DescribeApplicationVersion reflect every version-bumping change, not
// just the version-1 snapshot from CreateApplication.
func TestBackend_ApplicationVersionHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "version-history-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	err = b.AddApplicationCloudWatchLoggingOption(ctx, "version-history-app", 0,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s", "")
	require.NoError(t, err)

	versions, _, err := b.ListApplicationVersions(ctx, "version-history-app", "")
	require.NoError(t, err)
	require.Len(t, versions, 2, "expected a version-history entry for both CreateApplication and the Add* call")
	assert.Equal(t, int64(1), versions[0].ApplicationVersionID)
	assert.Equal(t, int64(2), versions[1].ApplicationVersionID)

	v2, err := b.DescribeApplicationVersion(ctx, "version-history-app", 2)
	require.NoError(t, err)
	assert.Len(t, v2.CloudWatchLoggingOptionDescs, 1)
}

// TestBackend_SeedApplicationConfiguration verifies that inline
// configuration supplied at creation time (real AWS's CreateApplication
// ApplicationConfiguration/CloudWatchLoggingOptions parameters) populates
// the application without bumping ApplicationVersionId past 1 -- real AWS
// keeps a freshly created application, even with inline config, at version 1.
func TestBackend_SeedApplicationConfiguration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.CreateApplication(ctx, "seed-app", "SQL-1_0", "", "", "", nil)
		require.NoError(t, err)

		cwlOption := kinesisanalyticsv2.CloudWatchLoggingOptionDesc{
			LogStreamARN: "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
		}

		err = b.SeedApplicationConfiguration(
			ctx,
			"seed-app",
			[]kinesisanalyticsv2.InputDescription{{NamePrefix: "SOURCE"}},
			[]kinesisanalyticsv2.OutputDescription{{Name: "OUT"}},
			[]kinesisanalyticsv2.ReferenceDataSourceDescription{{TableName: "REF"}},
			[]kinesisanalyticsv2.VpcConfigurationDescription{{SubnetIDs: []string{"subnet-1"}}},
			[]kinesisanalyticsv2.CloudWatchLoggingOptionDesc{cwlOption},
		)
		require.NoError(t, err)

		app, err := b.DescribeApplication(ctx, "seed-app")
		require.NoError(t, err)
		assert.Equal(t, int64(1), app.ApplicationVersionID, "inline config must not bump the version past 1")
		require.Len(t, app.InputDescriptions, 1)
		assert.NotEmpty(t, app.InputDescriptions[0].InputID)
		require.Len(t, app.OutputDescriptions, 1)
		assert.NotEmpty(t, app.OutputDescriptions[0].OutputID)
		require.Len(t, app.ReferenceDataSourceDescriptions, 1)
		assert.NotEmpty(t, app.ReferenceDataSourceDescriptions[0].ReferenceID)
		require.Len(t, app.VpcConfigurationDescriptions, 1)
		assert.NotEmpty(t, app.VpcConfigurationDescriptions[0].VpcConfigurationID)
		require.Len(t, app.CloudWatchLoggingOptionDescs, 1)
		assert.NotEmpty(t, app.CloudWatchLoggingOptionDescs[0].CloudWatchLoggingOptionID)

		// The version-1 history snapshot must also reflect the seeded config,
		// not just the live application.
		v1, err := b.DescribeApplicationVersion(ctx, "seed-app", 1)
		require.NoError(t, err)
		assert.Len(t, v1.InputDescriptions, 1)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		err := b.SeedApplicationConfiguration(ctx, "missing-seed-app", nil, nil, nil, nil, nil)
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)
	})
}
