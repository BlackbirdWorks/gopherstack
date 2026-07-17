package kinesisanalyticsv2_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

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

// TestBackend_DescribeApplication_DeepCopy verifies DescribeApplication
// returns a copy that callers can freely mutate without affecting stored state.
func TestBackend_DescribeApplication_DeepCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	_, err := b.CreateApplication(
		ctx,
		"copy-app",
		"FLINK-1_18",
		"",
		"",
		"",
		[]kinesisanalyticsv2.Tag{{Key: "k", Value: "v"}},
	)
	require.NoError(t, err)

	app1, err := b.DescribeApplication(ctx, "copy-app")
	require.NoError(t, err)

	// Mutate returned copy
	app1.Tags[0].Value = "mutated"
	app1.ApplicationDescription = "mutated"

	app2, err := b.DescribeApplication(ctx, "copy-app")
	require.NoError(t, err)

	assert.Equal(t, "v", app2.Tags[0].Value, "mutation of returned copy must not affect stored state")
	assert.Empty(t, app2.ApplicationDescription)
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
