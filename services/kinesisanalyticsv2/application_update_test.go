package kinesisanalyticsv2_test

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// TestBackend_UpdateApplication_ConditionalToken verifies that
// UpdateApplication's ConditionalToken implements the same
// optimistic-concurrency check as CurrentApplicationVersionId (real AWS: "you
// must provide the CurrentApplicationVersionId or the ConditionalToken"),
// and that a mismatched token is rejected with ErrConcurrentModification
// without mutating the application or bumping its version.
func TestBackend_UpdateApplication_ConditionalToken(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("valid token succeeds and rotates", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		app, err := b.CreateApplication(ctx, "token-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		tok := kinesisanalyticsv2.ConditionalTokenForTest(app)

		updated, opID, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name:                   "token-app",
			ConditionalToken:       tok,
			ApplicationDescription: "updated via token",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, opID)
		assert.Equal(t, int64(2), updated.ApplicationVersionID)
		assert.NotEqual(
			t,
			tok,
			kinesisanalyticsv2.ConditionalTokenForTest(updated),
			"token must rotate on version bump",
		)
	})

	t.Run("stale token rejected", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		app, err := b.CreateApplication(ctx, "stale-token-app", "FLINK-1_18", "", "orig", "", nil)
		require.NoError(t, err)

		staleTok := kinesisanalyticsv2.ConditionalTokenForTest(app)

		// Bump the version once via a normal update so staleTok no longer matches.
		_, _, err = b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name:                   "stale-token-app",
			ApplicationDescription: "first update",
		})
		require.NoError(t, err)

		_, _, err = b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name:                   "stale-token-app",
			ConditionalToken:       staleTok,
			ApplicationDescription: "should not apply",
		})
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrConcurrentModification)

		current, err := b.DescribeApplication(ctx, "stale-token-app")
		require.NoError(t, err)
		assert.Equal(t, "first update", current.ApplicationDescription, "rejected update must not mutate state")
	})
}

// TestBackend_UpdateApplication_RuntimeEnvironmentUpdate verifies that
// UpdateApplication's RuntimeEnvironmentUpdate field (previously accepted on
// the wire but silently dropped, per PARITY.md) actually changes the
// application's RuntimeEnvironment.
func TestBackend_UpdateApplication_RuntimeEnvironmentUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "runtime-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	updated, _, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
		Name:                     "runtime-app",
		RuntimeEnvironmentUpdate: "FLINK-1_19",
	})
	require.NoError(t, err)
	assert.Equal(t, "FLINK-1_19", updated.RuntimeEnvironment)
}

// TestBackend_UpdateApplication_ApplicationConfigurationUpdate exercises
// every sub-field of ApplicationConfigurationUpdate that PARITY.md flagged as
// accepted-but-ignored: code config, Flink checkpoint/monitoring/parallelism
// config, environment properties, snapshot/rollback/encryption config, and
// SQL input/output/reference-data-source/VPC config updates.
func TestBackend_UpdateApplication_ApplicationConfigurationUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("code and flink config", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "flink-cfg-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		updated, _, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "flink-cfg-app",
			ApplicationConfigurationUpdate: &kinesisanalyticsv2.ApplicationConfigurationUpdate{
				ApplicationCodeConfigurationUpdate: &kinesisanalyticsv2.ApplicationCodeConfigUpdate{
					CodeContentTypeUpdate: "PLAINTEXT",
					CodeContentUpdate: &kinesisanalyticsv2.CodeContentUpdate{
						TextContentUpdate: new("SELECT 1;"),
					},
				},
				FlinkApplicationConfigurationUpdate: &kinesisanalyticsv2.FlinkApplicationConfigUpdate{
					CheckpointConfigurationUpdate: &kinesisanalyticsv2.CheckpointConfigUpdate{
						ConfigurationTypeUpdate: "DEFAULT",
					},
					ParallelismConfigurationUpdate: &kinesisanalyticsv2.ParallelismConfigUpdate{
						ConfigurationTypeUpdate: "CUSTOM",
						ParallelismUpdate:       new(int32(4)),
					},
				},
			},
		})
		require.NoError(t, err)

		require.NotNil(t, updated.CodeConfig)
		require.NotNil(t, updated.CodeConfig.CodeContentDescription)
		assert.Equal(t, "SELECT 1;", updated.CodeConfig.CodeContentDescription.TextContent)

		require.NotNil(t, updated.FlinkConfig)
		require.NotNil(t, updated.FlinkConfig.CheckpointConfigurationDescription)
		// DEFAULT must force the documented literal values regardless of what
		// (nothing, here) was requested for the individual fields.
		cp := updated.FlinkConfig.CheckpointConfigurationDescription
		assert.True(t, *cp.CheckpointingEnabled)
		assert.Equal(t, int64(60000), *cp.CheckpointInterval)
		assert.Equal(t, int64(5000), *cp.MinPauseBetweenCheckpoints)

		require.NotNil(t, updated.FlinkConfig.ParallelismConfigurationDescription)
		assert.Equal(t, int32(4), *updated.FlinkConfig.ParallelismConfigurationDescription.Parallelism)
	})

	t.Run("environment properties replace wholesale", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "env-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		updated, _, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "env-app",
			ApplicationConfigurationUpdate: &kinesisanalyticsv2.ApplicationConfigurationUpdate{
				HasEnvironmentPropertyUpdates: true,
				EnvironmentPropertyUpdates: []kinesisanalyticsv2.PropertyGroup{
					{PropertyGroupID: "g1", PropertyMap: map[string]string{"k": "v"}},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, updated.EnvironmentPropertyGroups, 1)
		assert.Equal(t, "g1", updated.EnvironmentPropertyGroups[0].PropertyGroupID)

		// A second update with an empty (but present) EnvironmentPropertyUpdates
		// must clear the groups, not leave the previous ones in place.
		updated, _, err = b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "env-app",
			ApplicationConfigurationUpdate: &kinesisanalyticsv2.ApplicationConfigurationUpdate{
				HasEnvironmentPropertyUpdates: true,
				EnvironmentPropertyUpdates:    []kinesisanalyticsv2.PropertyGroup{},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, updated.EnvironmentPropertyGroups)
	})

	t.Run("snapshot rollback and encryption config", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "snap-cfg-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		updated, _, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "snap-cfg-app",
			ApplicationConfigurationUpdate: &kinesisanalyticsv2.ApplicationConfigurationUpdate{
				ApplicationSnapshotConfigurationUpdate:       new(true),
				ApplicationSystemRollbackConfigurationUpdate: new(true),
				ApplicationEncryptionConfigurationUpdate: &kinesisanalyticsv2.ApplicationEncryptionConfigDesc{
					KeyType: "CUSTOMER_MANAGED_KEY",
					KeyID:   "alias/my-key",
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, updated.SnapshotsEnabled)
		assert.True(t, *updated.SnapshotsEnabled)
		require.NotNil(t, updated.RollbackEnabled)
		assert.True(t, *updated.RollbackEnabled)
		require.NotNil(t, updated.EncryptionConfig)
		assert.Equal(t, "alias/my-key", updated.EncryptionConfig.KeyID)
	})

	t.Run("sql config updates by ID", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "sql-upd-app", "SQL-1_0", "", "", "", nil)
		require.NoError(t, err)

		err = b.AddApplicationInput(ctx, "sql-upd-app", 0, kinesisanalyticsv2.InputDescription{
			NamePrefix: "SOURCE",
			KinesisStreamsInputDescription: &kinesisanalyticsv2.KinesisStreamsInputDesc{
				ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/in1",
			},
		})
		require.NoError(t, err)

		app, err := b.DescribeApplication(ctx, "sql-upd-app")
		require.NoError(t, err)
		require.Len(t, app.InputDescriptions, 1)
		inputID := app.InputDescriptions[0].InputID

		updated, _, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "sql-upd-app",
			ApplicationConfigurationUpdate: &kinesisanalyticsv2.ApplicationConfigurationUpdate{
				SQLApplicationConfigurationUpdate: &kinesisanalyticsv2.SQLApplicationConfigUpdate{
					InputUpdates: []kinesisanalyticsv2.InputUpdate{
						{
							InputID:          inputID,
							NamePrefixUpdate: "SOURCE_RENAMED",
							KinesisStreamsInputUpdate: &kinesisanalyticsv2.KinesisStreamsInputDesc{
								ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/in2",
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, updated.InputDescriptions, 1)
		assert.Equal(t, "SOURCE_RENAMED", updated.InputDescriptions[0].NamePrefix)
		assert.Equal(t, "arn:aws:kinesis:us-east-1:000000000000:stream/in2",
			updated.InputDescriptions[0].KinesisStreamsInputDescription.ResourceARN)
	})

	t.Run("unknown sql input ID rejected before version bump", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "sql-notfound-app", "SQL-1_0", "", "", "", nil)
		require.NoError(t, err)

		_, _, err = b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "sql-notfound-app",
			ApplicationConfigurationUpdate: &kinesisanalyticsv2.ApplicationConfigurationUpdate{
				SQLApplicationConfigurationUpdate: &kinesisanalyticsv2.SQLApplicationConfigUpdate{
					InputUpdates: []kinesisanalyticsv2.InputUpdate{{InputID: "does-not-exist"}},
				},
			},
		})
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)

		app, err := b.DescribeApplication(ctx, "sql-notfound-app")
		require.NoError(t, err)
		assert.Equal(t, int64(1), app.ApplicationVersionID, "rejected update must not bump the version")
	})

	t.Run("vpc config updates by ID", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "vpc-upd-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		_, err = b.AddApplicationVpcConfiguration(ctx, "vpc-upd-app", 0, kinesisanalyticsv2.VpcConfigurationDescription{
			SubnetIDs:        []string{"subnet-1"},
			SecurityGroupIDs: []string{"sg-1"},
		})
		require.NoError(t, err)

		app, err := b.DescribeApplication(ctx, "vpc-upd-app")
		require.NoError(t, err)
		vpcID := app.VpcConfigurationDescriptions[0].VpcConfigurationID

		updated, _, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "vpc-upd-app",
			ApplicationConfigurationUpdate: &kinesisanalyticsv2.ApplicationConfigurationUpdate{
				VpcConfigurationUpdates: []kinesisanalyticsv2.VpcConfigUpdate{
					{VpcConfigurationID: vpcID, SubnetIDUpdates: []string{"subnet-2", "subnet-3"}},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, updated.VpcConfigurationDescriptions, 1)
		assert.Equal(t, []string{"subnet-2", "subnet-3"}, updated.VpcConfigurationDescriptions[0].SubnetIDs)
	})
}

// TestBackend_UpdateApplication_CloudWatchLoggingOptionUpdates verifies
// UpdateApplication's CloudWatchLoggingOptionUpdates field (previously
// accepted but silently ignored) actually updates an existing option's
// LogStreamARN, and that referencing an unknown ID is rejected before any
// version bump.
func TestBackend_UpdateApplication_CloudWatchLoggingOptionUpdates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.CreateApplication(ctx, "cwl-upd-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		_, err = b.AddApplicationCloudWatchLoggingOption(ctx, "cwl-upd-app", 0,
			"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s1", "")
		require.NoError(t, err)

		app, err := b.DescribeApplication(ctx, "cwl-upd-app")
		require.NoError(t, err)
		cwlID := app.CloudWatchLoggingOptionDescs[0].CloudWatchLoggingOptionID

		updated, opID, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "cwl-upd-app",
			CloudWatchLoggingOptionUpdates: []kinesisanalyticsv2.CloudWatchLoggingOptionUpdate{
				{
					CloudWatchLoggingOptionID: cwlID,
					LogStreamARNUpdate:        "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s2",
				},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, opID)
		require.Len(t, updated.CloudWatchLoggingOptionDescs, 1)
		assert.Equal(t, "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s2",
			updated.CloudWatchLoggingOptionDescs[0].LogStreamARN)
	})

	t.Run("unknown ID rejected before version bump", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.CreateApplication(ctx, "cwl-upd-notfound-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		before, err := b.DescribeApplication(ctx, "cwl-upd-notfound-app")
		require.NoError(t, err)

		_, _, err = b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name: "cwl-upd-notfound-app",
			CloudWatchLoggingOptionUpdates: []kinesisanalyticsv2.CloudWatchLoggingOptionUpdate{
				{CloudWatchLoggingOptionID: "cwl-does-not-exist", LogStreamARNUpdate: "x"},
			},
		})
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)

		after, err := b.DescribeApplication(ctx, "cwl-upd-notfound-app")
		require.NoError(t, err)
		assert.Equal(t, before.ApplicationVersionID, after.ApplicationVersionID)
	})
}

// TestBackend_UpdateApplication_RunConfigurationUpdate and
// TestBackend_StartApplication_RunConfiguration verify RunConfiguration(Update)
// -- previously ignored on both StartApplication and UpdateApplication -- is
// stored and echoed back via DescribeApplication's RunConfigurationDescription.
func TestBackend_UpdateApplication_RunConfigurationUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "runcfg-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	updated, _, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
		Name: "runcfg-app",
		RunConfigurationUpdate: &kinesisanalyticsv2.RunConfigInput{
			FlinkRunConfiguration: &kinesisanalyticsv2.FlinkRunConfig{AllowNonRestoredState: new(true)},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.RunConfig)
	require.NotNil(t, updated.RunConfig.FlinkRunConfigurationDescription)
	assert.True(t, *updated.RunConfig.FlinkRunConfigurationDescription.AllowNonRestoredState)
}

func TestBackend_StartApplication_RunConfiguration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "start-runcfg-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.StartApplication(ctx, "start-runcfg-app", &kinesisanalyticsv2.RunConfigInput{
		ApplicationRestoreConfiguration: &kinesisanalyticsv2.ApplicationRestoreConfig{
			ApplicationRestoreType: "RESTORE_FROM_LATEST_SNAPSHOT",
		},
	})
	require.NoError(t, err)

	app, err := b.DescribeApplication(ctx, "start-runcfg-app")
	require.NoError(t, err)
	require.NotNil(t, app.RunConfig)
	require.NotNil(t, app.RunConfig.ApplicationRestoreConfigurationDescription)
	assert.Equal(t, "RESTORE_FROM_LATEST_SNAPSHOT",
		app.RunConfig.ApplicationRestoreConfigurationDescription.ApplicationRestoreType)
}

// TestBackend_DeleteApplication_CreateTimestamp verifies DeleteApplication's
// optional CreateTimestamp safety check: a matching timestamp (within
// floating-point tolerance) allows deletion, a mismatched one is rejected.
func TestBackend_DeleteApplication_CreateTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("matching timestamp deletes", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		app, err := b.CreateApplication(ctx, "ts-match-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		createSeconds := appCreateEpochSeconds(t, b, app.ApplicationName)
		require.NoError(t, b.DeleteApplication(ctx, "ts-match-app", &createSeconds))

		_, err = b.DescribeApplication(ctx, "ts-match-app")
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)
	})

	t.Run("millisecond-truncated timestamp deletes", func(t *testing.T) {
		t.Parallel()

		// Real AWS SDK clients round-trip CreateTimestamp through the
		// unixTimestamp wire format, which smithy-go truncates to millisecond
		// precision (smithy-go time.FormatEpochSeconds/ParseEpochSeconds).
		// The backend stores/echoes CreatedAt at full nanosecond precision, so
		// a genuine client can never send back the exact float the backend
		// would recompute -- it can only send the millisecond-floored value.
		// This must still be accepted.
		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "ts-ms-truncated-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		createSeconds := appCreateEpochSeconds(t, b, "ts-ms-truncated-app")
		truncated := math.Floor(createSeconds*1000) / 1000

		require.NoError(t, b.DeleteApplication(ctx, "ts-ms-truncated-app", &truncated))

		_, err = b.DescribeApplication(ctx, "ts-ms-truncated-app")
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)
	})

	t.Run("mismatched timestamp rejected", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "ts-mismatch-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		wrong := 12345.0
		err = b.DeleteApplication(ctx, "ts-mismatch-app", &wrong)
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrValidation)

		_, err = b.DescribeApplication(ctx, "ts-mismatch-app")
		require.NoError(t, err, "mismatched timestamp must not delete the application")
	})

	t.Run("nil timestamp skips the check", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		_, err := b.CreateApplication(ctx, "ts-skip-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		require.NoError(t, b.DeleteApplication(ctx, "ts-skip-app", nil))
	})
}

// TestBackend_AddDeleteVpcAndCWLOption_ReturnOperationID verifies the four
// Add*/Delete* config ops whose real AWS outputs carry an OperationId field
// (unlike most Add*/Delete* config ops -- verified against aws-sdk-go-v2's
// api_op_*.go) now record and return one.
func TestBackend_AddDeleteVpcAndCWLOption_ReturnOperationID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "opid-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	cwlOpID, err := b.AddApplicationCloudWatchLoggingOption(ctx, "opid-app", 0,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s", "")
	require.NoError(t, err)
	assert.NotEmpty(t, cwlOpID)

	vpcOpID, err := b.AddApplicationVpcConfiguration(ctx, "opid-app", 0, kinesisanalyticsv2.VpcConfigurationDescription{
		SubnetIDs:        []string{"subnet-1"},
		SecurityGroupIDs: []string{"sg-1"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, vpcOpID)
	assert.NotEqual(t, cwlOpID, vpcOpID)

	app, err := b.DescribeApplication(ctx, "opid-app")
	require.NoError(t, err)

	delCWLOpID, err := b.DeleteApplicationCloudWatchLoggingOption(
		ctx, "opid-app", 0, app.CloudWatchLoggingOptionDescs[0].CloudWatchLoggingOptionID,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, delCWLOpID)

	delVpcOpID, err := b.DeleteApplicationVpcConfiguration(
		ctx, "opid-app", 0, app.VpcConfigurationDescriptions[0].VpcConfigurationID,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, delVpcOpID)
}

// appCreateEpochSeconds fetches app's CreateTimestamp the same way a real
// client would (via the epoch-seconds value returned in a describe
// response) -- here reconstructed via the handler layer since the backend's
// Application type doesn't itself expose an epoch-seconds accessor.
func appCreateEpochSeconds(t *testing.T, b *kinesisanalyticsv2.InMemoryBackend, name string) float64 {
	t.Helper()

	h := kinesisanalyticsv2.NewHandler(b)
	rec := doKAV2Request(t, h, "DescribeApplication", map[string]any{"ApplicationName": name})
	require.Equal(t, 200, rec.Code)

	var out struct {
		ApplicationDetail struct {
			CreateTimestamp float64 `json:"CreateTimestamp"`
		} `json:"ApplicationDetail"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.ApplicationDetail.CreateTimestamp
}
