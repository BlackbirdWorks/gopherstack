package lightsail_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

const (
	dbTestPassword = "Sup3rSecret!"
	// synctest's clock starts Sat 2000-01-01 00:00 UTC; the default window opens Sun 08:00.
	pastDefaultMaintenance = 33 * time.Hour
)

func newDBTestBackend(t *testing.T) *lightsail.InMemoryBackend {
	t.Helper()

	b := lightsail.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateRelationalDatabase(
		"src-db", "appdb", "admin", dbTestPassword, "mysql_8_0", "small_2_0", "", "", "", false, nil,
	)
	require.NoError(t, err)

	return b
}

func TestUpdateRelationalDatabase_DeferredToMaintenanceWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantPassword string
		req          lightsail.UpdateDatabaseRequest
		wantPending  bool
	}{
		{
			name:        "password_deferred",
			req:         lightsail.UpdateDatabaseRequest{MasterUserPassword: "N3wPassw0rd"},
			wantPending: true, wantPassword: "N3wPassw0rd",
		},
		{
			name:         "password_immediate",
			req:          lightsail.UpdateDatabaseRequest{MasterUserPassword: "N3wPassw0rd", ApplyImmediately: true},
			wantPassword: "N3wPassw0rd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newDBTestBackend(t)
				tt.req.Name = "src-db"

				_, err := b.UpdateRelationalDatabase(&tt.req)
				require.NoError(t, err)

				db, err := b.GetRelationalDatabase("src-db")
				require.NoError(t, err)
				assert.Equal(t, tt.wantPending, db.Pending != nil)

				current, _, err := b.GetRelationalDatabaseMasterUserPassword("src-db", lightsail.PasswordVersionCurrent)
				require.NoError(t, err)

				if tt.wantPending {
					assert.Equal(t, dbTestPassword, current)

					pending, _, pendErr := b.GetRelationalDatabaseMasterUserPassword(
						"src-db", lightsail.PasswordVersionPending,
					)
					require.NoError(t, pendErr)
					assert.Equal(t, tt.wantPassword, pending)
				} else {
					assert.Equal(t, tt.wantPassword, current)
				}

				time.Sleep(pastDefaultMaintenance)

				current, _, err = b.GetRelationalDatabaseMasterUserPassword("src-db", lightsail.PasswordVersionCurrent)
				require.NoError(t, err)
				assert.Equal(t, tt.wantPassword, current)

				previous, _, err := b.GetRelationalDatabaseMasterUserPassword(
					"src-db",
					lightsail.PasswordVersionPrevious,
				)
				require.NoError(t, err)
				assert.Equal(t, dbTestPassword, previous)

				_, _, err = b.GetRelationalDatabaseMasterUserPassword("src-db", lightsail.PasswordVersionPending)
				require.Error(t, err, "pending password is gone once promoted")
			})
		})
	}
}

func TestUpdateRelationalDatabase_BackupRetentionWaitsForWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newDBTestBackend(t)

		off := true
		_, err := b.UpdateRelationalDatabase(&lightsail.UpdateDatabaseRequest{
			Name: "src-db", DisableBackupRetention: &off, ApplyImmediately: true,
		})
		require.NoError(t, err)

		db, err := b.GetRelationalDatabase("src-db")
		require.NoError(t, err)
		assert.True(t, db.BackupRetentionEnabled, "backup retention changes always wait for the window")
		require.NotNil(t, db.Pending)
		require.NotNil(t, db.Pending.BackupRetentionEnabled)
		assert.False(t, *db.Pending.BackupRetentionEnabled)

		time.Sleep(pastDefaultMaintenance)

		db, err = b.GetRelationalDatabase("src-db")
		require.NoError(t, err)
		assert.False(t, db.BackupRetentionEnabled)
		assert.Nil(t, db.Pending)
	})
}

func TestUpdateRelationalDatabase_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  lightsail.UpdateDatabaseRequest
	}{
		{name: "short_password", req: lightsail.UpdateDatabaseRequest{MasterUserPassword: "short"}},
		{name: "slash_in_password", req: lightsail.UpdateDatabaseRequest{MasterUserPassword: "has/slash123"}},
		{name: "bad_backup_window", req: lightsail.UpdateDatabaseRequest{PreferredBackupWindow: "5am-6am"}},
		{name: "short_backup_window", req: lightsail.UpdateDatabaseRequest{PreferredBackupWindow: "05:00-05:10"}},
		{
			name: "conflicting_windows",
			req:  lightsail.UpdateDatabaseRequest{PreferredBackupWindow: "08:00-09:00"},
		},
		{name: "bad_maintenance_window", req: lightsail.UpdateDatabaseRequest{PreferredMaintenance: "sun:08:00"}},
		{name: "unknown_blueprint", req: lightsail.UpdateDatabaseRequest{BlueprintID: "mysql_9_9"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newDBTestBackend(t)
			tt.req.Name = "src-db"

			_, err := b.UpdateRelationalDatabase(&tt.req)
			require.Error(t, err)
		})
	}
}

func TestCreateRelationalDatabaseFromSnapshot_PointInTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		req        lightsail.RestoreDatabaseRequest
		wait       time.Duration
		restoreAgo time.Duration
		wantErr    bool
		disable    bool
	}{
		{name: "latest", req: lightsail.RestoreDatabaseRequest{SourceName: "src-db", UseLatestRestorableTime: true}},
		{
			name: "restore_time_ok", req: lightsail.RestoreDatabaseRequest{SourceName: "src-db"},
			wait: time.Hour, restoreAgo: 30 * time.Minute,
		},
		{
			name: "restore_time_before_created", req: lightsail.RestoreDatabaseRequest{SourceName: "src-db"},
			wait: time.Hour, restoreAgo: 2 * time.Hour, wantErr: true,
		},
		{
			name: "restore_time_future", req: lightsail.RestoreDatabaseRequest{SourceName: "src-db"},
			restoreAgo: -time.Hour, wantErr: true,
		},
		{
			name: "time_and_latest",
			req: lightsail.RestoreDatabaseRequest{
				SourceName: "src-db", UseLatestRestorableTime: true,
			},
			restoreAgo: time.Minute, wantErr: true,
		},
		{name: "neither", req: lightsail.RestoreDatabaseRequest{SourceName: "src-db"}, wantErr: true},
		{name: "no_source", req: lightsail.RestoreDatabaseRequest{UseLatestRestorableTime: true}, wantErr: true},
		{
			name:    "unknown_source",
			req:     lightsail.RestoreDatabaseRequest{SourceName: "nope", UseLatestRestorableTime: true},
			wantErr: true,
		},
		{
			name: "smaller_bundle",
			req: lightsail.RestoreDatabaseRequest{
				SourceName:              "src-db",
				UseLatestRestorableTime: true,
				BundleID:                "micro_2_0",
			},
			wantErr: true,
		},
		{
			name:    "backups_disabled",
			req:     lightsail.RestoreDatabaseRequest{SourceName: "src-db", UseLatestRestorableTime: true},
			wantErr: true, disable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newDBTestBackend(t)

				if tt.disable {
					off := true
					_, err := b.UpdateRelationalDatabase(&lightsail.UpdateDatabaseRequest{
						Name: "src-db", DisableBackupRetention: &off,
					})
					require.NoError(t, err)

					time.Sleep(pastDefaultMaintenance)
				}

				time.Sleep(tt.wait)

				if tt.restoreAgo != 0 {
					tt.req.RestoreTime = aws.Time(time.Now().Add(-tt.restoreAgo))
				}

				tt.req.Name = "restored-db"

				_, err := b.CreateRelationalDatabaseFromSnapshot(&tt.req)
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)

				restored, err := b.GetRelationalDatabase("restored-db")
				require.NoError(t, err)
				assert.Equal(t, "admin", restored.MasterUsername)
				assert.Equal(t, "appdb", restored.MasterDatabaseName)
				assert.Equal(t, "small_2_0", restored.BundleID)
			})
		})
	}
}

func TestUpdateRelationalDatabase_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateRelationalDatabase(ctx, &lightsailsdk.CreateRelationalDatabaseInput{
		RelationalDatabaseName:        aws.String("sdk-pend-db"),
		MasterDatabaseName:            aws.String("appdb"),
		MasterUsername:                aws.String("dbadmin"),
		RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
		RelationalDatabaseBundleId:    aws.String("micro_2_0"),
		PubliclyAccessible:            aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.UpdateRelationalDatabase(ctx, &lightsailsdk.UpdateRelationalDatabaseInput{
		RelationalDatabaseName:   aws.String("sdk-pend-db"),
		RotateMasterUserPassword: aws.Bool(true),
	})
	require.NoError(t, err)

	got, err := client.GetRelationalDatabase(ctx, &lightsailsdk.GetRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("sdk-pend-db"),
	})
	require.NoError(t, err)
	assert.True(
		t,
		aws.ToBool(got.RelationalDatabase.PubliclyAccessible),
		"omitted PubliclyAccessible leaves it unchanged",
	)
	require.NotNil(t, got.RelationalDatabase.PendingModifiedValues)
	assert.NotEmpty(t, aws.ToString(got.RelationalDatabase.PendingModifiedValues.MasterUserPassword))

	pending, err := client.GetRelationalDatabaseMasterUserPassword(
		ctx,
		&lightsailsdk.GetRelationalDatabaseMasterUserPasswordInput{
			RelationalDatabaseName: aws.String("sdk-pend-db"),
			PasswordVersion:        lightsailtypes.RelationalDatabasePasswordVersionPending,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(got.RelationalDatabase.PendingModifiedValues.MasterUserPassword),
		aws.ToString(pending.MasterUserPassword))
}

func TestCreateRelationalDatabaseFromSnapshot_SDKPointInTime(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateRelationalDatabase(ctx, &lightsailsdk.CreateRelationalDatabaseInput{
		RelationalDatabaseName:        aws.String("pitr-src"),
		MasterDatabaseName:            aws.String("appdb"),
		MasterUsername:                aws.String("dbadmin"),
		RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
		RelationalDatabaseBundleId:    aws.String("micro_2_0"),
	})
	require.NoError(t, err)

	_, err = client.CreateRelationalDatabaseFromSnapshot(ctx, &lightsailsdk.CreateRelationalDatabaseFromSnapshotInput{
		RelationalDatabaseName:       aws.String("pitr-copy"),
		SourceRelationalDatabaseName: aws.String("pitr-src"),
		UseLatestRestorableTime:      aws.Bool(true),
	})
	require.NoError(t, err)

	got, err := client.GetRelationalDatabase(ctx, &lightsailsdk.GetRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("pitr-copy"),
	})
	require.NoError(t, err)
	assert.Equal(t, "dbadmin", aws.ToString(got.RelationalDatabase.MasterUsername))

	_, err = client.CreateRelationalDatabaseFromSnapshot(ctx, &lightsailsdk.CreateRelationalDatabaseFromSnapshotInput{
		RelationalDatabaseName:       aws.String("pitr-bad"),
		SourceRelationalDatabaseName: aws.String("pitr-src"),
		RestoreTime:                  aws.Time(time.Now().Add(-48 * time.Hour)),
	})
	require.Error(t, err)
}
