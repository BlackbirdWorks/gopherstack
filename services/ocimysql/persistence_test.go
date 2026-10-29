package ocimysql_test

import (
	"net/http"
	"testing"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ocimysql "github.com/blackbirdworks/gopherstack/services/ocimysql"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	db := e.mkDB(t, "persist", func(d *my.CreateDbSystemDetails) {
		d.FreeformTags = map[string]string{"k": "v"}
		d.DeletionPolicy = &my.CreateDeletionPolicyDetails{IsDeleteProtected: new(true)}
	})
	bk := e.mkBackup(t, db.Id, "snap")

	cfg, err := e.aas.CreateConfiguration(ctx, my.CreateConfigurationRequest{
		CompartmentId: new(
			testCompartment,
		),
		ShapeName: new(shapeSmall),
		Variables: &my.ConfigurationVariables{MaxConnections: new(321)},
	})
	require.NoError(t, err)

	ch, err := e.ch.CreateChannel(ctx, my.CreateChannelRequest{CreateChannelDetails: channelDetails(db.Id)})
	require.NoError(t, err)
	e.settle()

	snap := e.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)
	assert.NotContains(t, string(snap), "S0urce#pass", "channel passwords are not stored")

	restored := newEnv(t, envOpts{})
	require.NoError(t, restored.backend.Restore(ctx, snap))
	restored.clock.ns.Store(e.clock.ns.Load())

	got := restored.getDB(t, db.Id)
	assert.Equal(t, "persist", *got.DisplayName)
	assert.Equal(t, my.DbSystemLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, *db.IpAddress, *got.IpAddress)
	assert.Equal(t, "v", got.FreeformTags["k"])
	assert.True(t, *got.DeletionPolicy.IsDeleteProtected)
	assert.Len(t, got.Channels, 1)

	gb, err := restored.bk.GetBackup(ctx, my.GetBackupRequest{BackupId: bk.Id})
	require.NoError(t, err)
	assert.Equal(t, adminUser, *gb.DbSystemSnapshot.AdminUsername)

	gc, err := restored.aas.GetConfiguration(ctx, my.GetConfigurationRequest{ConfigurationId: cfg.Id})
	require.NoError(t, err)
	assert.Equal(t, 321, *gc.Variables.MaxConnections)

	gch, err := restored.ch.GetChannel(ctx, my.GetChannelRequest{ChannelId: ch.Id})
	require.NoError(t, err)
	assert.Equal(t, my.ChannelLifecycleStateActive, gch.LifecycleState)

	wr, err := restored.wr.GetWorkRequest(
		ctx,
		my.GetWorkRequestRequest{WorkRequestId: new(workRequestOf(t, restored, db.Id))},
	)
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationStatusSucceeded, wr.Status)

	fromBackup := restored.mkDB(t, "again", func(d *my.CreateDbSystemDetails) {
		d.AdminUsername, d.AdminPassword = nil, nil
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: bk.Id}
	})
	assert.Equal(
		t,
		my.DbSystemLifecycleStateActive,
		fromBackup.LifecycleState,
		"admin credentials survive the round trip",
	)

	restored.backend.Reset()

	_, err = restored.db.GetDbSystem(ctx, my.GetDbSystemRequest{DbSystemId: db.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func workRequestOf(t *testing.T, e *env, dbID *string) string {
	t.Helper()

	lst, err := e.wr.ListWorkRequests(t.Context(), my.ListWorkRequestsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)

	for _, w := range lst.Items {
		if w.OperationType == my.WorkRequestOperationTypeCreateDbsystem {
			return *w.Id
		}
	}

	require.Fail(t, "no CREATE_DBSYSTEM work request for "+*dbID)

	return ""
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"null db system", `{"version":1,"dbSystems":{"a":null}}`, true},
		{"null backup", `{"version":1,"backups":{"a":null}}`, true},
		{"null channel", `{"version":1,"channels":{"a":null}}`, true},
		{"null configuration", `{"version":1,"configurations":{"a":null}}`, true},
		{"null work request", `{"version":1,"workRequests":{"a":null}}`, true},
		{"old version starts empty", `{"version":99}`, false},
		{"empty maps initialised", `{"version":1}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ocimysql.NewInMemoryBackend()
			err := b.Restore(t.Context(), []byte(tt.data))

			if tt.wantErr {
				require.ErrorIs(t, err, ocimysql.ErrSnapshotNull)

				return
			}

			require.NoError(t, err)

			_, _, err = b.GetDBSystem("ocid1.mysqldbsystem.oc1.iad.x")
			require.Error(t, err)
		})
	}
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	_, err := (&ocimysql.Provider{}).Init(nil)
	require.ErrorIs(t, err, ocimysql.ErrNilAppContext)

	assert.Equal(t, "OCIMySQL", (&ocimysql.Provider{}).Name())
	assert.Equal(t, ocimysql.DefaultPort, ocimysql.DefaultSettings().Port)
}
