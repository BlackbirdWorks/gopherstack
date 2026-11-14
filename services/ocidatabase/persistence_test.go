package ocidatabase_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svc "github.com/blackbirdworks/gopherstack/services/ocidatabase"
)

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true})
	db := e.available(t, createOpts{token: "tok"})
	bk := e.manualBackup(t, db.Id)

	_, err := e.client.StopAutonomousDatabase(
		t.Context(),
		database.StopAutonomousDatabaseRequest{AutonomousDatabaseId: db.Id},
	)
	require.NoError(t, err)

	snap := e.backend.Snapshot(t.Context())
	assert.NotContains(t, string(snap), testPassword)

	e2 := newEnv(t, envOpts{})
	require.NoError(t, e2.backend.Restore(t.Context(), snap))

	got := e2.get(t, db.Id)
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateStopping, got.LifecycleState)
	assert.Equal(t, "mydb", *got.DbName)
	assert.Equal(t, *db.TimeCreated, *got.TimeCreated)

	e2.clock.Advance(time.Minute)
	e2.backend.Sweep()
	assert.Equal(t, database.AutonomousDatabaseLifecycleStateStopped, e2.get(t, db.Id).LifecycleState,
		"pending transitions resume on the clock")

	back, err := e2.client.GetAutonomousDatabaseBackup(
		t.Context(),
		database.GetAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: bk.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "manual", *back.DisplayName)

	again, err := e2.create(t, createOpts{token: "tok"})
	require.NoError(t, err)
	assert.Equal(t, *db.Id, *again.Id, "retry tokens survive")

	_, err = e2.update(t, db.Id, database.UpdateAutonomousDatabaseDetails{DisplayName: new("after")})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestRestoreRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
		wantNew bool
	}{
		{name: "null-database", data: `{"version":1,"databases":{"a":null}}`, wantErr: true},
		{name: "null-backup", data: `{"version":1,"backups":{"a":null}}`, wantErr: true},
		{name: "null-work-request", data: `{"version":1,"workRequests":{"a":null}}`, wantErr: true},
		{name: "future-version", data: `{"version":99,"databases":{"a":{"id":"a"}}}`, wantNew: true},
		{name: "empty", data: `{"version":1}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := svc.NewInMemoryBackend()
			snap := []byte(tc.data)

			err := b.Restore(t.Context(), snap)
			if tc.wantErr {
				require.ErrorIs(t, err, svc.ErrSnapshotNull)

				return
			}

			require.NoError(t, err)

			_, _, getErr := b.GetDatabase("a")
			require.Error(t, getErr)
		})
	}
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{})

	e.handler.Reset()

	_, err := e.client.GetAutonomousDatabase(
		t.Context(),
		database.GetAutonomousDatabaseRequest{AutonomousDatabaseId: db.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.create(t, createOpts{})
	require.NoError(t, err, "the dbName is free again")
}

func TestAdminKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})

	_, err := e.create(t, createOpts{})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode(), "unregistered key under enforcement")

	_, err = e.backend.Auth.Register(testTenancy, testUser, e.kp.pubPEM)
	require.NoError(t, err)

	_, err = e.create(t, createOpts{})
	require.NoError(t, err)
}
