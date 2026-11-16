package ocimysql_test

import (
	"slices"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineServerSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(*my.CreateDbSystemDetails)
		name string
		want []string
		miss []string
	}{
		{
			name: "read only", mod: func(d *my.CreateDbSystemDetails) { d.DatabaseMode = my.DbSystemDatabaseModeOnly },
			want: []string{"SET GLOBAL read_only = ON", "SET GLOBAL super_read_only = ON"},
		},
		{
			name: "restricted",
			mod:  func(d *my.CreateDbSystemDetails) { d.AccessMode = my.DbSystemAccessModeRestricted },
			want: []string{"SET GLOBAL offline_mode = ON"},
		},
		{
			name: "crash recovery off",
			mod:  func(d *my.CreateDbSystemDetails) { d.CrashRecovery = my.CrashRecoveryStatusDisabled },
			want: []string{
				"ALTER INSTANCE DISABLE INNODB REDO_LOG",
				"SET PERSIST innodb_doublewrite = OFF",
				"SET PERSIST sync_binlog = 0",
			},
		},
		{name: "defaults", miss: []string{"SET GLOBAL read_only = ON", "ALTER INSTANCE DISABLE INNODB REDO_LOG"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, l := newEngineEnv(t)
			db := e.mkEngineDB(t, "settings", tt.mod)

			qs := l.queriesOf(*db.Id)
			for _, w := range tt.want {
				assert.Contains(t, qs, w)
			}

			for _, m := range tt.miss {
				assert.NotContains(t, qs, m)
			}
		})
	}
}

func TestEngineSettingsUpdate(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	db := e.mkEngineDB(t, "upd", nil)

	_, err := e.db.UpdateDbSystem(t.Context(), my.UpdateDbSystemRequest{
		DbSystemId:   db.Id,
		DatabaseMode: my.DbSystemDatabaseModeOnly,
		AccessMode:   my.DbSystemAccessModeRestricted,
	})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	require.Eventually(t, func() bool {
		return slices.Contains(l.queriesOf(*db.Id), "SET GLOBAL super_read_only = ON")
	}, e.waitFor(), 20*time.Millisecond)
	assert.Contains(t, l.queriesOf(*db.Id), "SET GLOBAL offline_mode = ON")
}

func TestEngineShutdownType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode my.InnoDbShutdownModeEnum
		want string
	}{
		{my.InnoDbShutdownModeSlow, "SET GLOBAL innodb_fast_shutdown = 0"},
		{my.InnoDbShutdownModeFast, "SET GLOBAL innodb_fast_shutdown = 1"},
		{my.InnoDbShutdownModeImmediate, "SET GLOBAL innodb_fast_shutdown = 2"},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			t.Parallel()

			e, l := newEngineEnv(t)
			db := e.mkEngineDB(t, "stop", nil)

			_, err := e.db.StopDbSystem(t.Context(), my.StopDbSystemRequest{DbSystemId: db.Id, ShutdownType: tt.mode})
			require.NoError(t, err)
			e.awaitState(t, db.Id, my.DbSystemLifecycleStateInactive)
			assert.Contains(t, l.queriesOf(*db.Id), tt.want)

			_, err = e.db.StartDbSystem(t.Context(), my.StartDbSystemRequest{DbSystemId: db.Id})
			require.NoError(t, err)
			e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

			_, err = e.db.RestartDbSystem(t.Context(), my.RestartDbSystemRequest{
				DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeImmediate,
			})
			require.NoError(t, err)
			e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)
			assert.Contains(t, l.queriesOf(*db.Id), "SET GLOBAL innodb_fast_shutdown = 2")
		})
	}
}
