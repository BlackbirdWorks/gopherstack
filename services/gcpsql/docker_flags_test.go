package gcpsql_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestDockerFlagReload(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "dfl-pg", "dfl-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runFlagReloadFlow(ctx, t, e, version)
		})
	}
}

func patchFlags(ctx context.Context, t *testing.T, e *env, name string, flags []*sqladmin.DatabaseFlags) {
	t.Helper()

	op, err := e.svc.Instances.Patch(project, name, &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{DatabaseFlags: flags},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)
}

func runFlagReloadFlow(ctx context.Context, t *testing.T, e *env, version string) {
	t.Helper()

	name := "dfl-" + shortKind(version)
	seedInstance(ctx, t, e, name, version, true)

	flag, flagQuery := "work_mem", "SELECT setting FROM pg_settings WHERE name='work_mem'"
	upTime := "SELECT extract(epoch FROM pg_postmaster_start_time())::bigint::text"
	restartFlag, restartQuery := "max_connections", "SELECT setting FROM pg_settings WHERE name='max_connections'"
	value := "8192"

	if version == mysqlVersion {
		flag, flagQuery = "wait_timeout", "SELECT @@global.wait_timeout"
		upTime = "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime'"
		restartFlag, restartQuery, value = "character_set_server", "SELECT @@global.character_set_server", "900"
	}

	db := openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
	started := scalar(t, db, upTime)

	patchFlags(ctx, t, e, name, dbFlags(flag, value))

	db = openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
	assert.Equal(t, value, scalar(t, db, flagQuery), "a dynamic flag takes effect")

	if version == mysqlVersion {
		up, err := strconv.Atoi(scalar(t, db, upTime))
		require.NoError(t, err)
		before, err := strconv.Atoi(started)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, up, before, "the server was not restarted for a dynamic flag")
	} else {
		assert.Equal(t, started, scalar(t, db, upTime), "the server was not restarted for a dynamic flag")
		assert.Equal(t, "on", scalar(t, db, "SELECT setting FROM pg_settings WHERE name='archive_mode'"),
			"changing flags keeps log archiving on")
	}

	restartValue := "120"
	if version == mysqlVersion {
		restartValue = "latin1"
	}

	patchFlags(ctx, t, e, name, dbFlags(flag, value, restartFlag, restartValue))

	db = openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
	assert.Equal(t, restartValue, scalar(t, db, restartQuery), "a static flag takes effect after the restart")
	assert.Equal(t, value, scalar(t, db, flagQuery), "the dynamic flag survives the restart")

	if version != mysqlVersion {
		assert.Equal(t, "on", scalar(t, db, "SELECT setting FROM pg_settings WHERE name='archive_mode'"),
			"the restart keeps log archiving on")
	}

	patchFlags(ctx, t, e, name, []*sqladmin.DatabaseFlags{})
}
