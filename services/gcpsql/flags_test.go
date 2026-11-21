package gcpsql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

func dbFlags(pairs ...string) []*sqladmin.DatabaseFlags {
	out := make([]*sqladmin.DatabaseFlags, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &sqladmin.DatabaseFlags{Name: pairs[i], Value: pairs[i+1]})
	}

	return out
}

func TestDatabaseFlagsAreValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		flags   []*sqladmin.DatabaseFlags
		status  int
	}{
		{name: "known pg flag", version: "POSTGRES_16", flags: dbFlags("work_mem", "4096"), status: 200},
		{name: "known mysql flag", version: "MYSQL_8_4", flags: dbFlags("wait_timeout", "600"), status: 200},
		{name: "bool on", version: "POSTGRES_16", flags: dbFlags("log_connections", "on"), status: 200},
		{name: "unknown flag", version: "POSTGRES_16", flags: dbFlags("not_a_flag", "1"), status: 200},
		{
			name: "cloudsql iam", version: "POSTGRES_16", status: 200,
			flags: dbFlags("cloudsql.iam_authentication", "on"),
		},
		{
			name: "cloudsql pgaudit", version: "POSTGRES_17", status: 200,
			flags: dbFlags("cloudsql.enable_pgaudit", "on", "pgaudit.log", "ddl"),
		},
		{
			name: "cloudsql logical decoding", version: "POSTGRES_15", status: 200,
			flags: dbFlags("cloudsql.logical_decoding", "on"),
		},
		{name: "mixed case name", version: "POSTGRES_16", flags: dbFlags("TimeZone", "UTC"), status: 200},
		{name: "mysql iam", version: "MYSQL_8_0", flags: dbFlags("cloudsql_iam_authentication", "on"), status: 200},
		{name: "mysql default tz", version: "MYSQL_8_4", flags: dbFlags("default_time_zone", "+00:00"), status: 200},
		{
			name: "mysql sql_mode list", version: "MYSQL_8_4", status: 200,
			flags: dbFlags("sql_mode", "STRICT_TRANS_TABLES,NO_ZERO_DATE"),
		},
		{
			name: "mysql flag on pg", version: "POSTGRES_16", status: 200,
			flags: dbFlags("innodb_buffer_pool_size", "5242880"),
		},
		{name: "pg flag on mysql", version: "MYSQL_8_4", flags: dbFlags("work_mem", "4096"), status: 200},
		{name: "nul in name", version: "POSTGRES_16", flags: dbFlags("a\x00b", "1"), status: 400},
		{name: "empty name", version: "POSTGRES_16", flags: dbFlags("", "4096"), status: 400},
		{name: "huge name", version: "POSTGRES_16", flags: dbFlags(strings.Repeat("a", 200), "1"), status: 400},
		{name: "duplicate unknown", version: "POSTGRES_16", flags: dbFlags("zz", "1", "zz", "2"), status: 400},
		{name: "below minimum", version: "POSTGRES_16", flags: dbFlags("max_connections", "13"), status: 400},
		{name: "above maximum", version: "POSTGRES_16", flags: dbFlags("max_connections", "262143"), status: 400},
		{name: "not an integer", version: "POSTGRES_16", flags: dbFlags("work_mem", "lots"), status: 400},
		{name: "bool true", version: "POSTGRES_16", flags: dbFlags("log_connections", "true"), status: 200},
		{name: "bool junk", version: "POSTGRES_16", flags: dbFlags("log_connections", "maybe"), status: 400},
		{name: "enum value", version: "POSTGRES_16", flags: dbFlags("log_statement", "bogus"), status: 400},
		{name: "enum ok", version: "POSTGRES_16", flags: dbFlags("log_statement", "ddl"), status: 200},
		{name: "float", version: "POSTGRES_16", flags: dbFlags("random_page_cost", "1.1"), status: 200},
		{name: "float nan", version: "POSTGRES_16", flags: dbFlags("random_page_cost", "NaN"), status: 400},
		{name: "empty value", version: "POSTGRES_16", flags: dbFlags("work_mem", ""), status: 400},
		{name: "duplicate", version: "POSTGRES_16", flags: dbFlags("work_mem", "100", "work_mem", "200"), status: 400},
		{name: "injection in name", version: "POSTGRES_16", flags: dbFlags("work_mem; DROP TABLE x", "1"), status: 200},
		{
			name:    "injection in value",
			version: "POSTGRES_16",
			flags:   dbFlags("timezone", "UTC'; DROP TABLE x; --"),
			status:  200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			_, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
				Name: "fl", DatabaseVersion: tt.version, Region: "us-central1",
				Settings: &sqladmin.Settings{Tier: "db-custom-1-3840", DatabaseFlags: tt.flags},
			}).Context(t.Context()).Do()

			if tt.status == 200 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.status, apiStatus(err))

			_, gerr := e.svc.Instances.Get(project, "fl").Context(t.Context()).Do()
			assert.Equal(t, 404, apiStatus(gerr), "a rejected insert creates nothing")
		})
	}
}

func TestInvalidFlagPatchChangesNothing(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "fp", "POSTGRES_16", &sqladmin.Settings{DatabaseFlags: dbFlags("work_mem", "4096")})

	_, err := e.svc.Instances.Patch(project, "fp", &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{DatabaseFlags: dbFlags("work_mem", "bogus")},
	}).Context(ctx).Do()
	assert.Equal(t, 400, apiStatus(err))

	inst, err := e.svc.Instances.Get(project, "fp").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(1), inst.Settings.SettingsVersion)
	require.Len(t, inst.Settings.DatabaseFlags, 1)
	assert.Equal(t, "4096", inst.Settings.DatabaseFlags[0].Value)
}

func TestFlagChangeRestartsOnlyWhenRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		version      string
		initial      []*sqladmin.DatabaseFlags
		patch        []*sqladmin.DatabaseFlags
		wantReloads  int
		wantRestarts int
	}{
		{
			name: "dynamic flag reloads", version: "POSTGRES_16", initial: dbFlags("work_mem", "4096"),
			patch: dbFlags("work_mem", "8192"), wantReloads: 1, wantRestarts: 1,
		},
		{
			name: "static flag restarts", version: "POSTGRES_16", initial: dbFlags("max_connections", "100"),
			patch: dbFlags("max_connections", "200"), wantReloads: 0, wantRestarts: 2,
		},
		{
			name: "mixed change restarts", version: "POSTGRES_16", initial: dbFlags("work_mem", "4096"),
			patch: dbFlags("work_mem", "8192", "max_connections", "200"), wantReloads: 0, wantRestarts: 2,
		},
		{
			name: "removing a dynamic flag reloads", version: "POSTGRES_16", initial: dbFlags("work_mem", "4096"),
			patch: []*sqladmin.DatabaseFlags{}, wantReloads: 1, wantRestarts: 1,
		},
		{
			name: "removing a static flag restarts", version: "POSTGRES_16", initial: dbFlags("max_connections", "100"),
			patch: []*sqladmin.DatabaseFlags{}, wantReloads: 0, wantRestarts: 2,
		},
		{
			name: "mysql dynamic flag reloads", version: "MYSQL_8_4", initial: dbFlags("wait_timeout", "600"),
			patch: dbFlags("wait_timeout", "900"), wantReloads: 1, wantRestarts: 1,
		},
		{
			name:         "mysql static flag restarts",
			version:      "MYSQL_8_4",
			initial:      dbFlags("character_set_server", "utf8mb4"),
			patch:        dbFlags("character_set_server", "latin1"),
			wantReloads:  0,
			wantRestarts: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "fr", tt.version, &sqladmin.Settings{DatabaseFlags: tt.initial})

			patch := &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{
				DatabaseFlags: tt.patch, ForceSendFields: []string{"DatabaseFlags"},
			}}
			op, err := e.svc.Instances.Patch(project, "fr", patch).Context(t.Context()).Do()
			e.mustWait(t, op, err)

			eng := e.launcher.engine(0).snapshot()
			assert.Len(t, eng.reloads, tt.wantReloads)
			assert.Equal(t, tt.wantRestarts, eng.restarts)
		})
	}
}

func TestListFlagsMarksRestart(t *testing.T) {
	t.Parallel()

	byName := map[string]gcpsql.FlagInfo{}
	for _, f := range gcpsql.ListFlags("POSTGRES_16") {
		byName[f.Name] = f
	}

	assert.True(t, byName["max_connections"].RequiresRestart)
	assert.False(t, byName["work_mem"].RequiresRestart)
}
