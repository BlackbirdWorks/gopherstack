package gcpsql_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func apiCode(t *testing.T, err error) (int, string) {
	t.Helper()

	var gerr *googleapi.Error

	require.ErrorAs(t, err, &gerr)

	reason := ""
	if len(gerr.Errors) > 0 {
		reason = gerr.Errors[0].Reason
	}

	return gerr.Code, reason
}

func TestInstanceLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	inst := e.createInstance(t, "pg1", "POSTGRES_16", &sqladmin.Settings{
		DatabaseFlags: []*sqladmin.DatabaseFlags{{Name: "max_connections", Value: "250"}},
	})

	assert.Equal(t, "sql#instance", inst.Kind)
	assert.Equal(t, project+":us-central1:pg1", inst.ConnectionName)
	assert.Equal(t, "POSTGRES_16", inst.DatabaseVersion)
	assert.Equal(t, "CLOUD_SQL_INSTANCE", inst.InstanceType)
	require.Len(t, inst.IpAddresses, 1)
	assert.Equal(t, "PRIMARY", inst.IpAddresses[0].Type)
	assert.Equal(t, int64(10), inst.Settings.DataDiskSizeGb)
	assert.Equal(t, int64(1), inst.Settings.SettingsVersion)
	assert.Equal(t, "ALWAYS", inst.Settings.ActivationPolicy)
	assert.NotEmpty(t, inst.ServerCaCert.Cert)
	assert.NotEmpty(t, inst.Etag)

	eng := e.launcher.engine(0).snapshot()
	assert.Equal(t, 1, eng.restarts, "flags are applied after launch")
	assert.Equal(t, "max_connections", eng.flags[0].Name)
	assert.Equal(t, "pw-root", e.launcher.specs[0].Password)

	list, err := e.svc.Instances.List(project).Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "sql#instancesList", list.Kind)

	patch := &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{
		Tier: "db-custom-2-7680", DatabaseFlags: []*sqladmin.DatabaseFlags{{Name: "max_connections", Value: "300"}},
	}}
	op, err := e.svc.Instances.Patch(project, "pg1", patch).Context(ctx).Do()
	e.mustWait(t, op, err)

	inst, err = e.svc.Instances.Get(project, "pg1").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "db-custom-2-7680", inst.Settings.Tier)
	assert.Equal(t, int64(2), inst.Settings.SettingsVersion)
	assert.Equal(t, int64(10), inst.Settings.DataDiskSizeGb, "unpatched settings survive")
	assert.Equal(t, 2, e.launcher.engine(0).snapshot().restarts)

	op, err = e.svc.Instances.Restart(project, "pg1").Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.Equal(t, 3, e.launcher.engine(0).snapshot().restarts)

	op, err = e.svc.Instances.Delete(project, "pg1").Context(ctx).Do()
	e.mustWait(t, op, err)
	assert.True(t, e.launcher.engine(0).snapshot().closed)

	_, err = e.svc.Instances.Get(project, "pg1").Context(ctx).Do()
	code, reason := apiCode(t, err)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "instanceDoesNotExist", reason)
}

func TestInstanceUpdateReplaceChecksSettingsVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	inst := e.createInstance(t, "m1", "MYSQL_8_0", nil)

	stale := &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{Tier: "db-g1-small", SettingsVersion: 99}}
	_, err := e.svc.Instances.Update(project, "m1", stale).Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusPreconditionFailed, code)

	fresh := &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{
		Tier: "db-g1-small", SettingsVersion: inst.Settings.SettingsVersion,
	}}
	op, err := e.svc.Instances.Update(project, "m1", fresh).Context(ctx).Do()
	e.mustWait(t, op, err)

	got, err := e.svc.Instances.Get(project, "m1").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "db-g1-small", got.Settings.Tier)
	assert.Equal(t, inst.Settings.SettingsVersion+1, got.Settings.SettingsVersion)
}

func TestInstanceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantMsg  string
		in       sqladmin.DatabaseInstance
		wantCode int
	}{
		{name: "sqlserver", in: sqladmin.DatabaseInstance{
			Name:            "ms",
			DatabaseVersion: "SQLSERVER_2019_STANDARD",
			Settings:        &sqladmin.Settings{Tier: "db-custom-2-7680"},
		}, wantCode: http.StatusBadRequest, wantMsg: "not supported by the emulator"},
		{name: "old postgres", in: sqladmin.DatabaseInstance{
			Name: "old", DatabaseVersion: "POSTGRES_9_6", Settings: &sqladmin.Settings{Tier: "db-f1-micro"},
		}, wantCode: http.StatusBadRequest, wantMsg: "supported: POSTGRES_14"},
		{
			name:     "no tier",
			in:       sqladmin.DatabaseInstance{Name: "nt", DatabaseVersion: "MYSQL_8_0"},
			wantCode: http.StatusBadRequest,
			wantMsg:  "Tier",
		},
		{name: "bad name", in: sqladmin.DatabaseInstance{
			Name: "Bad_Name", DatabaseVersion: "MYSQL_8_0", Settings: &sqladmin.Settings{Tier: "db-f1-micro"},
		}, wantCode: http.StatusBadRequest, wantMsg: "Invalid instance"},
		{name: "replica", in: sqladmin.DatabaseInstance{
			Name:               "rep",
			DatabaseVersion:    "MYSQL_8_0",
			MasterInstanceName: "m",
			Settings:           &sqladmin.Settings{Tier: "db-f1-micro"},
		}, wantCode: http.StatusNotFound, wantMsg: "does not exist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.svc.Instances.Insert(project, &tt.in).Context(t.Context()).Do()
			require.Error(t, err)

			var gerr *googleapi.Error

			require.ErrorAs(t, err, &gerr)
			assert.Equal(t, tt.wantCode, gerr.Code)
			assert.Contains(t, gerr.Message, tt.wantMsg)
		})
	}
}

func TestInstanceDuplicateAndDeletionProtection(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "dup", "MYSQL_8_4", &sqladmin.Settings{DeletionProtectionEnabled: true})

	_, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: "dup", DatabaseVersion: "MYSQL_8_4", Settings: &sqladmin.Settings{Tier: "db-f1-micro"},
	}).Context(ctx).Do()
	code, reason := apiCode(t, err)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "instanceAlreadyExists", reason)

	_, err = e.svc.Instances.Delete(project, "dup").Context(ctx).Do()
	code, _ = apiCode(t, err)
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestStopAndStartViaActivationPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "st", "POSTGRES_15", nil)

	op, err := e.svc.Instances.Patch(project, "st", &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{ActivationPolicy: "NEVER"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	got, err := e.svc.Instances.Get(project, "st").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "SUSPENDED", got.State)
	assert.Equal(t, 1, e.launcher.engine(0).snapshot().stops)

	_, err = e.svc.Instances.Restart(project, "st").Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusConflict, code)

	op, err = e.svc.Instances.Patch(project, "st", &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{ActivationPolicy: "ALWAYS"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	got, err = e.svc.Instances.Get(project, "st").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "RUNNABLE", got.State)
	assert.Equal(t, 1, e.launcher.engine(0).snapshot().starts)
}

func TestLaunchFailureMarksInstanceFailed(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, &fakeLauncher{err: errFakeLaunch})
	ctx := t.Context()

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: "bad", DatabaseVersion: "POSTGRES_17", Settings: &sqladmin.Settings{Tier: "db-f1-micro"},
	}).Context(ctx).Do()
	require.NoError(t, err)

	done := e.wait(t, op)
	require.NotNil(t, done.Error)
	assert.Contains(t, done.Error.Errors[0].Message, "launch refused")

	got, err := e.svc.Instances.Get(project, "bad").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "FAILED", got.State)
}

func TestStateOnlyWithoutLauncher(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, nil)
	ctx := t.Context()
	e.createInstance(t, "so", "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, "so", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	db, err := e.svc.Databases.Get(project, "so", "app").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "UTF8", db.Charset)

	exp, err := e.svc.Instances.Export(project, "so", &sqladmin.InstancesExportRequest{
		ExportContext: &sqladmin.ExportContext{FileType: "SQL", Uri: "gs://b/o.sql"},
	}).Context(ctx).Do()
	require.NoError(t, err)

	done := e.wait(t, exp)
	require.NotNil(t, done.Error)
	assert.Contains(t, done.Error.Errors[0].Message, "state-only")
}
