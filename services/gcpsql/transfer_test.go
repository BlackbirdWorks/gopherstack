package gcpsql_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestBackupAndRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "b1", "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, "b1", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.BackupRuns.Insert(project, "b1", &sqladmin.BackupRun{Description: "nightly"}).Context(ctx).Do()
	done := e.mustWait(t, op, err)
	assert.Equal(t, "BACKUP_VOLUME", done.OperationType)

	list, err := e.svc.BackupRuns.List(project, "b1").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	run := list.Items[0]
	assert.Equal(t, "SUCCESSFUL", run.Status)
	assert.Equal(t, "ON_DEMAND", run.Type)
	assert.Equal(t, "nightly", run.Description)

	got, err := e.svc.BackupRuns.Get(project, "b1", run.Id).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, run.Id, got.Id)

	op, err = e.svc.Instances.RestoreBackup(project, "b1", &sqladmin.InstancesRestoreBackupRequest{
		RestoreBackupContext: &sqladmin.RestoreBackupContext{BackupRunId: run.Id},
	}).Context(ctx).Do()
	done = e.mustWait(t, op, err)
	assert.Equal(t, "RESTORE_VOLUME", done.OperationType)

	restored := e.launcher.engine(0).snapshot().restored
	require.Len(t, restored, 1)
	assert.Contains(t, restored[0], "-- dump of app")
	assert.Contains(t, restored[0], "\\connect \"postgres\"")

	op, err = e.svc.BackupRuns.Delete(project, "b1", run.Id).Context(ctx).Do()
	e.mustWait(t, op, err)

	_, err = e.svc.BackupRuns.Get(project, "b1", run.Id).Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, 404, code)
}

func TestClone(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "src", "POSTGRES_17", &sqladmin.Settings{Tier: "db-custom-2-7680"})

	op, err := e.svc.Databases.Insert(project, "src", &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, "src", &sqladmin.User{Name: "alice", Password: "pw"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Instances.Clone(project, "src", &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{DestinationInstanceName: "dst"},
	}).Context(ctx).Do()
	done := e.mustWait(t, op, err)
	assert.Equal(t, "CLONE", done.OperationType)
	assert.Equal(t, "src", done.TargetId)

	dst, err := e.svc.Instances.Get(project, "dst").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "RUNNABLE", dst.State)
	assert.Equal(t, "db-custom-2-7680", dst.Settings.Tier)
	assert.Equal(t, int64(1), dst.Settings.SettingsVersion)

	eng := e.launcher.engine(1).snapshot()
	assert.True(t, eng.dbs["app"])
	assert.Equal(t, "pw", eng.users["alice@"])
	require.Len(t, eng.restored, 1)
	assert.Contains(t, eng.restored[0], "-- dump of app")

	_, err = e.svc.Instances.Clone(project, "src", &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{DestinationInstanceName: "dst"},
	}).Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, 409, code)

	_, err = e.svc.Instances.Clone(project, "src", &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{DestinationInstanceName: "pit", PointInTime: "2026-01-01T00:00:00Z"},
	}).Context(ctx).Do()
	code, _ = apiCode(t, err)
	assert.Equal(t, 400, code)
}

func gunzip(t *testing.T, data []byte) string {
	t.Helper()

	zr, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	out, err := io.ReadAll(zr)
	require.NoError(t, err)

	return string(out)
}

func TestExportImport(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "x1", "POSTGRES_16", nil)

	for _, n := range []string{"one", "two"} {
		op, err := e.svc.Databases.Insert(project, "x1", &sqladmin.Database{Name: n}).Context(ctx).Do()
		e.mustWait(t, op, err)
	}

	export := func(ec *sqladmin.ExportContext) {
		t.Helper()

		op, err := e.svc.Instances.Export(project, "x1", &sqladmin.InstancesExportRequest{ExportContext: ec}).
			Context(ctx).Do()
		done := e.mustWait(t, op, err)
		assert.Equal(t, "EXPORT", done.OperationType)
		assert.Equal(t, "SQL", done.ExportContext.FileType)
	}

	export(&sqladmin.ExportContext{FileType: "SQL", Uri: "gs://bkt/one.sql", Databases: []string{"one"}})
	assert.Equal(t, "-- dump of one\n", string(e.objects.get("bkt/one.sql")))

	export(&sqladmin.ExportContext{FileType: "SQL", Uri: "gs://bkt/all.sql.gz"})
	all := gunzip(t, e.objects.get("bkt/all.sql.gz"))
	assert.Contains(t, all, "\\connect \"one\"")
	assert.Contains(t, all, "\\connect \"two\"")

	op, err := e.svc.Instances.Export(project, "x1", exportReq(&sqladmin.ExportContext{
		FileType: "CSV", Uri: "gs://bkt/rows.csv", Databases: []string{"one"},
		CsvExportOptions: &sqladmin.ExportContextCsvExportOptions{SelectQuery: "select 1"},
	})).
		Context(ctx).
		Do()
	e.mustWait(t, op, err)
	assert.Equal(t, "1,\"a,b\"\n2,c\n", string(e.objects.get("bkt/rows.csv")))

	e.objects.data["bkt/in.sql"] = []byte("-- create newdb\n")
	op, err = e.svc.Instances.Import(project, "x1", importReq(&sqladmin.ImportContext{
		FileType: "SQL", Uri: "gs://bkt/in.sql", Database: "one",
	})).
		Context(ctx).
		Do()
	e.mustWait(t, op, err)

	eng := e.launcher.engine(0).snapshot()
	assert.Equal(t, "one|-- create newdb\n", eng.restored[0])

	db, err := e.svc.Databases.Get(project, "x1", "newdb").Context(ctx).Do()
	require.NoError(t, err, "databases created by an import are reconciled")
	assert.Equal(t, "newdb", db.Name)

	e.objects.data["bkt/in.csv"] = []byte("1,alpha\n2,beta\n")
	op, err = e.svc.Instances.Import(project, "x1", importReq(&sqladmin.ImportContext{
		FileType: "CSV", Uri: "gs://bkt/in.csv", Database: "one",
		CsvImportOptions: &sqladmin.ImportContextCsvImportOptions{Table: "t", Columns: []string{"id", "name"}},
	})).
		Context(ctx).
		Do()
	e.mustWait(t, op, err)
	assert.Equal(t, []string{"t", "1", "alpha"}, e.launcher.engine(0).snapshot().inserted[0])
}

func TestExportImportErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		do   func(e *env) error
		name string
		want string
	}{
		{name: "bad uri", want: "expected gs://", do: func(e *env) error {
			_, err := e.svc.Instances.Export(project, "x", &sqladmin.InstancesExportRequest{
				ExportContext: &sqladmin.ExportContext{Uri: "http://nope"},
			}).Context(context.Background()).Do()

			return err
		}},
		{name: "bak export", want: "use SQL or CSV", do: func(e *env) error {
			_, err := e.svc.Instances.Export(project, "x", &sqladmin.InstancesExportRequest{
				ExportContext: &sqladmin.ExportContext{FileType: "BAK", Uri: "gs://b/o"},
			}).Context(context.Background()).Do()

			return err
		}},
		{name: "csv export without query", want: "selectQuery", do: func(e *env) error {
			_, err := e.svc.Instances.Export(project, "x", &sqladmin.InstancesExportRequest{
				ExportContext: &sqladmin.ExportContext{
					FileType:  "CSV",
					Uri:       "gs://b/o",
					Databases: []string{"postgres"},
				},
			}).Context(context.Background()).Do()

			return err
		}},
		{name: "csv import without table", want: "csvImportOptions.table", do: func(e *env) error {
			_, err := e.svc.Instances.Import(project, "x", &sqladmin.InstancesImportRequest{
				ImportContext: &sqladmin.ImportContext{FileType: "CSV", Uri: "gs://b/o", Database: "postgres"},
			}).Context(context.Background()).Do()

			return err
		}},
		{name: "unknown database", want: "does not exist", do: func(e *env) error {
			_, err := e.svc.Instances.Export(project, "x", &sqladmin.InstancesExportRequest{
				ExportContext: &sqladmin.ExportContext{Uri: "gs://b/o", Databases: []string{"ghost"}},
			}).Context(context.Background()).Do()

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "x", "POSTGRES_16", nil)

			err := tt.do(e)
			code, _ := apiCode(t, err)
			assert.Equal(t, 400, code)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestImportMissingObjectFailsOperation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "x", "POSTGRES_16", nil)

	op, err := e.svc.Instances.Import(project, "x", &sqladmin.InstancesImportRequest{
		ImportContext: &sqladmin.ImportContext{FileType: "SQL", Uri: "gs://b/missing.sql", Database: "postgres"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	done := e.wait(t, op)
	require.NotNil(t, done.Error)
	assert.Contains(t, done.Error.Errors[0].Message, "gs://b/missing.sql")
}
