package gcpspanner_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	tblT   = "CREATE TABLE t (id INT64, a STRING(10)) PRIMARY KEY (id)"
	tblIdx = "CREATE INDEX i ON t (a)"
)

func TestDDL_FoldWithoutDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		initial  []string
		batch    []string
		want     []string
		wantCode codes.Code
	}{
		{
			name: "add column", initial: []string{tblT}, batch: []string{"ALTER TABLE t ADD COLUMN b INT64"},
			want: []string{"CREATE TABLE t (\n  id INT64,\n  a STRING(10),\n  b INT64\n) PRIMARY KEY (id)"},
		},
		{
			name: "drop column", initial: []string{tblT}, batch: []string{"ALTER TABLE t DROP COLUMN a"},
			want: []string{"CREATE TABLE t (\n  id INT64\n) PRIMARY KEY (id)"},
		},
		{
			name:    "alter column",
			initial: []string{tblT},
			batch:   []string{"ALTER TABLE t ALTER COLUMN a STRING(100) NOT NULL"},
			want:    []string{"CREATE TABLE t (\n  id INT64,\n  a STRING(100) NOT NULL\n) PRIMARY KEY (id)"},
		},
		{
			name: "drop table", initial: []string{tblT}, batch: []string{"DROP TABLE t"}, want: []string{},
		},
		{
			name: "drop index", initial: []string{tblT, tblIdx}, batch: []string{"DROP INDEX i"}, want: []string{tblT},
		},
		{
			name:    "drop if exists",
			initial: []string{tblT},
			batch:   []string{"DROP INDEX IF EXISTS nope"},
			want:    []string{tblT},
		},
		{
			name:    "create view replace",
			initial: []string{tblT, "CREATE VIEW v SQL SECURITY INVOKER AS SELECT id FROM t"},
			batch:   []string{"CREATE OR REPLACE VIEW v SQL SECURITY INVOKER AS SELECT a FROM t"},
			want:    []string{tblT, "CREATE OR REPLACE VIEW v SQL SECURITY INVOKER AS SELECT a FROM t"},
		},
		{
			name: "constraint round trip", initial: []string{tblT},
			batch: []string{"ALTER TABLE t ADD CONSTRAINT c1 CHECK (id > 0)", "ALTER TABLE t DROP CONSTRAINT c1"},
			want:  []string{"CREATE TABLE t (\n  id INT64,\n  a STRING(10)\n) PRIMARY KEY (id)"},
		},
		{
			name:    "on delete",
			initial: []string{tblT, "CREATE TABLE c (id INT64, k INT64) PRIMARY KEY (id, k), INTERLEAVE IN PARENT t"},
			batch:   []string{"ALTER TABLE c SET ON DELETE CASCADE"},
			want: []string{
				tblT,
				"CREATE TABLE c (\n  id INT64,\n  k INT64\n) PRIMARY KEY (id, k), INTERLEAVE IN PARENT t ON DELETE CASCADE",
			},
		},
		{
			name:    "partial batch",
			initial: []string{tblT},
			batch:   []string{"ALTER TABLE t ADD COLUMN b INT64", "ALTER TABLE nope ADD COLUMN x INT64"},
			want: []string{
				"CREATE TABLE t (\n  id INT64,\n  a STRING(10),\n  b INT64\n) PRIMARY KEY (id)",
			},
			wantCode: codes.NotFound,
		},
		{
			name:     "duplicate table",
			initial:  []string{tblT},
			batch:    []string{tblT},
			want:     []string{tblT},
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "duplicate namespace",
			initial:  []string{tblT},
			batch:    []string{"CREATE INDEX t ON t (a)"},
			want:     []string{tblT},
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "duplicate column",
			initial:  []string{tblT},
			batch:    []string{"ALTER TABLE t ADD COLUMN a INT64"},
			want:     []string{tblT},
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "drop pk column",
			initial:  []string{tblT},
			batch:    []string{"ALTER TABLE t DROP COLUMN id"},
			want:     []string{tblT},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "drop missing column",
			initial:  []string{tblT},
			batch:    []string{"ALTER TABLE t DROP COLUMN zz"},
			want:     []string{tblT},
			wantCode: codes.NotFound,
		},
		{
			name:     "drop indexed table",
			initial:  []string{tblT, tblIdx},
			batch:    []string{"DROP TABLE t"},
			want:     []string{tblT, tblIdx},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "drop parent table",
			initial:  []string{tblT, "CREATE TABLE c (id INT64, k INT64) PRIMARY KEY (id, k), INTERLEAVE IN PARENT t"},
			batch:    []string{"DROP TABLE t"},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "drop missing",
			initial:  []string{tblT},
			batch:    []string{"DROP TABLE nope"},
			want:     []string{tblT},
			wantCode: codes.NotFound,
		},
		{
			name:     "drop wrong kind",
			initial:  []string{tblT},
			batch:    []string{"DROP INDEX t"},
			want:     []string{tblT},
			wantCode: codes.NotFound,
		},
		{
			name:     "index on missing table",
			batch:    []string{"CREATE INDEX i ON nope (a)"},
			want:     []string{},
			wantCode: codes.NotFound,
		},
		{
			name:     "index on missing column",
			initial:  []string{tblT},
			batch:    []string{"CREATE INDEX i ON t (zz)"},
			want:     []string{tblT},
			wantCode: codes.NotFound,
		},
		{
			name:     "no primary key",
			batch:    []string{"CREATE TABLE t (id INT64)"},
			want:     []string{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "pk unknown column",
			batch:    []string{"CREATE TABLE t (id INT64) PRIMARY KEY (zz)"},
			want:     []string{},
			wantCode: codes.NotFound,
		},
		{
			name:     "missing parent",
			batch:    []string{"CREATE TABLE c (id INT64) PRIMARY KEY (id), INTERLEAVE IN PARENT nope"},
			want:     []string{},
			wantCode: codes.NotFound,
		},
		{
			name:     "duplicate column in create",
			batch:    []string{"CREATE TABLE t (id INT64, id INT64) PRIMARY KEY (id)"},
			want:     []string{},
			wantCode: codes.AlreadyExists,
		},
		{name: "not ddl", batch: []string{"SELECT 1"}, want: []string{}, wantCode: codes.InvalidArgument},
		{
			name:     "unbalanced",
			batch:    []string{"CREATE TABLE t (id INT64 PRIMARY KEY (id)"},
			want:     []string{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "unterminated string",
			batch:    []string{"CREATE VIEW v AS SELECT 'abc"},
			want:     []string{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "unterminated comment",
			batch:    []string{"CREATE TABLE t (id INT64) /* PRIMARY KEY (id)"},
			want:     []string{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "too deep",
			batch: []string{
				"CREATE TABLE t (id INT64 CHECK " + strings.Repeat(
					"(",
					300,
				) + "1" + strings.Repeat(
					")",
					300,
				) + ") PRIMARY KEY (id)",
			},
			want:     []string{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "too many tokens",
			batch:    []string{"CREATE VIEW v AS SELECT " + strings.Repeat("a ", 250000)},
			want:     []string{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "empty table",
			batch:    []string{"CREATE TABLE t () PRIMARY KEY ()"},
			want:     []string{},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "ddl")

			db := e.createDB(t, "ddl", tt.initial...)

			op, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
				Database: db.GetName(), Statements: tt.batch,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, status.Code(op.Wait(t.Context())))

			got, err := e.databases.GetDatabaseDdl(
				t.Context(),
				&databasepb.GetDatabaseDdlRequest{Database: db.GetName()},
			)
			require.NoError(t, err)

			if tt.want != nil {
				assert.Equal(t, tt.want, append([]string{}, got.GetStatements()...))
			} else {
				assert.Len(t, got.GetStatements(), len(tt.initial))
			}
		})
	}
}

func TestDDL_CreateDatabaseFoldsAndRejects(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "ddl")

	_, err := e.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
		Parent: parent + "/instances/ddl", CreateStatement: "CREATE DATABASE `bad`",
		ExtraStatements: []string{tblT, "DROP TABLE nope"},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.databases.GetDatabase(
		t.Context(),
		&databasepb.GetDatabaseRequest{Name: parent + "/instances/ddl/databases/bad"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	db := e.createDB(t, "ddl", tblT, "ALTER TABLE t ADD COLUMN b INT64", tblIdx)
	got, err := e.databases.GetDatabaseDdl(t.Context(), &databasepb.GetDatabaseDdlRequest{Database: db.GetName()})
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"CREATE TABLE t (\n  id INT64,\n  a STRING(10),\n  b INT64\n) PRIMARY KEY (id)", tblIdx},
		got.GetStatements(),
	)
}
