package gcpspanner_test

import (
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const dataDB = parent + "/instances/data/databases/app"

func sv(s string) *structpb.Value { return structpb.NewStringValue(s) }

func seededTables() []fakeTable {
	return []fakeTable{
		{
			name:    "Albums",
			parent:  "Singers",
			columns: []string{"id", "aid", "title"},
			rows:    [][]*structpb.Value{{sv("1"), sv("7"), sv("Blue")}},
		},
		{
			name:    "Singers",
			columns: []string{"id", "name"},
			rows:    [][]*structpb.Value{{sv("1"), sv("Ann")}, {sv("2"), sv("Bo")}},
		},
	}
}

func startDataEnv(t *testing.T, tables []fakeTable) *env {
	t.Helper()

	fake, addr := newFakeEmulator(t)
	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	e.createInstance(t, "data")
	e.createDB(t, "data")
	fake.seed(dataDB, tables...)

	return e
}

func touchDataPlane(t *testing.T, e *env) {
	t.Helper()

	_, err := spannerClient(t, e).CreateSession(t.Context(), &spannerpb.CreateSessionRequest{Database: dataDB})
	require.NoError(t, err)
}

func TestTableData_SurvivesRestart(t *testing.T) {
	t.Parallel()

	src := startDataEnv(t, seededTables())
	snap := src.handler.Snapshot(t.Context())

	fake, addr := newFakeEmulator(t)
	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	require.NoError(t, dst.handler.Restore(t.Context(), snap))
	assert.Empty(t, fake.committed())

	touchDataPlane(t, dst)

	got := fake.committed()
	require.Len(t, got, 2)
	assert.Equal(t, "Singers", got[0].table, "parents load before interleaved children")
	assert.Equal(t, []string{"id", "name"}, got[0].columns)
	assert.Equal(t, [][]string{{"1", "Ann"}, {"2", "Bo"}}, got[0].rows)
	assert.Equal(t, "Albums", got[1].table)
	assert.Equal(t, [][]string{{"1", "7", "Blue"}}, got[1].rows)
	assert.Equal(t, dataDB, got[0].db)

	touchDataPlane(t, dst)
	assert.Len(t, fake.committed(), 2, "data is replayed once")
}

func TestTableData_CarriedThroughSnapshotWithoutEmulator(t *testing.T) {
	t.Parallel()

	src := startDataEnv(t, seededTables())
	snap := src.handler.Snapshot(t.Context())

	idle := newEnv(t)
	require.NoError(t, idle.handler.Restore(t.Context(), snap))

	fake, addr := newFakeEmulator(t)
	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	require.NoError(t, dst.handler.Restore(t.Context(), idle.handler.Snapshot(t.Context())))
	touchDataPlane(t, dst)

	assert.Len(t, fake.committed(), 2)
}

func TestTableData_Bounds(t *testing.T) {
	t.Parallel()

	many := make([]fakeTable, 1001)
	for i := range many {
		many[i] = fakeTable{name: fmt.Sprintf("T%d", i), columns: []string{"id"}, rows: [][]*structpb.Value{{sv("1")}}}
	}

	tests := []struct {
		name   string
		tables []fakeTable
		want   int
	}{
		{"too many tables", many, 0},
		{"short row", []fakeTable{{name: "S", columns: []string{"a", "b"}, rows: [][]*structpb.Value{{sv("1")}}}}, 0},
		{"no rows", []fakeTable{{name: "S", columns: []string{"a"}}}, 0},
		{"ok", []fakeTable{{name: "S", columns: []string{"a"}, rows: [][]*structpb.Value{{sv("1")}}}}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := startDataEnv(t, tt.tables)
			snap := src.handler.Snapshot(t.Context())

			fake, addr := newFakeEmulator(t)
			dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
			require.NoError(t, dst.handler.Restore(t.Context(), snap))
			touchDataPlane(t, dst)

			n := 0

			for _, c := range fake.committed() {
				n += len(c.rows)
			}

			assert.Equal(t, tt.want, n)
		})
	}
}

func TestTableData_DroppedDatabaseIsNotReplayed(t *testing.T) {
	t.Parallel()

	src := startDataEnv(t, seededTables())
	snap := src.handler.Snapshot(t.Context())

	fake, addr := newFakeEmulator(t)
	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	require.NoError(t, dst.handler.Restore(t.Context(), snap))
	require.NoError(t, dst.databases.DropDatabase(t.Context(), &databasepb.DropDatabaseRequest{Database: dataDB}))
	dst.createDB(t, "data")
	touchDataPlane(t, dst)

	assert.Empty(t, fake.committed())
}

func TestTableData_PostgreSQLDialect(t *testing.T) {
	t.Parallel()

	fake, addr := newFakeEmulator(t)
	src := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	src.createInstance(t, "data")

	op, err := src.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
		Parent: parent + "/instances/data", CreateStatement: `CREATE DATABASE "app"`,
		DatabaseDialect: databasepb.DatabaseDialect_POSTGRESQL,
	})
	require.NoError(t, err)

	_, err = await(t, op)
	require.NoError(t, err)

	fake.seed(dataDB, fakeTable{name: "t", columns: []string{"id"}, rows: [][]*structpb.Value{{sv("1")}}})
	snap := src.handler.Snapshot(t.Context())

	var sawPublic bool

	for _, q := range fake.queries() {
		sawPublic = sawPublic || strings.Contains(q, "table_schema = 'public'")
	}

	assert.True(t, sawPublic, "PostgreSQL databases are listed through information_schema with the public schema")

	fresh, freshAddr := newFakeEmulator(t)
	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(freshAddr, nil) })
	require.NoError(t, dst.handler.Restore(t.Context(), snap))
	touchDataPlane(t, dst)

	got := fresh.committed()
	require.Len(t, got, 1)
	assert.Equal(t, [][]string{{"1"}}, got[0].rows)
}
