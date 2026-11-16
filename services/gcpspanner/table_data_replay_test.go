package gcpspanner_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func restoreInto(t *testing.T, snap []byte, setup func(*fakeEmulator)) *fakeEmulator {
	t.Helper()

	fake, addr := newFakeEmulator(t)
	setup(fake)

	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	require.NoError(t, dst.handler.Restore(t.Context(), snap))
	touchDataPlane(t, dst)

	return fake
}

func TestTableData_ReplayForeignKeyOrder(t *testing.T) {
	t.Parallel()

	src := startDataEnv(t, []fakeTable{
		{name: "Refs", columns: []string{"id"}, rows: [][]*structpb.Value{{sv("1")}}},
		{name: "Singers", columns: []string{"id"}, rows: [][]*structpb.Value{{sv("1")}}},
	})
	snap := src.handler.Snapshot(t.Context())

	fake := restoreInto(t, snap, func(f *fakeEmulator) { f.require("Refs", "Singers") })

	committed := fake.committed()
	order := make([]string, 0, len(committed))

	for _, c := range committed {
		order = append(order, c.table)
	}

	assert.Equal(t, []string{"Singers", "Refs"}, order)
	assert.Empty(t, fake.deletedTables())
}

func TestTableData_FailedReplayLeavesDatabaseEmpty(t *testing.T) {
	t.Parallel()

	src := startDataEnv(t, []fakeTable{
		{name: "A", columns: []string{"id"}, rows: [][]*structpb.Value{{sv("1")}}},
		{name: "B", columns: []string{"id"}, rows: [][]*structpb.Value{{sv("1")}}},
	})
	snap := src.handler.Snapshot(t.Context())

	fake := restoreInto(t, snap, func(f *fakeEmulator) { f.failCommits("B", codes.InvalidArgument) })

	assert.ElementsMatch(t, []string{"A", "B"}, fake.deletedTables(), "a half-replayed database is emptied")
}

func TestTableData_OverLimitDatabaseDoesNotStarveOthers(t *testing.T) {
	t.Parallel()

	rows := func(n int) [][]*structpb.Value {
		out := make([][]*structpb.Value, n)
		for i := range out {
			out[i] = []*structpb.Value{sv("1")}
		}

		return out
	}

	fake, addr := newFakeEmulator(t)
	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	e.createInstance(t, "data")
	e.createDB(t, "data")

	op, err := e.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
		Parent: parent + "/instances/data", CreateStatement: "CREATE DATABASE `zz`",
	})
	require.NoError(t, err)

	_, err = await(t, op)
	require.NoError(t, err)

	fake.seed(dataDB,
		fakeTable{name: "T1", columns: []string{"id"}, rows: rows(150_000)},
		fakeTable{name: "T2", columns: []string{"id"}, rows: rows(100_000)},
	)
	fake.seed(
		parent+"/instances/data/databases/zz",
		fakeTable{name: "T3", columns: []string{"id"}, rows: rows(100_000)},
	)

	snap := e.handler.Snapshot(t.Context())
	assert.Greater(t, len(snap), 100_000, "the second database is persisted")
	assert.NotContains(t, string(snap), `"name":"T1"`, "the over-limit database persists schema only")
}

func TestTableData_OversizedReadIsSchemaOnly(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("x", 66<<20)
	src := startDataEnv(t, []fakeTable{{name: "Big", columns: []string{"v"}, rows: [][]*structpb.Value{{sv(big)}}}})
	snap := src.handler.Snapshot(t.Context())
	assert.Less(t, len(snap), 4096)
}
