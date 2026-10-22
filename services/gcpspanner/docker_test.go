package gcpspanner_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const dockerWait = 8 * time.Minute

func dockerEnv(t *testing.T) *env {
	t.Helper()

	if testing.Short() {
		t.Skip("docker emulator test skipped in short mode")
	}

	lr := gcpserve.NewLazyRuntime(slog.Default(), "gcpspanner.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	pool, err := portalloc.New(24000, 24100)
	require.NoError(t, err)

	return newEnvWith(t, func(b *gcpspanner.InMemoryBackend) {
		b.SetLauncher(gcpspanner.NewDockerLauncher(slog.Default(), pool, gcpspanner.DefaultImage, dockerWait))
	})
}

func TestDocker_DataPlane(t *testing.T) {
	t.Parallel()

	e := dockerEnv(t)
	e.createInstance(t, "docker-inst")

	dbPath := parent + "/instances/docker-inst/databases/app"
	op, err := e.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
		Parent:          parent + "/instances/docker-inst",
		CreateStatement: "CREATE DATABASE `app`",
		ExtraStatements: []string{"CREATE TABLE singers (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY (id)"},
	})
	require.NoError(t, err)

	db, err := await(t, op)
	require.NoError(t, err)
	assert.Equal(t, dbPath, db.GetName())

	ddl, err := e.databases.GetDatabaseDdl(t.Context(), &databasepb.GetDatabaseDdlRequest{Database: dbPath})
	require.NoError(t, err)
	assert.Len(t, ddl.GetStatements(), 1)

	client, err := spanner.NewClientWithConfig(t.Context(), dbPath,
		spanner.ClientConfig{DisableNativeMetrics: true}, e.opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.Apply(t.Context(), []*spanner.Mutation{
		spanner.Insert("singers", []string{"id", "name"}, []any{int64(1), "Marc"}),
		spanner.Insert("singers", []string{"id", "name"}, []any{int64(2), "Catalina"}),
	})
	require.NoError(t, err)

	names := queryNames(t, client, "SELECT name FROM singers ORDER BY id")
	assert.Equal(t, []string{"Marc", "Catalina"}, names)

	_, err = client.ReadWriteTransaction(
		t.Context(),
		func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, uerr := tx.Update(ctx, spanner.NewStatement("UPDATE singers SET name = 'Marcus' WHERE id = 1"))

			return uerr
		},
	)
	require.NoError(t, err)

	assert.Equal(t, []string{"Marcus"}, queryNames(t, client, "SELECT name FROM singers WHERE id = 1"))

	uop, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: []string{"CREATE INDEX singers_by_name ON singers(name)"},
	})
	require.NoError(t, err)
	require.NoError(t, uop.Wait(t.Context()))

	ddl, err = e.databases.GetDatabaseDdl(t.Context(), &databasepb.GetDatabaseDdlRequest{Database: dbPath})
	require.NoError(t, err)
	assert.Len(t, ddl.GetStatements(), 2)
}

func queryNames(t *testing.T, c *spanner.Client, sql string) []string {
	t.Helper()

	it := c.Single().Query(t.Context(), spanner.NewStatement(sql))
	defer it.Stop()

	var out []string

	for {
		row, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		var name string
		require.NoError(t, row.Columns(&name))

		out = append(out, name)
	}
}
