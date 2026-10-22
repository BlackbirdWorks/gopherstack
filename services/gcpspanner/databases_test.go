package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func (e *env) createDB(t *testing.T, inst string, ddl ...string) *databasepb.Database {
	t.Helper()

	op, err := e.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
		Parent: parent + "/instances/" + inst, CreateStatement: "CREATE DATABASE `app`", ExtraStatements: ddl,
	})
	require.NoError(t, err)

	db, err := await(t, op)
	require.NoError(t, err)

	return db
}

func TestDatabase_CreateValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "dbv")
	inst := parent + "/instances/dbv"

	tests := []struct {
		req  *databasepb.CreateDatabaseRequest
		name string
		want codes.Code
	}{
		{
			&databasepb.CreateDatabaseRequest{Parent: inst, CreateStatement: "CREATE TABLE x"},
			"not create db",
			codes.InvalidArgument,
		},
		{
			&databasepb.CreateDatabaseRequest{Parent: inst, CreateStatement: "CREATE DATABASE `A`"},
			"bad id",
			codes.InvalidArgument,
		},
		{
			&databasepb.CreateDatabaseRequest{
				Parent:          parent + "/instances/none",
				CreateStatement: "CREATE DATABASE `ok-db`",
			},
			"no instance",
			codes.NotFound,
		},
		{
			&databasepb.CreateDatabaseRequest{
				Parent:          inst,
				CreateStatement: "CREATE DATABASE `ok-db`",
				ExtraStatements: []string{" "},
			},
			"blank ddl",
			codes.InvalidArgument,
		},
		{
			&databasepb.CreateDatabaseRequest{
				Parent:           inst,
				CreateStatement:  "CREATE DATABASE `ok-db`",
				EncryptionConfig: &databasepb.EncryptionConfig{},
			},
			"cmek",
			codes.Unimplemented,
		},
		{&databasepb.CreateDatabaseRequest{Parent: inst, CreateStatement: "CREATE DATABASE `ok-db`"}, "ok", codes.OK},
		{
			&databasepb.CreateDatabaseRequest{Parent: inst, CreateStatement: "CREATE DATABASE `ok-db`"},
			"duplicate",
			codes.AlreadyExists,
		},
	}

	for _, tt := range tests {
		_, err := e.databases.CreateDatabase(t.Context(), tt.req)
		assert.Equal(t, tt.want, status.Code(err), tt.name)
	}
}

func TestDatabase_MetadataOnly(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseNoRuntime() })
	e.createInstance(t, "meta")

	dbPath := parent + "/instances/meta/databases/app"
	db := e.createDB(t, "meta", "CREATE TABLE t (id INT64) PRIMARY KEY (id)")

	assert.Equal(t, dbPath, db.GetName())
	assert.Equal(t, databasepb.Database_READY, db.GetState())
	assert.Equal(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, db.GetDatabaseDialect())

	uop, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database: dbPath, Statements: []string{"CREATE INDEX i ON t(id)"}, OperationId: "add_index",
	})
	require.NoError(t, err)
	require.NoError(t, uop.Wait(t.Context()))
	assert.Equal(t, dbPath+"/operations/add_index", uop.Name())

	_, err = e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database: dbPath, Statements: []string{"CREATE INDEX j ON t(id)"}, OperationId: "add_index",
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	ddl, err := e.databases.GetDatabaseDdl(t.Context(), &databasepb.GetDatabaseDdlRequest{Database: dbPath})
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"CREATE TABLE t (id INT64) PRIMARY KEY (id)", "CREATE INDEX i ON t(id)"},
		ddl.GetStatements(),
	)

	prot, err := e.databases.UpdateDatabase(t.Context(), &databasepb.UpdateDatabaseRequest{
		Database:   &databasepb.Database{Name: dbPath, EnableDropProtection: true},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"enable_drop_protection"}},
	})
	require.NoError(t, err)
	_, err = prot.Wait(t.Context())
	require.NoError(t, err)

	err = e.databases.DropDatabase(t.Context(), &databasepb.DropDatabaseRequest{Database: dbPath})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	off, err := e.databases.UpdateDatabase(t.Context(), &databasepb.UpdateDatabaseRequest{
		Database:   &databasepb.Database{Name: dbPath},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"enable_drop_protection"}},
	})
	require.NoError(t, err)
	_, err = off.Wait(t.Context())
	require.NoError(t, err)

	it := e.databases.ListDatabases(t.Context(), &databasepb.ListDatabasesRequest{Parent: parent + "/instances/meta"})
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, dbPath, listed.GetName())

	require.NoError(t, e.databases.DropDatabase(t.Context(), &databasepb.DropDatabaseRequest{Database: dbPath}))

	_, err = e.databases.GetDatabase(t.Context(), &databasepb.GetDatabaseRequest{Name: dbPath})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func spannerClient(t *testing.T, e *env) spannerpb.SpannerClient {
	t.Helper()

	cc, err := grpc.NewClient(e.handler.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	return spannerpb.NewSpannerClient(cc)
}

func TestDataPlane_Unavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*gcpspanner.InMemoryBackend)
		name  string
		inst  bool
		want  codes.Code
	}{
		{func(b *gcpspanner.InMemoryBackend) { b.UseNoRuntime() }, "no runtime", true, codes.Unavailable},
		{nil, "engine none", true, codes.Unavailable},
		{func(b *gcpspanner.InMemoryBackend) { b.UseNoRuntime() }, "no instance", false, codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnvWith(t, tt.setup)
			if tt.inst {
				e.createInstance(t, "dp")
			}

			_, err := spannerClient(t, e).CreateSession(t.Context(), &spannerpb.CreateSessionRequest{
				Database: parent + "/instances/dp/databases/app",
			})
			assert.Equal(t, tt.want, status.Code(err))
		})
	}
}

func TestProxy_ForwardsToEmulator(t *testing.T) {
	t.Parallel()

	fake, addr := newFakeEmulator(t)
	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	e.createInstance(t, "px")

	insts, _ := fake.snapshot()
	assert.True(t, insts[parent+"/instances/px"], "instance mirrored to the emulator")

	dbPath := parent + "/instances/px/databases/app"
	e.createDB(t, "px", "CREATE TABLE a (id INT64) PRIMARY KEY (id)")

	_, ddl := fake.snapshot()
	assert.Equal(t, []string{"CREATE TABLE a (id INT64) PRIMARY KEY (id)"}, ddl[dbPath])

	ok, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database: dbPath, Statements: []string{"CREATE TABLE b (id INT64) PRIMARY KEY (id)"},
	})
	require.NoError(t, err)
	require.NoError(t, ok.Wait(t.Context()))

	bad, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database: dbPath, Statements: []string{"BAD"},
	})
	require.NoError(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(bad.Wait(t.Context())))

	got, err := e.databases.GetDatabaseDdl(t.Context(), &databasepb.GetDatabaseDdlRequest{Database: dbPath})
	require.NoError(t, err)
	assert.Len(t, got.GetStatements(), 2, "DDL read back from the emulator")

	client := spannerClient(t, e)
	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-goog-request-params", "database="+dbPath)

	sess, err := client.CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: dbPath})
	require.NoError(t, err)
	assert.Equal(t, dbPath+"/sessions/s1", sess.GetName())

	stream, err := client.ExecuteStreamingSql(
		ctx,
		&spannerpb.ExecuteSqlRequest{Session: sess.GetName(), Sql: "SELECT 1"},
	)
	require.NoError(t, err)

	var vals []string

	for {
		part, rerr := stream.Recv()
		if rerr != nil {
			break
		}

		vals = append(vals, part.GetValues()[0].GetStringValue())
	}

	assert.Equal(t, []string{"SELECT 1", "second"}, vals)
	assert.Equal(t, []string{"database=" + dbPath}, fake.headers)

	require.NoError(t, e.databases.DropDatabase(t.Context(), &databasepb.DropDatabaseRequest{Database: dbPath}))
	assert.Equal(t, []string{dbPath}, fake.dropped)

	require.NoError(t, e.instances.DeleteInstance(t.Context(), deleteReq(parent+"/instances/px")))

	insts, _ = fake.snapshot()
	assert.Empty(t, insts)
}

func TestProxy_RestoreReplaysSchema(t *testing.T) {
	t.Parallel()

	_, addr := newFakeEmulator(t)
	src := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	src.createInstance(t, "rp")
	src.createDB(t, "rp", "CREATE TABLE a (id INT64) PRIMARY KEY (id)")

	snap := src.handler.Snapshot(t.Context())

	fresh, freshAddr := newFakeEmulator(t)
	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(freshAddr, nil) })
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	_, err := spannerClient(t, dst).CreateSession(t.Context(), &spannerpb.CreateSessionRequest{
		Database: parent + "/instances/rp/databases/app",
	})
	require.NoError(t, err)

	insts, ddl := fresh.snapshot()
	assert.True(t, insts[parent+"/instances/rp"])
	assert.Equal(t, []string{"CREATE TABLE a (id INT64) PRIMARY KEY (id)"}, ddl[parent+"/instances/rp/databases/app"])
}
